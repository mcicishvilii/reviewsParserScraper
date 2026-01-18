import os
import re
from typing import Optional, Tuple, List, Dict

import psycopg2
import psycopg2.extras


def title_norm(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip().lower()
    t = t.replace("ё", "е")
    t = re.sub(r"[\"'`“”„’]", "", t)
    t = re.sub(r"[\(\)\[\]\{\}]", " ", t)
    t = re.sub(r"[^0-9a-zA-Z\u10A0-\u10FF\u0400-\u04FF\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


class PostgresStore:
    def __init__(self):
        # Prefer DATABASE_URL, fallback to discrete env vars
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            dsn = (
                "dbname={db} user={user} password={pw} host={host} port={port}".format(
                    db=os.getenv("PGDATABASE"),
                    user=os.getenv("PGUSER"),
                    pw=os.getenv("PGPASSWORD"),
                    host=os.getenv("PGHOST"),
                    port=os.getenv("PGPORT", "5432"),
                )
            )

        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False

    def close(self):
        self.conn.close()

    def init_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
              id BIGSERIAL PRIMARY KEY,
              isbn13 TEXT NOT NULL UNIQUE,
              title TEXT,
              title_norm TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS store_products (
              id BIGSERIAL PRIMARY KEY,
              store TEXT NOT NULL,
              store_product_id TEXT NOT NULL,
              url TEXT NOT NULL,
              book_id BIGINT REFERENCES books(id),
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              UNIQUE(store, store_product_id)
            );

            CREATE TABLE IF NOT EXISTS offers (
              id BIGSERIAL PRIMARY KEY,
              store_product_id BIGINT NOT NULL REFERENCES store_products(id),
              captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              price_gel NUMERIC,
              in_stock BOOLEAN
            );

            CREATE INDEX IF NOT EXISTS idx_books_title_norm ON books(title_norm);
            CREATE INDEX IF NOT EXISTS idx_store_products_book_id ON store_products(book_id);
            CREATE INDEX IF NOT EXISTS idx_offers_storeprod_time ON offers(store_product_id, captured_at DESC);
            """
        )
        self.conn.commit()

    def _upsert_book(self, isbn13: str, title: Optional[str]) -> int:
        tnorm = title_norm(title)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO books(isbn13, title, title_norm)
            VALUES (%s, %s, %s)
            ON CONFLICT (isbn13) DO UPDATE SET
              title = COALESCE(EXCLUDED.title, books.title),
              title_norm = COALESCE(EXCLUDED.title_norm, books.title_norm)
            RETURNING id
            """,
            (isbn13, title, tnorm),
        )
        return int(cur.fetchone()[0])

    def _upsert_store_product(self, store: str, store_product_id: str, url: str, book_id: Optional[int]) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO store_products(store, store_product_id, url, book_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (store, store_product_id) DO UPDATE SET
              url = EXCLUDED.url,
              book_id = COALESCE(EXCLUDED.book_id, store_products.book_id)
            RETURNING id
            """,
            (store, store_product_id, url, book_id),
        )
        return int(cur.fetchone()[0])

    def _last_offer(self, store_product_row_id: int):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT price_gel, in_stock
            FROM offers
            WHERE store_product_id = %s
            ORDER BY captured_at DESC, id DESC
            LIMIT 1
            """,
            (store_product_row_id,),
        )
        return cur.fetchone()

    def upsert_offer(self, offer) -> None:
        cur = self.conn.cursor()
        try:
            book_id = None
            if offer.isbn:
                book_id = self._upsert_book(offer.isbn, offer.title)

            sp_id = self._upsert_store_product(
                offer.store, str(offer.store_product_id), offer.url, book_id
            )

            last = self._last_offer(sp_id)

            changed = True
            if last is not None:
                changed = (str(last["price_gel"]) != str(offer.price_gel)) or (last["in_stock"] != offer.in_stock)

            if changed:
                cur.execute(
                    "INSERT INTO offers(store_product_id, price_gel, in_stock) VALUES (%s, %s, %s)",
                    (sp_id, offer.price_gel, offer.in_stock),
                )

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def get_book_by_isbn(self, isbn13: str):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT * FROM books WHERE isbn13=%s", (isbn13,))
        book = cur.fetchone()
        if not book:
            return None

        cur.execute(
            """
            SELECT sp.store, sp.url, o.price_gel, o.in_stock, o.captured_at
            FROM store_products sp
            JOIN LATERAL (
              SELECT price_gel, in_stock, captured_at
              FROM offers
              WHERE store_product_id = sp.id
              ORDER BY captured_at DESC, id DESC
              LIMIT 1
            ) o ON TRUE
            WHERE sp.book_id = %s
            ORDER BY (o.in_stock IS NULL) ASC, o.in_stock DESC, o.price_gel ASC
            """,
            (book["id"],),
        )
        offers = cur.fetchall()
        return dict(book), [dict(x) for x in offers]

    def search_books(self, q: str, limit: int = 20):
        qn = title_norm(q) or ""
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id, isbn13, title
            FROM books
            WHERE title_norm ILIKE %s
            ORDER BY id DESC
            LIMIT %s
            """,
            (f"%{qn}%", limit),
        )
        return [dict(r) for r in cur.fetchall()]
