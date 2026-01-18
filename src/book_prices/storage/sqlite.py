import re
import sqlite3
from dataclasses import asdict
from typing import Optional

from book_prices.core.models import Offer


def title_norm(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip().lower()
    t = t.replace("ё", "е")
    t = re.sub(r"[\"'`“”„’]", "", t)
    t = re.sub(r"[\(\)\[\]\{\}]", " ", t)
    t = re.sub(r"[^0-9a-zA-Z\u10A0-\u10FF\u0400-\u04FF\s]+", " ", t)  # latin+ge+ru + digits
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


class SqliteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS books (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              isbn13        TEXT NOT NULL UNIQUE,
              title         TEXT,
              title_norm    TEXT,
              created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS store_products (
              id               INTEGER PRIMARY KEY AUTOINCREMENT,
              store            TEXT NOT NULL,
              store_product_id TEXT NOT NULL,
              url              TEXT NOT NULL,
              book_id          INTEGER,
              created_at       TEXT NOT NULL DEFAULT (datetime('now')),
              UNIQUE(store, store_product_id),
              FOREIGN KEY(book_id) REFERENCES books(id)
            );

            CREATE TABLE IF NOT EXISTS offers (
              id               INTEGER PRIMARY KEY AUTOINCREMENT,
              store_product_id INTEGER NOT NULL,
              captured_at      TEXT NOT NULL DEFAULT (datetime('now')),
              price_gel        REAL,
              in_stock         INTEGER,
              FOREIGN KEY(store_product_id) REFERENCES store_products(id)
            );

            CREATE INDEX IF NOT EXISTS idx_books_title_norm ON books(title_norm);
            CREATE INDEX IF NOT EXISTS idx_store_products_book_id ON store_products(book_id);
            CREATE INDEX IF NOT EXISTS idx_offers_storeprod_time ON offers(store_product_id, captured_at DESC);
            """
        )
        self.conn.commit()

    def _upsert_book(self, isbn13: str, title: Optional[str]) -> int:
        tnorm = title_norm(title)
        self.conn.execute(
            """
            INSERT INTO books(isbn13, title, title_norm)
            VALUES(?, ?, ?)
            ON CONFLICT(isbn13) DO UPDATE SET
              title = COALESCE(excluded.title, books.title),
              title_norm = COALESCE(excluded.title_norm, books.title_norm)
            """,
            (isbn13, title, tnorm),
        )
        row = self.conn.execute("SELECT id FROM books WHERE isbn13 = ?", (isbn13,)).fetchone()
        return int(row["id"])

    def _upsert_store_product(self, store: str, store_product_id: str, url: str, book_id: Optional[int]) -> int:
        self.conn.execute(
            """
            INSERT INTO store_products(store, store_product_id, url, book_id)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(store, store_product_id) DO UPDATE SET
              url = excluded.url,
              book_id = COALESCE(excluded.book_id, store_products.book_id)
            """,
            (store, store_product_id, url, book_id),
        )
        row = self.conn.execute(
            "SELECT id FROM store_products WHERE store=? AND store_product_id=?",
            (store, store_product_id),
        ).fetchone()
        return int(row["id"])

    def _last_offer(self, store_product_row_id: int):
        return self.conn.execute(
            """
            SELECT price_gel, in_stock
            FROM offers
            WHERE store_product_id=?
            ORDER BY captured_at DESC, id DESC
            LIMIT 1
            """,
            (store_product_row_id,),
        ).fetchone()

    def upsert_offer(self, offer: Offer) -> None:
        """
        Matching rule v1:
        - If offer.isbn exists -> create/find book by isbn13 and attach store_product to it.
        - If isbn missing -> store_product.book_id stays NULL (unmatched for now).
        Insert into offers only if (price_gel/in_stock) changed vs last snapshot.
        """
        self.conn.execute("BEGIN")

        book_id = None
        if offer.isbn:
            book_id = self._upsert_book(offer.isbn, offer.title)

        sp_id = self._upsert_store_product(
            store=offer.store,
            store_product_id=str(offer.store_product_id),
            url=offer.url,
            book_id=book_id,
        )

        last = self._last_offer(sp_id)

        changed = True
        if last is not None:
            last_price = last["price_gel"]
            last_stock = last["in_stock"]
            cur_stock = None if offer.in_stock is None else (1 if offer.in_stock else 0)
            # note: float compare is fine here; prices are 2-decimal.
            changed = (last_price != offer.price_gel) or (last_stock != cur_stock)

        if changed:
            self.conn.execute(
                "INSERT INTO offers(store_product_id, price_gel, in_stock) VALUES(?, ?, ?)",
                (
                    sp_id,
                    offer.price_gel,
                    None if offer.in_stock is None else (1 if offer.in_stock else 0),
                ),
            )

        self.conn.commit()

    # ---------- read API helpers ----------
    def get_book_by_isbn(self, isbn13: str):
        book = self.conn.execute("SELECT * FROM books WHERE isbn13=?", (isbn13,)).fetchone()
        if not book:
            return None

        offers = self.conn.execute(
            """
            SELECT sp.store, sp.url,
                   o.price_gel, o.in_stock, o.captured_at
            FROM store_products sp
            JOIN (
              SELECT store_product_id, MAX(captured_at) AS captured_at
              FROM offers
              GROUP BY store_product_id
            ) latest ON latest.store_product_id = sp.id
            JOIN offers o
              ON o.store_product_id = latest.store_product_id
             AND o.captured_at = latest.captured_at
            WHERE sp.book_id = ?
            ORDER BY (o.in_stock IS NULL) ASC, o.in_stock DESC, o.price_gel ASC
            """,
            (book["id"],),
        ).fetchall()

        return dict(book), [dict(x) for x in offers]

    def search_books(self, q: str, limit: int = 20):
        qn = title_norm(q) or ""
        rows = self.conn.execute(
            """
            SELECT id, isbn13, title
            FROM books
            WHERE title_norm LIKE ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (f"%{qn}%", limit),
        ).fetchall()
        return [dict(r) for r in rows]
