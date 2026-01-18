import time
import requests

from book_prices.core.http import HttpClient
from book_prices.adapters.biblusi import BiblusiAdapter
from book_prices.adapters.parnasi import ParnasiAdapter
from book_prices.storage.sqlite import SqliteStore

SLEEP_SECONDS = 0.25
DB_PATH = "book_prices.sqlite3"


def scrape_adapter(list_products_fn, fetch_offer_fn, upsert_fn, store_name: str) -> None:
    products = list_products_fn()
    print(f"[{store_name}] products={len(products)}")

    for i in range(len(products)):
        p = products[i]
        try:
            offer = fetch_offer_fn(p)
            upsert_fn(offer)
            print(
                f"[{store_name} {i+1}/{len(products)}] "
                f"price={offer.price_gel} isbn={offer.isbn} stock={offer.in_stock}"
            )
        except requests.RequestException as e:
            url = getattr(p, "url", None)
            print(f"[{store_name} {i+1}/{len(products)}] ERROR url={url} err={e}")

        time.sleep(SLEEP_SECONDS)


def main():
    http = HttpClient()
    db = SqliteStore(DB_PATH)
    db.init_schema()

    biblusi = BiblusiAdapter(http)
    parnasi = ParnasiAdapter(http)

    # Biblusi
    scrape_adapter(
        list_products_fn=lambda: biblusi.list_products(category_id=291, start_page=1, pages=2),
        fetch_offer_fn=biblusi.fetch_offer,
        upsert_fn=db.upsert_offer,
        store_name="biblusi",
    )

    # Parnasi
    scrape_adapter(
        list_products_fn=lambda: parnasi.list_products(start_page=1, pages=2),
        fetch_offer_fn=parnasi.fetch_offer,
        upsert_fn=db.upsert_offer,
        store_name="parnasi",
    )

    db.close()


if __name__ == "__main__":
    main()
