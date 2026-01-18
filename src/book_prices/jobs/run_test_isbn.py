import time

from book_prices.core.http import HttpClient
from book_prices.core.models import ProductRef
from book_prices.adapters.biblusi import BiblusiAdapter
from book_prices.adapters.parnasi import ParnasiAdapter
from book_prices.storage.sqlite import SqliteStore

DB_PATH = "book_prices.sqlite3"
SLEEP_SECONDS = 0.25


def main():
    http = HttpClient()
    db = SqliteStore(DB_PATH)
    db.init_schema()

    biblusi = BiblusiAdapter(http)
    parnasi = ParnasiAdapter(http)

    # Same ISBN on both sites:
    biblusi_ref = ProductRef(
        store="biblusi",
        url="https://www.biblusi.ge/products/2228",
        store_product_id="2228",
    )

    parnasi_ref = ProductRef(
        store="parnasi",
        url="https://parnasi.ge/product/%E1%83%AF%E1%83%98%E1%83%9C%E1%83%A1%E1%83%94%E1%83%91%E1%83%98%E1%83%A1-%E1%83%97%E1%83%90%E1%83%9D%E1%83%91%E1%83%90/",
        store_product_id="ჯინსების-თაობა",
    )

    offer1 = biblusi.fetch_offer(biblusi_ref)
    db.upsert_offer(offer1)
    print("Biblusi:", offer1)

    time.sleep(SLEEP_SECONDS)

    offer2 = parnasi.fetch_offer(parnasi_ref)
    db.upsert_offer(offer2)
    print("Parnasi:", offer2)

    # Now verify overlap exists in DB:
    isbn = "9789941233449"
    book, offers = db.get_book_by_isbn(isbn)
    print("\nCOMPARE RESULT")
    print(book)
    for o in offers:
        print(o)

    db.close()


if __name__ == "__main__":
    main()
