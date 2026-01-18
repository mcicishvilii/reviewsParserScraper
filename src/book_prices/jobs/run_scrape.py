import json
import time
from dataclasses import asdict

import requests

from book_prices.core.http import HttpClient
from book_prices.adapters.parnasi import ParnasiAdapter


SLEEP_SECONDS = 0.25


def main():
    http = HttpClient()
    parnasi = ParnasiAdapter(http)

    products = parnasi.list_products(start_page=1, pages=2)
    print(f"[parnasi] products={len(products)}")

    rows = []
    for i in range(len(products)):
        p = products[i]
        try:
            offer = parnasi.fetch_offer(p)
            rows.append(asdict(offer))
            print(
                f"[parnasi {i+1}/{len(products)}] "
                f"price={offer.price_gel} isbn={offer.isbn} stock={offer.in_stock}"
            )
        except requests.RequestException as e:
            print(f"[parnasi {i+1}/{len(products)}] ERROR url={p.url} err={e}")

        time.sleep(SLEEP_SECONDS)

    with open("parnasi_2pages.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[parnasi] saved {len(rows)} -> parnasi_2pages.json")


if __name__ == "__main__":
    main()
