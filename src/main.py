import json
import re
import time
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; reviewsParserScraper/0.1; +https://example.com)"
}
CATEGORY_ID = 291
START_PAGE = 1
MAX_PAGES = 10000

SLEEP_SECONDS = 0.25          # polite but faster than 0.6
CHECKPOINT_EVERY = 10         # pages
CHECKPOINT_FILE = "data_checkpoint.json"
OUTPUT_FILE = "products.json"

PRODUCT_HREF_RE = re.compile(r"^/products/\d+$")

# Reuse connections (faster)
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ---------- Types ----------
@dataclass
class Product:
    product_id: int
    url: str


# ---------- Helpers ----------
def fetch_soup(url: str) -> BeautifulSoup:
    r = SESSION.get(url, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def extract_products_from_listing(listing_url: str) -> list[Product]:
    soup = fetch_soup(listing_url)

    products: list[Product] = []
    seen_ids: set[int] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        # only /products/<id>
        if not PRODUCT_HREF_RE.match(href):
            continue

        product_id = int(href.split("/")[-1])
        if product_id in seen_ids:
            continue

        seen_ids.add(product_id)
        products.append(Product(product_id=product_id, url=urljoin(listing_url, href)))

    return products


def save_checkpoint(products: list[Product], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([p.__dict__ for p in products], f, ensure_ascii=False, indent=2)


def scrape_all_pages(category_id: int, start_page: int = 1, max_pages: int = 10000) -> list[Product]:
    all_products: list[Product] = []
    seen_ids: set[int] = set()

    page = start_page
    t0 = time.time()

    while page <= max_pages:
        url = f"https://biblusi.ge/products?category={category_id}&page={page}"

        try:
            page_products = extract_products_from_listing(url)
        except requests.RequestException as e:
            print(f"[page {page}] ERROR: {e} | url={url}")
            break

        # stop when page has no products
        if len(page_products) == 0:
            elapsed = time.time() - t0
            print(f"Stopping: no products on page {page} | total={len(all_products)} | elapsed={elapsed:.1f}s")
            break

        new_added = 0
        for p in page_products:
            if p.product_id in seen_ids:
                continue
            seen_ids.add(p.product_id)
            all_products.append(p)
            new_added += 1

        elapsed = time.time() - t0
        print(
            f"[page {page}] found={len(page_products)} new={new_added} "
            f"total={len(all_products)} elapsed={elapsed:.1f}s"
        )

        # checkpoint
        if page % CHECKPOINT_EVERY == 0:
            save_checkpoint(all_products, CHECKPOINT_FILE)
            print(f"[checkpoint] saved {len(all_products)} products -> {CHECKPOINT_FILE}")

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_products


# ---------- Main ----------
def main() -> None:
    products = scrape_all_pages(CATEGORY_ID, START_PAGE, MAX_PAGES)

    out = {
        "category_id": CATEGORY_ID,
        "product_count": len(products),
        "products": [p.__dict__ for p in products],
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Done. Saved {len(products)} products -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
