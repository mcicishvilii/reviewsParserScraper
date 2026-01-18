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
PAGES_TO_SCRAPE = 2
SLEEP_SECONDS = 0.25

PRODUCT_HREF_RE = re.compile(r"^/products/\d+$")
PRICE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*₾")

# IMPORTANT: ISBN must appear AFTER the "ISBN" label
ISBN_LABELED_RE = re.compile(
    r"\bISBN\b\s*[:#]?\s*([0-9Xx][0-9Xx\s\-]{8,20})"
)

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ---------- Data ----------
@dataclass
class Product:
    product_id: int
    url: str


# ---------- Helpers ----------
def fetch_soup(url: str) -> BeautifulSoup:
    r = SESSION.get(url, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def normalize_price(price_str: str) -> float:
    return float(price_str.replace(",", "."))


def _clean_isbn(raw: str) -> str:
    return re.sub(r"[\s\-]", "", raw).upper()


def is_valid_isbn10(isbn10: str) -> bool:
    if len(isbn10) != 10:
        return False
    if not re.match(r"^\d{9}[\dX]$", isbn10):
        return False

    total = 0
    for i in range(9):
        total += (10 - i) * int(isbn10[i])

    check = 10 if isbn10[9] == "X" else int(isbn10[9])
    total += check
    return total % 11 == 0


def is_valid_isbn13(isbn13: str) -> bool:
    if len(isbn13) != 13 or not isbn13.isdigit():
        return False

    total = 0
    for i in range(12):
        total += int(isbn13[i]) * (1 if i % 2 == 0 else 3)

    check = (10 - (total % 10)) % 10
    return check == int(isbn13[12])


def extract_isbn(soup: BeautifulSoup) -> str | None:
    text = soup.get_text(" ", strip=True)

    m = ISBN_LABELED_RE.search(text)
    if not m:
        return None

    candidate = _clean_isbn(m.group(1))

    if len(candidate) == 13 and is_valid_isbn13(candidate):
        return candidate
    if len(candidate) == 10 and is_valid_isbn10(candidate):
        return candidate

    return None


# ---------- Scraping ----------
def extract_products_from_listing(listing_url: str) -> list[Product]:
    soup = fetch_soup(listing_url)

    products: list[Product] = []
    seen_ids: set[int] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not PRODUCT_HREF_RE.match(href):
            continue

        product_id = int(href.split("/")[-1])
        if product_id in seen_ids:
            continue

        seen_ids.add(product_id)
        products.append(Product(product_id, urljoin(listing_url, href)))

    return products


def extract_title_price_isbn_from_product_page(product_url: str) -> dict:
    soup = fetch_soup(product_url)

    h1 = soup.find("h1")
    title = (
        h1.get_text(strip=True)
        if h1
        else soup.title.get_text(strip=True) if soup.title else None
    )

    text = soup.get_text(" ", strip=True)
    m_price = PRICE_RE.search(text)
    price_gel = normalize_price(m_price.group(1)) if m_price else None

    isbn = extract_isbn(soup)

    return {
        "url": product_url,
        "title": title,
        "price_gel": price_gel,
        "isbn": isbn,
    }


def scrape_pages(category_id: int, start_page: int, pages_to_scrape: int) -> list[Product]:
    all_products: list[Product] = []
    seen_ids: set[int] = set()

    for page in range(start_page, start_page + pages_to_scrape):
        url = f"https://biblusi.ge/products?category={category_id}&page={page}"
        page_products = extract_products_from_listing(url)

        print(f"[listing page {page}] found={len(page_products)}")

        for p in page_products:
            if p.product_id in seen_ids:
                continue
            seen_ids.add(p.product_id)
            all_products.append(p)

        time.sleep(SLEEP_SECONDS)

    return all_products


# ---------- Main ----------
def main() -> None:
    products = scrape_pages(CATEGORY_ID, START_PAGE, PAGES_TO_SCRAPE)

    rows = []
    for i in range(len(products)):
        p = products[i]
        try:
            details = extract_title_price_isbn_from_product_page(p.url)
            details["product_id"] = p.product_id
            rows.append(details)

            print(
                f"[{i+1}/{len(products)}] id={p.product_id} "
                f"price={details['price_gel']} isbn={details['isbn']}"
            )
        except requests.RequestException as e:
            print(f"[{i+1}/{len(products)}] ERROR id={p.product_id} err={e}")

        time.sleep(SLEEP_SECONDS)

    with open("sample_2pages_details.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Saved: {len(rows)} rows -> sample_2pages_details.json")


if __name__ == "__main__":
    main()
