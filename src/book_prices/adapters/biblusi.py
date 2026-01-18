import re
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from ..core.models import ProductRef, Offer
from ..core.parsing import (
    extract_price_gel_from_text,
    extract_isbn_labeled,
    extract_availability_from_text,
)

PRODUCT_HREF_RE = re.compile(r"^/products/\d+$")

IN_STOCK_TEXT = "მარაგშია"
OUT_OF_STOCK_TEXT = "არ არის მარაგში"

class BiblusiAdapter:
    store = "biblusi"

    def __init__(self, http, sleep_seconds=0.25):
        self.http = http
        self.sleep_seconds = sleep_seconds

    def list_products(self, category_id: int, start_page: int, pages: int) -> list[ProductRef]:
        out: list[ProductRef] = []
        seen: set[str] = set()

        for page in range(start_page, start_page + pages):
            listing_url = f"https://biblusi.ge/products?category={category_id}&page={page}"
            soup = self.http.fetch_soup(listing_url)

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not PRODUCT_HREF_RE.match(href):
                    continue
                full = urljoin(listing_url, href)
                if full in seen:
                    continue
                seen.add(full)
                out.append(ProductRef(store=self.store, url=full, store_product_id=href.split("/")[-1]))

            time.sleep(self.sleep_seconds)

        return out

    def fetch_offer(self, product: ProductRef) -> Offer:
        soup = self.http.fetch_soup(product.url)

        h1 = soup.find("h1")
        title = (
            h1.get_text(strip=True)
            if h1
            else soup.title.get_text(strip=True) if soup.title else None
        )

        text = soup.get_text(" ", strip=True)

        return Offer(
            store=self.store,
            url=product.url,
            title=title,
            price_gel=extract_price_gel_from_text(text),
            isbn=extract_isbn_labeled(text),
            in_stock=extract_availability_from_text(text, IN_STOCK_TEXT, OUT_OF_STOCK_TEXT),
            store_product_id=product.store_product_id,
        )
