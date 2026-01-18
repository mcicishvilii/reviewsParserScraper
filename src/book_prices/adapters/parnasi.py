import time
import re
from urllib.parse import urljoin, unquote

from ..core.models import ProductRef, Offer
from ..core.parsing import (
    extract_isbn_labeled,
    extract_availability_from_text,
    normalize_price,
)

PRICE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*₾")
PRODUCT_URL_RE = re.compile(r"^https?://parnasi\.ge/product/[^/]+/?$")

IN_STOCK_TEXT = "მარაგში"
OUT_OF_STOCK_TEXT = "არ არის მარაგში"


def extract_price_from_price_block(soup) -> float | None:
    # WooCommerce product price is typically inside summary/price
    price_el = (
        soup.select_one("div.product div.summary p.price") or
        soup.select_one("div.product div.summary .price") or
        soup.select_one("p.price") or
        soup.select_one(".price")
    )
    if not price_el:
        return None

    txt = price_el.get_text(" ", strip=True)
    m = PRICE_RE.search(txt)
    return normalize_price(m.group(1)) if m else None


def extract_price_fallback_ignore_cart(text: str) -> float | None:
    matches = PRICE_RE.findall(text)
    if not matches:
        return None
    vals = [normalize_price(x) for x in matches]
    return max(vals) if vals else None


class ParnasiAdapter:
    store = "parnasi"

    def __init__(self, http, sleep_seconds: float = 0.25):
        self.http = http
        self.sleep_seconds = sleep_seconds

    def _listing_url(self, page: int) -> str:
        return "https://parnasi.ge/shop/" if page == 1 else f"https://parnasi.ge/shop/page/{page}/"

    def list_products(self, start_page: int, pages: int) -> list[ProductRef]:
        out: list[ProductRef] = []
        seen: set[str] = set()

        for page in range(start_page, start_page + pages):
            listing_url = self._listing_url(page)
            soup = self.http.fetch_soup(listing_url)

            links = soup.select("li.product a.woocommerce-LoopProduct-link")
            if not links:
                links = soup.find_all("a", href=True)

            for a in links:
                href = a.get("href") if hasattr(a, "get") else None
                if not href:
                    continue

                full = urljoin(listing_url, href.strip())
                if "/product/" not in full:
                    continue
                if not PRODUCT_URL_RE.match(full):
                    continue

                if full in seen:
                    continue
                seen.add(full)

                slug = full.rstrip("/").split("/product/")[-1]
                out.append(
                    ProductRef(
                        store=self.store,
                        url=full,
                        store_product_id=unquote(slug),  # decode %d0%... into readable slug
                    )
                )

            time.sleep(self.sleep_seconds)

        return out

    def fetch_offer(self, product: ProductRef) -> Offer:
        soup = self.http.fetch_soup(product.url)

        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None

        text = soup.get_text(" ", strip=True)

        price_gel = extract_price_from_price_block(soup)
        if price_gel is None:
            price_gel = extract_price_fallback_ignore_cart(text)

        return Offer(
            store=self.store,
            url=product.url,
            title=title,
            price_gel=price_gel,
            isbn=extract_isbn_labeled(text),
            in_stock=extract_availability_from_text(text, IN_STOCK_TEXT, OUT_OF_STOCK_TEXT),
            store_product_id=product.store_product_id,
        )
