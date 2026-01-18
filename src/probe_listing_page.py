import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; reviewsParserScraper/0.1; +https://example.com)"
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

LISTING_URL = "https://biblusi.ge/products?category=291&page=1"

PRICE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*₾")


def main() -> None:
    r = SESSION.get(LISTING_URL, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Heuristic: show the first 5 product cards/anchors and print nearby text.
    # We don't know their classes; we just find /products/<id> links and inspect parent text.
    product_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/products/") and href.split("/products/")[-1].isdigit():
            product_links.append(a)

    print("Total /products/<id> links found on listing:", len(product_links))

    # sample first 5 unique product links
    seen = set()
    samples = []
    for a in product_links:
        href = a["href"].strip()
        if href in seen:
            continue
        seen.add(href)
        samples.append(a)
        if len(samples) == 5:
            break

    for idx in range(len(samples)):
        a = samples[idx]
        url = urljoin(LISTING_URL, a["href"])

        # inspect local context around the link
        container = a.parent
        context_text = container.get_text(" ", strip=True) if container else a.get_text(" ", strip=True)

        m_price = PRICE_RE.search(context_text)
        price = m_price.group(1) if m_price else None

        print("\n--- SAMPLE", idx + 1, "---")
        print("url:", url)
        print("link_text:", a.get_text(" ", strip=True))
        print("context_text:", context_text[:250], "..." if len(context_text) > 250 else "")
        print("price_in_context:", price)


if __name__ == "__main__":
    main()
