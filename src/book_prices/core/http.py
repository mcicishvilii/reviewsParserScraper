import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; bookPriceBot/0.1)"
}

class HttpClient:
    def __init__(self, headers=None, timeout=25):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(headers or DEFAULT_HEADERS)

    def fetch_soup(self, url: str) -> BeautifulSoup:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
