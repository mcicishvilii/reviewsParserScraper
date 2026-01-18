from abc import ABC, abstractmethod
from typing import Iterable
from ..core.models import ProductRef, Offer
from ..core.http import HttpClient

class StoreAdapter(ABC):
    store: str

    def __init__(self, http: HttpClient):
        self.http = http

    @abstractmethod
    def list_products(self, start_page: int, pages: int) -> list[ProductRef]:
        raise NotImplementedError

    @abstractmethod
    def fetch_offer(self, product: ProductRef) -> Offer:
        raise NotImplementedError
