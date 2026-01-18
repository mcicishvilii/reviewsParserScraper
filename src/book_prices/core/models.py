from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class ProductRef:
    store: str
    url: str
    store_product_id: Optional[str] = None

@dataclass
class Offer:
    store: str
    url: str
    title: Optional[str]
    price_gel: Optional[float]
    isbn: Optional[str]
    in_stock: Optional[bool]
    store_product_id: Optional[str] = None
