import re

PRICE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*₾")

ISBN_LABELED_RE = re.compile(r"\bISBN\b\s*[:#]?\s*([0-9Xx][0-9Xx\s\-]{8,20})")

def normalize_price(price_str: str) -> float:
    return float(price_str.replace(",", ".").replace("\xa0", " ").strip())

def extract_price_gel_from_text(text: str) -> float | None:
    m = PRICE_RE.search(text)
    return normalize_price(m.group(1)) if m else None

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

def extract_isbn_labeled(text: str) -> str | None:
    m = ISBN_LABELED_RE.search(text)
    if not m:
        return None
    candidate = _clean_isbn(m.group(1))
    if len(candidate) == 13 and is_valid_isbn13(candidate):
        return candidate
    if len(candidate) == 10 and is_valid_isbn10(candidate):
        return candidate
    return None

def extract_availability_from_text(
    text: str,
    in_stock_text: str,
    out_of_stock_text: str
) -> bool | None:
    if out_of_stock_text in text:
        return False
    if in_stock_text in text:
        return True
    return None
