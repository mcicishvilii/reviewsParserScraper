from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import os

from book_prices.storage.postgres import PostgresStore

app = FastAPI(title="Book Price Compare API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://<YOUR_GH_USERNAME>.github.io",
        "http://localhost:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = PostgresStore()
db.init_schema()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/compare/by-isbn/{isbn13}")
def compare_by_isbn(isbn13: str):
    res = db.get_book_by_isbn(isbn13)
    if not res:
        raise HTTPException(status_code=404, detail="Book not found")
    book, offers = res
    return {"book": book, "offers": offers}

@app.get("/search")
def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)):
    return {"items": db.search_books(q, limit=limit)}
