from a2wsgi import ASGIMiddleware

from book_prices.api.app import app as fastapi_app

application = ASGIMiddleware(fastapi_app)
