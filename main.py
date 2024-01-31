import contextlib
import datetime
import asyncio
import httpx
import aioschedule
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from bonbast.server import get_prices_from_api, get_token_from_main_page
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

FastAPICache.init(InMemoryBackend())

BONBAST_URL = "https://www.bonbast.com"

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

def merge_and_extract_tables(tables_soup):
    tables = []
    for table_soup in tables_soup:
        for tr in table_soup.find_all("tr")[1:]:
            table = [td.text for td in tr.find_all("td")]
            tables.append(table)
    return tables

def crawl_soup(url: str, post_data: dict) -> BeautifulSoup:
    response = httpx.post(url, data=post_data)
    if response.status_code != 200:
        raise Exception(f"Failed to crawl {url}")
    html = response.text
    return BeautifulSoup(html, 'html.parser')

cache_backend = InMemoryBackend()

async def fetch_and_cache_data():
    try:
        data = await read_latest()
        logger.info(f"Data fetched by scheduler: {data}")  # Log the fetched data
        await cache_backend.set('latest_data', data, expire=60 * 30)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")



async def scheduler():
    while True:
        logger.info("Running scheduled task")
        await fetch_and_cache_data()  # Execute the task
        await asyncio.sleep(10)  # Wait for 10 seconds before next run



async def fetch_crypto_data():
    try:
        crypto_data = await get_crypto_prices()
        await cache_backend.set('crypto_data', crypto_data, expire=60 * 30)
        logger.info(f"Crypto fetched by scheduler")
    except Exception as e:
        logger.error(f"Error fetching crypto data: {e}")

async def scheduler():
    while True:
        await fetch_and_cache_data()  # For general data
        await fetch_crypto_data()     # For crypto data
        await asyncio.sleep(10)       # Adjust the sleep time as needed


@app.on_event("startup")
async def start_scheduler():
    asyncio.create_task(scheduler())

@app.get("/historical/{currency}")
@cache(expire=60 * 30)
async def read_historical_currency(currency: str, date: str = datetime.date.today().strftime("%Y-%m")):
    try:
        date = datetime.datetime.strptime(date, "%Y-%m")
    except ValueError as err:
        raise HTTPException(
            status_code=422, detail="Invalid Date format. Expected YYYY-MM"
        ) from err
    soup = crawl_soup(
        f"{BONBAST_URL}/historical",
        {"date": date.strftime("%Y-%m-%d"), "currency": currency},
    )
    table_soup = soup.find("table")
    table = [[td.text for td in tr.findAll("td")]
             for tr in table_soup.findAll("tr")[1:]]
    prices = {}
    for row in table:
        with contextlib.suppress(ValueError):
            exact_date = row[0]
            sell, buy = int(row[1]), int(row[2])
            if sell > 0 and buy > 0:
                prices[exact_date] = {
                    "sell": sell,
                    "buy": buy
                }
    return prices

@app.get("/archive/")
@cache(expire=60 * 30)
async def read_archive(date: str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")):
    try:
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError as err:
        raise HTTPException(
            status_code=422, detail="Invalid Date format. Expected YYYY-MM-DD"
        ) from err

    soup = crawl_soup(
        f"{BONBAST_URL}/archive", {"date": date.strftime("%Y-%m-%d")}
    )
    table_soup = soup.find_all("table")
    table = merge_and_extract_tables(table_soup[:-1])
    prices = {"date": date.strftime("%Y-%m-%d")}
    for row in table:
        with contextlib.suppress(ValueError):
            currency = row[0].lower()
            sell, buy = int(row[2]), int(row[3])
            if sell > 0 and buy > 0:
                prices[currency] = {
                    "sell": sell,
                    "buy": buy
                }
    return prices

@app.get("/latest")
@cache(expire=60 * 30)
async def read_latest():
    token = get_token_from_main_page()
    currencies, _, _ = get_prices_from_api(token)
    return {c.code.lower(): {"sell": c.sell, "buy": c.buy} for c in currencies}

@app.get("/archive/range")
@cache(expire=60 * 30)
async def read_archive_range(
        start_date: str,
        end_date: str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")):
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as err:
        raise HTTPException(
            status_code=422, detail="Invalid Date format. Expected YYYY-MM-DD"
        ) from err

    price_range = {}
    duration = end_date - start_date

    for i in range(duration.days + 1):
        day = start_date + datetime.timedelta(days=i)
        price = await read_archive(day.strftime("%Y-%m-%d"))
        date = price.pop("date")
        price_range[date] = price
    return price_range

@app.get("/crypto")
@cache(expire=60 * 30)  # Cache for 30 minutes
async def get_crypto_prices():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.wallex.ir/v1/markets")
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))

    data = response.json()
    return data["result"]["symbols"]

