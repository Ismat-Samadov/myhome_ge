import asyncio
import csv
import json
import sys
from pathlib import Path

from curl_cffi.requests import AsyncSession

BASE_URL = "https://www.myhome.ge/_next/data/amMEKQzjIvb_mJwpn39oA/ka/s/yvela-gancxadeba.json"

HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
    "dnt": "1",
    "referer": "https://www.myhome.ge/",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "x-nextjs-data": "1",
}

PARAMS_BASE = {
    "currency_id": "1",
    "CardView": "1",
    "slug": "yvela-gancxadeba",
}

OUTPUT_FILE = Path(__file__).parent.parent / "data" / "data.csv"


def flatten_listing(item: dict) -> dict:
    """Flatten a listing dict into a flat row for CSV."""
    row = {}
    for key, value in item.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                row[f"{key}_{sub_key}"] = sub_value
        elif isinstance(value, list):
            row[key] = json.dumps(value, ensure_ascii=False)
        else:
            row[key] = value
    return row


async def fetch_page(session: AsyncSession, page: int) -> dict:
    params = {**PARAMS_BASE, "page": str(page)}
    response = await session.get(BASE_URL, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def extract_listings(data: dict) -> list[dict]:
    """Navigate the Next.js data structure to extract listings."""
    try:
        page_props = data["pageProps"]
        # Try common paths in Next.js data
        for key in ("data", "listings", "items", "cards"):
            if key in page_props and isinstance(page_props[key], list):
                return page_props[key]
        # Fallback: search one level deeper
        for value in page_props.values():
            if isinstance(value, dict):
                for key in ("data", "listings", "items", "cards"):
                    if key in value and isinstance(value[key], list):
                        return value[key]
        return []
    except (KeyError, TypeError):
        return []


def extract_total_pages(data: dict) -> int:
    """Extract total page count from response."""
    try:
        page_props = data["pageProps"]
        for key in ("last_page", "totalPages", "total_pages", "pageCount"):
            if key in page_props:
                return int(page_props[key])
        for value in page_props.values():
            if isinstance(value, dict):
                for key in ("last_page", "totalPages", "total_pages", "pageCount"):
                    if key in value:
                        return int(value[key])
        return 1
    except (KeyError, TypeError, ValueError):
        return 1


async def scrape(max_pages: int | None = None) -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    async with AsyncSession(impersonate="chrome110") as session:
        # Fetch first page to discover total pages and fieldnames
        print("Fetching page 1 ...", flush=True)
        first_data = await fetch_page(session, 1)
        total_pages = extract_total_pages(first_data)
        listings = extract_listings(first_data)

        if not listings:
            print("No listings found in response. Dumping raw structure for inspection:")
            print(json.dumps(first_data, indent=2, ensure_ascii=False)[:3000])
            sys.exit(1)

        if max_pages is not None:
            total_pages = min(total_pages, max_pages)

        print(f"Total pages to scrape: {total_pages}", flush=True)

        rows = [flatten_listing(item) for item in listings]
        fieldnames = list(rows[0].keys()) if rows else []

        with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            print(f"Fetching page {page}/{total_pages} ...", flush=True)
            try:
                data = await fetch_page(session, page)
                page_listings = extract_listings(data)
                page_rows = [flatten_listing(item) for item in page_listings]
                with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writerows(page_rows)
            except Exception as exc:
                print(f"Error on page {page}: {exc}", flush=True)

    print(f"Done. Data saved to {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    max_pages = int(sys.argv[1]) if len(sys.argv) > 1 else None
    asyncio.run(scrape(max_pages))
