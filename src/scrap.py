import json
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup, ResultSet, Tag

DATA_DIR = Path("data/json_data")

REGIONS = [
    "sul",
    "leste",
    "oeste",
    "norte",
    "central",
]
URL_BASE = "https://www.chavesnamao.com.br/apartamentos-a-venda/sp-sao-paulo/zona-{}/"

logging.basicConfig(
    filename="log/apartment_scraper.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def request_data(region, page_number):
    URL_PARAMS = {
        "filtro": json.dumps({"pmin": 100_000, "pmax": 500_000, "or": 1}),
        "pg": page_number,
    }
    url = URL_BASE.format(region)
    req = requests.get(url, params=URL_PARAMS)
    return req


def fetch_apartment_listings(response: requests.Response):
    soup = BeautifulSoup(response.content, "lxml")
    listings: ResultSet[Tag] = soup.find_all("script", {"type": "application/ld+json"})

    if not listings:
        logging.error(f"No data found for url: {response.url}")
        return []

    parsed_data = []
    for listing_data in listings:
        try:
            parsed_data.append(json.loads(listing_data.getText(strip=True)))
        except json.JSONDecodeError:
            continue

    return parsed_data


def save_listings_to_file(parsed_data, file_path, page_num):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(parsed_data, file, ensure_ascii=False, indent=4)
    logging.info(f"Saved results_{page_num}.json")


def main():
    for region in REGIONS:
        logging.debug(f"Fetching apartment listings for region {region}")
        for page_num in range(1, 20):
            response = request_data(region, page_num)
            parsed_data = fetch_apartment_listings(response)
            FILE_PATH = DATA_DIR / f"results_{region}_{page_num}.json"
            save_listings_to_file(parsed_data, FILE_PATH, page_num)


if __name__ == "__main__":
    main()
