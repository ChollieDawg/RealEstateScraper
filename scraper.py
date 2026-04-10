#!/usr/bin/env python3
"""Scrape listing links and listing details from Realtor.ca and export to Excel."""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from typing import Dict, List, Set

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

DEFAULT_URL = (
    "https://www.realtor.ca/map#ZoomLevel=11&Center=49.162262%2C-122.742322"
    "&LatitudeMax=49.30932&LongitudeMax=-122.20365&LatitudeMin=49.01477"
    "&LongitudeMin=-123.28099&Sort=6-D&PropertyTypeGroupID=1&TransactionTypeId=2"
    "&PropertySearchTypeId=0&PriceMax=650000&BedRange=1-0&BathRange=1-0&Currency=CAD"
)

KEYWORD_RULES: Dict[str, List[str]] = {
    "in_suite_laundry": [r"in[-\s]?suite laundry", r"washer", r"dryer", r"laundry"],
    "dogs_no_restrictions": [r"dogs? allowed", r"pets? allowed", r"dog[-\s]?friendly"],
    "parking_spot": [r"parking", r"underground parking", r"garage"],
    "storage_unit": [r"storage", r"locker"],
    "large_closets": [r"large closets?", r"walk[-\s]?in closet", r"ample closet"],
    "closed_office_space": [r"den", r"office", r"home office"],
    "functional_open_layout": [r"open layout", r"functional layout", r"semi-open"],
    "split_layout": [r"split layout", r"bedroom.*separate", r"split bedroom"],
    "space_to_entertain": [r"large living room", r"spacious living", r"entertain"],
    "second_bathroom": [r"2 bath", r"two bath", r"second bathroom", r"2 bathrooms"],
    "south_east_facing": [r"south[ -]?east", r"se facing"],
    "north_east_facing": [r"north[ -]?east", r"ne facing"],
    "quiet_unit": [r"quiet", r"low traffic", r"away from road", r"no noise"],
    "fireplace": [r"fireplace"],
    "well_maintained_newer_building": [r"well-maintained", r"well maintained", r"newer building"],
    "newer_windows": [r"new windows?", r"updated windows?"],
    "rain_screened": [r"rain[ -]?screen"],
    "move_in_ready": [r"move[-\s]?in ready", r"renovated", r"updated"],
    "fixer_upper": [r"fixer[-\s]?upper", r"needs renovation", r"handyman special"],
    "quiet_residential_location": [r"quiet", r"residential", r"cul-de-sac"],
    "close_to_transit": [r"transit", r"skytrain", r"bus routes?"],
    "walkable_essentials": [r"shopping", r"groceries", r"walkable", r"amenities nearby"],
    "access_to_nature": [r"trails?", r"parks?", r"lake", r"waterfront"],
    "outdoor_quiet": [r"quiet", r"no busy roads?", r"peaceful"],
    "outdoor_views": [r"views?", r"city view", r"mountain view"],
    "ground_floor_privacy": [r"ground floor", r"private patio", r"private yard"],
    "gym": [r"gym", r"fitness"],
    "ping_pong_pool_sauna": [r"ping pong", r"pool", r"sauna"],
    "gardens": [r"garden", r"courtyard"],
}


@dataclass
class ListingRecord:
    listing_url: str
    listing_id: str = ""
    title: str = ""
    address: str = ""
    price: str = ""
    description: str = ""
    property_type: str = ""
    building_type: str = ""
    square_footage: str = ""
    built_in: str = ""
    annual_property_taxes: str = ""
    parking_type: str = ""
    time_on_realtor: str = ""
    maintenance_fees: str = ""
    full_text: str = ""


def build_driver(headless: bool) -> uc.Chrome:
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1400,2000")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options)


def _normalize_listing_url(link: str) -> str:
    if not link:
        return ""
    if link.startswith("http://") or link.startswith("https://"):
        return link
    return f"https://www.realtor.ca{link}"


def extract_value(text: str, label: str) -> str:
    pattern = rf"{re.escape(label)}\s*\n?\s*([^\n]+)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _extract_first_money_value(text: str) -> str:
    for pattern in [r"(?:CAD|\$)\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?", r"\$\s?\d+(?:\.\d{2})?"]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return ""


def matches_keywords(text: str) -> Dict[str, bool]:
    normalized = re.sub(r"\s+", " ", text.lower())
    return {k: any(re.search(p, normalized, flags=re.IGNORECASE) for p in pats) for k, pats in KEYWORD_RULES.items()}


def collect_listing_links(driver: uc.Chrome, max_scroll_rounds: int = 20, max_page_turns: int = 20) -> List[str]:
    links: Set[str] = set()

    def collect() -> None:
        selectors = [
            "a[href*='/real-estate/']",
            "#mapSidebarBodyCon .cardCon a[href*='/real-estate/']",
            "div.cardCon > span > div > a[href*='/real-estate/']",
        ]
        for sel in selectors:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                href = el.get_attribute("href") or ""
                if "/real-estate/" in href:
                    links.add(_normalize_listing_url(href))

        xpath_selectors = [
            "//div[@id='mapSidebarBodyCon']//a[contains(@href, '/real-estate/')]",
            "/html/body/form/div[5]/div[2]/span/div/div[3]/div/div[1]/div[2]/div[3]/div[1]/span/div/a",
        ]
        for xp in xpath_selectors:
            for el in driver.find_elements(By.XPATH, xp):
                href = el.get_attribute("href") or ""
                if "/real-estate/" in href:
                    links.add(_normalize_listing_url(href))

    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(3)
    collect()

    for _ in range(max_page_turns):
        for _ in range(max_scroll_rounds):
            driver.execute_script(
                """
                const sidebar = document.querySelector('#mapSidebarBodyCon');
                if (sidebar) { sidebar.scrollTop += 1200; }
                else { window.scrollBy(0, 600); }
                """
            )
            time.sleep(0.8)
            collect()

        next_xpaths = [
            "/html/body/form/div[5]/div[2]/span/div/div[3]/div/div[1]/div[2]/div[4]/span/div/a[3]",
            "/html/body/form/div[5]/div[2]/span/div/div[3]/div/div[1]/div[2]/div[4]/span/div/a[3]/div",
            "//i[contains(@class,'fa-angle-right')]",
        ]
        clicked = False
        for xp in next_xpaths:
            els = driver.find_elements(By.XPATH, xp)
            if not els:
                continue
            try:
                driver.execute_script("arguments[0].click();", els[0])
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            break
        time.sleep(2.0)
        collect()

    if not links:
        raise RuntimeError("No listing links found from map page after scrolling and pagination.")
    return sorted(links)


def extract_structured_fields_from_html(html: str, body_text: str) -> Dict[str, str]:
    fields = {"price": "", "address": ""}
    soup = BeautifulSoup(html, "html.parser")

    for script in soup.select("script[type='application/ld+json']"):
        raw = script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            offers = item.get("offers") or {}
            addr = item.get("address") or {}
            if not fields["price"] and isinstance(offers, dict):
                p = offers.get("price")
                c = offers.get("priceCurrency")
                if p:
                    fields["price"] = f"{c} {p}".strip()
            if not fields["address"] and isinstance(addr, dict):
                parts = [addr.get("streetAddress", ""), addr.get("addressLocality", ""), addr.get("addressRegion", "")]
                address = ", ".join([x for x in parts if x])
                if address:
                    fields["address"] = address

    if not fields["price"]:
        fields["price"] = _extract_first_money_value(body_text) or extract_value(body_text, "Price")
    if not fields["address"]:
        fields["address"] = extract_value(body_text, "Address") or extract_value(body_text, "Location")
    return fields


def scrape_listing(driver: uc.Chrome, url: str) -> Dict[str, object]:
    driver.get(url)
    time.sleep(1.8)
    html = driver.page_source
    body_text = driver.find_element(By.TAG_NAME, "body").text

    structured = extract_structured_fields_from_html(html, body_text)

    description = ""
    try:
        description = driver.find_element(By.XPATH, "//*[contains(text(),'Listing Description')]/following::p[1]").text
    except Exception:
        pass

    listing_id_match = re.search(r"real-estate/(\d+)", url)
    record = ListingRecord(
        listing_url=url,
        listing_id=listing_id_match.group(1) if listing_id_match else "",
        title=driver.title.strip(),
        address=structured.get("address", ""),
        price=structured.get("price", ""),
        description=description.strip(),
        property_type=extract_value(body_text, "Property Type"),
        building_type=extract_value(body_text, "Building Type"),
        square_footage=extract_value(body_text, "Square Footage"),
        built_in=extract_value(body_text, "Built in"),
        annual_property_taxes=extract_value(body_text, "Annual Property Taxes"),
        parking_type=extract_value(body_text, "Parking Type"),
        time_on_realtor=extract_value(body_text, "Time on REALTOR.ca"),
        maintenance_fees=extract_value(body_text, "Maintenance Fees"),
        full_text=body_text,
    )
    row = asdict(record)
    row.update(matches_keywords(body_text + "\n" + description))
    return row


def run(start_url: str, out_file: str, max_listings: int | None = None, headless: bool = False) -> None:
    driver = build_driver(headless=headless)
    try:
        driver.get(start_url)
        links = collect_listing_links(driver)

        if max_listings:
            links = links[:max_listings]

        rows: List[Dict[str, object]] = []
        for idx, link in enumerate(links, start=1):
            try:
                print(f"[{idx}/{len(links)}] Scraping {link}")
                rows.append(scrape_listing(driver, link))
            except TimeoutException:
                print(f"Timeout while scraping {link}")
            except Exception as exc:
                print(f"Error scraping {link}: {exc}")

        if not rows:
            raise RuntimeError("No listing rows were scraped.")

        df = pd.DataFrame(rows)
        link_col = df.pop("listing_url")
        df.insert(0, "listing_url", link_col)
        df.to_excel(out_file, index=False)
        print(f"Wrote {len(df)} rows to {out_file}")
    finally:
        driver.quit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realtor.ca listing scraper to Excel")
    parser.add_argument("--url", default=DEFAULT_URL, help="Map search URL containing your criteria")
    parser.add_argument("--output", default="realtor_listings.xlsx", help="Output Excel file path")
    parser.add_argument("--max-listings", type=int, default=None, help="Optional cap for number of listings")
    parser.add_argument("--headless", action="store_true", help="Run browser headless (default: headed)")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(start_url=args.url, out_file=args.output, max_listings=args.max_listings, headless=args.headless)
