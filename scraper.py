#!/usr/bin/env python3
"""Scrape listing links and listing details from Realtor.ca and export to Excel."""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Set
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from playwright.sync_api import Browser, Page, TimeoutError, sync_playwright

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


def extract_value(text: str, label: str) -> str:
    pattern = rf"{re.escape(label)}\s*\n?\s*([^\n]+)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def matches_keywords(text: str) -> Dict[str, bool]:
    normalized = re.sub(r"\s+", " ", text.lower())
    output: Dict[str, bool] = {}
    for key, patterns in KEYWORD_RULES.items():
        output[key] = any(re.search(p, normalized, flags=re.IGNORECASE) for p in patterns)
    return output


def _normalize_listing_url(link: str) -> str:
    if not link:
        return ""
    if link.startswith("http://") or link.startswith("https://"):
        return link
    return f"https://www.realtor.ca{link}"


def _extract_hash_filters(start_url: str) -> Dict[str, str]:
    parsed = urlparse(start_url)
    hash_part = parsed.fragment or ""
    params = parse_qs(hash_part, keep_blank_values=True)
    return {k: v[0] for k, v in params.items() if v}


def collect_listing_links_via_api(start_url: str, max_pages: int = 30) -> List[str]:
    """
    Fallback-independent link collection using Realtor's search API.
    This avoids brittle DOM selectors when map/sidebar markup changes.
    """
    filters = _extract_hash_filters(start_url)
    endpoint = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.realtor.ca",
        "Referer": "https://www.realtor.ca/",
        "X-Requested-With": "XMLHttpRequest",
    }

    links: Set[str] = set()
    for page_num in range(1, max_pages + 1):
        payload = {
            "ApplicationId": "1",
            "CultureId": "1",
            "Version": "7.0",
            "CurrentPage": str(page_num),
            "RecordsPerPage": "200",
            "Sort": filters.get("Sort", "6-D"),
            "PropertySearchTypeId": filters.get("PropertySearchTypeId", "0"),
            "TransactionTypeId": filters.get("TransactionTypeId", "2"),
            "PropertyTypeGroupID": filters.get("PropertyTypeGroupID", "1"),
            "LatitudeMin": filters.get("LatitudeMin", ""),
            "LatitudeMax": filters.get("LatitudeMax", ""),
            "LongitudeMin": filters.get("LongitudeMin", ""),
            "LongitudeMax": filters.get("LongitudeMax", ""),
            "PriceMin": filters.get("PriceMin", ""),
            "PriceMax": filters.get("PriceMax", ""),
            "BedRange": filters.get("BedRange", ""),
            "BathRange": filters.get("BathRange", ""),
            "Currency": filters.get("Currency", "CAD"),
        }
        response = requests.post(endpoint, data=payload, headers=headers, timeout=45)
        response.raise_for_status()
        data = response.json()
        results = data.get("Results") or []
        if not results:
            break
        page_links = 0
        for item in results:
            rel = item.get("RelativeURLEn") or item.get("RelativeURL")
            if rel:
                links.add(_normalize_listing_url(rel))
                page_links += 1
        if page_links == 0:
            break

    return sorted(link for link in links if "/real-estate/" in link)


def collect_listing_links(
    page: Page,
    max_scroll_rounds: int = 20,
    max_page_turns: int = 25,
    pause: float = 0.75,
) -> List[str]:
    listing_links: Set[str] = set()
    captured_response_urls: Set[str] = set()

    def capture_from_response(response) -> None:
        if "PropertySearch_Post" not in response.url:
            return
        try:
            payload = response.json()
        except Exception:
            return

        if not isinstance(payload, dict):
            return
        results = payload.get("Results") or []
        for item in results:
            if not isinstance(item, dict):
                continue
            rel = item.get("RelativeURLEn") or item.get("RelativeURL")
            if rel:
                listing_links.add(_normalize_listing_url(rel))
        captured_response_urls.add(response.url)

    def discover_links_from_dom() -> None:
        hrefs = page.eval_on_selector_all(
            "a[href*='/real-estate/']",
            "els => els.map(e => e.getAttribute('href') || e.href).filter(Boolean)",
        )
        for href in hrefs:
            if "/real-estate/" in href:
                listing_links.add(_normalize_listing_url(href))

    def discover_links_from_cards() -> None:
        # Realtor card container supplied by user example.
        hrefs = page.eval_on_selector_all(
            "#mapSidebarBodyCon .cardCon a[href*='/real-estate/'], .cardCon a[href*='/real-estate/']",
            "els => els.map(e => e.getAttribute('href') || e.href).filter(Boolean)",
        )
        for href in hrefs:
            if "/real-estate/" in href:
                listing_links.add(_normalize_listing_url(href))

    def click_next_page() -> bool:
        # Realtor next-page icon supplied by user example:
        # <i class=\"fa fa-angle-right\"></i>
        return bool(
            page.evaluate(
                """
                () => {
                  const icons = [...document.querySelectorAll('i.fa.fa-angle-right')];
                  for (const icon of icons) {
                    const clickable = icon.closest('button, a, li, div, span');
                    if (!clickable) continue;
                    const attrs = (
                      (clickable.getAttribute('class') || '') + ' ' +
                      (clickable.getAttribute('aria-disabled') || '') + ' ' +
                      (clickable.getAttribute('disabled') || '')
                    ).toLowerCase();
                    if (attrs.includes('disabled') || attrs.includes('true')) continue;
                    clickable.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    return true;
                  }
                  return false;
                }
                """
            )
        )

    page.on("response", capture_from_response)
    page.wait_for_timeout(5000)
    discover_links_from_dom()
    discover_links_from_cards()

    for _ in range(max_page_turns):
        for _ in range(max_scroll_rounds):
            page.evaluate(
                """
                () => {
                  const sidebar = document.querySelector('#mapSidebarBodyCon');
                  if (sidebar) {
                    sidebar.scrollTop = sidebar.scrollTop + 1200;
                  } else {
                    window.scrollBy(0, 600);
                  }
                }
                """
            )
            time.sleep(pause)
            discover_links_from_cards()
            discover_links_from_dom()

        if not click_next_page():
            break
        page.wait_for_timeout(2000)
        discover_links_from_dom()
        discover_links_from_cards()

    # Fallback: scan raw HTML for relative listing links if DOM query finds nothing.
    if not listing_links:
        content = page.content()
        for match in re.findall(r"\\/real-estate\\/\\d+\\/[^\"'\\\\<]+", content):
            listing_links.add(_normalize_listing_url(match.replace("\\/", "/")))

    page.remove_listener("response", capture_from_response)
    if not listing_links:
        card_count = page.locator(".cardCon").count()
        details = {
            "captured_search_calls": len(captured_response_urls),
            "current_url": page.url,
            "card_containers_found": card_count,
        }
        raise RuntimeError(
            "No listing links found from map page. "
            f"Debug details: {json.dumps(details)}"
        )

    return sorted(link for link in listing_links if "/real-estate/" in link)


def scrape_listing(browser: Browser, url: str, timeout: int = 35000) -> Dict[str, object]:
    page = browser.new_page()
    record = ListingRecord(listing_url=url)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        page.wait_for_timeout(1800)

        title = page.title() or ""
        text = page.locator("body").inner_text(timeout=timeout)
        description = ""

        try:
            description = page.locator("text=Listing Description").locator("xpath=following::p[1]").inner_text(timeout=2500)
        except Exception:
            pass

        listing_id_match = re.search(r"real-estate/(\d+)", url)
        record = ListingRecord(
            listing_url=url,
            listing_id=listing_id_match.group(1) if listing_id_match else "",
            title=title.strip(),
            address=extract_value(text, "Address") or extract_value(text, "Location"),
            price=extract_value(text, "Price"),
            description=description.strip(),
            property_type=extract_value(text, "Property Type"),
            building_type=extract_value(text, "Building Type"),
            square_footage=extract_value(text, "Square Footage"),
            built_in=extract_value(text, "Built in"),
            annual_property_taxes=extract_value(text, "Annual Property Taxes"),
            parking_type=extract_value(text, "Parking Type"),
            time_on_realtor=extract_value(text, "Time on REALTOR.ca"),
            maintenance_fees=extract_value(text, "Maintenance Fees"),
            full_text=text,
        )
        row = asdict(record)
        row.update(matches_keywords(text + "\n" + description))
        return row
    finally:
        page.close()


def run(start_url: str, out_file: str, max_listings: int | None = None) -> None:
    links: List[str] = []

    # Primary approach: API-based collection (most stable against DOM changes)
    try:
        links = collect_listing_links_via_api(start_url)
        print(f"Collected {len(links)} listing links via API.")
    except Exception as exc:
        print(f"API link collection failed: {exc}")

    # Fallback approach: browser/sidebar scraping
    if not links:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
            links = collect_listing_links(page)
            browser.close()

    if max_listings:
        links = links[:max_listings]

    rows: List[Dict[str, object]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for idx, link in enumerate(links, start=1):
            try:
                print(f"[{idx}/{len(links)}] Scraping {link}")
                rows.append(scrape_listing(browser, link))
            except TimeoutError:
                print(f"Timeout while scraping {link}")
            except Exception as exc:
                print(f"Error scraping {link}: {exc}")
        browser.close()

    if not rows:
        raise RuntimeError("No listing rows were scraped. Try increasing scroll rounds or checking selectors.")

    df = pd.DataFrame(rows)
    link_col = df.pop("listing_url")
    df.insert(0, "listing_url", link_col)
    df.to_excel(out_file, index=False)
    print(f"Wrote {len(df)} rows to {out_file}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realtor.ca listing scraper to Excel")
    parser.add_argument("--url", default=DEFAULT_URL, help="Map search URL containing your criteria")
    parser.add_argument("--output", default="realtor_listings.xlsx", help="Output Excel file path")
    parser.add_argument("--max-listings", type=int, default=None, help="Optional cap for number of listings")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(start_url=args.url, out_file=args.output, max_listings=args.max_listings)
