#!/usr/bin/env python3
"""Scrape listing links and listing details from Realtor.ca and export to Excel."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import time
from dataclasses import asdict, dataclass
from typing import Dict, List, Set

import pandas as pd
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Suppress noisy undetected-chromedriver destructor double-quit on some Windows setups.
try:
    uc.Chrome.__del__ = lambda self: None  # type: ignore[method-assign]
except Exception:
    pass

DEFAULT_URL = (
    "https://www.realtor.ca/map#ZoomLevel=11&Center=49.162262%2C-122.742322"
    "&LatitudeMax=49.30932&LongitudeMax=-122.20365&LatitudeMin=49.01477"
    "&LongitudeMin=-123.28099&Sort=6-D&PropertyTypeGroupID=1&TransactionTypeId=2"
    "&PropertySearchTypeId=0&PriceMax=650000&BedRange=1-0&BathRange=1-0&Currency=CAD"
)

KNOWN_CITIES = [
    "Vancouver",
    "Surrey",
    "New Westminster",
    "Abbotsford",
    "Coquitlam",
    "Langley",
    "Maple Ridge",
    "Aldergrove",
    "Tsawwassen",
    "Burnaby",
    "Port Coquitlam",
]

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


def log(message: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


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


def _parse_currency_to_float(value: str) -> float | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.]", "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_time_on_realtor_days(value: str) -> int | None:
    if not value:
        return None
    v = value.lower()
    if "hour" in v or "min" in v:
        return 1
    m = re.search(r"(\d+)", v)
    if not m:
        return None
    n = int(m.group(1))
    if "day" in v:
        return n
    return n


def _parse_built_in_year(value: str, fallback_text: str = "", reference_year: int = 2026) -> int | None:
    candidates = [value or "", fallback_text or ""]
    for text in candidates:
        if not text:
            continue
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
        if year_match:
            return int(year_match.group(1))
        age_match = re.search(r"\b(\d+)\s*year", text.lower())
        if age_match:
            age = int(age_match.group(1))
            if 0 <= age <= 200:
                return reference_year - age
    return None


def _infer_city(address: str, body_text: str = "") -> str:
    # Prefer parsing directly from listing address to avoid false matches like "Metro Vancouver".
    lines = [ln.strip() for ln in (address or "").splitlines() if ln.strip()]
    locality_line = lines[-1] if lines else address
    locality_line = (locality_line or "").lower()
    for city in sorted(KNOWN_CITIES, key=len, reverse=True):
        if city.lower() in locality_line:
            return city
    # Fallback only if address parsing fails.
    haystack = (body_text or "").lower()
    for city in sorted(KNOWN_CITIES, key=len, reverse=True):
        if re.search(rf"\\b{re.escape(city.lower())}\\b", haystack):
            return city
    return ""


def _parse_open_house_date_2026(month_text: str, day_text: str) -> str:
    month_text = (month_text or "").strip()
    day_match = re.search(r"\d{1,2}", day_text or "")
    if not month_text or not day_match:
        return ""
    try:
        month_num = dt.datetime.strptime(month_text[:3], "%b").month
        day_num = int(day_match.group(0))
        return dt.date(2026, month_num, day_num).isoformat()
    except Exception:
        return ""


def _parse_sqft_to_float(value: str) -> float | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.]", "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def matches_keywords(text: str) -> Dict[str, bool]:
    normalized = re.sub(r"\s+", " ", text.lower())
    return {k: any(re.search(p, normalized, flags=re.IGNORECASE) for p in pats) for k, pats in KEYWORD_RULES.items()}


def collect_current_page_links(driver: uc.Chrome) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()

    def add_links_from_selector(selector: str) -> None:
        try:
            hrefs = driver.execute_script(
                """
                const selector = arguments[0];
                return [...document.querySelectorAll(selector)]
                  .map(el => el.getAttribute('href') || el.href || '')
                  .filter(Boolean);
                """,
                selector,
            ) or []
        except Exception:
            hrefs = []
        for href in hrefs:
            normalized = _normalize_listing_url(href)
            if "/real-estate/" in normalized and normalized not in seen:
                seen.add(normalized)
                links.append(normalized)

    # Explicit left-side card slots requested by user: div.cardCon:nth-child(1..13)
    for i in range(1, 14):
        add_links_from_selector(f"div.cardCon:nth-child({i}) a[href*='/real-estate/']")

    # Fallback: any visible card links on current page.
    add_links_from_selector("div.cardCon a[href*='/real-estate/']")

    log(f"Found {len(links)} listing link(s) on current sidebar page.")
    return links


def click_next_page(driver: uc.Chrome) -> bool:
    # Use the anchor itself (not just nested icon) because icon may be off-screen/virtualized.
    anchor_selector = "#SideBarPagination > div:nth-child(1) > a:nth-child(4)"
    anchors = driver.find_elements(By.CSS_SELECTOR, anchor_selector)
    if not anchors:
        log("Next-page button not found; pagination is exhausted.")
        return False
    try:
        driver.execute_script(
            """
            const sidebar = document.querySelector('#mapSidebarBodyCon');
            if (sidebar) sidebar.scrollTop = 0;
            """
        )
        time.sleep(0.3)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", anchors[0])
        driver.execute_script("arguments[0].click();", anchors[0])
        log("Clicked next-page button (anchor click).")
        return True
    except Exception:
        pass
    try:
        driver.execute_script(
            """
            const anchor = document.querySelector('#SideBarPagination > div:nth-child(1) > a:nth-child(4)');
            if (!anchor) return false;
            anchor.click();
            return true;
            """,
        )
        log("Clicked next-page button (JS fallback).")
        return True
    except Exception:
        log("Failed to click next-page button.")
        return False


def wait_for_initial_sidebar_links(
    driver: uc.Chrome,
    initial_timeout_sec: int = 45,
    poll_interval_sec: float = 1.5,
) -> List[str]:
    log(f"Waiting up to {initial_timeout_sec}s for initial sidebar listings to render...")
    deadline = time.time() + initial_timeout_sec
    attempts = 0
    while time.time() < deadline:
        attempts += 1
        links = collect_current_page_links(driver)
        if links:
            log(f"Initial listings detected after {attempts} checks.")
            return links
        # Nudge map/sidebar in case lazy-render did not trigger yet.
        try:
            driver.execute_script(
                """
                const sidebar = document.querySelector('#mapSidebarBodyCon');
                if (sidebar) sidebar.scrollTop += 300;
                window.scrollBy(0, 100);
                """
            )
        except Exception:
            pass
        time.sleep(poll_interval_sec)
    log("Initial sidebar listings did not appear before timeout.")
    return []


def _text_or_empty(driver: uc.Chrome, css_selector: str) -> str:
    els = driver.find_elements(By.CSS_SELECTOR, css_selector)
    if not els:
        return ""
    return els[0].text.strip()


def _wait_for_listing_page_ready(driver: uc.Chrome, timeout_sec: int = 25) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        has_address = bool(driver.find_elements(By.CSS_SELECTOR, "#listingAddress"))
        has_price = bool(driver.find_elements(By.CSS_SELECTOR, "#listingPriceValue"))
        has_description = bool(driver.find_elements(By.CSS_SELECTOR, "#propertyDescriptionCon"))
        if has_address or has_price or has_description:
            return True
        time.sleep(0.5)
    return False


def scrape_listing(driver: uc.Chrome, url: str) -> Dict[str, object]:
    log(f"Scraping listing detail: {url}")
    # Always navigate explicitly in the tab to avoid stale/blocked new-tab loads.
    driver.get(url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    ready = _wait_for_listing_page_ready(driver)
    if not ready:
        raise RuntimeError("Listing page did not expose expected selectors (#listingAddress/#listingPriceValue/#propertyDescriptionCon).")

    listing_id_match = re.search(r"real-estate/(\d+)", url)
    if listing_id_match and listing_id_match.group(1) not in driver.current_url:
        log(f"Warning: current_url mismatch after navigation: {driver.current_url}")

    log(f"Loaded listing page URL: {driver.current_url}")
    body_text = driver.find_element(By.TAG_NAME, "body").text
    description = _text_or_empty(driver, "#propertyDescriptionCon")
    address = _text_or_empty(driver, "#listingAddress") or extract_value(body_text, "Address")
    price_raw = _text_or_empty(driver, "#listingPriceValue") or _extract_first_money_value(body_text)
    square_footage = _text_or_empty(driver, "#SquareFootageIcon > div:nth-child(2)") or extract_value(body_text, "Square Footage")
    built_in_raw = _text_or_empty(driver, "#propertyDetailsSectionContentSubCon_AgeOfBuilding > div:nth-child(2)")
    time_on_realtor_raw = _text_or_empty(driver, "#propertyDetailsSectionContentSubCon_TimeOnRealtor > div:nth-child(2)")
    maintenance_raw = _text_or_empty(driver, "#propertyDetailsSectionVal_MonthlyMaintenanceFees > div:nth-child(2)")
    annual_taxes_raw = extract_value(body_text, "Annual Property Taxes")
    next_open_house_date = _parse_open_house_date_2026(
        _text_or_empty(driver, ".nextEventMonth"),
        _text_or_empty(driver, ".nextEventDay"),
    )

    built_in_year = _parse_built_in_year(built_in_raw, extract_value(body_text, "Built in"))
    time_on_realtor_days = _parse_time_on_realtor_days(time_on_realtor_raw)
    price_num = _parse_currency_to_float(price_raw)
    maintenance_num = _parse_currency_to_float(maintenance_raw)
    annual_taxes_num = _parse_currency_to_float(annual_taxes_raw)

    record = ListingRecord(
        listing_url=url,
        listing_id=listing_id_match.group(1) if listing_id_match else "",
        title=driver.title.strip(),
        address=address,
        price=price_num if price_num is not None else 0.0,
        description=description.strip(),
        property_type=_text_or_empty(driver, "#propertyDetailsSectionContentSubCon_Title > div:nth-child(2)"),
        building_type=_text_or_empty(driver, "#propertyDetailsSectionContentSubCon_BuildingType > div:nth-child(2)"),
        square_footage=square_footage,
        built_in=built_in_year if built_in_year is not None else "",
        annual_property_taxes=annual_taxes_num if annual_taxes_num is not None else 0.0,
        parking_type=_text_or_empty(driver, "#propertyDetailsSectionContentSubCon_ParkingType > div:nth-child(2)"),
        time_on_realtor=time_on_realtor_days if time_on_realtor_days is not None else "",
        maintenance_fees=maintenance_num if maintenance_num is not None else 0.0,
        full_text=body_text,
    )
    row = asdict(record)
    row["bedrooms"] = _text_or_empty(driver, "#BedroomIcon > div:nth-child(2)")
    row["bathrooms"] = _text_or_empty(driver, "#BathroomIcon > div:nth-child(2)")
    row["appliances"] = _text_or_empty(driver, "#propertyDetailsSectionVal_AppliancesIncluded > div:nth-child(2)")
    row["building_amenities"] = _text_or_empty(driver, "#propertyDetailsSectionVal_BuildingAmenities > div:nth-child(2)")

    sqft_num = _parse_sqft_to_float(square_footage)
    row["price_numeric"] = price_num
    row["square_footage_numeric"] = sqft_num
    row["price_per_sqft"] = (price_num / sqft_num) if (price_num and sqft_num and sqft_num > 0) else None
    row["next_open_house_date"] = next_open_house_date
    row["city"] = _infer_city(address, body_text)

    row.update(matches_keywords(body_text + "\n" + description))
    log(
        "Scraped fields: "
        f"address={'yes' if row.get('address') else 'no'}, "
        f"price={'yes' if row.get('price') else 'no'}, "
        f"sqft={'yes' if row.get('square_footage') else 'no'}"
    )
    return row


def run(
    start_url: str,
    out_file: str,
    max_listings: int | None = None,
    headless: bool = False,
    max_pages: int = 200,
    autosave_every: int = 10,
) -> None:
    driver = build_driver(headless=headless)
    try:
        log(f"Opening start URL: {start_url}")
        driver.get(start_url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)
        log("Map page loaded. Starting sidebar iteration.")

        seen_links: Set[str] = set()
        rows: List[Dict[str, object]] = []
        pages_processed = 0
        initial_links = wait_for_initial_sidebar_links(driver)
        if not initial_links:
            log("No initial links found yet; continuing with pagination loop for additional retries.")
        main_handle = driver.current_window_handle
        driver.switch_to.new_window("tab")
        worker_handle = driver.current_window_handle
        driver.get("about:blank")
        driver.switch_to.window(main_handle)
        log("Opened dedicated worker tab for listing scrapes via new_window('tab').")

        while pages_processed < max_pages:
            driver.switch_to.window(main_handle)
            log(f"Processing sidebar page index {pages_processed + 1}.")
            current_links = initial_links if pages_processed == 0 and initial_links else collect_current_page_links(driver)
            if not current_links:
                log("No links found on this page; stopping.")
                break

            for link in current_links:
                if link in seen_links:
                    log(f"Skipping duplicate link: {link}")
                    continue
                seen_links.add(link)
                if max_listings and len(rows) >= max_listings:
                    log(f"Reached max-listings limit: {max_listings}")
                    break
                try:
                    log(f"[{len(rows)+1}] Scraping in worker tab: {link}")
                    driver.switch_to.window(worker_handle)
                    row = scrape_listing(driver, link)
                    if not row.get("address") and not row.get("description"):
                        log(f"Skipping row due to missing core fields for {link}")
                    else:
                        rows.append(row)
                    log(f"Rows collected so far: {len(rows)}")
                    if autosave_every > 0 and len(rows) % autosave_every == 0:
                        df_partial = pd.DataFrame(rows)
                        link_col = df_partial.pop("listing_url")
                        df_partial.insert(0, "listing_url", link_col)
                        df_partial.to_excel(out_file, index=False)
                        log(f"Autosaved partial output ({len(rows)} rows) to {out_file}")
                except TimeoutException:
                    log(f"Timeout while scraping {link}")
                except Exception as exc:
                    log(f"Error scraping {link}: {exc}")
                finally:
                    driver.switch_to.window(main_handle)
                    log("Returned to results tab.")

            if max_listings and len(rows) >= max_listings:
                break
            driver.switch_to.window(main_handle)
            if not click_next_page(driver):
                break
            pages_processed += 1
            time.sleep(2.0)
            # Post-pagination settle to avoid stale/transition states.
            retry_deadline = time.time() + 10
            while time.time() < retry_deadline:
                probe_links = collect_current_page_links(driver)
                if probe_links:
                    break
                time.sleep(0.8)

        if not rows:
            log("No listing rows were scraped. Writing empty workbook for visibility.")
            pd.DataFrame([{"listing_url": "", "error": "No listing rows were scraped."}]).to_excel(out_file, index=False)
            return

        # Close worker tab before final write to keep browser state clean.
        if "worker_handle" in locals() and len(driver.window_handles) > 1:
            try:
                driver.switch_to.window(worker_handle)
                driver.close()
                driver.switch_to.window(main_handle)
                log("Closed worker tab.")
            except Exception:
                pass

        df = pd.DataFrame(rows)
        ordered_front = [
            "listing_url",
            "time_on_realtor",
            "price_per_sqft",
            "price",
            "square_footage_numeric",
            "built_in",
            "city",
        ]
        existing_front = [col for col in ordered_front if col in df.columns]
        remaining = [col for col in df.columns if col not in existing_front]
        df = df[existing_front + remaining]
        df.to_excel(out_file, index=False)
        log(f"Final write complete. Wrote {len(df)} rows to {out_file}")
    finally:
        log("Closing browser.")
        try:
            driver.quit()
        except OSError as exc:
            log(f"Ignoring browser shutdown handle error: {exc}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realtor.ca listing scraper to Excel")
    parser.add_argument("--url", default=DEFAULT_URL, help="Map search URL containing your criteria")
    parser.add_argument("--output", default="realtor_listings.xlsx", help="Output Excel file path")
    parser.add_argument("--max-listings", type=int, default=None, help="Optional cap for number of listings")
    parser.add_argument("--max-pages", type=int, default=200, help="Maximum number of sidebar pages to process")
    parser.add_argument("--autosave-every", type=int, default=10, help="Autosave Excel every N rows (0 disables)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless (default: headed)")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(
        start_url=args.url,
        out_file=args.output,
        max_listings=args.max_listings,
        headless=args.headless,
        max_pages=args.max_pages,
        autosave_every=args.autosave_every,
    )
