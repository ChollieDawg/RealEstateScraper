# RealEstateScraper

Scrapes Realtor.ca listings from a criteria URL and exports data to Excel, including listing links and keyword flags for your unit/condition/location/amenities preferences.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
python scraper.py \
  --url "https://www.realtor.ca/map#ZoomLevel=11&Center=49.162262%2C-122.742322&LatitudeMax=49.30932&LongitudeMax=-122.20365&LatitudeMin=49.01477&LongitudeMin=-123.28099&Sort=6-D&PropertyTypeGroupID=1&TransactionTypeId=2&PropertySearchTypeId=0&PriceMax=650000&BedRange=1-0&BathRange=1-0&Currency=CAD" \
  --output realtor_listings.xlsx
```

By default the script runs in headed mode (visible browser) for better compatibility with Realtor dynamic rendering. Use `--headless` if you want background execution.

Optional limit for quick trial:

```bash
python scraper.py --max-listings 10
```

## Output

The Excel file includes:
- `listing_url`
- core fields such as `price`, `description`, `square_footage`, `parking_type`, etc.
- boolean columns for requested attributes (e.g. `in_suite_laundry`, `close_to_transit`, `gym`, `fireplace`)

## Notes

- Realtor.ca is a dynamic site; selectors and field labels may change.
- Link collection uses Playwright sidebar/card scraping and pagination; tune selectors/xpaths if Realtor.ca updates markup.
- The scraper uses structured data (`application/ld+json`) plus selector/text fallbacks for more reliable price/address extraction.
- Some attributes are inferred via keyword matching from listing text and may be imperfect.
- If needed, tune `KEYWORD_RULES` and scroll behavior in `scraper.py`.
