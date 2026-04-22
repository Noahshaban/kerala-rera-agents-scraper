# Kerala RERA Agents — Full Dataset Scraper

> Automated pipeline that collects, cleans, and exports the complete registry of real-estate agents from the Kerala RERA government portal into a formatted Excel workbook.

---

## Results

| Metric | Value |
|---|---|
| Pages scraped | 45 |
| Agents collected | 894 |
| Duplicate rows removed | Automatic |
| Output format | `.xlsx` (formatted) |

Sample output:

| Name | Mobile No | Email ID | Address |
|---|---|---|---|
| CHATHAIYALI AGNEL THOMAS | 9349001913 | tchathely@gmail.com | CHATHELY (H), 101-SRA, 120-KSHB, Thrissur, 680125 |
| ARHAM K ASHRAF | 9575009509 | arhamashraf2030@gmail.com | KUNNUMPURATH HOUSE, Ernakulam, 683547 |
| Muhammed Thahir | 9847600430 | thahir.mkdv@gmail.com | Kunnummal House, Koduvally, Kozhikode, 673572 |

---

## What It Does

1. **Scrapes** all 45 pages of `rera.kerala.gov.in/agents` using a headless Chromium browser
2. **Cleans** the raw data — normalizes whitespace, fixes obfuscated emails (`[at]` → `@`), removes duplicates
3. **Exports** a professionally formatted `.xlsx` file with a dark-navy header, alternating row colors, frozen header row, auto-fitted columns, and a metadata sheet

---

## Tech Stack

| Tool | Purpose |
|---|---|
| `Playwright` | Headless browser automation (handles JavaScript-rendered pages) |
| `pandas` | Data cleaning and deduplication |
| `openpyxl` | Excel formatting and export |
| `Python 3.11+` | Core language |

---

## Project Structure

```
kerala-rera-agents-scraper/
│
├── kerala_rera_full_scraper.py   # Main script
├── requirements.txt
└── README.md
```

---

## How to Run

### 1. Install dependencies

```bash
pip install playwright pandas openpyxl
playwright install chromium
```

### 2. Run the scraper

```bash
python kerala_rera_full_scraper.py
```

The script will log progress to the console and save the output file when done.

---

## Configuration

All settings are at the top of the script — no config file needed:

```python
BASE_URL       = "https://rera.kerala.gov.in/agents"
FIRST_PAGE     = 1
LAST_PAGE      = 45
OUTPUT_FILE    = "Kerala_RERA_Full_Dataset.xlsx"
PAGE_DELAY_SEC = 3.0    # polite delay between requests
```

---

## Error Handling

- **Navigation timeout** → logs a warning and still attempts to parse the page
- **3 consecutive failures** → stops early and saves all data collected so far
- **Keyboard interrupt** (`Ctrl+C`) → graceful exit with partial save

---

## Output File

The `.xlsx` file contains two sheets:

**Sheet 1 — All Agents**
- Columns: `Name`, `Mobile No`, `Email ID`, `Address`
- Dark-navy bold header row
- Alternating light-blue / white rows
- Frozen header for easy scrolling
- Auto-fitted column widths

**Sheet 2 — Scrape Info**
- Source URL, pages scraped, total agents, timestamp

---

## Use Cases

This scraper pattern can be adapted for any paginated government or real-estate data portal that renders content with JavaScript. Contact details, pagination logic, and output columns are all configurable.

---

## Author

**Noah Shaban** — Data Engineer  
[GitHub](https://github.com/Noahshaban)

---

*Scraped January 2026 · Kerala RERA public registry · For research and data analysis purposes only*
