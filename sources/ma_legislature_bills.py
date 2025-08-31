"""Massachusetts Legislature Bills module for the aggregator."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


UA = "Mozilla/5.0 (Aggregator MWE) Python/3.x (+https://localhost)"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_bills_table(html_content: str, base_url: str) -> List[Dict[str, Any]]:
    """Parse both Recent Bills and Popular Bills tables from the HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    bills = []
    
    # Find both Recent Bills and Popular Bills tables
    tables = soup.find_all('table')
    recent_bills_table = None
    popular_bills_table = None
    
    # Find both Recent Bills and Popular Bills tables
    tables = soup.find_all('table')
    recent_bills_table = None
    popular_bills_table = None
    
    # Find all elements containing "Popular Bills" or "Recent Bills"
    popular_elements = soup.find_all(string=lambda text: text and 'Popular Bills' in text)
    recent_elements = soup.find_all(string=lambda text: text and 'Recent Bills' in text)
    
    # Look for tables near these elements
    for elem in popular_elements:
        # Find the next table after this element
        next_table = elem.find_next('table')
        if next_table and next_table not in [recent_bills_table, popular_bills_table]:
            popular_bills_table = next_table
            break  # Only take the first unique table
    
    # If we still don't have a Popular Bills table, try a different approach
    if not popular_bills_table:
        # Look for any table that comes after a Popular Bills heading
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            if 'Popular Bills' in heading.get_text():
                # Find the next table after this heading
                next_table = heading.find_next('table')
                if next_table and next_table != recent_bills_table:
                    popular_bills_table = next_table
                    break
    
    # Look for tables by their preceding headings - check multiple levels
    for table in tables:
        # Check for Recent Bills table
        prev_sibling = table.find_previous_sibling()
        if prev_sibling and 'Recent Bills' in prev_sibling.get_text():
            recent_bills_table = table
        elif prev_sibling and 'Popular Bills' in prev_sibling.get_text():
            popular_bills_table = table
        
        # Also check for tables with nearby text
        if table.find_previous(string=lambda text: text and 'Recent Bills' in text):
            recent_bills_table = table
        elif table.find_previous(string=lambda text: text and 'Popular Bills' in text):
            popular_bills_table = table
        
        # Check for headings that might be further up in the DOM
        prev_elements = table.find_previous_siblings()
        for prev_elem in prev_elements[:5]:  # Check last 5 previous siblings
            if prev_elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                if 'Recent Bills' in prev_elem.get_text():
                    recent_bills_table = table
                elif 'Popular Bills' in prev_elem.get_text():
                    popular_bills_table = table
    
    # Parse Recent Bills table
    if recent_bills_table:
        recent_bills = _parse_single_table(recent_bills_table, base_url, "Recent")
        bills.extend(recent_bills)
    
    # Parse Popular Bills table
    if popular_bills_table:
        popular_bills = _parse_single_table(popular_bills_table, base_url, "Popular")
        bills.extend(popular_bills)
    
    # Fallback: if we didn't find the specific tables, try to find any bills table
    if not bills:
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) > 1:  # Has data rows
                first_row = rows[0]
                cells = first_row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    # Check if this looks like a bills table
                    cell_texts = [cell.get_text().strip() for cell in cells]
                    if any('Bill' in text for text in cell_texts) or any('Title' in text for text in cell_texts):
                        bills.extend(_parse_single_table(table, base_url, "Bills"))
                        break
    
    return bills


def _parse_single_table(table, base_url: str, table_type: str) -> List[Dict[str, Any]]:
    """Parse a single bills table and return list of bills."""
    bills = []
    rows = table.find_all('tr')[1:]  # Skip header row
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:  # Need at least Bill No and Title
            
            # Extract bill number and link
            bill_cell = cells[0] if len(cells) == 2 else cells[1]
            bill_link = bill_cell.find('a')
            if bill_link:
                bill_number = bill_link.get_text().strip()
                bill_url = urljoin(base_url, bill_link.get('href', ''))
                
                # Extract title
                title_cell = cells[1] if len(cells) == 2 else cells[2]
                title = title_cell.get_text().strip()
                
                if bill_number and title:
                    bills.append({
                        "ts": _now_iso(),
                        "title": f"{bill_number}: {title}",
                        "source": f"Massachusetts Legislature ({table_type})",
                        "url": bill_url,
                        "bill_number": bill_number,
                        "bill_title": title,
                        "table_type": table_type,
                    })
    
    return bills


async def _fetch(
    client: httpx.AsyncClient, url: str
) -> Tuple[str, Optional[str], Optional[str]]:
    """Fetch the bills page and return the URL, content, and any error."""
    try:
        r = await client.get(url, headers={"User-Agent": UA})
        r.raise_for_status()
        return url, r.text, None
    except Exception as e:
        return url, None, f"{type(e).__name__}: {e}"


async def run(config: Dict[str, Any]):
    """
    Async generator for Massachusetts Legislature bills.
    Config keys:
      url: str                    # URL of the bills page
      limit: int                  # maximum number of bills to return (default 20)
      mode: 'append'|'replace'   # default 'replace'
    Emits DataChunk(s) with row schema: ts, title, source, url, bill_number, bill_title
    """
    url = config.get("url", "https://malegislature.gov/Bills/RecentBills")
    limit = int(config.get("limit", 20))
    mode = config.get("mode", "replace")
    
    timeout = httpx.Timeout(20.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": UA},
    ) as client:
        url, content, err = await _fetch(client, url)
    
    if err or not content:
        rows = [{
            "ts": _now_iso(),
            "title": f"Fetch error for {url}: {err}",
            "source": "Massachusetts Legislature",
            "url": url,
            "bill_number": "",
            "bill_title": "",
        }]
    else:
        rows = _parse_bills_table(content, url)
        # Apply limit
        rows = rows[:limit]
    
    yield {
        "template": config["template"],
        "stream": config["stream"],
        "rows": rows,
        "mode": mode,
        "ts": _now_iso(),
    } 