import re
from typing import Any, List, Dict, Optional
from datetime import datetime, timezone
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Legistar Meeting Scraper) Python/3.x (+https://localhost)"


def _iso(dt: datetime) -> str:
    """Convert datetime to ISO format with UTC timezone."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string from Legistar format (e.g., '8/29/2025')."""
    try:
        # Handle various date formats
        for fmt in ['%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


def _parse_time(time_str: str) -> Optional[str]:
    """Parse time string from Legistar format (e.g., '10:00 AM')."""
    if not time_str or time_str.strip() == '':
        return None
    return time_str.strip()


def _extract_meeting_details(html: str, base_url: str) -> List[Dict[str, Any]]:
    """Extract meeting details from Legistar meeting detail page HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    meetings = []
    
    # Look for the main meeting table
    table = soup.find('table', {'id': re.compile(r'gridCalendar')})
    if not table:
        return meetings
    
    # Find all table rows (skip header)
    rows = table.find_all('tr', class_=re.compile(r'rgRow|rgAltRow'))
    
    for row in rows:
        try:
            cells = row.find_all('td')
            if len(cells) < 8:  # Need at least 8 columns for full meeting info
                continue
                
            # Extract basic meeting info
            body_name_cell = cells[0]
            date_cell = cells[1]
            time_cell = cells[3]
            location_cell = cells[4]
            details_cell = cells[5]
            agenda_cell = cells[6]
            
            # Extract body name and link
            body_link = body_name_cell.find('a')
            body_name = body_link.get_text(strip=True) if body_link else "Unknown Body"
            body_url = urljoin(base_url, body_link.get('href', '')) if body_link else ""
            
            # Extract meeting date
            meeting_date = date_cell.get_text(strip=True)
            parsed_date = _parse_date(meeting_date)
            
            # Extract meeting time
            meeting_time = time_cell.get_text(strip=True)
            parsed_time = _parse_time(meeting_time)
            
            # Extract location
            location = location_cell.get_text(strip=True)
            
            # Extract meeting details/description
            details = details_cell.get_text(strip=True)
            
            # Extract meeting detail link
            detail_link = details_cell.find('a')
            meeting_detail_url = ""
            if detail_link:
                detail_url = detail_link.get('href', '')
                if detail_url.startswith('MeetingDetail.aspx'):
                    meeting_detail_url = urljoin(base_url, detail_url)
            
            # Extract agenda link
            agenda_url = ""
            if agenda_cell:
                agenda_link = agenda_cell.find('a')
                if agenda_link:
                    agenda_url = urljoin(base_url, agenda_link.get('href', ''))
            
            # Create meeting record
            meeting = {
                "ts": (_iso(parsed_date) if parsed_date 
                       else datetime.now(timezone.utc).isoformat()),
                "title": f"{body_name}: {details}" if details else body_name,
                "body_name": body_name,
                "meeting_date": meeting_date,
                "meeting_time": parsed_time,
                "location": location,
                "details": details,
                "body_url": body_url,
                "meeting_detail_url": meeting_detail_url,
                "agenda_url": agenda_url,
                "url": meeting_detail_url or body_url,  # Primary URL
            }
            
            meetings.append(meeting)
            
        except Exception as e:
            # Log error but continue processing other rows
            print(f"Error processing meeting row: {e}")
            continue
    
    return meetings


async def _fetch_meeting_page(client: httpx.AsyncClient, url: str) -> tuple[str, str, Optional[str]]:
    """Fetch meeting page HTML content."""
    try:
        r = await client.get(url, headers={"User-Agent": UA})
        r.raise_for_status()
        return url, r.text, None
    except Exception as e:
        return url, "", f"{type(e).__name__}: {e}"


async def run(config: dict[str, Any]):
    """
    Fetch and parse Legistar meeting detail pages.
    
    Config:
      url: str - Base URL for the Legistar site (e.g., https://boston.legistar.com/)
      meeting_list_url: str - URL to the meeting list page (e.g., Calendar.aspx)
      limit: int - Maximum number of meetings to fetch (default: 50)
      mode: 'replace'|'append' (default 'replace')
    
    Emits rows with basic meeting information including:
    ts, title, body_name, meeting_date, meeting_time, location, details,
    body_url, meeting_detail_url, agenda_url
    """
    base_url = config.get("url", "").rstrip('/')
    meeting_list_url = config.get("meeting_list_url", "")
    limit = config.get("limit", 50)
    mode = config.get("mode", "replace")
    
    if not base_url or not meeting_list_url:
        yield {
            "template": config["template"],
            "stream": config["stream"],
            "rows": [{
                "ts": datetime.now(timezone.utc).isoformat(),
                "title": "Legistar: Missing URL configuration",
                "body_name": "",
                "meeting_date": "",
                "meeting_time": "",
                "location": "",
                "details": "",
                "body_url": "",
                "meeting_detail_url": "",
                "agenda_url": "",
                "url": ""
            }],
            "mode": mode,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        return
    
    # Construct full meeting list URL
    if not meeting_list_url.startswith('http'):
        meeting_list_url = urljoin(base_url, meeting_list_url)
    
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Fetch the meeting list page
        url, html, err = await _fetch_meeting_page(client, meeting_list_url)
        
        if err or not html:
            yield {
                "template": config["template"],
                "stream": config["stream"],
                "rows": [{
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "title": f"Legistar: Fetch error - {err}",
                    "body_name": "",
                    "meeting_date": "",
                    "meeting_time": "",
                    "location": "",
                    "details": "",
                    "body_url": "",
                    "meeting_detail_url": "",
                    "agenda_url": "",
                    "url": meeting_list_url
                }],
                "mode": mode,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            return
        
        # Parse meeting details from the HTML
        meetings = _extract_meeting_details(html, base_url)
        
        # Apply limit and sort by date (newest first)
        meetings = sorted(meetings, key=lambda x: x.get("ts", ""), reverse=True)[:limit]
        
        # Yield the results (no need to enrich with detailed content)
        yield {
            "template": config["template"],
            "stream": config["stream"],
            "rows": meetings,
            "mode": mode,
            "ts": datetime.now(timezone.utc).isoformat(),
        } 