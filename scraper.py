"""RoImobiliare.com - Scraper v5 (ASCII clean)"""
import os, re, sys, time, json, hashlib, logging, requests
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GHL_WEBHOOK_URL = os.environ.get("GHL_WEBHOOK_URL", "")
GHL_PRIVATE_TOKEN = os.environ["GHL_PRIVATE_TOKEN"]
GHL_LOCATION_ID = "AojtIWqW6PK1qoRK1zLm"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

HEADERS_OLX_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.olx.ro/",
    "Origin": "https://www.olx.ro",
}

LOCALITATI_SIBIU = [
    "sibiu", "cisnadie", "saliste", "ocna-sibiului", "miercurea-sibiului",
    "avrig", "agnita", "dumbraveni", "copsa-mica", "medias", "talmaciu",
    "cristian", "selimbar", "rasinari", "poplaca", "gura-raului",
    "riu-sadului", "tilisca",
]


def gen_id(sursa, url):
    return hashlib.sha256(f"{sursa}:{url}".encode()).hexdigest()[:32]


def curata_pret(text):
    if not text:
        return None
    n = re.sub(r"[^\d]", "", text)
    return float(n) if n else None


def normalize_phone(raw):
    """Return phone in +407XXXXXXXX format or None if invalid/landline."""
    if not raw:
        return None
    n = re.sub(r"[\s\-\.\(\)]", "", str(raw))
    if n.startswith("07") and len(n) == 10:
        n = "+40" + n[1:]
    elif n.startswith("0040") and len(n) == 12:
        n = "+40" + n[4:]
    elif n.startswith("40") and len(n) == 11:
        n = "+" + n
    # Only Romanian mobile: +4072x to +4079x
    if re.match(r"^\+407[2-9]\d{7}$", n):
        return n
    return None


# --- imobiliare.ro ---
def scrape_imobiliare(localitate):
    anunturi = []
    url = f"https://www.imobiliare.ro/vanzare-case-vile/judetul-sibiu/{localitate}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            log.warning(f"imobiliare.ro/{localitate}: HTTP {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        container = soup.find(id="search-listing-results")
        if not container:
            log.warning(f"imobiliare.ro/{localitate}: no results container")
            return []
        links = container.find_all("a", href=re.compile(r"/oferta/"))
        seen = set()
        for a in links[:30]:
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.imobiliare.ro" + href
            if href in seen:
                continue
            seen.add(href)
            parent = a.find_parent()
            pret_el = parent.find(class_=re.compile(r"pret|price")) if parent else None
            titlu = a.get_text(strip=True)[:80] or None
            anunturi.append({
                "id": gen_id("imobiliare.ro", href),
                "sursa": "imobiliare.ro",
                "localitate": localitate,
                "titlu": titlu,
                "pret_eur": curata_pret(pret_el.text if pret_el else ""),
                "zona": localitate,
                "suprafata_mp": None,
                "telefon": None,
                "url_anunt": href,
                "data_scraping": datetime.utcnow().isoformat(),
                "synced_to_ghl": False,
            })
        log.info(f"imobiliare.ro/{localitate}: {len(anunturi)} listings")
    except Exception as e:
        log.error(f"imobiliare.ro/{localitate}: {e}")
    return anunturi


# --- storia.ro ---
def scrape_storia(localitate):
    anunturi = []
    url = f"https://www.storia.ro/ro/rezultate/vanzare/casa/sibiu/{localitate}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            log.warning(f"storia.ro/{localitate}: HTTP {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        articles = soup.find_all("article")
        for art in articles[:30]:
            a = art.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.storia.ro" + href
            if "/oferta/" not in href and "/ro/oferta/" not in href:
                continue
            titlu = art.find("h3") or art.find("h2")
            pret_el = art.find(attrs={"data-testid": re.compile(r"price")}) or art.find(class_=re.compile(r"price|pret"))
            anunturi.append({
                "id": gen_id("storia.ro", href),
                "sursa": "storia.ro",
                "localitate": localitate,
                "titlu": titlu.get_text(strip=True)[:80] if titlu else None,
                "pret_eur": curata_pret(pret_el.get_text() if pret_el else ""),
                "zona": localitate,
                "suprafata_mp": None,
                "telefon": None,
                "url_anunt": href,
                "data_scraping": datetime.utcnow().isoformat(),
                "synced_to_ghl": False,
            })
        log.info(f"storia.ro/{localitate}: {len(anunturi)} listings")
    except Exception as e:
        log.error(f"storia.ro/{localitate}: {e}")
    return anunturi


# --- olx.ro (phone API) ---
def extract_numeric_id(html_text):
    for pat in [r'"\"sku\"\s*:\s*\"([0-9]{6,12})\"', r'ad_id=([0-9]{6,12})', r'/offers/([0-9]{6,12})/']:
        m = re.search(pat, html_text)
        if m:
            return m.group(1)
    return None


def fetch_olx_phone(numeric_id):
    try:
        r = requests.get(
            f"https://www.olx.ro/api/v1/offers/{numeric_id}/limited-phones/",
            headers=HEADERS_OLX_API, timeout=15
        )
        if r.status_code == 200:
            phones = r.json().get("data", {}).get("phones", [])
            if phones:
                return phones[0]
    except Exception as e:
        log.warning(f"OLX phone API {numeric_id}: {e}")
    return None


def scrape_olx():
    anunturi = []
    for page in range(1, 6):
        url = f"https://www.olx.ro/imobiliare/case-de-vanzare/sibiu/?page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            cards = soup.select('[data-cy="l-card"]')
            log.info(f"olx.ro page {page}: {len(cards)} cards")
            if not cards:
                break
            for card in cards:
                a = card.select_one('a[href*="/d/"]')
                if not a:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = "https://www.olx.ro" + href
                href = href.split("?")[0]
                titlu_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                pret_el = card.select_one('[data-testid="ad-price"]')
                loc_el = card.select_one('[data-testid="location-date"]')
                pret_text = pret_el.get_text(strip=True) if pret_el else ""
                nums = re.findall(r"[0-9]+", pret_text.replace(".", "").replace(" ", ""))
                pret_val = float("".join(nums[:2])) if nums else None
                cur = "RON" if "RON" in pret_text.upper() else "EUR"
                time.sleep(0.5)
                try:
                    detail_r = requests.get(href, headers=HEADERS, timeout=20)
                    numeric_id = extract_numeric_id(detail_r.text)
                    telefon = fetch_olx_phone(numeric_id) if numeric_id else None
                except Exception:
                    telefon = None
                anunturi.append({
                    "id": gen_id("olx.ro", href),
                    "sursa": "olx.ro",
                    "localitate": "sibiu",
                    "titlu": titlu_el.get_text(strip=True)[:80] if titlu_el else None,
                    "pret_eur": pret_val if cur == "EUR" else None,
                    "zona": loc_el.get_text(strip=True).split(",")[0] if loc_el else "Sibiu",
                    "suprafata_mp": None,
                    "telefon": telefon,
                    "url_anunt": href,
                    "data_scraping": datetime.utcnow().isoformat(),
                    "synced_to_ghl": False,
                })
            time.sleep(3)
        except Exception as e:
            log.error(f"olx.ro page {page}: {e}")
            break
    log.info(f"olx.ro TOTAL: {len(anunturi)} listings")
    return anunturi


# --- Supabase upsert ---
def upsert_supabase(anunturi):
    if not anunturi:
        return 0
    total = 0
    for i in range(0, len(anunturi), 50):
        batch = anunturi[i:i+50]
        try:
            supabase.table("listings").upsert(batch, on_conflict="id").execute()
            total += len(batch)
            log.info(f"Supabase: batch {i//50+1} -> {len(batch)} rows")
        except Exception as e:
            log.error(f"Supabase upsert batch {i//50+1}: {e}")
    return total


# --- GHL helpers ---
def ghl_contact_exists(phone_norm):
    """Check if contact with this phone already exists in GHL."""
    try:
        r = requests.get(
            "https://services.leadconnectorhq.com/contacts/",
            headers={
                "Authorization": f"Bearer {GHL_PRIVATE_TOKEN}",
                "Version": "2021-07-28",
            },
            params={"locationId": GHL_LOCATION_ID, "query": phone_norm, "limit": 1},
            timeout=10
        )
        if r.status_code == 200:
            return len(r.json().get("contacts", [])) > 0
    except Exception as e:
        log.warning(f"GHL check exists {phone_norm}: {e}")
    return False


def ghl_create_contact(lead, phone_norm):
    """Create new contact in GHL via API."""
    payload = {
        "locationId": GHL_LOCATION_ID,
        "firstName": (lead.get("titlu") or "Vanzator")[:50],
        "phone": phone_norm,
        "source": lead.get("sursa", "scraper"),
        "tags": ["scraper", f"sursa_{lead.get('sursa','').replace('.','_')}", lead.get("localitate", "sibiu")],
        "customFields": [
            {"key": "sursa_anunt", "field_value": lead.get("sursa", "")},
            {"key": "url_anunt", "field_value": lead.get("url_anunt", "")},
            {"key": "localitate", "field_value": lead.get("localitate", "")},
            {"key": "pret_eur", "field_value": str(lead.get("pret_eur") or "")},
        ]
    }
    r = requests.post(
        "https://services.leadconnectorhq.com/contacts/",
        headers={
            "Authorization": f"Bearer {GHL_PRIVATE_TOKEN}",
            "Version": "2021-07-28",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=10
    )
    if r.status_code not in (200, 201):
        log.warning(f"GHL create failed {phone_norm}: {r.status_code} {r.text[:100]}")
    return r.status_code in (200, 201)


# --- GHL sync: mobile RO only + must have price ---
def sync_ghl():
    try:
        result = (supabase.table("listings").select("*")
                  .not_.is_("telefon", "null")
                  .neq("synced_to_ghl", True)
                  .execute())
        leads = result.data or []
        log.info(f"GHL sync: {len(leads)} leads to process")
        synced = skip_no_price = skip_landline = skip_dup = 0
        for lead in leads:
            # Filter 1: must have price
            if not lead.get("pret_eur") or float(lead.get("pret_eur") or 0) <= 0:
                skip_no_price += 1
                continue
            # Filter 2: mobile RO only
            phone_norm = normalize_phone(lead.get("telefon"))
            if not phone_norm:
                skip_landline += 1
                log.info(f"Skip non-mobile: {lead.get('telefon')}")
                continue
            # Filter 3: dedup
            if ghl_contact_exists(phone_norm):
                skip_dup += 1
                supabase.table("listings").update(
                    {"synced_to_ghl": True, "ghl_sync_at": datetime.utcnow().isoformat()}
                ).eq("id", lead["id"]).execute()
                continue
            # Create contact
            try:
                if ghl_create_contact(lead, phone_norm):
                    supabase.table("listings").update(
                        {"synced_to_ghl": True, "ghl_sync_at": datetime.utcnow().isoformat()}
                    ).eq("id", lead["id"]).execute()
                    synced += 1
                    log.info(f"GHL OK: {phone_norm} ({lead.get('sursa')}/{lead.get('localitate')})")
            except Exception as e:
                log.error(f"GHL create {lead.get('id')}: {e}")
            time.sleep(0.5)
        log.info(f"GHL DONE: {synced} created | {skip_no_price} no-price | {skip_landline} landline | {skip_dup} duplicate")
        return synced
    except Exception as e:
        log.error(f"sync_ghl: {e}")
        return 0


# --- Main ---
def main():
    log.info("=" * 60)
    log.info("RoImobiliare Scraper v5 - START")
    log.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    log.info("=" * 60)
    toate = []
    for loc in LOCALITATI_SIBIU:
        log.info(f"--- {loc.upper()} ---")
        toate.extend(scrape_imobiliare(loc))
        time.sleep(2)
        toate.extend(scrape_storia(loc))
        time.sleep(2)
    log.info("--- OLX Sibiu ---")
    toate.extend(scrape_olx())
    log.info(f"TOTAL scraped: {len(toate)}")
    cu_tel = len([a for a in toate if a.get("telefon")])
    log.info(f"With phone: {cu_tel} | Without: {len(toate)-cu_tel}")
    saved = upsert_supabase(toate)
    log.info(f"Supabase: {saved} rows saved")
    synced = sync_ghl()
    log.info("=" * 60)
    log.info("SUMMARY:")
    log.info(f"  Total -> Supabase: {len(toate)}")
    log.info(f"  GHL synced: {synced}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()