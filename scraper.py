"""
RoImobiliare.com â Scraper v4
URLs verificate live pe 2026-04-06:
  - imobiliare.ro: /vanzare-case-vile/judetul-sibiu/{localitate}
  - storia.ro:     /ro/rezultate/vanzare/casa/sibiu/{localitate}
  - olx.ro:        /imobiliare/case-de-vanzare/sibiu/ (cu phone API)
  - publi24.ro:    ELIMINAT (SPA, incompatibil cu requests)

Flux: Supabase (toate) â GHL (doar cu telefon)
"""

import os, re, sys, time, json, hashlib, logging, requests
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stdout)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GHL_WEBHOOK_URL = os.environ["GHL_WEBHOOK_URL"]

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
    "sibiu", "cisnadie", "saliste", "ocna-sibiului",
    "miercurea-sibiului", "avrig", "agnita", "dumbraveni",
    "copsa-mica", "medias", "talmaciu", "cristian",
    "selimbar", "rasinari", "poplaca", "gura-raului",
    "riu-sadului", "tilisca",
]

def gen_id(sursa, url):
    return hashlib.sha256(f"{sursa}:{url}".encode()).hexdigest()[:32]

def curata_pret(text):
    if not text: return None
    n = re.sub(r"[^\d]", "", text)
    return float(n) if n else None

def curata_telefon(text):
    if not text: return None
    cifre = re.sub(r"[^\d+]", "", text)
    if len(cifre) < 9: return None
    if cifre.startswith("07") or cifre.startswith("02"):
        cifre = "+4" + cifre
    return cifre

# âââ imobiliare.ro ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
            log.warning(f"imobiliare.ro/{localitate}: no #search-listing-results")
            return []
        links = container.find_all("a", href=re.compile(r"/oferta/"))
        seen = set()
        for a in links[:30]:
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.imobiliare.ro" + href
            if href in seen: continue
            seen.add(href)
            pret_el = a.find_parent().find(class_=re.compile(r"pret|price")) if a.find_parent() else None
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
        log.info(f"imobiliare.ro/{localitate}: {len(anunturi)} anunturi")
    except Exception as e:
        log.error(f"imobiliare.ro/{localitate}: {e}")
    return anunturi

# âââ storia.ro ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
            if not a: continue
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.storia.ro" + href
            if "/oferta/" not in href and "/ro/oferta/" not in href: continue
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
        log.info(f"storia.ro/{localitate}: {len(anunturi)} anunturi")
    except Exception as e:
        log.error(f"storia.ro/{localitate}: {e}")
    return anunturi

# âââ olx.ro (cu phone API â dovedit funcÈional Ã®n v8) ââââââââââââââââââââââââ
def extract_numeric_id(html_text):
    for pat in [r'"sku"\s*:\s*"([0-9]{6,12})"', r'ad_id=([0-9]{6,12})', r'/offers/([0-9]{6,12})/']:
        m = re.search(pat, html_text)
        if m: return m.group(1)
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
                return curata_telefon(phones[0])
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
            log.info(f"olx.ro page {page}: {len(cards)} carduri")
            if not cards: break
            for card in cards:
                a = card.select_one('a[href*="/d/"]')
                if not a: continue
                href = a["href"]
                if not href.startswith("http"):
                    href = "https://www.olx.ro" + href
                href = href.split("?")[0]
                lid = href.rstrip("/").split("/")[-1]

                titlu_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                pret_el = card.select_one('[data-testid="ad-price"]')
                loc_el = card.select_one('[data-testid="location-date"]')

                pret_text = pret_el.get_text(strip=True) if pret_el else ""
                nums = re.findall(r"[0-9]+", pret_text.replace(".", "").replace(" ", ""))
                pret_val = float("".join(nums[:2])) if nums else None
                cur = "RON" if "RON" in pret_text.upper() else "EUR"
                pret_str = f"{pret_val} {cur}" if pret_val else None

                # Fetch phone via API
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
                time.sleep(1)
            time.sleep(3)
        except Exception as e:
            log.error(f"olx.ro page {page}: {e}")
            break
    log.info(f"olx.ro TOTAL: {len(anunturi)} anunturi")
    return anunturi

# âââ Supabase upsert ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def upsert_supabase(anunturi):
    if not anunturi: return 0
    total = 0
    for i in range(0, len(anunturi), 50):
        batch = anunturi[i:i+50]
        try:
            supabase.table("listings").upsert(batch, on_conflict="id").execute()
            total += len(batch)
            log.info(f"Supabase: batch {i//50+1} â {len(batch)} randuri")
        except Exception as e:
            log.error(f"Supabase upsert batch {i//50+1}: {e}")
    return total

# âââ GHL sync (doar cu telefon) âââââââââââââââââââââââââââââââââââââââââââââââ
def sync_ghl():
    try:
        result = (supabase.table("listings").select("*")
            .not_.is_("telefon", "null")
            .neq("synced_to_ghl", True)
            .execute())
        leads = result.data or []
        log.info(f"GHL sync: {len(leads)} leads cu telefon")
        synced = 0
        for lead in leads:
            payload = {
                "firstName": (lead.get("titlu") or "Vanzator")[:50],
                "phone": lead["telefon"],
                "email": "",
                "customField": {
                    "sursa_anunt": lead.get("sursa", ""),
                    "url_anunt": lead.get("url_anunt", ""),
                    "localitate": lead.get("localitate", ""),
                    "pret_eur": str(lead.get("pret_eur") or ""),
                    "zona": lead.get("zona", ""),
                },
                "tags": ["scraper", f"sursa_{lead.get('sursa','').replace('.','_')}"],
            }
            try:
                r = requests.post(GHL_WEBHOOK_URL, json=payload,
                    headers={"Content-Type": "application/json"}, timeout=10)
                if r.status_code in (200, 201, 202):
                    supabase.table("listings").update(
                        {"synced_to_ghl": True, "ghl_sync_at": datetime.utcnow().isoformat()}
                    ).eq("id", lead["id"]).execute()
                    synced += 1
                    log.info(f"â GHL: {lead['telefon']} ({lead.get('sursa')}/{lead.get('localitate')})")
            except Exception as e:
                log.error(f"GHL lead {lead.get('id')}: {e}")
            time.sleep(1)
        return synced
    except Exception as e:
        log.error(f"sync_ghl: {e}")
        return 0

# âââ Main âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def main():
    log.info("=" * 60)
    log.info("RoImobiliare Scraper v4 â START")
    log.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    toate = []

    # imobiliare.ro + storia.ro â per localitate
    for loc in LOCALITATI_SIBIU:
        log.info(f"\nâââ {loc.upper()} âââ")
        rez = scrape_imobiliare(loc)
        toate.extend(rez)
        time.sleep(2)
        rez = scrape_storia(loc)
        toate.extend(rez)
        time.sleep(2)

    # olx.ro â county level (are phone API)
    log.info("\nâââ OLX Sibiu âââ")
    toate.extend(scrape_olx())

    log.info(f"\nTOTAL scraped: {len(toate)}")
    cu_tel = len([a for a in toate if a.get("telefon")])
    log.info(f"Cu telefon: {cu_tel} | Fara telefon: {len(toate)-cu_tel}")

    saved = upsert_supabase(toate)
    log.info(f"Supabase: {saved} randuri salvate")

    synced = sync_ghl()
    log.info(f"GHL: {synced} leads trimise")

    log.info("\n" + "=" * 60)
    log.info("SUMMARY:")
    log.info(f"  Total â Supabase: {len(toate)}")
    log.info(f"  Cu telefon â GHL: {synced}")
    log.info(f"  Fara telefon (arhiva): {len(toate) - cu_tel}")
    log.info("=" * 60)
    log.info("Scraper v4 â DONE")

if __name__ == "__main__":
    main()
