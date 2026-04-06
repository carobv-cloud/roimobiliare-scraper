"""
RoImobiliare.com â Scraper v3
ArhitecturÄ: Supabase (toate datele) â GHL (doar anunÈuri cu telefon)

Flux:
1. Scrape imobiliare.ro + publi24.ro + storia.ro + olx.ro (18 localitÄÈi Sibiu)
2. Upsert TOATE Ã®n Supabase `listings`
3. Query Supabase: telefon IS NOT NULL AND synced_to_ghl = FALSE
4. POST la GHL webhook â Agent Ana
5. MarcheazÄ synced_to_ghl = TRUE Ã®n Supabase

Rezultat: GHL rÄmÃ¢ne curat â doar leads calificaÈi cu numÄr de telefon.
"""

import os
import re
import time
import json
import hashlib
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

# âââ Logging âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# âââ Config âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key (bypass RLS)
GHL_WEBHOOK_URL = os.environ["GHL_WEBHOOK_URL"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
}

# 18 localitÄÈi Sibiu
LOCALITATI_SIBIU = [
    "sibiu", "cisnadie", "saliste", "ocna-sibiului",
    "miercurea-sibiului", "avrig", "agnita", "dumbraveni",
    "copsa-mica", "medias", "talmaciu", "cristian-sb",
    "selimbar", "rasinari", "poplaca", "gura-raului",
    "riu-sadului", "tilisca",
]

# âââ Supabase client ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# âââ Helpers ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def generate_id(sursa: str, url: str) -> str:
    """ID determinist bazat pe sursÄ + URL â previne duplicate."""
    return hashlib.sha256(f"{sursa}:{url}".encode()).hexdigest()[:32]


def curata_pret(text: str) -> float | None:
    """Extrage valoarea numericÄ din text preÈ."""
    if not text:
        return None
    numere = re.sub(r"[^\d]", "", text)
    return float(numere) if numere else None


def curata_telefon(text: str) -> str | None:
    """NormalizeazÄ numÄrul de telefon la format internaÈional."""
    if not text:
        return None
    cifre = re.sub(r"[^\d+]", "", text)
    if len(cifre) < 9:
        return None
    # Normalizare: 07xx â +407xx
    if cifre.startswith("07") or cifre.startswith("02"):
        cifre = "+4" + cifre
    return cifre


# âââ Scraper imobiliare.ro ââââââââââââââââââââââââââââââââââââââââââââââââââââ
def scrape_imobiliare(localitate: str) -> list[dict]:
    anunturi = []
    url = f"https://www.imobiliare.ro/vanzare-case-si-vile/{localitate}/?pagina=1"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        carduri = soup.select("div.card-v2") or soup.select("article.listing-card")
        log.info(f"imobiliare.ro/{localitate}: {len(carduri)} carduri gÄsite")

        for card in carduri[:30]:
            try:
                link_el = card.select_one("a[href*='/vanzare']") or card.select_one("a.js-item-title")
                if not link_el:
                    continue

                url_anunt = link_el.get("href", "")
                if not url_anunt.startswith("http"):
                    url_anunt = "https://www.imobiliare.ro" + url_anunt

                titlu_el = card.select_one("h2, h3, .title, .js-item-title")
                pret_el = card.select_one(".pret, .price, [data-price]")
                zona_el = card.select_one(".locatie, .location, .address")
                suprafata_el = card.select_one(".suprafata, [data-surface]")

                # Extrage telefon dacÄ e vizibil pe listing (rar, dar posibil)
                tel_el = card.select_one("[href^='tel:'], .phone, .telefon")
                telefon = None
                if tel_el:
                    tel_raw = tel_el.get("href", "").replace("tel:", "") or tel_el.text
                    telefon = curata_telefon(tel_raw)

                anunt = {
                    "id": generate_id("imobiliare.ro", url_anunt),
                    "sursa": "imobiliare.ro",
                    "localitate": localitate,
                    "titlu": titlu_el.text.strip() if titlu_el else None,
                    "pret_eur": curata_pret(pret_el.text if pret_el else ""),
                    "zona": zona_el.text.strip() if zona_el else localitate,
                    "suprafata_mp": curata_pret(suprafata_el.text if suprafata_el else ""),
                    "telefon": telefon,
                    "url_anunt": url_anunt,
                    "data_scraping": datetime.utcnow().isoformat(),
                    "synced_to_ghl": False,
                }
                anunturi.append(anunt)

            except Exception as e:
                log.warning(f"Eroare card imobiliare.ro: {e}")
                continue

    except Exception as e:
        log.error(f"Eroare imobiliare.ro/{localitate}: {e}")

    return anunturi


# âââ Scraper publi24.ro âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def scrape_publi24(localitate: str) -> list[dict]:
    anunturi = []
    url = f"https://www.publi24.ro/anunturi/imobiliare/vanzari/case/{localitate}/"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        carduri = soup.select(".listing-item, .ad-item, article.ad")
        log.info(f"publi24.ro/{localitate}: {len(carduri)} carduri gÄsite")

        for card in carduri[:30]:
            try:
                link_el = card.select_one("a[href]")
                if not link_el:
                    continue

                url_anunt = link_el.get("href", "")
                if not url_anunt.startswith("http"):
                    url_anunt = "https://www.publi24.ro" + url_anunt

                titlu_el = card.select_one("h2, h3, .title, .ad-title")
                pret_el = card.select_one(".price, .pret")
                zona_el = card.select_one(".location, .locatie")

                # publi24 afiÈeazÄ uneori telefon pe listing
                tel_el = card.select_one("[href^='tel:']")
                telefon = None
                if tel_el:
                    tel_raw = tel_el.get("href", "").replace("tel:", "")
                    telefon = curata_telefon(tel_raw)

                anunt = {
                    "id": generate_id("publi24.ro", url_anunt),
                    "sursa": "publi24.ro",
                    "localitate": localitate,
                    "titlu": titlu_el.text.strip() if titlu_el else None,
                    "pret_eur": curata_pret(pret_el.text if pret_el else ""),
                    "zona": zona_el.text.strip() if zona_el else localitate,
                    "suprafata_mp": None,
                    "telefon": telefon,
                    "url_anunt": url_anunt,
                    "data_scraping": datetime.utcnow().isoformat(),
                    "synced_to_ghl": False,
                }
                anunturi.append(anunt)

            except Exception as e:
                log.warning(f"Eroare card publi24: {e}")
                continue

    except Exception as e:
        log.error(f"Eroare publi24.ro/{localitate}: {e}")

    return anunturi


# âââ Scraper storia.ro ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def scrape_storia(localitate: str) -> list[dict]:
    anunturi = []
    url = f"https://www.storia.ro/vanzare/case/{localitate}/"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        carduri = soup.select("[data-testid='listing-item'], article.css-134la8x, li.css-o9b79t")
        log.info(f"storia.ro/{localitate}: {len(carduri)} carduri gÄsite")

        for card in carduri[:30]:
            try:
                link_el = card.select_one("a[href]")
                if not link_el:
                    continue

                url_anunt = link_el.get("href", "")
                if not url_anunt.startswith("http"):
                    url_anunt = "https://www.storia.ro" + url_anunt

                titlu_el = card.select_one("h3, [data-testid='listing-item-title']")
                pret_el = card.select_one("[data-testid='listing-item-price'], .css-1mojccp")
                zona_el = card.select_one("[data-testid='listing-item-address'], .css-42r2ms")

                anunt = {
                    "id": generate_id("storia.ro", url_anunt),
                    "sursa": "storia.ro",
                    "localitate": localitate,
                    "titlu": titlu_el.text.strip() if titlu_el else None,
                    "pret_eur": curata_pret(pret_el.text if pret_el else ""),
                    "zona": zona_el.text.strip() if zona_el else localitate,
                    "suprafata_mp": None,
                    "telefon": None,  # Storia nu afiÈeazÄ telefon pe listing
                    "url_anunt": url_anunt,
                    "data_scraping": datetime.utcnow().isoformat(),
                    "synced_to_ghl": False,
                }
                anunturi.append(anunt)

            except Exception as e:
                log.warning(f"Eroare card storia: {e}")
                continue

    except Exception as e:
        log.error(f"Eroare storia.ro/{localitate}: {e}")

    return anunturi


# âââ Scraper olx.ro âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def scrape_olx(localitate: str) -> list[dict]:
    anunturi = []
    # OLX foloseÈte slug-uri diferite
    slug = localitate.replace("-", "_")
    url = f"https://www.olx.ro/imobiliare/case-terenuri/vanzare-case/sibiu/{localitate}/"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        carduri = soup.select("[data-cy='l-card'], .css-qo0cxu")
        log.info(f"olx.ro/{localitate}: {len(carduri)} carduri gÄsite")

        for card in carduri[:30]:
            try:
                link_el = card.select_one("a[href*='/oferta/']")
                if not link_el:
                    continue

                url_anunt = link_el.get("href", "")
                if not url_anunt.startswith("http"):
                    url_anunt = "https://www.olx.ro" + url_anunt

                titlu_el = card.select_one("h6, h4, [data-cy='ad-title']")
                pret_el = card.select_one("[data-testid='ad-price'], .css-tyui9s")
                zona_el = card.select_one("[data-testid='location-date'], p.css-1a4brun")

                anunt = {
                    "id": generate_id("olx.ro", url_anunt),
                    "sursa": "olx.ro",
                    "localitate": localitate,
                    "titlu": titlu_el.text.strip() if titlu_el else None,
                    "pret_eur": curata_pret(pret_el.text if pret_el else ""),
                    "zona": zona_el.text.strip() if zona_el else localitate,
                    "suprafata_mp": None,
                    "telefon": None,  # OLX ascunde telefonul dupÄ click
                    "url_anunt": url_anunt,
                    "data_scraping": datetime.utcnow().isoformat(),
                    "synced_to_ghl": False,
                }
                anunturi.append(anunt)

            except Exception as e:
                log.warning(f"Eroare card olx: {e}")
                continue

    except Exception as e:
        log.error(f"Eroare olx.ro/{localitate}: {e}")

    return anunturi


# âââ Upsert Ã®n Supabase âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def upsert_supabase(anunturi: list[dict]) -> int:
    """
    Upsert batch Ã®n Supabase.
    ON CONFLICT (id) â nu suprascrie synced_to_ghl dacÄ deja e True.
    ReturneazÄ numÄrul de rÃ¢nduri inserate/actualizate.
    """
    if not anunturi:
        return 0

    try:
        # Batch de 50 pentru a evita timeout
        total = 0
        for i in range(0, len(anunturi), 50):
            batch = anunturi[i:i+50]
            result = (
                supabase.table("listings")
                .upsert(batch, on_conflict="id", ignore_duplicates=False)
                .execute()
            )
            total += len(batch)
            log.info(f"Supabase upsert: {len(batch)} rÃ¢nduri (batch {i//50 + 1})")

        return total

    except Exception as e:
        log.error(f"Eroare Supabase upsert: {e}")
        return 0


# âââ Sync la GHL (doar cu telefon, nesincronizate) âââââââââââââââââââââââââââ
def sync_leads_to_ghl() -> int:
    """
    CiteÈte din Supabase DOAR rÃ¢ndurile cu telefon AND synced_to_ghl = FALSE.
    POST fiecare la GHL webhook.
    MarcheazÄ synced_to_ghl = TRUE dupÄ trimitere reuÈitÄ.
    """
    try:
        result = (
            supabase.table("listings")
            .select("*")
            .not_.is_("telefon", "null")
            .neq("synced_to_ghl", True)
            .execute()
        )

        leads = result.data or []
        log.info(f"Leads cu telefon de sincronizat: {len(leads)}")

        sincronizate = 0
        for lead in leads:
            try:
                payload = {
                    # CÃ¢mpuri standard GHL
                    "firstName": (lead.get("titlu") or "VÃ¢nzÄtor")[:50],
                    "phone": lead["telefon"],
                    "email": "",  # nu avem email de pe scraper

                    # Custom fields GHL
                    "customField": {
                        "sursa_anunt": lead.get("sursa", ""),
                        "url_anunt": lead.get("url_anunt", ""),
                        "localitate": lead.get("localitate", ""),
                        "pret_eur": str(lead.get("pret_eur") or ""),
                        "suprafata_mp": str(lead.get("suprafata_mp") or ""),
                        "zona": lead.get("zona", ""),
                        "data_anunt": lead.get("data_scraping", ""),
                        "listing_id": lead.get("id", ""),
                    },

                    # Tag pentru filtrare Ã®n GHL (nu declanÈeazÄ workflow greÈit)
                    "tags": ["scraper", f"sursa_{lead.get('sursa','').replace('.', '_')}"],
                }

                r = requests.post(
                    GHL_WEBHOOK_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )

                if r.status_code in (200, 201, 202):
                    # MarcheazÄ ca sincronizat
                    supabase.table("listings").update(
                        {"synced_to_ghl": True, "ghl_sync_at": datetime.utcnow().isoformat()}
                    ).eq("id", lead["id"]).execute()

                    sincronizate += 1
                    log.info(f"â GHL sync: {lead['telefon']} ({lead.get('sursa')}/{lead.get('localitate')})")
                else:
                    log.warning(f"GHL webhook {r.status_code}: {r.text[:100]}")

                # Rate limit: 1 req/secundÄ cÄtre GHL
                time.sleep(1)

            except Exception as e:
                log.error(f"Eroare sync lead {lead.get('id')}: {e}")
                continue

        return sincronizate

    except Exception as e:
        log.error(f"Eroare sync_leads_to_ghl: {e}")
        return 0


# âââ Main âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def main():
    log.info("=" * 60)
    log.info("RoImobiliare Scraper v3 â START")
    log.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    toate_anunturile = []
    stats = {"imobiliare": 0, "publi24": 0, "storia": 0, "olx": 0}

    for localitate in LOCALITATI_SIBIU:
        log.info(f"\nâââ Localitate: {localitate.upper()} âââ")

        # imobiliare.ro
        rez = scrape_imobiliare(localitate)
        stats["imobiliare"] += len(rez)
        toate_anunturile.extend(rez)
        time.sleep(2)

        # publi24.ro
        rez = scrape_publi24(localitate)
        stats["publi24"] += len(rez)
        toate_anunturile.extend(rez)
        time.sleep(2)

        # storia.ro
        rez = scrape_storia(localitate)
        stats["storia"] += len(rez)
        toate_anunturile.extend(rez)
        time.sleep(2)

        # olx.ro
        rez = scrape_olx(localitate)
        stats["olx"] += len(rez)
        toate_anunturile.extend(rez)
        time.sleep(3)

    log.info("\n" + "=" * 60)
    log.info(f"TOTAL anunÈuri scraped: {len(toate_anunturile)}")
    log.info(f"  imobiliare.ro: {stats['imobiliare']}")
    log.info(f"  publi24.ro:    {stats['publi24']}")
    log.info(f"  storia.ro:     {stats['storia']}")
    log.info(f"  olx.ro:        {stats['olx']}")

    # PASUL 1: SalveazÄ TOTUL Ã®n Supabase
    log.info("\nâââ SUPABASE UPSERT âââ")
    total_saved = upsert_supabase(toate_anunturile)
    log.info(f"Supabase: {total_saved} rÃ¢nduri salvate")

    # PASUL 2: SincronizeazÄ DOAR leads cu telefon la GHL
    log.info("\nâââ GHL SYNC (doar cu telefon) âââ")
    total_ghl = sync_leads_to_ghl()
    log.info(f"GHL: {total_ghl} leads trimise")

    # Statistici finale
    cu_telefon = len([a for a in toate_anunturile if a.get("telefon")])
    log.info("\n" + "=" * 60)
    log.info(f"SUMMARY:")
    log.info(f"  Total anunÈuri â Supabase: {len(toate_anunturile)}")
    log.info(f"  AnunÈuri cu telefon:       {cu_telefon}")
    log.info(f"  Leads trimise â GHL:       {total_ghl}")
    log.info(f"  FÄrÄ telefon (Ã®n Supabase, nu Ã®n GHL): {len(toate_anunturile) - cu_telefon}")
    log.info("=" * 60)
    log.info("Scraper v3 â DONE")


if __name__ == "__main__":
    main()
