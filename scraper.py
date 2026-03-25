#!/usr/bin/env python3
"""
RoImobiliare - OLX Sibiu → GHL
Scrapeaza OLX imobiliare Sibiu si creeaza contacte direct in GoHighLevel.
Fiecare contact = un anunt cu link direct spre anunt.
"""

import os, re, hashlib, time, logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

GHL_API_KEY  = os.environ['GHL_API_KEY']
GHL_LOCATION = 'AojtIWqW6PK1qoRK1zLm'
GHL_API_URL  = 'https://services.leadconnectorhq.com'

HEADERS_SCRAPER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9',
}

HEADERS_GHL = {
    'Authorization': f'Bearer {GHL_API_KEY}',
    'Content-Type': 'application/json',
    'Version': '2021-07-28',
}

SEEN_FILE = '/tmp/olx_seen.txt'


def load_seen():
    try:
        return set(open(SEEN_FILE).read().splitlines())
    except:
        return set()

def save_seen(seen):
    open(SEEN_FILE, 'w').write('\n'.join(seen))


def parse_price(text):
    if not text: return None, 'EUR'
    t = text.upper().replace('.','').replace(' ','').replace('\xa0','')
    cur = 'RON' if 'RON' in t or 'LEI' in t else 'EUR'
    nums = re.findall(r'[0-9]+', t)
    p = int(''.join(nums[:2])) if nums else None
    return p, cur


def scrape_olx_page(url):
    """Returneaza lista de anunturi de pe o pagina OLX."""
    try:
        r = requests.get(url, headers=HEADERS_SCRAPER, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log.warning(f'OLX GET failed: {e}')
        return []

    soup = BeautifulSoup(r.text, 'lxml')
    cards = soup.select('[data-cy="l-card"]')
    listings = []

    for card in cards:
        try:
            a = card.select_one('a[href*="/d/"]')
            if not a: continue
            href = a['href']
            if not href.startswith('http'):
                href = 'https://www.olx.ro' + href
            href = href.split('?')[0]  # curata parametrii

            # ID anunt
            m = re.search(r'ID([A-Za-z0-9]+)\.html', href)
            listing_id = m.group(1) if m else href.split('/')[-1]

            # Titlu
            title_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
            title = title_el.get_text(strip=True) if title_el else 'Anunt OLX Sibiu'

            # Pret
            price_el = card.select_one('[data-testid="ad-price"]')
            price_text = price_el.get_text(strip=True) if price_el else ''
            price, currency = parse_price(price_text)

            # Locatie
            loc_el = card.select_one('[data-testid="location-date"]')
            location = loc_el.get_text(strip=True) if loc_el else 'Sibiu'

            listings.append({
                'id': listing_id,
                'title': title,
                'url': href,
                'price': price,
                'currency': currency,
                'price_text': price_text,
                'location': location,
            })
        except Exception as e:
            log.warning(f'Card parse error: {e}')

    return listings


def create_ghl_contact(listing):
    """Creeaza contact in GHL cu datele anuntului."""
    price_str = f"{listing['price']:,} {listing['currency']}" if listing['price'] else 'Pret negociabil'
    
    # Folosim email fals unic pentru deduplicare (GHL necesita email sau telefon)
    fake_email = f"olx-{listing['id']}@leads.roimobiliare.ro"

    payload = {
        'locationId': GHL_LOCATION,
        'firstName': listing['title'][:50],  # primii 50 chars din titlu
        'lastName': f"[OLX] {price_str}",
        'email': fake_email,
        'website': listing['url'],           # link direct spre anunt
        'source': 'OLX Scraper',
        'tags': ['scraper', 'olx', 'sibiu', 'de-sunat'],
        'customFields': [
            {'key': 'anunt_url', 'field_value': listing['url']},
            {'key': 'pret', 'field_value': price_str},
            {'key': 'locatie', 'field_value': listing['location']},
            {'key': 'sursa', 'field_value': 'OLX'},
        ],
        'notes': f"Anunt OLX: {listing['title']}\nPret: {price_str}\nLocatie: {listing['location']}\nLink: {listing['url']}",
    }

    try:
        r = requests.post(
            f'{GHL_API_URL}/contacts/',
            headers=HEADERS_GHL,
            json=payload,
            timeout=15
        )
        if r.status_code in (200, 201):
            contact_id = r.json().get('contact', {}).get('id', '?')
            log.info(f'✅ GHL contact creat: {listing["title"][:40]} | {price_str} | ID: {contact_id}')
            return True
        elif r.status_code == 422:
            # Contact deja exista (email duplicat) - OK
            log.info(f'⏭️  Deja exista: {listing["title"][:40]}')
            return False
        else:
            log.error(f'❌ GHL error {r.status_code}: {r.text[:200]}')
            return False
    except Exception as e:
        log.error(f'❌ GHL request failed: {e}')
        return False


def run():
    seen = load_seen()
    total_new = 0
    total_found = 0

    cats = [
        ('apartamente-garsoniere-de-vanzare', 'apartament'),
        ('case-de-vanzare', 'casa'),
        ('terenuri-de-vanzare', 'teren'),
    ]

    for slug, ptype in cats:
        for page in range(1, 6):  # max 5 pagini per categorie
            url = f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}'
            log.info(f'Scraping: {url}')

            listings = scrape_olx_page(url)
            if not listings:
                log.info(f'Nicio listare pe pagina {page}, stop.')
                break

            total_found += len(listings)
            new_on_page = 0

            for listing in listings:
                lid = listing['id']
                if lid in seen:
                    continue  # deja procesat

                # Creeaza contact in GHL
                success = create_ghl_contact(listing)
                if success:
                    seen.add(lid)
                    total_new += 1
                    new_on_page += 1

                time.sleep(0.5)  # pauza intre contacte

            log.info(f'{slug} p{page}: {len(listings)} anunturi, {new_on_page} noi')
            save_seen(seen)
            time.sleep(2)  # pauza intre pagini

    log.info(f'=== DONE: {total_found} anunturi gasite, {total_new} contacte noi in GHL ===')


if __name__ == '__main__':
    run()
