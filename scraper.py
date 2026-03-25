#!/usr/bin/env python3
"""RoImobiliare OLX -> GHL - v3 fix email + website"""

import os, re, sys, time, logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
log = logging.getLogger(__name__)

GHL_API_KEY  = os.environ['GHL_API_KEY']
APIFY_TOKEN  = os.environ.get('APIFY_TOKEN', '')
GHL_LOCATION = 'AojtIWqW6PK1qoRK1zLm'

HEADERS_GHL = {
    'Authorization': f'Bearer {GHL_API_KEY}',
    'Content-Type': 'application/json',
    'Version': '2021-07-28',
}

def get_proxies():
    if not APIFY_TOKEN:
        return None
    proxy = f'http://groups-RESIDENTIAL,country-RO:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': proxy, 'https': proxy}

def extract_olx_id(href):
    """Extrage ID-ul numeric/alfanumeric din URL OLX.
    Ex: .../anunt-IDkjNTh.html -> IDkjNTh
        .../anunt-ID123abc.html -> ID123abc
    """
    m = re.search(r'-(ID[A-Za-z0-9]+)\.html', href)
    if m:
        return m.group(1)
    # fallback: ultimul segment fara .html
    return href.rstrip('/').split('/')[-1].replace('.html', '')

def create_ghl_contact(listing):
    lid = listing['id']
    price_str = listing['price_str']
    href = listing['url']
    title = listing['title']

    # Email placeholder unic si curat - fara caractere speciale
    fake_email = f"olx-{lid}@leads.roimobiliare.ro"

    payload = {
        'locationId': GHL_LOCATION,
        'firstName': title[:50],
        'lastName': f'[OLX] {price_str}',
        'email': fake_email,
        'website': href,
        'source': 'OLX Scraper',
        'tags': ['scraper', 'olx', 'sibiu', 'de-sunat'],
    }

    r = requests.post(
        'https://services.leadconnectorhq.com/contacts/',
        headers=HEADERS_GHL, json=payload, timeout=15
    )

    if r.status_code in (200, 201):
        cid = r.json().get('contact', {}).get('id', '?')
        log.info(f'OK GHL id={cid} | {title[:35]} | {price_str}')
        return True
    elif r.status_code == 422:
        log.info(f'SKIP (exista deja): {lid}')
        return False
    else:
        log.error(f'GHL ERROR {r.status_code}: {r.text[:300]}')
        return False

def scrape_olx():
    proxies = get_proxies()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'ro-RO,ro;q=0.9',
    }

    cats = [
        ('apartamente-garsoniere-de-vanzare', 'apartament'),
        ('case-de-vanzare', 'casa'),
        ('terenuri-de-vanzare', 'teren'),
    ]

    seen = set()
    total_new = 0

    for slug, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}'
            log.info(f'Scraping: {url}')

            try:
                r = requests.get(url, headers=headers, proxies=proxies,
                                 timeout=30, verify=False if proxies else True)
                r.raise_for_status()
            except Exception as e:
                log.error(f'GET failed: {e}')
                break

            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            log.info(f'  {len(cards)} cards found')

            if not cards:
                break

            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a: continue

                    href = a['href']
                    if not href.startswith('http'):
                        href = 'https://www.olx.ro' + href
                    href = href.split('?')[0]

                    # ID corect din URL
                    lid = extract_olx_id(href)
                    if lid in seen:
                        continue
                    seen.add(lid)

                    # Titlu
                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                    title = title_el.get_text(strip=True) if title_el else 'Anunt OLX Sibiu'

                    # Pret
                    price_el = card.select_one('[data-testid="ad-price"]')
                    price_text = price_el.get_text(strip=True) if price_el else ''
                    nums = re.findall(r'[0-9]+', price_text.replace('.', '').replace(' ', ''))
                    price_val = int(''.join(nums[:2])) if nums else 0
                    cur = 'RON' if 'RON' in price_text.upper() or 'LEI' in price_text.upper() else 'EUR'
                    price_str = f'{price_val:,} {cur}' if price_val else 'Pret negociabil'

                    listing = {
                        'id': lid,
                        'title': title,
                        'url': href,
                        'price_str': price_str,
                        'type': ptype,
                    }

                    if create_ghl_contact(listing):
                        total_new += 1

                    time.sleep(0.3)

                except Exception as e:
                    log.error(f'Card error: {e}')

            time.sleep(2)

    log.info(f'=== DONE: {total_new} contacte noi in GHL ===')


if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings()
    log.info(f'Apify proxy: {"ON" if APIFY_TOKEN else "OFF"}')
    scrape_olx()
