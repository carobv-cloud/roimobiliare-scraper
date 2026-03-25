#!/usr/bin/env python3
"""RoImobiliare OLX -> GHL cu Apify proxy + debug complet"""

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
        log.warning('No APIFY_TOKEN - running without proxy')
        return None
    proxy = f'http://groups-RESIDENTIAL,country-RO:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': proxy, 'https': proxy}

def test_ghl():
    """Verifica ca GHL API functioneaza."""
    r = requests.post(
        'https://services.leadconnectorhq.com/contacts/',
        headers=HEADERS_GHL,
        json={
            'locationId': GHL_LOCATION,
            'firstName': 'API TEST',
            'lastName': 'Scraper OK',
            'email': f'api-test-{int(time.time())}@leads.roimobiliare.ro',
        },
        timeout=15
    )
    log.info(f'GHL test: status={r.status_code} response={r.text[:200]}')
    return r.status_code in (200, 201)

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
        for page in range(1, 4):
            url = f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}'
            log.info(f'GET {url}')
            try:
                r = requests.get(url, headers=headers, proxies=proxies,
                                 timeout=30, verify=False if proxies else True)
                log.info(f'  status={r.status_code} len={len(r.text)}')
            except Exception as e:
                log.error(f'  FAILED: {e}')
                break

            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            log.info(f'  cards found: {len(cards)}')

            if not cards:
                # Debug: arata primii 500 chars din HTML
                log.warning(f'  HTML preview: {r.text[:500]}')
                break

            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a: continue
                    href = a['href']
                    if not href.startswith('http'):
                        href = 'https://www.olx.ro' + href
                    href = href.split('?')[0]

                    m = re.search(r'ID([A-Za-z0-9]+)\\.html', href)
                    lid = m.group(1) if m else href.split('/')[-1]
                    if lid in seen: continue
                    seen.add(lid)

                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                    title = title_el.get_text(strip=True) if title_el else 'Anunt OLX'

                    price_el = card.select_one('[data-testid="ad-price"]')
                    price_text = price_el.get_text(strip=True) if price_el else ''
                    nums = re.findall(r'[0-9]+', price_text.replace('.','').replace(' ',''))
                    price_val = int(''.join(nums[:2])) if nums else 0
                    cur = 'RON' if 'RON' in price_text.upper() or 'LEI' in price_text.upper() else 'EUR'
                    price_str = f'{price_val:,} {cur}' if price_val else 'Pret negociabil'

                    loc_el = card.select_one('[data-testid="location-date"]')
                    loc = loc_el.get_text(strip=True) if loc_el else 'Sibiu'

                    log.info(f'  -> {title[:40]} | {price_str}')

                    # Creeaza contact in GHL
                    payload = {
                        'locationId': GHL_LOCATION,
                        'firstName': title[:50],
                        'lastName': f'[OLX] {price_str}',
                        'email': f'olx-{lid}@leads.roimobiliare.ro',
                        'website': href,
                        'source': 'OLX Scraper',
                        'tags': ['scraper', 'olx', 'sibiu', 'de-sunat'],
                    }
                    gr = requests.post(
                        'https://services.leadconnectorhq.com/contacts/',
                        headers=HEADERS_GHL, json=payload, timeout=15
                    )
                    if gr.status_code in (200, 201):
                        cid = gr.json().get('contact', {}).get('id', '?')
                        log.info(f'  OK GHL id={cid}')
                        total_new += 1
                    elif gr.status_code == 422:
                        log.info(f'  SKIP deja exista')
                    else:
                        log.error(f'  GHL ERROR {gr.status_code}: {gr.text[:200]}')

                    time.sleep(0.3)
                except Exception as e:
                    log.error(f'  card error: {e}')

            time.sleep(2)

    log.info(f'=== TOTAL CONTACTE NOI IN GHL: {total_new} ===')

if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings()
    log.info(f'Proxy: {"APIFY" if APIFY_TOKEN else "NONE"}')
    log.info('Testing GHL API...')
    if test_ghl():
        log.info('GHL OK - starting scrape')
        scrape_olx()
    else:
        log.error('GHL FAILED - abort')
        sys.exit(1)
