#!/usr/bin/env python3
"""RoImobiliare OLX -> GHL v5 - toate campurile custom populate"""

import os, re, sys, time, logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
log = logging.getLogger(__name__)

GHL_API_KEY  = os.environ['GHL_API_KEY']
APIFY_TOKEN  = os.environ.get('APIFY_TOKEN', '')
GHL_LOCATION = 'AojtIWqW6PK1qoRK1zLm'
GHL_URL      = 'https://services.leadconnectorhq.com'

HEADERS_GHL = {
    'Authorization': f'Bearer {GHL_API_KEY}',
    'Content-Type': 'application/json',
    'Version': '2021-07-28',
}

# Custom field keys FARA prefixul "contact." - acesta e formatul corect GHL
CF = {
    'titlu':          'titlu_anunt',
    'pret':           'pret_vanzare_cerut',
    'link':           'link_publicare',
    'nr_camere':      'nr_camere',
    'suprafata':      'suprafata',
    'oras':           'oras',
    'compartimentare':'compartimentare',
    'an_constructie': 'an_constructie',
    'etaj':           'etaj',
    'tip_vanzator':   'tip_vanzator',
}

def get_proxies():
    if not APIFY_TOKEN: return None
    p = f'http://groups-RESIDENTIAL,country-RO:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': p, 'https': p}

def extract_olx_id(href):
    m = re.search(r'-(ID[A-Za-z0-9]+)\.html', href)
    return m.group(1) if m else href.rstrip('/').split('/')[-1].replace('.html','')

def fetch_listing_details(href, proxies, headers):
    """Viziteaza pagina individuala OLX si extrage toate detaliile."""
    details = {'link': href}
    try:
        r = requests.get(href, headers=headers, proxies=proxies,
                         timeout=25, verify=False if proxies else True)
        soup = BeautifulSoup(r.text, 'lxml')
        body = soup.get_text()

        # Parametri structurati din body text
        param_map = {
            'Compartimentare':   'compartimentare',
            'Compartimentare:':  'compartimentare',
            'Suprafata utila':   'suprafata',
            'Suprafata utila:':  'suprafata',
            'Suprafata:':        'suprafata',
            'An constructie':    'an_constructie',
            'An constructie:':   'an_constructie',
            'Etaj:':             'etaj',
            'Etaj':              'etaj',
        }

        for label, field in param_map.items():
            if field in details: continue
            idx = body.find(label)
            if idx > 0:
                snippet = body[idx + len(label):idx + len(label) + 50].strip()
                val = snippet.split('\n')[0].strip().rstrip('.')
                if val: details[field] = val

        # Tip vanzator
        if 'Persoana fizica' in body:
            details['tip_vanzator'] = 'Persoana fizica'
        elif 'Agentie' in body or 'agentie' in body:
            details['tip_vanzator'] = 'Agentie imobiliara'
        elif 'Dezvoltator' in body:
            details['tip_vanzator'] = 'Dezvoltator'

        # Oras din breadcrumb sau location
        loc = soup.select_one('[data-testid="ad-contact-location"], [class*="location"]')
        if loc:
            details['oras'] = loc.get_text(strip=True).split(',')[0].strip()

    except Exception as e:
        log.warning(f'details fetch failed: {e}')
    return details

def create_ghl_contact(listing):
    lid   = listing['id']
    title = listing['title']
    price = listing['price_str']

    # Costruieste lista de custom fields - doar cele cu valoare
    cf_list = []
    for field_key, cf_key in CF.items():
        val = listing.get(field_key, '')
        if val:
            cf_list.append({'key': cf_key, 'field_value': str(val)})

    payload = {
        'locationId': GHL_LOCATION,
        'firstName': title[:50],
        'lastName': f'[OLX] {price}',
        'email': f'olx-{lid}@leads.roimobiliare.ro',
        'source': 'OLX Scraper',
        'tags': ['scraper', 'olx', 'sibiu', 'de-sunat'],
        'customFields': cf_list,
    }

    r = requests.post(f'{GHL_URL}/contacts/', headers=HEADERS_GHL, json=payload, timeout=15)
    if r.status_code in (200, 201):
        cid = r.json().get('contact', {}).get('id', '?')
        log.info(f'OK {cid} | {title[:35]} | {price} | cf={len(cf_list)}')
        return True
    elif r.status_code == 422:
        log.info(f'SKIP exista: {lid}')
        return False
    else:
        log.error(f'GHL {r.status_code}: {r.text[:200]}')
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
                log.error(f'GET failed: {e}'); break

            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            log.info(f'  {len(cards)} anunturi')
            if not cards: break

            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a: continue
                    href = a['href']
                    if not href.startswith('http'): href = 'https://www.olx.ro' + href
                    href = href.split('?')[0]

                    lid = extract_olx_id(href)
                    if lid in seen: continue
                    seen.add(lid)

                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                    title = title_el.get_text(strip=True) if title_el else 'Anunt OLX Sibiu'

                    price_el = card.select_one('[data-testid="ad-price"]')
                    price_text = price_el.get_text(strip=True) if price_el else ''
                    nums = re.findall(r'[0-9]+', price_text.replace('.','').replace(' ',''))
                    price_val = int(''.join(nums[:2])) if nums else 0
                    cur = 'RON' if 'RON' in price_text.upper() or 'LEI' in price_text.upper() else 'EUR'
                    price_str = f'{price_val:,} {cur}' if price_val else 'Pret negociabil'

                    loc_el = card.select_one('[data-testid="location-date"]')
                    oras = (loc_el.get_text(strip=True).split(',')[0].strip()
                            if loc_el else 'Sibiu')

                    # Fetch detalii din pagina individuala
                    time.sleep(0.5)
                    details = fetch_listing_details(href, proxies, headers)

                    listing = {
                        'id': lid,
                        'title': title,
                        'price_str': price_str,
                        'titlu': title,
                        'pret': price_str,
                        'link': href,
                        'oras': details.get('oras', oras),
                        'nr_camere': details.get('nr_camere', ''),
                        'suprafata': details.get('suprafata', ''),
                        'compartimentare': details.get('compartimentare', ''),
                        'an_constructie': details.get('an_constructie', ''),
                        'etaj': details.get('etaj', ''),
                        'tip_vanzator': details.get('tip_vanzator', ''),
                    }

                    if create_ghl_contact(listing):
                        total_new += 1

                    time.sleep(0.5)
                except Exception as e:
                    log.error(f'Card: {e}')

            time.sleep(2)

    log.info(f'=== DONE: {total_new} contacte noi in GHL ===')

if __name__ == '__main__':
    import urllib3; urllib3.disable_warnings()
    log.info(f'Proxy: {"APIFY" if APIFY_TOKEN else "OFF"}')
    scrape_olx()
