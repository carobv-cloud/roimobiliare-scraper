#!/usr/bin/env python3
"""
RoImobiliare OLX -> GHL v6
- Foloseste OLX API intern pentru date complete: descriere, telefon, parametri
- Toate campurile custom populate corect
"""

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

# Custom field keys - FARA prefixul "contact."
CF = {
    'titlu':           'titlu_anunt',
    'pret':            'pret_vanzare_cerut',
    'link':            'link_publicare',
    'nr_camere':       'nr_camere',
    'suprafata':       'suprafata',
    'oras':            'oras',
    'compartimentare': 'compartimentare',
    'an_constructie':  'an_constructie',
    'etaj':            'etaj',
    'tip_vanzator':    'tip_vanzator',
    'descriere':       'descriere_anunt',
    'telefon':         'telefon_vanzator',
}

HEADERS_OLX = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'ro-RO,ro;q=0.9',
    'Referer': 'https://www.olx.ro/',
}

def get_proxies():
    if not APIFY_TOKEN: return None
    p = f'http://groups-RESIDENTIAL,country-RO:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': p, 'https': p}

def extract_olx_id(href):
    """Extrage ID-ul alfanumeric din URL: casa-IDjGsKu.html -> IDjGsKu"""
    m = re.search(r'-(ID[A-Za-z0-9]+)\\.html', href)
    return m.group(1) if m else href.rstrip('/').split('/')[-1].replace('.html', '')

def extract_numeric_id(html_text):
    """Extrage ad_id numeric din HTML-ul paginii (folosit pentru API OLX)."""
    # Cauta in parametrii de reclama sau in script tags
    m = re.search(r'"ad_id"[:\s]+"?(\\d{7,12})"?', html_text)
    if m: return m.group(1)
    m = re.search(r'ad_id=(\\d{7,12})', html_text)
    if m: return m.group(1)
    m = re.search(r'"id":(\\d{7,12})', html_text)
    if m: return m.group(1)
    return None

def fetch_olx_api(numeric_id, proxies):
    """Apeleaza OLX API pentru detalii complete + telefon."""
    result = {}
    if not numeric_id:
        return result

    # 1. Detalii complete anunt
    try:
        r = requests.get(
            f'https://www.olx.ro/api/v1/offers/{numeric_id}/',
            headers=HEADERS_OLX, proxies=proxies,
            timeout=20, verify=False if proxies else True
        )
        if r.status_code == 200:
            data = r.json().get('data', {})

            # Descriere
            result['descriere'] = data.get('description', '')

            # Parametri structurati
            params = data.get('params', [])
            param_map = {
                'rooms':          'nr_camere',
                'm':              'suprafata',
                'builttype':      'compartimentare',
                'built_year':     'an_constructie',
                'floor_select':   'etaj',
            }
            for p in params:
                key = p.get('key', '')
                val = p.get('value', {})
                label = val.get('label') or val.get('key', '')
                if key in param_map and label:
                    result[param_map[key]] = label

            # Oras
            loc = data.get('location', {})
            if loc.get('city', {}).get('name'):
                result['oras'] = loc['city']['name']

            # Tip vanzator
            user = data.get('user', {})
            result['tip_vanzator'] = 'Agentie imobiliara' if user.get('is_business') else 'Persoana fizica'
            result['nume_vanzator'] = user.get('name', '')

            log.info(f'  API OK: {len(params)} params, descriere {len(result.get("descriere",""))} chars')
    except Exception as e:
        log.warning(f'  API details error: {e}')

    # 2. Telefon
    try:
        r2 = requests.get(
            f'https://www.olx.ro/api/v1/offers/{numeric_id}/limited-phones/',
            headers=HEADERS_OLX, proxies=proxies,
            timeout=20, verify=False if proxies else True
        )
        if r2.status_code == 200:
            phones = r2.json()
            # Format: {"data": [{"phone": "0733060088"}]} sau similar
            data2 = phones.get('data', phones)
            if isinstance(data2, list) and data2:
                result['telefon'] = data2[0].get('phone', '')
            elif isinstance(data2, dict):
                result['telefon'] = data2.get('phone', '')
            if result.get('telefon'):
                log.info(f'  Telefon gasit: {result["telefon"]}')
        else:
            log.info(f'  Telefon API status: {r2.status_code}')
    except Exception as e:
        log.warning(f'  API phone error: {e}')

    return result

def create_ghl_contact(listing):
    lid   = listing['id']
    title = listing['title']
    price = listing['price_str']

    cf_list = []
    for field_key, cf_key in CF.items():
        val = str(listing.get(field_key, '')).strip()
        if val:
            cf_list.append({'key': cf_key, 'field_value': val})

    # Daca avem telefon real, il punem si in campul standard Phone
    phone = listing.get('telefon', '')

    payload = {
        'locationId': GHL_LOCATION,
        'firstName': title[:50],
        'lastName': f'[OLX] {price}',
        'email': f'olx-{lid}@leads.roimobiliare.ro',
        'source': 'OLX Scraper',
        'tags': ['scraper', 'olx', 'sibiu', 'de-sunat'],
        'customFields': cf_list,
    }
    if phone:
        payload['phone'] = phone

    r = requests.post(f'{GHL_URL}/contacts/', headers=HEADERS_GHL, json=payload, timeout=15)
    if r.status_code in (200, 201):
        cid = r.json().get('contact', {}).get('id', '?')
        log.info(f'OK {cid} | {title[:35]} | {price} | cf={len(cf_list)} | tel={"DA" if phone else "NU"}')
        return True
    elif r.status_code == 422:
        log.info(f'SKIP exista: {lid}')
        return False
    else:
        log.error(f'GHL {r.status_code}: {r.text[:200]}')
        return False

def scrape_olx():
    proxies = get_proxies()
    headers_html = {
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
                r = requests.get(url, headers=headers_html, proxies=proxies,
                                 timeout=30, verify=False if proxies else True)
                r.raise_for_status()
            except Exception as e:
                log.error(f'GET failed: {e}'); break

            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            log.info(f'  {len(cards)} anunturi gasite')
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
                    oras = loc_el.get_text(strip=True).split(',')[0].strip() if loc_el else 'Sibiu'

                    # Fetch pagina individuala pentru ad_id numeric
                    time.sleep(0.5)
                    try:
                        rp = requests.get(href, headers=headers_html, proxies=proxies,
                                          timeout=25, verify=False if proxies else True)
                        numeric_id = extract_numeric_id(rp.text)
                    except Exception:
                        numeric_id = None

                    # Fetch OLX API cu ad_id numeric
                    api_data = fetch_olx_api(numeric_id, proxies) if numeric_id else {}

                    listing = {
                        'id': lid,
                        'title': title,
                        'price_str': price_str,
                        # Campuri custom
                        'titlu': title,
                        'pret': price_str,
                        'link': href,
                        'oras': api_data.get('oras', oras),
                        'nr_camere': api_data.get('nr_camere', ''),
                        'suprafata': api_data.get('suprafata', ''),
                        'compartimentare': api_data.get('compartimentare', ''),
                        'an_constructie': api_data.get('an_constructie', ''),
                        'etaj': api_data.get('etaj', ''),
                        'tip_vanzator': api_data.get('tip_vanzator', ''),
                        'descriere': api_data.get('descriere', '')[:2000],  # max 2000 chars
                        'telefon': api_data.get('telefon', ''),
                    }

                    if create_ghl_contact(listing):
                        total_new += 1

                    time.sleep(0.5)
                except Exception as e:
                    log.error(f'Card error: {e}')

            time.sleep(2)

    log.info(f'=== DONE: {total_new} contacte noi in GHL ===')

if __name__ == '__main__':
    import urllib3; urllib3.disable_warnings()
    log.info(f'Proxy: {"APIFY" if APIFY_TOKEN else "OFF"}')
    scrape_olx()
