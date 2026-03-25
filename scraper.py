#!/usr/bin/env python3
"""
RoImobiliare OLX -> GHL v7
- Regex corecte (fara escape issues)
- Descriere extrasa din HTML direct
- Telefon din OLX API v2 (fara auth) sau din HTML
- Toti parametrii din OLX API
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

HEADERS_SCRAPER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

HEADERS_API = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'ro-RO,ro;q=0.9',
    'Referer': 'https://www.olx.ro/',
    'Origin': 'https://www.olx.ro',
}

def get_proxies():
    if not APIFY_TOKEN:
        return None
    p = f'http://groups-RESIDENTIAL,country-RO:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': p, 'https': p}

def extract_olx_id(href):
    """Extrage ID alfanumeric: casa-IDjGsKu.html -> IDjGsKu"""
    m = re.search(r'-(ID[A-Za-z0-9]+)\.html', href)
    return m.group(1) if m else href.rstrip('/').split('/')[-1].replace('.html', '')

def extract_numeric_id(html_text):
    """Extrage ad_id numeric din HTML: 290870674"""
    # Metoda 1: din parametrii de reclama Google
    m = re.search(r'ad_id[=:]["\s]*([0-9]{6,12})', html_text)
    if m:
        return m.group(1)
    # Metoda 2: din JSON embedded
    m = re.search(r'"id"\s*:\s*([0-9]{6,12})', html_text)
    if m:
        return m.group(1)
    # Metoda 3: orice numar de 8-10 cifre in context de oferta
    m = re.search(r'offers[/=]([0-9]{6,12})', html_text)
    if m:
        return m.group(1)
    return None

def fetch_details(href, proxies):
    """Viziteaza pagina individuala si extrage date din HTML + OLX API."""
    result = {}
    
    try:
        r = requests.get(href, headers=HEADERS_SCRAPER, proxies=proxies,
                        timeout=25, verify=False if proxies else True)
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        body_text = soup.get_text(separator='\n')

        # Extrage descrierea din HTML (OLX o randeaza server-side)
        # Cauta sectiunea de descriere
        desc = ''
        for selector in ['[data-cy="ad-description"]', '[class*="description"]', 
                         'section[class*="Description"]']:
            el = soup.select_one(selector)
            if el and len(el.get_text(strip=True)) > 30:
                desc = el.get_text(separator=' ', strip=True)
                break
        
        # Fallback: extrage din body text intre "DESCRIERE" si urmatoarea sectiune
        if not desc:
            m = re.search(r'DESCRIERE\s*\n(.+?)(?:\n[A-Z]{3,}|\Z)', body_text, re.DOTALL)
            if m:
                desc = m.group(1).strip()[:2000]

        if desc:
            result['descriere'] = desc[:2000]
            log.info(f'  Descriere: {len(desc)} chars')

        # Extrage parametrii din body text
        param_patterns = {
            'Compartimentare': 'compartimentare',
            'Suprafata utila':  'suprafata',
            'Suprafata':        'suprafata',
            'An constructie':   'an_constructie',
            'Etaj':             'etaj',
        }
        for label, field in param_patterns.items():
            if field in result:
                continue
            idx = body_text.find(label + ':')
            if idx == -1:
                idx = body_text.find(label + '\n')
            if idx > 0:
                snippet = body_text[idx + len(label) + 1:idx + len(label) + 60].strip()
                val = snippet.split('\n')[0].strip().strip(':').strip()
                if val and len(val) < 50:
                    result[field] = val

        # Tip vanzator
        if 'Persoana fizica' in body_text or 'PRIVAT' in body_text.upper():
            result['tip_vanzator'] = 'Persoana fizica'
        elif 'Agentie' in body_text or 'PROFESIONIST' in body_text.upper():
            result['tip_vanzator'] = 'Agentie imobiliara'

        # Oras
        loc_el = soup.select_one('[class*="location"], [data-testid*="location"]')
        if loc_el:
            result['oras'] = loc_el.get_text(strip=True).split(',')[0].strip()

        # Extrage numeric ID pentru API calls
        numeric_id = extract_numeric_id(html)
        if not numeric_id:
            log.warning(f'  Nu am gasit numeric_id in {href}')
            return result

        log.info(f'  numeric_id: {numeric_id}')

        # OLX API - date complete
        try:
            api_r = requests.get(
                f'https://www.olx.ro/api/v1/offers/{numeric_id}/',
                headers=HEADERS_API, proxies=proxies,
                timeout=20, verify=False if proxies else True
            )
            if api_r.status_code == 200:
                data = api_r.json().get('data', {})

                # Suprascrie descrierea cu cea din API daca e mai buna
                api_desc = data.get('description', '')
                if api_desc and len(api_desc) > len(result.get('descriere', '')):
                    result['descriere'] = api_desc[:2000]

                # Parametri structurati din API
                for p in data.get('params', []):
                    key = p.get('key', '')
                    val = p.get('value', {})
                    label = val.get('label') or str(val.get('key', ''))
                    if not label:
                        continue
                    if key == 'rooms':
                        result['nr_camere'] = label
                    elif key == 'm':
                        result['suprafata'] = label
                    elif key == 'builttype':
                        result['compartimentare'] = label
                    elif key == 'built_year':
                        result['an_constructie'] = label
                    elif key == 'floor_select':
                        result['etaj'] = label

                # Oras din API
                loc = data.get('location', {})
                city = loc.get('city', {}).get('name', '')
                if city:
                    result['oras'] = city

                # Tip vanzator din API
                user = data.get('user', {})
                if user.get('is_business'):
                    result['tip_vanzator'] = 'Agentie imobiliara'
                else:
                    result['tip_vanzator'] = 'Persoana fizica'

                log.info(f'  API OK: {len(data.get("params",[]))} params')
            else:
                log.warning(f'  OLX API status: {api_r.status_code}')

        except Exception as e:
            log.warning(f'  OLX API error: {e}')

        # Telefon din OLX API limited-phones
        try:
            ph_r = requests.get(
                f'https://www.olx.ro/api/v1/offers/{numeric_id}/limited-phones/',
                headers=HEADERS_API, proxies=proxies,
                timeout=20, verify=False if proxies else True
            )
            log.info(f'  Phone API status: {ph_r.status_code}')
            if ph_r.status_code == 200:
                ph_data = ph_r.json()
                # Format posibil: {"data": [{"phone": "0733060088"}]}
                phones_list = ph_data.get('data', [])
                if isinstance(phones_list, list) and phones_list:
                    phone = phones_list[0].get('phone', '')
                    if phone:
                        result['telefon'] = phone
                        log.info(f'  Telefon: {phone}')
                elif isinstance(phones_list, dict):
                    phone = phones_list.get('phone', '')
                    if phone:
                        result['telefon'] = phone
                        log.info(f'  Telefon: {phone}')
            elif ph_r.status_code == 403:
                log.info('  Phone API 403 - necesita autentificare')
                # Fallback: cauta in HTML
                phone_m = re.search(r'0[0-9]{2,3}[\s\-]?[0-9]{3}[\s\-]?[0-9]{3,4}', body_text)
                if phone_m:
                    result['telefon'] = phone_m.group(0)
                    log.info(f'  Telefon din HTML: {result["telefon"]}')
        except Exception as e:
            log.warning(f'  Phone error: {e}')

    except Exception as e:
        log.error(f'  fetch_details error: {e}')

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

    phone = listing.get('telefon', '').replace(' ', '').replace('-', '')

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
        log.info(f'GHL OK {cid} | {title[:35]} | {price} | cf={len(cf_list)} | tel={"DA" if phone else "NU"}')
        return True
    elif r.status_code == 422:
        log.info(f'SKIP exista: {lid}')
        return False
    else:
        log.error(f'GHL {r.status_code}: {r.text[:200]}')
        return False

def scrape_olx():
    proxies = get_proxies()
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
            log.info(f'--- Scraping: {url}')
            try:
                r = requests.get(url, headers=HEADERS_SCRAPER, proxies=proxies,
                                 timeout=30, verify=False if proxies else True)
                r.raise_for_status()
            except Exception as e:
                log.error(f'GET failed: {e}')
                break

            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            log.info(f'  {len(cards)} anunturi gasite')
            if not cards:
                break

            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a:
                        continue
                    href = a['href']
                    if not href.startswith('http'):
                        href = 'https://www.olx.ro' + href
                    href = href.split('?')[0]

                    lid = extract_olx_id(href)
                    if lid in seen:
                        continue
                    seen.add(lid)

                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h6, h4')
                    title = title_el.get_text(strip=True) if title_el else 'Anunt OLX Sibiu'

                    price_el = card.select_one('[data-testid="ad-price"]')
                    price_text = price_el.get_text(strip=True) if price_el else ''
                    nums = re.findall(r'[0-9]+', price_text.replace('.', '').replace(' ', ''))
                    price_val = int(''.join(nums[:2])) if nums else 0
                    cur = 'RON' if 'RON' in price_text.upper() or 'LEI' in price_text.upper() else 'EUR'
                    price_str = f'{price_val:,} {cur}' if price_val else 'Pret negociabil'

                    loc_el = card.select_one('[data-testid="location-date"]')
                    oras = loc_el.get_text(strip=True).split(',')[0].strip() if loc_el else 'Sibiu'

                    log.info(f'  Processing: {title[:40]} | {price_str}')
                    time.sleep(0.5)

                    details = fetch_details(href, proxies)

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
                        'descriere': details.get('descriere', ''),
                        'telefon': details.get('telefon', ''),
                    }

                    if create_ghl_contact(listing):
                        total_new += 1

                    time.sleep(0.5)

                except Exception as e:
                    log.error(f'Card error: {e}')

            time.sleep(2)

    log.info(f'=== DONE: {total_new} contacte noi in GHL ===')


if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings()
    log.info(f'Proxy: {"APIFY ON" if APIFY_TOKEN else "OFF - fara proxy"}')
    scrape_olx()
