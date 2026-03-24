#!/usr/bin/env python3
"""RoImobiliare Scraper v5.0
Selectori verificati live din browser cu IP romanesc:
- Publi24:    .article-content / h2.article-title a / .article-price
- Imobiliare: a[href*="/oferta/"] cu ID numeric din slug
- Storia:     __NEXT_DATA__.props.pageProps.data.searchAds.items
- Imoradar24: .listing-card / a[href*="/oferta/"] / data-bi-listing-price
"""

import os, re, json, hashlib, time, logging, urllib3
from datetime import datetime, timezone
from typing import Optional, Dict, List
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
APIFY_TOKEN  = os.environ.get('APIFY_TOKEN', '')

SIBIU_LOCALITIES = [
    'Sibiu','Cisnadie','Selimbar','Sura Mica','Sura Mare','Orlat','Rasinari',
    'Poplaca','Cristian','Talmaciu','Ocna Sibiului','Miercurea Sibiului',
    'Saliste','Avrig','Medias','Agnita','Sibiel','Aciliu','Axente Sever',
    'Bazna','Biertan','Boita','Brateiu','Carta','Chirpar','Darlos',
    'Hoghilag','Laslea','Loamnes','Marpod','Mosna','Nocrich','Rod',
    'Rosia','Sadu','Slimnic','Turnu Rosu','Vestem','Gura Raului',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── PROXY ──────────────────────────────────────────────────────────────────────

def get_proxies(country: str = 'RO') -> Optional[dict]:
    if not APIFY_TOKEN: return None
    p = f'http://groups-RESIDENTIAL,country-{country}:{APIFY_TOKEN}@proxy.apify.com:8000'
    return {'http': p, 'https': p}

def safe_get(url: str, use_proxy: bool = True) -> Optional[requests.Response]:
    proxies = get_proxies() if use_proxy and APIFY_TOKEN else None
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, proxies=proxies, verify=not bool(proxies))
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f'GET {url[:80]}: {e}')
        return None

# ── HELPERS ────────────────────────────────────────────────────────────────────

def fp(source: str, uid: str) -> str:
    return hashlib.sha256(f'{source}:{uid}'.encode()).hexdigest()[:32]

def norm_phone(raw: str) -> Optional[str]:
    if not raw: return None
    d = re.sub(r'[^0-9]', '', raw)
    if len(d) == 11 and d.startswith('40'): return '+' + d
    if len(d) == 10 and d.startswith('0'): return '+4' + d
    if len(d) == 9: return '+40' + d
    return None

def parse_price(text: str):
    if not text: return None, 'EUR'
    t = text.upper().replace('.', '').replace(' ', '').replace('\xa0', '')
    cur = 'RON' if 'RON' in t or 'LEI' in t else 'EUR'
    nums = re.findall(r'[0-9]+', t)
    p = float(''.join(nums[:2])) if nums else None
    return p, cur

def to_eur(p, cur) -> Optional[float]:
    if not p: return None
    return round(p, 2) if cur == 'EUR' else round(p / 5.0, 2)

def detect_city(text: str) -> str:
    if not text: return 'Sibiu'
    for loc in SIBIU_LOCALITIES:
        if loc.lower() in text.lower(): return loc
    return 'Sibiu'

def upsert_contact(phone_raw) -> Optional[int]:
    phone = norm_phone(phone_raw or '')
    if not phone: return None
    try:
        r = supabase.table('contacts').upsert(
            {'phone_normalized': phone, 'phone_raw': phone_raw,
             'type': 'proprietar', 'last_seen': datetime.now(timezone.utc).isoformat()},
            on_conflict='phone_normalized'
        ).execute()
        if r.data: return r.data[0]['id']
        ex = supabase.table('contacts').select('id').eq('phone_normalized', phone).execute()
        return ex.data[0]['id'] if ex.data else None
    except Exception as e:
        log.error(f'contact: {e}'); return None

def upsert_listing(rec: Dict, run_id: int) -> bool:
    fprint = rec['fingerprint']
    now = datetime.now(timezone.utc).isoformat()
    try:
        ex = supabase.table('listings').select('id,price_eur').eq('fingerprint', fprint).execute()
        if ex.data:
            upd = {'last_seen_at': now, 'is_active': True}
            op, np_ = ex.data[0].get('price_eur'), rec.get('price_eur')
            if op and np_ and abs(float(op) - float(np_)) > 200:
                upd.update({'price_eur': np_, 'price': rec.get('price')})
            supabase.table('listings').update(upd).eq('fingerprint', fprint).execute()
            return False
        rec.update({'first_seen_at': now, 'last_seen_at': now, 'is_active': True, 'notified_ghl': False})
        supabase.table('listings').insert(rec).execute()
        return True
    except Exception as e:
        log.error(f'listing {fprint[:8]}: {e}'); return False

def start_run(source: str) -> int:
    r = supabase.table('scraper_runs').insert(
        {'source': source, 'status': 'running', 'started_at': datetime.now(timezone.utc).isoformat()}
    ).execute()
    return r.data[0]['id'] if r.data else 0

def end_run(run_id, found, new, upd, err, ok=True):
    supabase.table('scraper_runs').update({
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'listings_found': found, 'listings_new': new, 'listings_updated': upd,
        'errors': err, 'status': 'success' if ok else 'failed'
    }).eq('id', run_id).execute()

def get_phone_from_page(url: str) -> Optional[str]:
    r = safe_get(url, use_proxy=True)
    if not r: return None
    soup = BeautifulSoup(r.text, 'lxml')
    tel = soup.select_one('a[href^="tel:"]')
    if tel: return tel['href'].replace('tel:', '').strip()
    m = re.search(r'(?:\+40|40|0)[0-9]{9}', soup.get_text().replace(' ', ''))
    return m.group(0) if m else None

# ── OLX (direct, fara proxy) ───────────────────────────────────────────────────

def scrape_olx():
    log.info('=== OLX ===')
    run_id = start_run('olx')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('apartamente-garsoniere-de-vanzare', 'apartament'),
        ('case-de-vanzare', 'casa'),
        ('terenuri-de-vanzare', 'teren'),
    ]
    for slug, ptype in cats:
        for page in range(1, 6):
            r = safe_get(f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}', use_proxy=False)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('[data-cy="l-card"]')
            if not cards: break
            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.olx.ro' + a['href']
                    m = re.search(r'ID([A-Za-z0-9]+)\.html', href)
                    sid = m.group(1) if m else href.split('/')[-1].split('?')[0]
                    fprint = fp('olx', sid)
                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h4, h6')
                    price_el = card.select_one('[data-testid="ad-price"], [class*="price"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    loc_el = card.select_one('[data-testid="location-date"]')
                    loc = loc_el.get_text(strip=True) if loc_el else ''
                    rec = {
                        'fingerprint': fprint, 'source': 'olx',
                        'source_url': href.split('?')[0], 'source_id': sid,
                        'property_type': ptype,
                        'title': title_el.get_text(strip=True) if title_el else '',
                        'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                        'address_raw': loc, 'city': detect_city(loc), 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'OLX: {e}'); err_c += 1
            log.info(f'OLX {slug} p{page}: {len(cards)}')
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'OLX: {found} total, {new_c} new')


# ── PUBLI24 (selectori verificati: .article-content) ──────────────────────────

def scrape_publi24():
    log.info('=== Publi24 ===')
    run_id = start_run('publi24')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('apartamente', 'apartament'),
        ('case', 'casa'),
        ('terenuri', 'teren'),
        ('garsoniere', 'garsoniera'),
    ]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.publi24.ro/anunturi/imobiliare/de-vanzare/{cat}/sibiu/?pagina={page}'
            r = safe_get(url, use_proxy=True)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('div.article-content')
            if not cards:
                log.warning(f'Publi24 no cards p{page}')
                break
            for card in cards:
                try:
                    # Link - din titlu h2
                    a = card.select_one('h2.article-title a') or card.select_one('a[href*="/anunt/"]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.publi24.ro' + a['href']
                    # ID din URL: .../slug/ID_ALPHANUM.html
                    m = re.search(r'/([a-z0-9]+)\.html$', href)
                    sid = m.group(1) if m else href.split('/')[-1].split('.')[0]
                    fprint = fp('publi24', sid)
                    title = a.get_text(strip=True)
                    # Pret - .article-price
                    price_el = card.select_one('.article-price')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    # Suprafata - .article-lbl-txt contine "m2"
                    area_el = card.select_one('.article-short-info, .article-lbl-txt')
                    area_text = area_el.get_text(strip=True) if area_el else ''
                    area_m = re.search(r'([0-9]+)\s*m2', area_text)
                    area = float(area_m.group(1)) if area_m else None
                    # Locatie
                    loc_el = card.select_one('.article-region, [class*="location"], [class*="region"]')
                    city = detect_city(loc_el.get_text(strip=True) if loc_el else 'Sibiu')
                    p_eur = to_eur(p, cur)
                    rec = {
                        'fingerprint': fprint, 'source': 'publi24',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': p, 'currency': cur, 'price_eur': p_eur,
                        'surface_useful': area,
                        'price_per_sqm_eur': round(p_eur/area, 2) if p_eur and area else None,
                        'city': city, 'county': 'Sibiu',
                    }
                    found += 1
                    is_new = upsert_listing(rec, run_id)
                    if is_new:
                        new_c += 1
                        time.sleep(0.5)
                        phone = get_phone_from_page(href)
                        if phone:
                            cid = upsert_contact(phone)
                            if cid:
                                supabase.table('listings').update({'contact_id': cid}).eq('fingerprint', fprint).execute()
                    else:
                        upd_c += 1
                except Exception as e:
                    log.warning(f'Publi24: {e}'); err_c += 1
            log.info(f'Publi24 {cat} p{page}: {len(cards)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Publi24: {found} total, {new_c} new')


# ── IMOBILIARE (a[href*="/oferta/"] + ID numeric) ──────────────────────────────

def scrape_imobiliare():
    log.info('=== Imobiliare.ro ===')
    run_id = start_run('imobiliare')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('vanzare-apartamente', 'apartament'),
        ('vanzare-case', 'casa'),
        ('vanzare-terenuri', 'teren'),
        ('vanzare-vile', 'vila'),
    ]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.imobiliare.ro/{cat}/judetul-sibiu/sibiu?pagina={page}'
            r = safe_get(url, use_proxy=True)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            # Toate link-urile spre oferte individuale
            links = list(dict.fromkeys([
                a['href'] for a in soup.select('a[href*="/oferta/"]')
                if re.search(r'(\d{6,})', a['href'])
            ]))
            if not links:
                log.warning(f'Imobiliare no links p{page}')
                break
            for href in links:
                try:
                    m = re.search(r'(\d{6,})', href)
                    sid = m.group(1) if m else None
                    if not sid: continue
                    fprint = fp('imobiliare', sid)
                    # Title din slug
                    slug_part = href.split('/oferta/')[-1]
                    title = slug_part.replace('-', ' ').rsplit(str(sid), 1)[0].strip()
                    # Price - imobiliare nu o arata in lista fara JS, lasam None
                    # vom prelua din pagina individuala doar pt listing-uri noi
                    rec = {
                        'fingerprint': fprint, 'source': 'imobiliare',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': None, 'currency': 'EUR', 'price_eur': None,
                        'city': 'Sibiu', 'county': 'Sibiu',
                    }
                    found += 1
                    is_new = upsert_listing(rec, run_id)
                    if is_new:
                        new_c += 1
                        # Fetch pagina individuala pentru pret + telefon
                        time.sleep(1)
                        rp = safe_get(href, use_proxy=True)
                        if rp:
                            sp = BeautifulSoup(rp.text, 'lxml')
                            # Pret
                            price_el = sp.select_one('[class*="price"],[class*="pret"],[itemprop="price"]')
                            if price_el:
                                p, cur = parse_price(price_el.get_text(strip=True))
                                p_eur = to_eur(p, cur)
                                supabase.table('listings').update({
                                    'price': p, 'currency': cur, 'price_eur': p_eur
                                }).eq('fingerprint', fprint).execute()
                            # Telefon
                            tel = sp.select_one('a[href^="tel:"]')
                            if tel:
                                cid = upsert_contact(tel['href'].replace('tel:','').strip())
                                if cid:
                                    supabase.table('listings').update({'contact_id': cid}).eq('fingerprint', fprint).execute()
                    else:
                        upd_c += 1
                except Exception as e:
                    log.warning(f'Imobiliare: {e}'); err_c += 1
            log.info(f'Imobiliare {cat} p{page}: {len(links)} links')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imobiliare: {found} total, {new_c} new')


# ── STORIA (__NEXT_DATA__ complet) ─────────────────────────────────────────────

def scrape_storia():
    log.info('=== Storia.ro ===')
    run_id = start_run('storia')
    found = new_c = upd_c = err_c = 0
    cats = [('apartament','apartament'),('casa','casa'),('teren','teren')]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.storia.ro/ro/rezultate/vanzare/{cat}/sibiu?page={page}'
            r = safe_get(url, use_proxy=True)
            if not r: err_c += 1; break
            # Extrage __NEXT_DATA__ din HTML
            soup = BeautifulSoup(r.text, 'lxml')
            nd_tag = soup.find('script', id='__NEXT_DATA__')
            if not nd_tag:
                log.warning(f'Storia no __NEXT_DATA__ p{page}')
                break
            try:
                nd = json.loads(nd_tag.string)
                items = nd['props']['pageProps']['data']['searchAds']['items']
            except Exception as e:
                log.warning(f'Storia parse: {e}')
                break
            if not items:
                break
            for item in items:
                try:
                    sid = str(item.get('id', ''))
                    slug = item.get('slug', sid)
                    href = f'https://www.storia.ro/ro/oferta/{slug}'
                    fprint = fp('storia', sid)
                    pr = item.get('totalPrice', {}) or {}
                    p = float(pr.get('value', 0) or 0) or None
                    cur = 'RON' if pr.get('currency') in ('RON',) else 'EUR'
                    p_eur = to_eur(p, cur)
                    area = item.get('areaInSquareMeters')
                    rooms = item.get('roomsNumber')
                    loc = item.get('location', {}).get('address', {})
                    city = loc.get('city', {}).get('name', 'Sibiu')
                    street = loc.get('street', {}).get('name', '')
                    is_private = item.get('isPrivateOwner', False)
                    rec = {
                        'fingerprint': fprint, 'source': 'storia',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': item.get('title', ''),
                        'price': p, 'currency': cur, 'price_eur': p_eur,
                        'surface_useful': float(area) if area else None,
                        'price_per_sqm_eur': round(p_eur/float(area),2) if p_eur and area else None,
                        'rooms': int(rooms) if rooms else None,
                        'address_raw': street, 'city': city, 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Storia item: {e}'); err_c += 1
            log.info(f'Storia {cat} p{page}: {len(items)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Storia: {found} total, {new_c} new')


# ── IMORADAR24 (.listing-card / data-bi-listing-price) ────────────────────────

def scrape_imoradar24():
    log.info('=== Imoradar24 ===')
    run_id = start_run('imoradar24')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('apartamente-de-vanzare', 'apartament'),
        ('case-de-vanzare', 'casa'),
        ('terenuri-de-vanzare', 'teren'),
        ('garsoniere-de-vanzare', 'garsoniera'),
    ]
    for cat, ptype in cats:
        for page in range(1, 5):
            url = f'https://www.imoradar24.ro/{cat}/judetul-sibiu' + (f'?page={page}' if page > 1 else '')
            r = safe_get(url, use_proxy=True)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            cards = soup.select('.listing-card')
            if not cards:
                log.warning(f'Imoradar24 no cards p{page}')
                break
            for card in cards:
                try:
                    a = card.select_one('a[href*="/oferta/"]') or card.select_one('a[href*="imoradar24"]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.imoradar24.ro' + a['href']
                    m = re.search(r'-(\d{5,})$', href)
                    sid = m.group(1) if m else href.split('-')[-1]
                    if not sid or not sid.isdigit(): continue
                    fprint = fp('imoradar24', sid)
                    # Pret din data-bi-listing-price
                    price_container = card.select_one('[data-bi-listing-price]')
                    p_raw = price_container.get('data-bi-listing-price') if price_container else None
                    cur_raw = price_container.get('data-bi-listing-currency', 'EUR') if price_container else 'EUR'
                    p = float(p_raw) if p_raw else None
                    cur = cur_raw if cur_raw in ('EUR','RON') else 'EUR'
                    p_eur = to_eur(p, cur)
                    # Title
                    title_el = card.select_one('h2,h3,[class*="title"]')
                    title = title_el.get_text(strip=True) if title_el else href.split('/oferta/')[-1].replace('-', ' ')
                    # Area
                    area_el = card.select_one('[class*="surface"],[class*="suprafata"],[class*="area"]')
                    area_m = re.search(r'([0-9]+)', area_el.get_text() if area_el else '')
                    area = float(area_m.group(1)) if area_m else None
                    rec = {
                        'fingerprint': fprint, 'source': 'imoradar24',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': p, 'currency': cur, 'price_eur': p_eur,
                        'surface_useful': area,
                        'price_per_sqm_eur': round(p_eur/area,2) if p_eur and area else None,
                        'city': 'Sibiu', 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Imoradar24: {e}'); err_c += 1
            log.info(f'Imoradar24 {cat} p{page}: {len(cards)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imoradar24: {found} total, {new_c} new')


# ── MAIN ────────────────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--source', choices=['olx','publi24','imobiliare','storia','imoradar24','all'], default='all')
    args = p.parse_args()
    log.info(f'Proxy: {"APIFY ENABLED" if APIFY_TOKEN else "DISABLED"}')
    scrapers = {
        'olx': scrape_olx, 'publi24': scrape_publi24,
        'imobiliare': scrape_imobiliare, 'storia': scrape_storia,
        'imoradar24': scrape_imoradar24,
    }
    targets = scrapers if args.source == 'all' else {args.source: scrapers[args.source]}
    for name, fn in targets.items():
        try:
            fn()
        except Exception as e:
            log.error(f'{name} FAILED: {e}')
        time.sleep(3)
    log.info('=== Done ===')

if __name__ == '__main__':
    main()
