#!/usr/bin/env python3
"""RoImobiliare Scraper v3.0 - Fixed all 5 sources + phone extraction"""

import os, re, json, hashlib, time, logging
from datetime import datetime, timezone
from typing import Optional, Dict, List
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
GHL_WEBHOOK  = 'https://services.leadconnectorhq.com/hooks/AojtIWqW6PK1qoRK1zLm/webhook-trigger/de8272fc-1a74-4f5c-a985-990e03c92508'

SIBIU_LOCALITIES = [
    'Sibiu','Cisnadie','Selimbar','Sura Mica','Sura Mare','Orlat','Rasinari',
    'Poplaca','Cristian','Talmaciu','Ocna Sibiului','Miercurea Sibiului',
    'Saliste','Avrig','Medias','Agnita','Copsa Mica','Dumbraveni','Sibiel',
    'Aciliu','Axente Sever','Bazna','Biertan','Boita','Brateiu','Carta',
    'Chirpar','Darlos','Hoghilag','Laslea','Loamnes','Marpod','Mosna',
    'Nocrich','Rod','Rosia','Sadu','Slimnic','Turnu Rosu','Vestem','Gura Raului',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9,en-US;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── HELPERS ───────────────────────────────────────────────────────────────────

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
    t = text.upper().replace('.','').replace(' ','')
    cur = 'RON' if ('RON' in t or 'LEI' in t) else 'EUR'
    nums = re.findall(r'[0-9]+', t)
    p = float(''.join(nums[:2])) if nums else None
    return p, cur

def to_eur(price, currency: str) -> Optional[float]:
    if not price: return None
    return round(price, 2) if currency == 'EUR' else round(price / 5.0, 2)

def detect_city(text: str) -> str:
    if not text: return 'Sibiu'
    for loc in SIBIU_LOCALITIES:
        if loc.lower() in text.lower(): return loc
    return 'Sibiu'

def safe_get(url: str, **kw) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25, **kw)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f'GET {url[:80]}: {e}')
        return None

def upsert_contact(name, phone_raw, ctype='proprietar') -> Optional[int]:
    phone = norm_phone(phone_raw or '')
    if not phone: return None
    try:
        r = supabase.table('contacts').upsert(
            {'phone_normalized': phone, 'phone_raw': phone_raw, 'name': name,
             'type': ctype, 'last_seen': datetime.now(timezone.utc).isoformat()},
            on_conflict='phone_normalized'
        ).execute()
        if r.data: return r.data[0]['id']
        ex = supabase.table('contacts').select('id').eq('phone_normalized', phone).execute()
        return ex.data[0]['id'] if ex.data else None
    except Exception as e:
        log.error(f'contact: {e}'); return None

def upsert_listing(rec: Dict, run_id: int) -> bool:
    fingerprint = rec['fingerprint']
    now = datetime.now(timezone.utc).isoformat()
    try:
        ex = supabase.table('listings').select('id,price_eur').eq('fingerprint', fingerprint).execute()
        if ex.data:
            upd = {'last_seen_at': now, 'is_active': True}
            op, np_ = ex.data[0].get('price_eur'), rec.get('price_eur')
            if op and np_ and abs(float(op) - float(np_)) > 200:
                upd.update({'price_eur': np_, 'price': rec.get('price')})
            supabase.table('listings').update(upd).eq('fingerprint', fingerprint).execute()
            return False
        rec.update({'first_seen_at': now, 'last_seen_at': now, 'is_active': True, 'notified_ghl': False})
        supabase.table('listings').insert(rec).execute()
        return True
    except Exception as e:
        log.error(f'listing {fingerprint[:8]}: {e}'); return False

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


# ── OLX ──────────────────────────────────────────────────────────────────────

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
            r = safe_get(f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}')
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
                    fingerprint = fp('olx', sid)
                    title_el = card.select_one('[data-cy="ad-card-title"] h6, h4, h6')
                    title = title_el.get_text(strip=True) if title_el else ''
                    price_el = card.select_one('[data-testid="ad-price"], [class*="price"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    loc_el = card.select_one('[data-testid="location-date"]')
                    loc = loc_el.get_text(strip=True) if loc_el else ''
                    # OLX phones require login — store URL for Ana to message via OLX
                    rec = {
                        'fingerprint': fingerprint, 'source': 'olx',
                        'source_url': href.split('?')[0], 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                        'address_raw': loc, 'city': detect_city(loc), 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'OLX card: {e}'); err_c += 1
            log.info(f'OLX {slug} p{page}: {len(cards)}')
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'OLX: {found} total, {new_c} new')


# ── PUBLI24 ───────────────────────────────────────────────────────────────────

def _publi24_phone(listing_url: str) -> Optional[str]:
    """Extract phone from individual Publi24 listing page."""
    r = safe_get(listing_url)
    if not r: return None
    soup = BeautifulSoup(r.text, 'lxml')
    # Phone in <a href="tel:..."> or visible text
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        return tel['href'].replace('tel:', '').strip()
    # Try text pattern
    text = soup.get_text()
    m = re.search(r'(?:\+40|40|0)[0-9]{9}', text.replace(' ','').replace('-',''))
    return m.group(0) if m else None

def scrape_publi24():
    log.info('=== Publi24 ===')
    run_id = start_run('publi24')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('apartamente', 'apartament'),
        ('case', 'casa'),
        ('terenuri', 'teren'),
    ]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.publi24.ro/anunturi/imobiliare/de-vanzare/{cat}/sibiu/?pagina={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            # Publi24 listing cards
            cards = (soup.select('li.announcement-item') or
                     soup.select('div.announcement-item') or
                     soup.select('article[class*="announcement"]') or
                     soup.select('[class*="listing-item"]'))
            if not cards:
                log.warning(f'Publi24 no cards: {url}'); break
            for card in cards:
                try:
                    a = card.select_one('a[href*="/anunt/"]') or card.select_one('a[href]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.publi24.ro' + a['href']
                    m = re.search(r'/anunt/([0-9]+)', href)
                    sid = m.group(1) if m else href.split('/')[-2]
                    fingerprint = fp('publi24', sid)
                    title_el = card.select_one('h2,h3,[class*="title"],[class*="announcement-title"]')
                    title = title_el.get_text(strip=True) if title_el else ''
                    price_el = card.select_one('[class*="price"],[class*="pret"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    # Try to get phone from card directly first
                    tel = card.select_one('a[href^="tel:"]')
                    phone_raw = tel['href'].replace('tel:','').strip() if tel else None
                    # If no phone in card, fetch listing page (throttled — only for new listings)
                    contact_id = None
                    if phone_raw:
                        contact_id = upsert_contact(None, phone_raw)
                    rec = {
                        'fingerprint': fingerprint, 'source': 'publi24',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                        'city': 'Sibiu', 'county': 'Sibiu',
                        'contact_id': contact_id,
                    }
                    found += 1
                    is_new = upsert_listing(rec, run_id)
                    if is_new:
                        new_c += 1
                        # Fetch phone from listing page only for NEW listings
                        if not phone_raw:
                            time.sleep(1)
                            phone_raw = _publi24_phone(href)
                            if phone_raw:
                                cid = upsert_contact(None, phone_raw)
                                if cid:
                                    supabase.table('listings').update({'contact_id': cid}).eq('fingerprint', fingerprint).execute()
                    else:
                        upd_c += 1
                except Exception as e:
                    log.warning(f'Publi24 card: {e}'); err_c += 1
            log.info(f'Publi24 {cat} p{page}: {len(cards)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Publi24: {found} total, {new_c} new')


# ── IMOBILIARE.RO ─────────────────────────────────────────────────────────────

def _imobiliare_phone(listing_url: str) -> Optional[str]:
    """Phone often visible on imobiliare.ro listing page."""
    r = safe_get(listing_url)
    if not r: return None
    soup = BeautifulSoup(r.text, 'lxml')
    tel = soup.select_one('a[href^="tel:"]')
    if tel: return tel['href'].replace('tel:', '').strip()
    m = re.search(r'(?:\+40|40|0)[0-9]{9}', soup.get_text().replace(' ',''))
    return m.group(0) if m else None

def scrape_imobiliare():
    log.info('=== Imobiliare.ro ===')
    run_id = start_run('imobiliare')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('vanzare-apartament', 'apartament'),
        ('vanzare-casa', 'casa'),
        ('vanzare-teren', 'teren'),
        ('vanzare-vila', 'vila'),
    ]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.imobiliare.ro/{cat}/sibiu/?pagina={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            cards_found = 0

            # Method 1: JSON-LD structured data
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    data = json.loads(script.string or '{}')
                    items = data.get('itemListElement', []) if data.get('@type') == 'ItemList' else []
                    for item in items:
                        offer = item.get('item', item)
                        if not isinstance(offer, dict): continue
                        item_url = offer.get('url', '')
                        if 'imobiliare.ro' not in item_url: continue
                        m = re.search(r'/(\d{5,})', item_url)
                        sid = m.group(1) if m else item_url.split('/')[-2]
                        fingerprint = fp('imobiliare', sid)
                        pr = offer.get('offers', {})
                        p = float(pr.get('price', 0) or 0) or None
                        cur = 'RON' if pr.get('priceCurrency') == 'RON' else 'EUR'
                        addr = offer.get('address', {})
                        city = addr.get('addressLocality', 'Sibiu')
                        rec = {
                            'fingerprint': fingerprint, 'source': 'imobiliare',
                            'source_url': item_url, 'source_id': sid,
                            'property_type': ptype, 'title': offer.get('name', ''),
                            'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                            'address_raw': addr.get('streetAddress', ''),
                            'city': city, 'county': 'Sibiu',
                        }
                        found += 1
                        is_new = upsert_listing(rec, run_id)
                        if is_new:
                            new_c += 1
                            time.sleep(1)
                            phone = _imobiliare_phone(item_url)
                            if phone:
                                cid = upsert_contact(None, phone)
                                if cid: supabase.table('listings').update({'contact_id': cid}).eq('fingerprint', fingerprint).execute()
                        else: upd_c += 1
                        cards_found += 1
                except Exception as e:
                    log.warning(f'Imobiliare JSON-LD: {e}')

            # Method 2: HTML cards fallback
            if cards_found == 0:
                selectors = [
                    'li.card-item', 'article[class*="card"]',
                    'div[class*="card-v2"]', '[class*="listing-card"]'
                ]
                for sel in selectors:
                    cards = soup.select(sel)
                    if cards:
                        for card in cards:
                            try:
                                a = card.select_one('a[href*="imobiliare.ro"]') or card.select_one('a[href]')
                                if not a: continue
                                href = a['href']
                                if not href.startswith('http'): href = 'https://www.imobiliare.ro' + href
                                m = re.search(r'/(\d{5,})/', href)
                                if not m: continue
                                sid = m.group(1)
                                fingerprint = fp('imobiliare', sid)
                                price_el = card.select_one('[class*="price"],[class*="pret"]')
                                p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                                title_el = card.select_one('h2,h3,[class*="title"]')
                                rec = {
                                    'fingerprint': fingerprint, 'source': 'imobiliare',
                                    'source_url': href, 'source_id': sid,
                                    'property_type': ptype,
                                    'title': title_el.get_text(strip=True) if title_el else '',
                                    'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                                    'city': 'Sibiu', 'county': 'Sibiu',
                                }
                                found += 1
                                if upsert_listing(rec, run_id): new_c += 1
                                else: upd_c += 1
                                cards_found += 1
                            except Exception as e:
                                log.warning(f'Imobiliare HTML: {e}'); err_c += 1
                        break

            log.info(f'Imobiliare {cat} p{page}: {cards_found}')
            if not cards_found: break
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imobiliare: {found} total, {new_c} new')


# ── STORIA.RO ─────────────────────────────────────────────────────────────────

def scrape_storia():
    log.info('=== Storia.ro ===')
    run_id = start_run('storia')
    found = new_c = upd_c = err_c = 0
    cats = [
        ('apartament', 'apartament'),
        ('casa', 'casa'),
        ('teren', 'teren'),
    ]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.storia.ro/ro/rezultate/vanzare/{cat}/sibiu?page={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')

            # Storia HTML listing cards
            cards = (soup.select('article[data-cy="listing-item"]') or
                     soup.select('[data-testid="listing-item"]') or
                     soup.select('li[class*="listing"]') or
                     soup.select('div[class*="listing-item"]'))
            if not cards:
                log.warning(f'Storia no cards p{page}: {url}'); break

            for card in cards:
                try:
                    a = card.select_one('a[href]')
                    href = a['href'] if a else ''
                    if not href.startswith('http'):
                        href = 'https://www.storia.ro' + href
                    # Extract ID from URL slug
                    m = re.search(r'-([A-Za-z0-9]{8,})\.html$', href) or re.search(r'/([a-z0-9-]+-ID[A-Za-z0-9]+)', href)
                    sid = m.group(1) if m else href.split('/')[-1].split('.')[0]
                    if not sid: continue
                    fingerprint = fp('storia', sid)

                    price_el = card.select_one('[data-cy="listing-item-price"],[aria-label*="pret"],[class*="price"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')

                    title_el = card.select_one('[data-cy="listing-item-title"],h3,h2')
                    title = title_el.get_text(strip=True) if title_el else ''

                    area_el = card.select_one('[aria-label*="suprafata"],[class*="area"],[data-cy*="area"]')
                    area_text = area_el.get_text(strip=True) if area_el else ''
                    am = re.search(r'([0-9]+)', area_text)
                    area = float(am.group(1)) if am else None

                    p_eur = to_eur(p, cur)
                    rec = {
                        'fingerprint': fingerprint, 'source': 'storia',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype, 'title': title,
                        'price': p, 'currency': cur, 'price_eur': p_eur,
                        'surface_useful': area,
                        'price_per_sqm_eur': round(p_eur/area, 2) if p_eur and area else None,
                        'city': 'Sibiu', 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Storia card: {e}'); err_c += 1

            log.info(f'Storia {cat} p{page}: {len(cards)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Storia: {found} total, {new_c} new')


# ── IMORADAR24.RO ─────────────────────────────────────────────────────────────

def scrape_imoradar24():
    log.info('=== Imoradar24 ===')
    run_id = start_run('imoradar24')
    found = new_c = upd_c = err_c = 0
    # Correct URL pattern for Imoradar24
    cats = [
        ('apartamente', 'apartament'),
        ('case-vile', 'casa'),
        ('terenuri', 'teren'),
    ]
    # Try multiple URL patterns
    base_patterns = [
        'https://www.imoradar24.ro/vanzare/{cat}/judetul-sibiu/',
        'https://www.imoradar24.ro/vanzare/sibiu/{cat}/',
        'https://www.imoradar24.ro/{cat}/vanzare/sibiu/',
    ]
    for cat, ptype in cats:
        working_url = None
        for pattern in base_patterns:
            test_url = pattern.format(cat=cat)
            r = safe_get(test_url)
            if r and r.status_code == 200:
                soup = BeautifulSoup(r.text, 'lxml')
                cards = soup.select('article, [class*="property"], [class*="listing"], [class*="card"]')
                if cards:
                    working_url = test_url
                    log.info(f'Imoradar24 working URL: {test_url}')
                    break
        if not working_url:
            log.warning(f'Imoradar24: no working URL for {cat}')
            err_c += 1
            continue
        for page in range(1, 4):
            url = working_url if page == 1 else working_url + f'?page={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'lxml')
            cards = (soup.select('article[class*="property"]') or
                     soup.select('[class*="listing-card"]') or
                     soup.select('[class*="property-item"]') or
                     soup.select('article'))
            if not cards: break
            for card in cards:
                try:
                    a = card.select_one('a[href]')
                    if not a: continue
                    href = a['href']
                    if not href.startswith('http'): href = 'https://www.imoradar24.ro' + href
                    m = re.search(r'/(\d{4,})(?:/|$)', href)
                    sid = m.group(1) if m else href.split('/')[-1].split('?')[0]
                    if not sid or sid == 'imoradar24.ro': continue
                    fingerprint = fp('imoradar24', sid)
                    price_el = card.select_one('[class*="price"],[class*="pret"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    title_el = card.select_one('h2,h3,[class*="title"]')
                    rec = {
                        'fingerprint': fingerprint, 'source': 'imoradar24',
                        'source_url': href, 'source_id': sid,
                        'property_type': ptype,
                        'title': title_el.get_text(strip=True) if title_el else '',
                        'price': p, 'currency': cur, 'price_eur': to_eur(p, cur),
                        'city': 'Sibiu', 'county': 'Sibiu',
                    }
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Imoradar24 card: {e}'); err_c += 1
            log.info(f'Imoradar24 {cat} p{page}: {len(cards)}')
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imoradar24: {found} total, {new_c} new')


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--source', choices=['olx','publi24','imobiliare','storia','imoradar24','all'], default='all')
    args = p.parse_args()
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
