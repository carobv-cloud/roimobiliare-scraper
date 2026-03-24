#!/usr/bin/env python3
"""
RoImobiliare Scraper v2.0
Sources: OLX, Publi24, Imobiliare.ro, Storia.ro, Imoradar24.ro
Targets: Sibiu oras + localitati rurale judetul Sibiu
Schedule: every 6h via GitHub Actions
"""

import os, re, json, hashlib, time, logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']

GHL_WEBHOOK = 'https://services.leadconnectorhq.com/hooks/AojtIWqW6PK1qoRK1zLm/webhook-trigger/de8272fc-1a74-4f5c-a985-990e03c92508'

SIBIU_LOCALITIES = [
    'Sibiu', 'Cisnadie', 'Selimbar', 'Sura Mica', 'Sura Mare',
    'Orlat', 'Rasinari', 'Poplaca', 'Cristian', 'Talmaciu',
    'Ocna Sibiului', 'Miercurea Sibiului', 'Saliste', 'Avrig',
    'Medias', 'Agnita', 'Copsa Mica', 'Dumbraveni', 'Sibiel',
    'Aciliu', 'Alamor', 'Axente Sever', 'Bazna', 'Biertan',
    'Boita', 'Brateiu', 'Carta', 'Chirpar', 'Darlos',
    'Hoghilag', 'Laslea', 'Loamnes', 'Marpod', 'Mosna',
    'Nocrich', 'Rod', 'Rosia', 'Sadu', 'Slimnic',
    'Turnu Rosu', 'Vestem', 'Gura Raului', 'Poiana Sibiului',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── HELPERS ───────────────────────────────────────────────────────────────────

def fingerprint(source: str, uid: str) -> str:
    return hashlib.sha256(f'{source}:{uid}'.encode()).hexdigest()[:32]

def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r'[^0-9]', '', raw)
    if digits.startswith('40') and len(digits) == 11:
        return '+' + digits
    if digits.startswith('0') and len(digits) == 10:
        return '+4' + digits
    if len(digits) == 9:
        return '+40' + digits
    return None

def parse_price(text: str):
    if not text:
        return None, 'EUR'
    t = text.upper().replace('.', '').replace(' ', '')
    currency = 'RON' if ('RON' in t or 'LEI' in t) else 'EUR'
    nums = re.findall(r'[0-9]+', t)
    if nums:
        return float(''.join(nums[:2])), currency
    return None, currency

def to_eur(price, currency: str) -> Optional[float]:
    if not price:
        return None
    return round(price, 2) if currency == 'EUR' else round(price / 5.0, 2)

def safe_get(url: str) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f'GET {url}: {e}')
        return None

def upsert_contact(name, phone_raw, ctype='proprietar') -> Optional[int]:
    phone = normalize_phone(phone_raw or '')
    if not phone:
        return None
    try:
        r = supabase.table('contacts').upsert(
            {'phone_normalized': phone, 'phone_raw': phone_raw, 'name': name,
             'type': ctype, 'last_seen': datetime.now(timezone.utc).isoformat()},
            on_conflict='phone_normalized'
        ).execute()
        if r.data:
            return r.data[0]['id']
        ex = supabase.table('contacts').select('id').eq('phone_normalized', phone).execute()
        return ex.data[0]['id'] if ex.data else None
    except Exception as e:
        log.error(f'upsert_contact: {e}')
        return None

def upsert_listing(record: Dict, run_id: int) -> bool:
    fp = record['fingerprint']
    now = datetime.now(timezone.utc).isoformat()
    try:
        ex = supabase.table('listings').select('id,price_eur').eq('fingerprint', fp).execute()
        if ex.data:
            upd = {'last_seen_at': now, 'is_active': True}
            old_p = ex.data[0].get('price_eur')
            new_p = record.get('price_eur')
            if old_p and new_p and abs(float(old_p) - float(new_p)) > 200:
                upd['price_eur'] = new_p
                upd['price'] = record.get('price')
                log.info(f'Price change {fp[:8]}: {old_p} -> {new_p}')
            supabase.table('listings').update(upd).eq('fingerprint', fp).execute()
            return False
        else:
            record.update({'first_seen_at': now, 'last_seen_at': now, 'is_active': True, 'notified_ghl': False})
            supabase.table('listings').insert(record).execute()
            return True
    except Exception as e:
        log.error(f'upsert_listing {fp}: {e}')
        return False

def start_run(source: str) -> int:
    r = supabase.table('scraper_runs').insert(
        {'source': source, 'status': 'running', 'started_at': datetime.now(timezone.utc).isoformat()}
    ).execute()
    return r.data[0]['id'] if r.data else 0

def end_run(run_id: int, found: int, new: int, updated: int, errors: int, ok: bool = True, msg: str = None):
    supabase.table('scraper_runs').update({
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'listings_found': found, 'listings_new': new, 'listings_updated': updated,
        'errors': errors, 'status': 'success' if ok else 'failed', 'error_log': msg
    }).eq('id', run_id).execute()

# ── OLX ──────────────────────────────────────────────────────────────────────

def scrape_olx():
    log.info('--- OLX ---')
    run_id = start_run('olx')
    found, new_c, upd_c, err_c = 0, 0, 0, 0
    cats = [('apartamente-garsoniere-de-vanzare','apartament'),('case-de-vanzare','casa'),('terenuri-de-vanzare','teren')]
    for slug, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.olx.ro/imobiliare/{slug}/sibiu/?page={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'html.parser')
            cards = soup.select('[data-cy="l-card"]')
            if not cards: break
            for card in cards:
                try:
                    a = card.select_one('a[href*="/d/"]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.olx.ro' + a['href']
                    m = re.search(r'-([A-Za-z0-9]+)\.html', href)
                    sid = m.group(1) if m else href.split('/')[-1]
                    fp = fingerprint('olx', sid)
                    title_el = card.select_one('[data-cy="ad-card-title"] h6') or card.select_one('h4,h6')
                    title = title_el.get_text(strip=True) if title_el else ''
                    price_el = card.select_one('[data-testid="ad-price"]') or card.select_one('[class*="price"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    loc_el = card.select_one('[data-testid="location-date"]')
                    loc_text = loc_el.get_text(strip=True) if loc_el else 'Sibiu'
                    city = next((l for l in SIBIU_LOCALITIES if l.lower() in loc_text.lower()), 'Sibiu')
                    rec = {'fingerprint':fp,'source':'olx','source_url':href,'source_id':sid,
                           'property_type':ptype,'title':title,'price':p,'currency':cur,
                           'price_eur':to_eur(p,cur),'address_raw':loc_text,'city':city,'county':'Sibiu'}
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'OLX card: {e}'); err_c += 1
            log.info(f'OLX {slug} p{page}: {len(cards)} cards')
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'OLX: {found} found, {new_c} new')

# ── PUBLI24 ───────────────────────────────────────────────────────────────────

def scrape_publi24():
    log.info('--- Publi24 ---')
    run_id = start_run('publi24')
    found, new_c, upd_c, err_c = 0, 0, 0, 0
    cats = [('apartamente','apartament'),('case-vile','casa'),('terenuri','teren')]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.publi24.ro/anunturi/imobiliare/de-vanzare/{cat}/sibiu/?page={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'html.parser')
            cards = soup.select('.announcement-item') or soup.select('article[class*="ad"]') or soup.select('div[class*="announcement"]')
            if not cards: break
            for card in cards:
                try:
                    a = card.select_one('a[href*="/anunt/"]')
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.publi24.ro' + a['href']
                    m = re.search(r'/anunt/(\d+)', href)
                    sid = m.group(1) if m else href.split('/')[-2]
                    fp = fingerprint('publi24', sid)
                    title = (card.select_one('h2,h3,.title') or card).get_text(strip=True)[:120]
                    price_el = card.select_one('[class*="price"]')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    rec = {'fingerprint':fp,'source':'publi24','source_url':href,'source_id':sid,
                           'property_type':ptype,'title':title,'price':p,'currency':cur,
                           'price_eur':to_eur(p,cur),'city':'Sibiu','county':'Sibiu'}
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Publi24 card: {e}'); err_c += 1
            log.info(f'Publi24 {cat} p{page}: {len(cards)} cards')
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Publi24: {found} found, {new_c} new')

# ── IMOBILIARE.RO ─────────────────────────────────────────────────────────────

def scrape_imobiliare():
    log.info('--- Imobiliare.ro ---')
    run_id = start_run('imobiliare')
    found, new_c, upd_c, err_c = 0, 0, 0, 0
    cats = [('vanzare-apartament','apartament'),('vanzare-casa','casa'),('vanzare-teren','teren'),('vanzare-vila','vila')]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.imobiliare.ro/{cat}/sibiu/?pagina={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'html.parser')
            cards_found = 0
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    data = json.loads(script.string or '{}')
                    items = data.get('itemListElement', []) if data.get('@type') == 'ItemList' else (data if isinstance(data, list) else [])
                    for item in items:
                        offer = item.get('item', item)
                        if not isinstance(offer, dict): continue
                        item_url = offer.get('url', '')
                        if 'imobiliare.ro' not in item_url: continue
                        m = re.search(r'/(\d{5,})', item_url)
                        sid = m.group(1) if m else item_url.split('/')[-2]
                        fp = fingerprint('imobiliare', sid)
                        pr = offer.get('offers', {})
                        p = float(pr.get('price', 0) or 0) or None
                        cur = 'RON' if pr.get('priceCurrency') == 'RON' else 'EUR'
                        addr = offer.get('address', {})
                        city = addr.get('addressLocality', 'Sibiu')
                        rec = {'fingerprint':fp,'source':'imobiliare','source_url':item_url,'source_id':sid,
                               'property_type':ptype,'title':offer.get('name',''),
                               'price':p,'currency':cur,'price_eur':to_eur(p,cur),
                               'address_raw':addr.get('streetAddress',''),'city':city,'county':'Sibiu'}
                        found += 1
                        if upsert_listing(rec, run_id): new_c += 1
                        else: upd_c += 1
                        cards_found += 1
                except Exception as e:
                    log.warning(f'Imobiliare JSON-LD: {e}'); err_c += 1
            if cards_found == 0:
                for card in soup.select('li.card-item, article[class*="card"]'):
                    try:
                        a = card.select_one('a[href]')
                        if not a: continue
                        href = a['href'] if a['href'].startswith('http') else 'https://www.imobiliare.ro'+a['href']
                        m = re.search(r'/(\d{5,})/', href)
                        if not m: continue
                        sid = m.group(1); fp = fingerprint('imobiliare', sid)
                        price_el = card.select_one('[class*="price"],.pret')
                        p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                        title_el = card.select_one('h2,h3,[class*="title"]')
                        rec = {'fingerprint':fp,'source':'imobiliare','source_url':href,'source_id':sid,
                               'property_type':ptype,'title':title_el.get_text(strip=True) if title_el else '',
                               'price':p,'currency':cur,'price_eur':to_eur(p,cur),'city':'Sibiu','county':'Sibiu'}
                        found += 1
                        if upsert_listing(rec, run_id): new_c += 1
                        else: upd_c += 1
                        cards_found += 1
                    except Exception as e:
                        log.warning(f'Imobiliare HTML: {e}'); err_c += 1
            log.info(f'Imobiliare {cat} p{page}: {cards_found}')
            if not cards_found: break
            time.sleep(2)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imobiliare: {found} found, {new_c} new')

# ── STORIA.RO ─────────────────────────────────────────────────────────────────

def scrape_storia():
    log.info('--- Storia.ro ---')
    run_id = start_run('storia')
    found, new_c, upd_c, err_c = 0, 0, 0, 0
    cats = [('FLAT','apartament'),('HOUSE','casa'),('TERRAIN','teren')]
    for cat, ptype in cats:
        for page in range(1, 6):
            # Try JSON API first
            api_url = 'https://www.storia.ro/api/v1/offer/listing'
            params = {'limit':36,'ownerTypeSingleSelect':'ALL','page':page,
                      'regionId':'180085','subType':'SELL','type':cat}
            items = []
            try:
                resp = requests.get(api_url, params=params, headers={**HEADERS,'Accept':'application/json'}, timeout=20)
                if resp.status_code == 200:
                    items = resp.json().get('data',{}).get('searchAds',{}).get('items',[])
            except Exception:
                pass
            # HTML fallback
            if not items:
                ptype_url = {'FLAT':'apartament','HOUSE':'casa','TERRAIN':'teren'}[cat]
                html_url = f'https://www.storia.ro/ro/rezultate/vanzare/{ptype_url}/sibiu?page={page}'
                r = safe_get(html_url)
                if r:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    for card in soup.select('article[data-cy="listing-item"]'):
                        try:
                            a = card.select_one('a')
                            href = a['href'] if a and a['href'].startswith('http') else ('https://www.storia.ro'+(a['href'] if a else ''))
                            m = re.search(r'-ID(\w+)\.html', href) or re.search(r'/([a-z0-9-]+-\d{5,})', href)
                            sid = m.group(1) if m else href.split('/')[-1]
                            fp = fingerprint('storia', sid)
                            price_el = card.select_one('[data-cy="listing-item-price"],[class*="price"]')
                            p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                            title_el = card.select_one('[data-cy="listing-item-title"],h3')
                            rec = {'fingerprint':fp,'source':'storia','source_url':href,'source_id':sid,
                                   'property_type':ptype,'title':title_el.get_text(strip=True) if title_el else '',
                                   'price':p,'currency':cur,'price_eur':to_eur(p,cur),'city':'Sibiu','county':'Sibiu'}
                            found += 1
                            if upsert_listing(rec, run_id): new_c += 1
                            else: upd_c += 1
                        except Exception as e:
                            log.warning(f'Storia HTML: {e}'); err_c += 1
                break
            for item in items:
                try:
                    sid = str(item.get('id',''))
                    if not sid: continue
                    fp = fingerprint('storia', sid)
                    pr = item.get('totalPrice',{})
                    p = float(pr.get('value',0) or 0) or None
                    cur = 'RON' if pr.get('currency') in ('RON','PLN') else 'EUR'
                    loc = item.get('location',{}).get('address',{})
                    city = loc.get('city',{}).get('name','Sibiu')
                    slug = item.get('slug', sid)
                    href = f'https://www.storia.ro/ro/oferta/{slug}'
                    area = item.get('areaInSquareMeters')
                    p_eur = to_eur(p, cur)
                    rec = {'fingerprint':fp,'source':'storia','source_url':href,'source_id':sid,
                           'property_type':ptype,'title':item.get('title',''),
                           'price':p,'currency':cur,'price_eur':p_eur,
                           'surface_useful':float(area) if area else None,
                           'price_per_sqm_eur':round(p_eur/float(area),2) if p_eur and area else None,
                           'rooms':item.get('roomsNum'),'city':city,'county':'Sibiu'}
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Storia API item: {e}'); err_c += 1
            log.info(f'Storia {cat} p{page}: {len(items)}')
            if not items: break
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Storia: {found} found, {new_c} new')

# ── IMORADAR24.RO ─────────────────────────────────────────────────────────────

def scrape_imoradar24():
    log.info('--- Imoradar24 ---')
    run_id = start_run('imoradar24')
    found, new_c, upd_c, err_c = 0, 0, 0, 0
    cats = [('apartamente','apartament'),('case','casa'),('terenuri','teren')]
    for cat, ptype in cats:
        for page in range(1, 6):
            url = f'https://www.imoradar24.ro/vanzare/{cat}/sibiu?page={page}'
            r = safe_get(url)
            if not r: err_c += 1; break
            soup = BeautifulSoup(r.text, 'html.parser')
            cards = (soup.select('.property-listing') or soup.select('.listing-card') or
                     soup.select('article[class*="property"]') or soup.select('div[class*="card"][class*="prop"]'))
            if not cards: break
            for card in cards:
                try:
                    a = (card.select_one('a[href*="/vanzare/"]') or
                         card.select_one('a[href*="/anunt/"]') or card.select_one('a[href]'))
                    if not a: continue
                    href = a['href'] if a['href'].startswith('http') else 'https://www.imoradar24.ro'+a['href']
                    m = re.search(r'/(\d{4,})(?:/|$)', href)
                    sid = m.group(1) if m else href.split('/')[-1].split('?')[0]
                    if not sid: continue
                    fp = fingerprint('imoradar24', sid)
                    price_el = card.select_one('[class*="price"],[class*="Price"],.pret')
                    p, cur = parse_price(price_el.get_text(strip=True) if price_el else '')
                    title_el = card.select_one('h2,h3,[class*="title"]')
                    area_el = card.select_one('[class*="surface"],[class*="area"],[class*="suprafata"]')
                    area_m = re.search(r'([0-9]+)', area_el.get_text() if area_el else '')
                    rec = {'fingerprint':fp,'source':'imoradar24','source_url':href,'source_id':sid,
                           'property_type':ptype,'title':title_el.get_text(strip=True) if title_el else '',
                           'price':p,'currency':cur,'price_eur':to_eur(p,cur),
                           'surface_useful':float(area_m.group(1)) if area_m else None,
                           'city':'Sibiu','county':'Sibiu'}
                    found += 1
                    if upsert_listing(rec, run_id): new_c += 1
                    else: upd_c += 1
                except Exception as e:
                    log.warning(f'Imoradar24 card: {e}'); err_c += 1
            log.info(f'Imoradar24 {cat} p{page}: {len(cards)}')
            time.sleep(1.5)
    end_run(run_id, found, new_c, upd_c, err_c)
    log.info(f'Imoradar24: {found} found, {new_c} new')

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--source', choices=['olx','publi24','imobiliare','storia','imoradar24','all'], default='all')
    args = p.parse_args()
    scrapers = {'olx':scrape_olx,'publi24':scrape_publi24,'imobiliare':scrape_imobiliare,
                'storia':scrape_storia,'imoradar24':scrape_imoradar24}
    targets = scrapers if args.source == 'all' else {args.source: scrapers[args.source]}
    for name, fn in targets.items():
        try:
            fn()
        except Exception as e:
            log.error(f'{name} FAILED: {e}')
        time.sleep(3)
    log.info('=== All scrapers done ===')

if __name__ == '__main__':
    main()
