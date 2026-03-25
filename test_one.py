import re, requests, sys

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'ro-RO,ro;q=0.9',
}
HEADERS_API = {**HEADERS, 'Accept': 'application/json', 'Referer': 'https://www.olx.ro/'}

URL = 'https://www.olx.ro/d/oferta/casa-in-cisnadie-IDjGsKu.html'

print('=== STEP 1: fetch HTML ===')
r = requests.get(URL, headers=HEADERS, timeout=30)
print(f'Status: {r.status_code}, len: {len(r.text)}')

print('\n=== STEP 2: extract numeric_id ===')
html = r.text
for pattern, name in [
    (r'"sku"\s*:\s*"([0-9]{6,12})"', 'sku'),
    (r'ad_id=([0-9]{6,12})', 'ad_id'),
    (r'/offers/([0-9]{6,12})/', 'offers'),
]:
    m = re.search(pattern, html)
    print(f'  {name}: {m.group(1) if m else "NOT FOUND"}')

print('\n=== STEP 3: find "290870674" in HTML ===')
idx = html.find('290870674')
if idx >= 0:
    print(f'  FOUND at pos {idx}: ...{html[idx-40:idx+40]}...')
else:
    print('  NOT FOUND - OLX serves different HTML on GitHub Actions IP!')
    print(f'  HTML preview: {html[:500]}')

numeric_id = re.search(r'"sku"\s*:\s*"([0-9]{6,12})"', html)
if not numeric_id:
    numeric_id = re.search(r'ad_id=([0-9]{6,12})', html)
if not numeric_id:
    print('\nABORTING - cannot find numeric_id')
    sys.exit(1)

nid = numeric_id.group(1)
print(f'\n=== STEP 4: OLX API /offers/{nid}/ ===')
r2 = requests.get(f'https://www.olx.ro/api/v1/offers/{nid}/', headers=HEADERS_API, timeout=20)
print(f'Status: {r2.status_code}')
if r2.status_code == 200:
    d = r2.json().get('data', {})
    print(f'Description: {d.get("description","")[:200]}')
    print(f'Params: {[(p["key"], p.get("value",{}).get("label","")) for p in d.get("params",[])]}')

print(f'\n=== STEP 5: OLX API /limited-phones/ ===')
r3 = requests.get(f'https://www.olx.ro/api/v1/offers/{nid}/limited-phones/', headers=HEADERS_API, timeout=20)
print(f'Status: {r3.status_code}')
print(f'Response: {r3.text[:300]}')
