# bot1.py: Utility functions and helpers
import os
import time
from typing import List, Dict, Optional
import threading

UPLOADS_DIR = "uploads"

def ensure_uploads_dir():
    try:
        os.makedirs(UPLOADS_DIR, exist_ok=True)
    except Exception:
        pass

def _sanitize_filename_component(s: str) -> str:
    try:
        bad = '<>:"/\\|?*'
        s = "".join(c for c in str(s) if c not in bad and ord(c) >= 32)
        s = s.strip().replace("\n", " ").replace("\r", " ")
        s = "_".join(s.split())
        return s[:64] if len(s) > 64 else s
    except Exception:
        return "Unknown"

def _username_prefix_for_file(user) -> str:
    try:
        name = (getattr(user, "full_name", "") or "").strip()
    except Exception:
        name = ""
    if not name:
        try:
            uname = (getattr(user, "username", "") or "").strip()
        except Exception:
            uname = ""
        name = f"@{uname}" if uname else "Unknown⚡️"
    if name.startswith("@"):
        name = name[1:]
    return _sanitize_filename_component(name) or "Unknown"

def parse_cards_from_text(text: str) -> List[Dict]:
    """Optimized card parsing with fast path for well-formatted cards"""
    import neww as checkout
    import re
    
    cards = []
    seen_cards = set()  # Track card numbers we've already found
    text = text or ""
    
    # OPTIMIZATION: Fast path for well-formatted cards (most common case)
    # This regex handles pipe-delimited format: 1234567890123456|12|2025|123
    fast_pattern = r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})'
    fast_matches = re.finditer(fast_pattern, text)
    
    for match in fast_matches:
        try:
            number = match.group(1)
            month = int(match.group(2))
            year = int(match.group(3))
            cvv = match.group(4)
            
            # Validate month and year
            if 1 <= month <= 12:
                # Handle 2-digit vs 4-digit year
                if year < 100:
                    year += 2000
                
                card_key = f"{number}|{month}|{year}|{cvv}"
                if card_key not in seen_cards:
                    seen_cards.add(card_key)
                    cards.append({
                        'number': number,
                        'month': month,
                        'year': year,
                        'verification_value': cvv,
                        'name': 'Test Card'
                    })
        except (ValueError, IndexError):
            continue
    
    # If fast path found many cards, we're done (no need for slower parsing)
    if len(cards) >= 10:
        return cards
    
    # Fallback: line-by-line extraction for other formats
    for line in text.splitlines():
        card = checkout.parse_cc_line(line)
        if card:
            card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
            if card_key not in seen_cards:
                seen_cards.add(card_key)
                cards.append(card)
    
    # Only use multi-line extraction as last resort if we found very few cards
    # This handles cases where card data is spread across multiple lines
    if len(cards) < 5:  # If we found fewer than 5 cards, try aggressive extraction
        multi_line_cards = _extract_multiline_cards(text)
        for card in multi_line_cards:
            card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
            if card_key not in seen_cards:
                seen_cards.add(card_key)
                cards.append(card)
    
    return cards

def _extract_multiline_cards(text: str) -> List[Dict]:
    """Extract card information that spans multiple lines with labels
    
    Can extract cards even when mixed with junk text like:
    4008950022280762dsfdsfewrwerewfew
    dsfdsf
    07sdfdsffds
    2029fdsf4wfwf753
    
    Extracts: 4008950022280762|07|2029|753
    """
    import re
    
    cards = []
    
    # Pattern to find card numbers with various labels
    # Card numbers are 13-19 digits (commonly 16)
    card_patterns = [
        r'(?:ccnum|cc num|card num|card number|card|cc|number|pan|cardnumber)\s*[:=\-]?\s*(\d{13,19})',
        r'\b(\d{13,19})\b',  # Standalone card number
        r'(\d{13,19})',  # Any sequence of 13-19 digits (even without word boundaries)
    ]
    
    # Pattern to find expiry dates with various labels
    # Month: 2 digits (01-12), Year: 2 or 4 digits
    exp_patterns = [
        r'(?:exp|expiry|expires|expiration|date|exp date|expiry date)\s*[:=\-]?\s*(\d{1,2})\s*[\/\-]\s*(\d{2,4})',
        r'(?:exp|expiry|expires|expiration|date|exp date|expiry date)\s*[:=\-]?\s*(\d{2})(\d{2,4})',  # MMYY or MMYYYY
        r'\b(\d{1,2})\s*[\/\-]\s*(\d{2,4})\b',  # Standalone MM/YY or MM/YYYY
        r'\b(\d{2})[^\d]*(\d{2,4})\b',  # 2 digits, junk, then 2-4 digits (aggressive)
    ]
    
    # Pattern to find CVV with various labels
    # CVV: 3 or 4 digits
    cvv_patterns = [
        r'(?:cvv|cvc|cv2|code|security code|security|pin)\s*[:=\-]?\s*(\d{3,4})',
        r'\b(\d{3,4})\b',  # Standalone 3-4 digit number (last resort)
    ]
    
    # Try to find all card numbers first
    card_numbers = []
    seen_card_nums = {}  # Track unique card numbers with their first occurrence position
    for pattern in card_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            card_num = match.group(1)
            # Validate it's a proper card number length
            if 13 <= len(card_num) <= 19:
                # Only add if we haven't seen this exact card number before
                if card_num not in seen_card_nums:
                    seen_card_nums[card_num] = (match.start(), match.end())
                    card_numbers.append((card_num, match.start(), match.end()))
    
    # For each card number found, try to find nearby exp and cvv
    for card_num, card_start, card_end in card_numbers:
        # Look in a window around the card number (200 chars before and 300 after)
        # This helps avoid picking up data from other cards
        window_start = max(0, card_start - 200)
        window_end = min(len(text), card_end + 300)
        window_text = text[window_start:window_end]
        
        # Calculate card number position in window (used for filtering)
        card_offset_in_window = card_start - window_start
        card_in_window_start = card_offset_in_window
        card_in_window_end = card_offset_in_window + len(card_num)
        
        # Find expiry date
        exp_month = None
        exp_year = None
        year_match_pos = None  # Track year position to filter CVV later
        for pattern in exp_patterns:
            match = re.search(pattern, window_text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    exp_month = int(match.group(1))
                    year_str = match.group(2)
                    exp_year = int(year_str)
                    # Handle 2-digit vs 4-digit year
                    if exp_year < 100:
                        exp_year += 2000
                    # Validate month (must be 01-12)
                    if 1 <= exp_month <= 12:
                        break
                    else:
                        exp_month = None
                        exp_year = None
        
        # If still no expiry found, try extracting all 2-digit and 2-4 digit numbers
        # and find valid month/year combinations (even when mixed with junk text)
        if not exp_month or not exp_year:
            # Find all 2-digit numbers (potential months) - NO word boundaries
            two_digit_matches = list(re.finditer(r'(\d{2})', window_text))
            # Find all 2-4 digit numbers (potential years) - NO word boundaries
            year_matches = list(re.finditer(r'(\d{2,4})', window_text))
            
            for month_match in two_digit_matches:
                # Skip if this match overlaps with the card number position
                if (month_match.start() >= card_in_window_start and 
                    month_match.start() < card_in_window_end):
                    continue
                    
                potential_month = int(month_match.group(1))
                if 1 <= potential_month <= 12:  # Valid month
                    # Look for a year that appears after this month
                    for year_match in year_matches:
                        # Skip if this overlaps with card number position
                        if (year_match.start() >= card_in_window_start and 
                            year_match.start() < card_in_window_end):
                            continue
                            
                        if year_match.start() > month_match.end():
                            potential_year = int(year_match.group(1))
                            # Validate year
                            if potential_year >= 20 and potential_year <= 99:
                                potential_year += 2000
                            elif potential_year >= 2020 and potential_year <= 2099:
                                pass  # Already 4-digit
                            else:
                                continue
                            
                            # Check if close enough (within 100 chars for junk text tolerance)
                            if year_match.start() - month_match.end() <= 100:
                                exp_month = potential_month
                                exp_year = potential_year
                                year_match_pos = (year_match.start(), year_match.end())
                                break
                if exp_month and exp_year:
                    break
        
        # Find CVV - prefer CVVs that appear AFTER the card number
        cvv = None
        
        # First, find all 3-digit numbers in the window (even when mixed with junk)
        all_3digit = list(re.finditer(r'(\d{3})', window_text))
        all_4digit = list(re.finditer(r'(\d{4})', window_text))
        
        # Filter out card number, month, year from potential CVVs
        valid_3digit = []
        for match in all_3digit:
            # Skip if this overlaps with card number position
            if (match.start() >= card_in_window_start and 
                match.start() < card_in_window_end):
                continue
            
            # Skip if this overlaps with year position
            if year_match_pos:
                year_start, year_end = year_match_pos
                if (match.start() >= year_start and match.start() < year_end):
                    continue
                
            potential = match.group(1)
            # Additional checks for month/year values
            is_valid = True
            if exp_month and potential == str(exp_month).zfill(2):
                is_valid = False
            if exp_year and potential == str(exp_year)[-3:]:
                is_valid = False
            if is_valid:
                valid_3digit.append((match, potential))
        
        # If there's only ONE 3-digit number, it's definitely the CVV
        if len(valid_3digit) == 1:
            cvv = valid_3digit[0][1]
        else:
            # Try with patterns (labeled CVV first)
            for i, pattern in enumerate(cvv_patterns):
                matches = list(re.finditer(pattern, window_text, re.IGNORECASE))
                # Sort matches by proximity to card (prefer those after the card)
                matches_after = [m for m in matches if m.start() > card_offset_in_window]
                matches_before = [m for m in matches if m.start() <= card_offset_in_window]
                sorted_matches = matches_after + matches_before
                
                for match in sorted_matches:
                    potential_cvv = match.group(1)
                    # Don't use the card number, exp date, month, or year as CVV
                    is_valid_cvv = (potential_cvv != card_num[-4:] and potential_cvv not in card_num)
                    if exp_year:
                        is_valid_cvv = is_valid_cvv and (potential_cvv != str(exp_year)[-4:] and 
                                                          potential_cvv != str(exp_year) and 
                                                          potential_cvv not in str(exp_year))
                    if exp_month:
                        is_valid_cvv = is_valid_cvv and (potential_cvv != str(exp_month).zfill(2))
                    
                    if is_valid_cvv:
                        # For standalone numbers (last pattern), be more careful
                        if i == len(cvv_patterns) - 1:
                            # Additional check for standalone patterns
                            if potential_cvv not in card_num:
                                cvv = potential_cvv
                                break
                        else:
                            cvv = potential_cvv
                            break
                if cvv:
                    break
        
        # If we found all components, create a card
        if exp_month and exp_year and cvv:
            cards.append({
                'number': card_num,
                'month': exp_month,
                'year': exp_year,
                'verification_value': cvv,
                'name': 'Test Card'
            })
    
    # Deduplicate cards before returning
    unique_cards = []
    seen = set()
    for card in cards:
        card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
        if card_key not in seen:
            seen.add(card_key)
            unique_cards.append(card)
    
    return unique_cards

def parse_cards_from_file(file_path: str) -> List[Dict]:
    """Parse cards from file with optimizations for large files"""
    try:
        with open(file_path, "r", encoding="utf-8", buffering=8192) as f:
            content = f.read()
        return parse_cards_from_text(content)
    except Exception:
        return []

def progress_block(total: int, processed: int, approved: int, declined: int, charged: int, start_ts: float) -> str:
    elapsed = time.time() - (start_ts or time.time())
    return (
        f"🐢Total CC     : {total}\n"
        f"💬 Progress     : {processed}/{total}\n"
        f"✅  Approved    : {approved}\n"
        f"❌Declined     :  {declined}\n"
        f"💎 Charged     :  {charged}  \n"
        f" Time Elapsed : {elapsed:.2f}s ⏱️\n"
        f"━━━━━━━━━━━━━\n"
        f"⌥ Dev: @Mr_Vempire"
    )

def format_site_label(url: str) -> str:
    import neww as checkout
    try:
        return checkout.format_site_label(url)
    except Exception:
        return (url or "").strip()

def classify_prefix(code_display: str) -> str:
    import neww as checkout
    u = str(code_display or "").upper()
    if u == "SUCCESS":
        return "charged"
    if ('"ACTION_REQUIRED"' in u) or ('ACTION_REQUIRED' in u) or ('3D' in u):
        return "approved"
    approved_tokens = (
        "INCORRECT_CVC",
        "INVALID_CVC",
        "CVC",
        "CVV",
        "CSC",
        "SECURITY",
        "VERIFICATION",
        "PAYMENTS_CREDIT_CARD_CVV_INVALID",
        "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_INVALID",
        "PAYMENTS_CREDIT_CARD_CSC_INVALID",
        "PAYMENTS_CREDIT_CARD_SECURITY_CODE_INVALID",
    )
    if any(tok in u for tok in approved_tokens):
        return "approved"
    # Check for HTTP errors like 403 that should be classified as unknown, not declined
    if "HTTP_403" in u or "403" in u:
        return "unknown"
    if checkout.is_terminal_failure_code_display(code_display):
        return "declined"
    if checkout.is_unknown_code_display(code_display):
        return "unknown"
    # If code_display is empty or None and we completed all steps successfully, treat as approved
    if not code_display or not code_display.strip():
        return "approved"
    return "declined"

def result_notify_text(card: Dict, status: str, code_display: str, amount_display: Optional[str] = None, site_label: Optional[str] = None, user_info: Optional[str] = None, receipt_id: Optional[str] = None, user_id: Optional[int] = None) -> str:
    pan = str(card.get("number", "") or "")
    mm = int(card.get("month", 0) or 0)
    yy = int(card.get("year", 0) or 0)
    cvv = str(card.get("verification_value", "") or "")
    mm_str = f"{mm:02d}"
    yy_str = f"{yy % 100:02d}"
    
    # Enhanced status emojis and formatting
    if status == "charged":
        status_emoji = "💎"
        status_title = "CHARGED"
        status_color = "🟢"
    elif status == "approved":
        # Check for 3D (ACTION_REQUIRED or 3D in code)
        code_upper = (code_display or "").upper()
        is_3d = "3D" in code_upper or "ACTION_REQUIRED" in code_upper
        
        # Check for Shopify (in site_label)
        is_shopify = False
        if isinstance(site_label, str) and site_label.strip():
            site_lower = site_label.lower()
            is_shopify = "shopify" in site_lower or "myshopify" in site_lower
        
        # Determine emoji based on conditions
        if is_3d:
            status_emoji = "🔐"
            status_title = "APPROVED (3D)"
        elif is_shopify:
            status_emoji = "❎"
            status_title = "APPROVED (Shopify)"
        else:
            status_emoji = "✅"
            status_title = "APPROVED"
        status_color = "🟡"
    else:
        status_emoji = "❌"
        status_title = "DECLINED"
        status_color = "🔴"
    
    # Build beautiful notification message
    parts = [
        f"{status_color} <b>{status_title} {status_emoji}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💳 <b>Card:</b> <code>{pan}|{mm_str}|{yy_str}|{cvv}</code>"
    ]
    
    if status == "charged":
        parts.append('🔐 <b>Code:</b> <code>ProcessedReceipt</code>')
    else:
        code_upper = (code_display or "").upper()
        if "ACTION_REQUIRED" in code_upper and status == "approved":
            parts.append(f'🔐 <b>Code:</b> <code>{code_display}</code>')
        else:
            parts.append(f'🔐 <b>Code:</b> <code>{code_display}</code>')
    
    if isinstance(site_label, str) and site_label.strip():
        parts.append(f"🌐 <b>Site:</b> <code>{site_label.strip()}</code>")
    
    if isinstance(amount_display, str) and amount_display.strip():
        parts.append(f"💰 <b>Amount:</b> <code>{amount_display.strip()}</code>")
    
    # Add receipt ID for approved and charged cards
    if receipt_id and isinstance(receipt_id, str) and receipt_id.strip() and status in ("approved", "charged"):
        parts.append(f"🧾 <b>Receipt:</b> <code>{receipt_id.strip()}</code>")
    
    # Add user info with clickable link
    if isinstance(user_info, str) and user_info.strip():
        if user_id:
            # Create clickable link
            user_link = f'<a href="tg://user?id={user_id}">{user_info.strip()}</a>'
            parts.append(f"👤 <b>User:</b> {user_link}")
        else:
            # Fallback to plain text if no user_id
            parts.append(f"👤 <b>User:</b> <code>{user_info.strip()}</code>")
    
    parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(parts)
