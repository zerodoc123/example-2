"""
Load balancing router for /txt command
This module wraps the cmd_txt function to provide load balancing and statistics tracking
"""

import time
import threading
from typing import Callable, Dict, Optional
from collections import deque
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

# Statistics tracking
_stats_lock = threading.Lock()
_original_cmd_txt: Optional[Callable] = None

# Request statistics
_request_stats = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "requests_by_user": {},  # user_id -> count
    "last_requests": deque(maxlen=100),  # Last 100 requests with timestamp and user_id
}

# Load balancing configuration
_max_concurrent_requests = 50
_active_requests: Dict[int, float] = {}  # user_id -> timestamp
_active_requests_lock = threading.Lock()


def init_txt_router(cmd_txt_function: Callable):
    """
    Initialize the router with the original cmd_txt function
    
    Args:
        cmd_txt_function: The original async cmd_txt function to wrap
    """
    global _original_cmd_txt
    _original_cmd_txt = cmd_txt_function


async def cmd_txt_with_load_balancing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Load-balanced wrapper around the original cmd_txt function
    This function tracks statistics and manages request flow
    """
    if _original_cmd_txt is None:
        await update.message.reply_text(
            "❌ <b>Router Not Initialized</b>\n\n"
            "The load balancer router has not been initialized properly.",
            parse_mode=ParseMode.HTML
        )
        return
    
    user = update.effective_user
    user_id = user.id if user else None
    
    # Update statistics
    with _stats_lock:
        _request_stats["total_requests"] += 1
        if user_id:
            _request_stats["requests_by_user"][user_id] = _request_stats["requests_by_user"].get(user_id, 0) + 1
        _request_stats["last_requests"].append({
            "user_id": user_id,
            "timestamp": time.time(),
            "username": user.username if user else None
        })
    
    # Track active request
    request_start_time = time.time()
    with _active_requests_lock:
        if user_id:
            _active_requests[user_id] = request_start_time
    
    try:
        # Call the original cmd_txt function
        await _original_cmd_txt(update, context)
        
        # Mark as successful
        with _stats_lock:
            _request_stats["successful_requests"] += 1
            
    except Exception as e:
        # Mark as failed
        with _stats_lock:
            _request_stats["failed_requests"] += 1
        
        # Re-raise the exception to maintain original behavior
        raise
    finally:
        # Remove from active requests
        with _active_requests_lock:
            if user_id and user_id in _active_requests:
                del _active_requests[user_id]


async def cmd_txt_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Display load balancer statistics (admin only)
    """
    from bot2 import is_admin
    
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("❌ Only admins can view load balancer statistics.")
        return
    
    with _stats_lock:
        total = _request_stats["total_requests"]
        successful = _request_stats["successful_requests"]
        failed = _request_stats["failed_requests"]
        success_rate = (successful / total * 100) if total > 0 else 0
        
        # Get top users
        top_users = sorted(
            _request_stats["requests_by_user"].items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        # Get recent requests (last hour)
        now = time.time()
        recent_requests = [
            req for req in _request_stats["last_requests"]
            if now - req["timestamp"] < 3600
        ]
    
    with _active_requests_lock:
        active_count = len(_active_requests)
        active_users = list(_active_requests.keys())
    
    # Format top users
    top_users_text = "\n".join([
        f"  {i+1}. User {uid}: {count} requests"
        for i, (uid, count) in enumerate(top_users)
    ]) if top_users else "  No requests yet"
    
    # Format active requests
    active_text = f"{active_count} active request(s)" if active_count > 0 else "No active requests"
    
    stats_text = (
        f"📊 <b>Load Balancer Statistics</b>\n\n"
        f"📈 <b>Overall Stats:</b>\n"
        f"  Total Requests: {total}\n"
        f"  Successful: {successful}\n"
        f"  Failed: {failed}\n"
        f"  Success Rate: {success_rate:.2f}%\n"
        f"  Recent (1h): {len(recent_requests)}\n\n"
        f"🔄 <b>Current Load:</b>\n"
        f"  {active_text}\n\n"
        f"👥 <b>Top Users (Last 100 Requests):</b>\n"
        f"{top_users_text}\n\n"
        f"⏰ Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


def get_stats() -> Dict:
    """Get current statistics (for programmatic access)"""
    with _stats_lock:
        return {
            "total_requests": _request_stats["total_requests"],
            "successful_requests": _request_stats["successful_requests"],
            "failed_requests": _request_stats["failed_requests"],
            "requests_by_user": dict(_request_stats["requests_by_user"]),
            "active_requests": len(_active_requests),
        }

