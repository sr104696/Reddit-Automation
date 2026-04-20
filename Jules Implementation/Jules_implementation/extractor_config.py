"""
Reddit Scraper Suite - Configuration
"""
import os
from pathlib import Path

# --- PATHS ---
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# Unified Shared Database path
SHARED_DB_PATH = os.getenv("SHARED_DB_PATH", "/data/shared_db/reddit_leads.db")
DB_PATH = Path(SHARED_DB_PATH)

# --- SCRAPER SETTINGS ---
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Sources: old.reddit.com for residential IPs, mirrors for data centers
MIRRORS = [
    "https://old.reddit.com",
    "https://redlib.catsarch.com",
    "https://redlib.vsls.cz",
    "https://r.nf",
    "https://libreddit.northboot.xyz",
    "https://redlib.tux.pizza"
]

# Rate limiting
REQUEST_TIMEOUT = 15
COOLDOWN_SECONDS = 3
RETRY_WAIT = 30

# Media settings
MAX_IMAGES_PER_POST = 10
MAX_VIDEOS_PER_POST = 2
MAX_GALLERY_IMAGES = 15

# Comment settings
MAX_COMMENT_DEPTH = 5

# --- ASYNC SETTINGS ---
ASYNC_MAX_CONCURRENT = 10
ASYNC_BATCH_SIZE = 50

# --- NOTIFICATION SETTINGS ---
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- DASHBOARD SETTINGS ---
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8501

# --- SCHEDULER SETTINGS ---
SCHEDULER_TIMEZONE = "Asia/Kolkata"

# --- DATABASE SETTINGS ---
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

# Ensure data directory and shared DB directory exists
DATA_DIR.mkdir(exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
