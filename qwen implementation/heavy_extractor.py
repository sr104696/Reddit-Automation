"""
Optimized Heavy Extractor with Rate Limiting and URL Logging
Implements token-bucket rate limiting and logs successful post URLs.
"""

import requests
import pandas as pd
import datetime
import time
import os
import argparse
import random
import json
from urllib.parse import urlparse
from pathlib import Path
from typing import Set, Optional
import threading


# =============================================================================
# Token Bucket Rate Limiter (same implementation as batch_api.py for consistency)
# =============================================================================

class TokenBucket:
    """Thread-safe token bucket rate limiter with exponential backoff."""
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()
    
    def _refill(self):
        now = time.time()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now
    
    def consume(self, tokens: int = 1) -> bool:
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def wait_for_token(self, tokens: int = 1, timeout: float = 60.0) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.consume(tokens):
                return True
            # Exponential backoff sleep
            wait_time = min(0.1 * (2 ** (min(10, int((time.time() - start_time) / 1)))), 2.0)
            time.sleep(wait_time)
        return False


# Global rate limiters
praw_bucket = TokenBucket(capacity=60, refill_rate=1.0)  # Reddit API: ~60 req/min


def acquire_praw_token(timeout: float = 30.0) -> bool:
    """Acquire a PRAW/Reddit API token with exponential backoff."""
    return praw_bucket.wait_for_token(1, timeout=timeout)


# =============================================================================
# URL History Logger
# =============================================================================

POST_HISTORY_FILE = "qwen implementation/post_history.txt"


def log_post_url(url: str, title: str = "", subreddit: str = ""):
    """
    Log the URL of every successful post to post_history.txt.
    Appends in idempotent manner - checks for duplicates before writing.
    
    Args:
        url: Full URL or permalink of the post
        title: Optional post title
        subreddit: Optional subreddit name
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(POST_HISTORY_FILE), exist_ok=True)
    
    # Normalize URL (use permalink as unique key)
    if not url.startswith('http'):
        url = f"https://reddit.com{url}"
    
    # Check if already logged (idempotent)
    if os.path.exists(POST_HISTORY_FILE):
        with open(POST_HISTORY_FILE, 'r') as f:
            for line in f:
                if url in line:
                    return  # Already logged
    
    # Append new entry
    timestamp = datetime.datetime.now().isoformat()
    entry = f"{timestamp} | {subreddit} | {title[:50]} | {url}\n"
    
    with open(POST_HISTORY_FILE, 'a') as f:
        f.write(entry)


def load_logged_urls() -> Set[str]:
    """Load previously logged URLs for deduplication."""
    urls = set()
    if os.path.exists(POST_HISTORY_FILE):
        with open(POST_HISTORY_FILE, 'r') as f:
            for line in f:
                parts = line.strip().split(' | ')
                if len(parts) >= 4:
                    urls.add(parts[3])
    return urls


# =============================================================================
# Optimized Scraper Functions
# =============================================================================

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

MIRRORS = [
    "https://old.reddit.com",
    "https://redlib.catsarch.com",
    "https://redlib.vsls.cz",
    "https://r.nf",
    "https://libreddit.northboot.xyz",
    "https://redlib.tux.pizza"
]

SEEN_URLS = set()
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def setup_directories(target, prefix):
    """Creates organized folder structure for scraped data."""
    base_dir = f"data/{prefix}_{target}"
    dirs = {
        "base": base_dir,
        "posts": f"{base_dir}/posts.csv",
        "comments": f"{base_dir}/comments.csv",
        "media": f"{base_dir}/media",
        "images": f"{base_dir}/media/images",
        "videos": f"{base_dir}/media/videos",
    }
    
    for key in ["base", "media", "images", "videos"]:
        if not os.path.exists(dirs[key]):
            os.makedirs(dirs[key])
    
    return dirs


def extract_post_data(post_json):
    """Extracts comprehensive post data."""
    p = post_json
    
    post_type = "text"
    if p.get('is_video'):
        post_type = "video"
    elif p.get('is_gallery'):
        post_type = "gallery"
    elif any(ext in p.get('url', '').lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']) or 'i.redd.it' in p.get('url', ''):
        post_type = "image"
    elif p.get('is_self'):
        post_type = "text"
    else:
        post_type = "link"
    
    permalink = p.get('permalink', '')
    url = p.get('url_overridden_by_dest', p.get('url', ''))
    
    return {
        "id": p.get('id'),
        "title": p.get('title'),
        "author": p.get('author'),
        "created_utc": datetime.datetime.fromtimestamp(p.get('created_utc', 0)).isoformat(),
        "permalink": permalink,
        "url": url,
        "score": p.get('score', 0),
        "upvote_ratio": p.get('upvote_ratio', 0),
        "num_comments": p.get('num_comments', 0),
        "num_crossposts": p.get('num_crossposts', 0),
        "selftext": p.get('selftext', ''),
        "post_type": post_type,
        "is_nsfw": p.get('over_18', False),
        "is_spoiler": p.get('spoiler', False),
        "flair": p.get('link_flair_text', ''),
        "total_awards": p.get('total_awards_received', 0),
        "has_media": p.get('is_video', False) or p.get('is_gallery', False) or 'i.redd.it' in p.get('url', ''),
        "media_downloaded": False,
        "source": "History-Full"
    }


def run_optimized_scrape(
    target: str, 
    limit: int, 
    is_user: bool = False, 
    download_media_flag: bool = True,
    scrape_comments_flag: bool = True,
    dry_run: bool = False,
    use_plugins: bool = False
):
    """
    Optimized scraper with rate limiting and URL logging.
    
    Features:
    - Token-bucket rate limiting with exponential backoff
    - Idempotent URL logging to post_history.txt
    - Efficient mirror rotation
    - Progress tracking
    
    Args:
        target: Subreddit or username
        limit: Maximum posts to scrape
        is_user: True if target is a user
        download_media_flag: Download images/videos
        scrape_comments_flag: Scrape comments
        dry_run: Simulate without saving data
        use_plugins: Run post-processing plugins
    """
    global SEEN_URLS
    
    prefix = "u" if is_user else "r"
    
    print(f"🚀 Starting OPTIMIZED scrape for {prefix}/{target}")
    print(f"   📊 Target posts: {limit}")
    print(f"   🔒 Rate limiting: Enabled (token bucket)")
    print(f"   📝 URL logging: Enabled")
    print("-" * 50)
    
    # Setup directories
    dirs = setup_directories(target, prefix)
    
    # Load existing URLs
    if os.path.exists(dirs["posts"]):
        try:
            df = pd.read_csv(dirs["posts"])
            for url in df['permalink']:
                SEEN_URLS.add(str(url))
            print(f"📚 Loaded {len(SEEN_URLS)} existing items from {dirs['posts']}")
        except:
            pass
    
    after = None
    total_posts = 0
    all_scraped_posts = []
    all_scraped_comments = []
    start_time = time.time()
    
    try:
        while total_posts < limit:
            random.shuffle(MIRRORS)
            success = False
            
            for base_url in MIRRORS:
                try:
                    if is_user:
                        path = f"/user/{target}/submitted.json"
                    else:
                        path = f"/r/{target}/new.json"
                    
                    batch_size = min(100, limit - total_posts)
                    target_url = f"{base_url}{path}?limit={batch_size}&raw_json=1"
                    if after:
                        target_url += f"&after={after}"
                    
                    # RATE LIMIT: Wait for token before making request
                    if not acquire_praw_token(timeout=60.0):
                        print(f"   ⚠️ Rate limit reached, waiting...")
                        continue
                    
                    print(f"\n📡 Fetching from: {base_url}")
                    response = SESSION.get(target_url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        posts = []
                        batch_comments = []
                        
                        children = data['data']['children']
                        print(f"   Found {len(children)} posts in this batch")
                        
                        for child in children:
                            p = child['data']
                            post = extract_post_data(p)
                            
                            if post['permalink'] in SEEN_URLS:
                                continue
                            
                            posts.append(post)
                            
                            # LOG URL: Log every successful post
                            full_url = f"https://reddit.com{post['permalink']}"
                            log_post_url(
                                url=full_url,
                                title=post['title'],
                                subreddit=target if not is_user else f"u/{target}"
                            )
                            
                            # Scrape comments
                            if scrape_comments_flag and post['num_comments'] > 0:
                                print(f"   💬 Fetching comments for: {post['title'][:40]}...")
                                
                                # RATE LIMIT: Wait for token before fetching comments
                                if not acquire_praw_token(timeout=30.0):
                                    print(f"   ⚠️ Skipping comments due to rate limit")
                                else:
                                    comments = scrape_comments(post['permalink'])
                                    batch_comments.extend(comments)
                                    time.sleep(1)  # Small delay between comment fetches
                        
                        all_scraped_posts.extend(posts)
                        all_scraped_comments.extend(batch_comments)
                        
                        if not dry_run:
                            saved = save_posts_csv(posts, dirs["posts"])
                            total_posts += saved
                            
                            if batch_comments:
                                save_comments_csv(batch_comments, dirs["comments"])
                        else:
                            total_posts += len(posts)
                            print(f"   🧪 [DRY RUN] Would save {len(posts)} posts")
                        
                        print(f"\n📊 Progress: {total_posts}/{limit} posts")
                        
                        after = data['data'].get('after')
                        if not after:
                            print("\n🏁 Reached end of available history.")
                            break
                        
                        success = True
                        break
                        
                except Exception as e:
                    print(f"   ⚠️ Error with {base_url}: {e}")
                    continue
            
            if not after:
                break
                
            if not success:
                print("\n❌ All sources failed. Waiting 30s...")
                time.sleep(30)
            else:
                print(f"\n⏸️ Cooling down (3s)...")
                time.sleep(3)
        
        elapsed = time.time() - start_time
        print(f"\n✅ Scraping completed in {elapsed:.1f}s")
        print(f"   📄 Total posts: {len(all_scraped_posts)}")
        print(f"   💬 Total comments: {len(all_scraped_comments)}")
        print(f"   📝 URLs logged to: {POST_HISTORY_FILE}")
        
        return all_scraped_posts, all_scraped_comments
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        return all_scraped_posts, all_scraped_comments
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return all_scraped_posts, all_scraped_comments


def save_posts_csv(posts, filepath):
    """Saves posts to CSV with all metadata."""
    if not posts:
        return 0
    
    new_posts = [p for p in posts if p['permalink'] not in SEEN_URLS]
    
    if new_posts:
        df = pd.DataFrame(new_posts)
        if os.path.exists(filepath):
            df.to_csv(filepath, mode='a', header=False, index=False)
        else:
            df.to_csv(filepath, index=False)
        
        for p in new_posts:
            SEEN_URLS.add(p['permalink'])
        
        print(f"✅ Saved {len(new_posts)} new posts")
        return len(new_posts)
    else:
        print("💤 No new unique posts found.")
        return 0


def save_comments_csv(comments, filepath):
    """Saves comments to CSV."""
    if not comments:
        return
    
    df = pd.DataFrame(comments)
    if os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
    else:
        df.to_csv(filepath, index=False)
    
    print(f"💬 Saved {len(comments)} comments")


def scrape_comments(permalink, max_depth=3):
    """Scrapes comments from a post with rate limiting."""
    comments = []
    
    try:
        if not permalink.startswith('http'):
            url = f"https://old.reddit.com{permalink}.json?limit=100"
        else:
            url = f"{permalink}.json?limit=100"
        
        response = SESSION.get(url, timeout=15)
        if response.status_code != 200:
            return comments
        
        data = response.json()
        
        if len(data) > 1:
            comment_data = data[1]['data']['children']
            comments = parse_comments(comment_data, permalink, depth=0, max_depth=max_depth)
    
    except Exception as e:
        pass
    
    if len(comments) > 0:
        print(f"   + Scraped {len(comments)} comments")
    
    return comments


def parse_comments(comment_list, post_permalink, depth=0, max_depth=3):
    """Recursively parses comments."""
    comments = []
    
    if depth > max_depth:
        return comments
    
    for item in comment_list:
        if item['kind'] != 't1':
            continue
        
        c = item['data']
        
        comment = {
            "post_permalink": post_permalink,
            "comment_id": c.get('id'),
            "parent_id": c.get('parent_id'),
            "author": c.get('author'),
            "body": c.get('body', ''),
            "score": c.get('score', 0),
            "created_utc": datetime.datetime.fromtimestamp(c.get('created_utc', 0)).isoformat(),
            "depth": depth,
            "is_submitter": c.get('is_submitter', False),
        }
        comments.append(comment)
        
        replies = c.get('replies')
        if replies and isinstance(replies, dict):
            reply_children = replies.get('data', {}).get('children', [])
            comments.extend(parse_comments(reply_children, post_permalink, depth + 1, max_depth))
    
    return comments


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimized Reddit Scraper")
    parser.add_argument("--target", required=True, help="Subreddit or username")
    parser.add_argument("--limit", type=int, default=100, help="Max posts to scrape")
    parser.add_argument("--is-user", action="store_true", help="Target is a user")
    parser.add_argument("--no-media", action="store_true", help="Skip media download")
    parser.add_argument("--no-comments", action="store_true", help="Skip comments")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without saving")
    
    args = parser.parse_args()
    
    run_optimized_scrape(
        target=args.target,
        limit=args.limit,
        is_user=args.is_user,
        download_media_flag=not args.no_media,
        scrape_comments_flag=not args.no_comments,
        dry_run=args.dry_run
    )
