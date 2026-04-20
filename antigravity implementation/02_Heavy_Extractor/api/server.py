"""
REST API Module - Modified for Antigravity Integration
Includes /trigger endpoint for Stage 1 Tripwire integration.
"""
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from export.database import (
    get_connection, search_posts, search_comments,
    get_subreddit_stats, get_all_subreddits,
    get_job_history, get_job_stats, get_database_info
)
from main import run_full_history

# Create FastAPI app
app = FastAPI(
    title="Reddit Scraper API (Integrated)",
    description="REST API for Reddit Scraper data with automated triggering support.",
    version="1.1.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TriggerRequest(BaseModel):
    target: str
    limit: int = 25
    is_user: bool = False

@app.get("/", tags=["Info"])
def root():
    return {"status": "online", "integration": "antigravity"}

@app.get("/health", tags=["Info"])
def health_check():
    try:
        get_database_info()
        return {"status": "healthy"}
    except:
        return {"status": "unhealthy"}

@app.get("/posts", tags=["Posts"])
def list_posts(subreddit: Optional[str] = None, limit: int = 100):
    return search_posts(subreddit=subreddit, limit=limit)

@app.post("/trigger", tags=["Automation"])
def trigger_scrape(request: TriggerRequest, background_tasks: BackgroundTasks):
    """
    Triggered by Stage 1 (Tripwire) when a keyword is matched.
    Initiates a deep-dive scrape of the target subreddit/user.
    """
    background_tasks.add_task(
        run_full_history,
        target=request.target,
        limit=request.limit,
        is_user=request.is_user,
        download_media_flag=True,
        scrape_comments_flag=True
    )
    return {"message": "Scrape task queued", "target": request.target}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
