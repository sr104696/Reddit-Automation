# Self-contained FastAPI server with safe local imports and optional trigger fallback
from typing import Optional
import os
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database_fixed import (
    get_database_info,
    search_posts,
    search_comments,
    get_subreddit_stats,
    get_all_subreddits,
    get_job_history,
    get_job_stats,
)

try:
    from main import run_full_history
except Exception:
    def run_full_history(*args, **kwargs):
        return None

app = FastAPI(
    title='Reddit Scraper API',
    description='REST API for Reddit Scraper data with automated triggering support.',
    version='1.2.0'
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


class TriggerRequest(BaseModel):
    target: str
    limit: int = 25
    is_user: bool = False


@app.get('/')
def root():
    return {'status': 'online', 'integration': 'antigravity'}


@app.get('/health')
def health_check():
    try:
        return {'status': 'healthy', 'database': get_database_info()}
    except Exception as exc:
        return {'status': 'unhealthy', 'error': str(exc)}


@app.get('/posts')
def list_posts(subreddit: Optional[str] = None, limit: int = 100):
    return search_posts(subreddit=subreddit, limit=limit)


@app.get('/comments')
def list_comments(subreddit: Optional[str] = None, limit: int = 100):
    return search_comments(subreddit=subreddit, limit=limit)


@app.get('/subreddits')
def list_subreddits():
    return get_all_subreddits()


@app.get('/subreddit-stats')
def subreddit_stats():
    return get_subreddit_stats()


@app.get('/jobs/history')
def jobs_history(limit: int = 100):
    return get_job_history(limit=limit)


@app.get('/jobs/stats')
def jobs_stats():
    return get_job_stats()


@app.post('/trigger')
def trigger_scrape(request: TriggerRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        run_full_history,
        target=request.target,
        limit=request.limit,
        is_user=request.is_user,
        download_media_flag=True,
        scrape_comments_flag=True
    )
    return {'message': 'Scrape task queued', 'target': request.target}


if __name__ == '__main__':
    import uvicorn
    port_val = int(os.environ.get('PORT', '8000'))
    uvicorn.run(app, host='0.0.0.0', port=port_val)
