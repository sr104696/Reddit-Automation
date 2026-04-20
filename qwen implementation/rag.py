"""
RAG Module - Unified Interface for Conversational RAG
Uses the optimized rag_manager with incremental SQLite indexing.
"""

import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the optimized RAG manager
try:
    from rag_manager import RAGManager, get_rag_manager
except ImportError:
    # Fallback for different import paths
    from .rag_manager import RAGManager, get_rag_manager


# Initialize RAG manager singleton
_rag_instance: Optional[RAGManager] = None


def initialize_rag(db_path: str = None) -> RAGManager:
    """
    Initialize the RAG system with the unified SQLite database.
    
    Args:
        db_path: Path to the SQLite database (default: reddit_leads.db)
    
    Returns:
        RAGManager instance
    """
    global _rag_instance
    _rag_instance = get_rag_manager(db_path=db_path)
    logger.info("RAG system initialized")
    return _rag_instance


def find_similar_documents(
    query: str, 
    top_k: int = 10,
    content_type: str = None
) -> List[Dict]:
    """
    Find similar documents using semantic search.
    
    Args:
        query: Search query
        top_k: Number of results to return
        content_type: Filter by 'post' or 'comment', or None for both
    
    Returns:
        List of matching documents with metadata
    """
    if _rag_instance is None:
        initialize_rag()
    
    results = _rag_instance.find_similar_documents(query, top_k=top_k, content_type=content_type)
    
    # Format results for display
    formatted_results = []
    for result in results:
        formatted = {
            'id': result['id'],
            'type': result['type'],
            'relevance': result.get('relevance', 'UNKNOWN'),
            'score': result['score'],
            'author': result.get('author', 'unknown'),
            'content': result.get('content', ''),
        }
        
        if result['type'] == 'post':
            formatted['title'] = result.get('title', '')
            formatted['subreddit'] = result.get('subreddit', '')
            formatted['url'] = result.get('url', '')
        else:
            formatted['post_id'] = result.get('post_id', '')
        
        formatted_results.append(formatted)
    
    return formatted_results


def update_rag_index(force_refresh: bool = False) -> bool:
    """
    Update the RAG index with new data from the unified database.
    Only processes rows that haven't been indexed yet (incremental update).
    
    Args:
        force_refresh: If True, rebuild all embeddings from scratch
    
    Returns:
        True if update was successful
    """
    if _rag_instance is None:
        initialize_rag()
    
    try:
        _rag_instance.update_embeddings(force_refresh=force_refresh)
        return True
    except Exception as e:
        logger.error(f"Failed to update RAG index: {e}")
        return False


def get_rag_stats() -> Dict:
    """
    Get statistics about the RAG system.
    
    Returns:
        Dictionary with stats about indexed content
    """
    if _rag_instance is None:
        initialize_rag()
    
    return _rag_instance.get_stats()


def clear_rag_index() -> bool:
    """
    Clear all indexed embeddings.
    
    Returns:
        True if successful
    """
    if _rag_instance is None:
        initialize_rag()
    
    try:
        _rag_instance.clear_index()
        return True
    except Exception as e:
        logger.error(f"Failed to clear RAG index: {e}")
        return False


# Convenience function for backward compatibility
def search(query: str, top_k: int = 10) -> List[str]:
    """
    Simple search interface returning formatted text results.
    
    Args:
        query: Search query
        top_k: Number of results
    
    Returns:
        List of formatted result strings
    """
    results = find_similar_documents(query, top_k=top_k)
    
    formatted = []
    for r in results:
        if r['type'] == 'post':
            text = f"[{r['relevance']}] POST by u/{r['author']} in r/{r.get('subreddit', '?')}: " \
                   f"{r.get('title', '')} - {r['content'][:200]}..."
        else:
            text = f"[{r['relevance']}] COMMENT by u/{r['author']}: {r['content'][:200]}..."
        formatted.append(text)
    
    return formatted if formatted else ["No results found"]
