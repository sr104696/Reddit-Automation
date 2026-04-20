"""
RAG Manager - Optimized for Unified SQLite Database
Supports incremental updates, only processing rows that haven't been indexed.
"""

import os
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
import sqlite3
from typing import List, Tuple, Optional, Set
import hashlib
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RAGManager:
    """Manages embeddings with persistence and incremental updates using unified SQLite DB."""
    
    def __init__(
        self, 
        model_name: str = 'all-MiniLM-L6-v2', 
        db_path: str = '/workspace/05_Conversational_RAG_Interface/backend/reddit_leads.db',
        embeddings_dir: str = 'embeddings'
    ):
        self.model_name = model_name
        self.db_path = db_path
        self.embeddings_dir = embeddings_dir
        self.embeddings_file = os.path.join(embeddings_dir, 'embeddings.pkl')
        self.metadata_file = os.path.join(embeddings_dir, 'metadata.pkl')
        self.indexed_ids_file = os.path.join(embeddings_dir, 'indexed_ids.pkl')
        
        # Create embeddings directory if it doesn't exist
        os.makedirs(embeddings_dir, exist_ok=True)
        
        # Initialize model
        logger.info(f"Loading SentenceTransformer model: {model_name}")
        self.model = SentenceTransformer(model_name)
        
        # Load existing embeddings and metadata
        self.embeddings: Optional[np.ndarray] = None
        self.metadata: dict = {}
        self.indexed_post_ids: Set[str] = set()
        self.indexed_comment_ids: Set[str] = set()
        self._load_embeddings()
    
    def _load_embeddings(self):
        """Load existing embeddings and metadata from disk."""
        try:
            if (os.path.exists(self.embeddings_file) and 
                os.path.exists(self.metadata_file) and
                os.path.exists(self.indexed_ids_file)):
                
                logger.info("Loading existing embeddings from disk...")
                with open(self.embeddings_file, 'rb') as f:
                    self.embeddings = pickle.load(f)
                with open(self.metadata_file, 'rb') as f:
                    self.metadata = pickle.load(f)
                with open(self.indexed_ids_file, 'rb') as f:
                    ids_data = pickle.load(f)
                    self.indexed_post_ids = ids_data.get('posts', set())
                    self.indexed_comment_ids = ids_data.get('comments', set())
                
                total_indexed = len(self.indexed_post_ids) + len(self.indexed_comment_ids)
                logger.info(f"Loaded {len(self.embeddings)} embeddings for {total_indexed} items")
            else:
                logger.info("No existing embeddings found")
                self.embeddings = np.array([]).reshape(0, 384)  # Empty array with correct dimensions
                self.metadata = {
                    'last_update': None,
                    'data_hash': None,
                    'post_count': 0,
                    'comment_count': 0
                }
                self.indexed_post_ids = set()
                self.indexed_comment_ids = set()
        except Exception as e:
            logger.error(f"Error loading embeddings: {e}")
            self.embeddings = np.array([]).reshape(0, 384)
            self.metadata = {
                'last_update': None,
                'data_hash': None,
                'post_count': 0,
                'comment_count': 0
            }
            self.indexed_post_ids = set()
            self.indexed_comment_ids = set()
    
    def _save_embeddings(self):
        """Save embeddings and metadata to disk."""
        try:
            logger.info("Saving embeddings to disk...")
            with open(self.embeddings_file, 'wb') as f:
                pickle.dump(self.embeddings, f)
            with open(self.metadata_file, 'wb') as f:
                pickle.dump(self.metadata, f)
            with open(self.indexed_ids_file, 'wb') as f:
                pickle.dump({
                    'posts': self.indexed_post_ids,
                    'comments': self.indexed_comment_ids
                }, f)
            logger.info("Embeddings saved successfully")
        except Exception as e:
            logger.error(f"Error saving embeddings: {e}")
    
    def _get_unindexed_posts(self, conn: sqlite3.Connection) -> List[Tuple]:
        """Fetch posts from DB that haven't been indexed yet."""
        cursor = conn.cursor()
        
        if not self.indexed_post_ids:
            # No posts indexed yet, get all
            cursor.execute("""
                SELECT id, title, selftext, author, subreddit, url, score, num_comments, created_utc
                FROM posts
                WHERE selftext IS NOT NULL AND selftext != ''
                ORDER BY created_utc DESC
            """)
        else:
            # Get only unindexed posts
            placeholders = ','.join('?' * len(self.indexed_post_ids))
            cursor.execute(f"""
                SELECT id, title, selftext, author, subreddit, url, score, num_comments, created_utc
                FROM posts
                WHERE id NOT IN ({placeholders})
                AND selftext IS NOT NULL AND selftext != ''
                ORDER BY created_utc DESC
            """, list(self.indexed_post_ids))
        
        return cursor.fetchall()
    
    def _get_unindexed_comments(self, conn: sqlite3.Connection) -> List[Tuple]:
        """Fetch comments from DB that haven't been indexed yet."""
        cursor = conn.cursor()
        
        if not self.indexed_comment_ids:
            # No comments indexed yet, get all
            cursor.execute("""
                SELECT id, body, author, post_id, score, created_utc
                FROM comments
                WHERE body IS NOT NULL AND body != ''
                ORDER BY created_utc DESC
                LIMIT 10000  -- Limit to prevent memory issues
            """)
        else:
            # Get only unindexed comments
            placeholders = ','.join('?' * len(self.indexed_comment_ids))
            cursor.execute(f"""
                SELECT id, body, author, post_id, score, created_utc
                FROM comments
                WHERE id NOT IN ({placeholders})
                AND body IS NOT NULL AND body != ''
                ORDER BY created_utc DESC
                LIMIT 10000
            """, list(self.indexed_comment_ids))
        
        return cursor.fetchall()
    
    def update_embeddings(self, force_refresh: bool = False, batch_size: int = 32):
        """
        Update embeddings incrementally - only processes rows not yet indexed.
        
        Args:
            force_refresh: If True, rebuild all embeddings from scratch
            batch_size: Number of texts to encode in each batch
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return
        
        try:
            if force_refresh:
                logger.info("Force refresh requested. Rebuilding all embeddings from scratch...")
                self.embeddings = np.array([]).reshape(0, 384)
                self.indexed_post_ids = set()
                self.indexed_comment_ids = set()
                self.metadata = {
                    'last_update': None,
                    'data_hash': None,
                    'post_count': 0,
                    'comment_count': 0
                }
            
            # Get unindexed posts
            unindexed_posts = self._get_unindexed_posts(conn)
            # Get unindexed comments
            unindexed_comments = self._get_unindexed_comments(conn)
            
            total_new = len(unindexed_posts) + len(unindexed_comments)
            
            if total_new == 0:
                logger.info("No new entries to index. Database is fully indexed.")
                return
            
            logger.info(f"Found {len(unindexed_posts)} new posts and {len(unindexed_comments)} new comments to index")
            
            # Prepare texts for embedding
            new_texts = []
            new_ids = []
            item_types = []
            
            # Process posts
            for row in unindexed_posts:
                text = f"Title: {row['title']}\nContent: {row['selftext']}"
                new_texts.append(text)
                new_ids.append(row['id'])
                item_types.append('post')
            
            # Process comments
            for row in unindexed_comments:
                text = f"Comment: {row['body']}"
                new_texts.append(text)
                new_ids.append(row['id'])
                item_types.append('comment')
            
            # Encode in batches for memory efficiency
            logger.info(f"Encoding {len(new_texts)} new texts...")
            all_new_embeddings = []
            
            for i in range(0, len(new_texts), batch_size):
                batch = new_texts[i:i + batch_size]
                batch_embeddings = self.model.encode(batch, show_progress_bar=True)
                all_new_embeddings.append(batch_embeddings)
                logger.info(f"Encoded batch {i // batch_size + 1}/{(len(new_texts) + batch_size - 1) // batch_size}")
            
            new_embeddings = np.vstack(all_new_embeddings) if all_new_embeddings else np.array([]).reshape(0, 384)
            
            # Append to existing embeddings
            if self.embeddings.size > 0:
                self.embeddings = np.vstack([self.embeddings, new_embeddings])
            else:
                self.embeddings = new_embeddings
            
            # Update indexed IDs
            for i, item_id in enumerate(new_ids):
                if item_types[i] == 'post':
                    self.indexed_post_ids.add(item_id)
                else:
                    self.indexed_comment_ids.add(item_id)
            
            # Update metadata
            self.metadata['last_update'] = datetime.now()
            self.metadata['post_count'] = len(self.indexed_post_ids)
            self.metadata['comment_count'] = len(self.indexed_comment_ids)
            
            self._save_embeddings()
            logger.info(f"Added {len(new_embeddings)} new embeddings. "
                       f"Total: {len(self.indexed_post_ids)} posts, {len(self.indexed_comment_ids)} comments")
            
        finally:
            conn.close()
    
    def find_similar_documents(
        self, 
        query: str, 
        top_k: int = 5,
        content_type: Optional[str] = None  # 'post', 'comment', or None for both
    ) -> List[dict]:
        """
        Find similar documents using cosine similarity.
        
        Args:
            query: Search query
            top_k: Number of results to return
            content_type: Filter by type ('post' or 'comment'), or None for both
            
        Returns:
            List of dicts with document info and similarity score
        """
        if self.embeddings is None or self.embeddings.size == 0:
            logger.warning("No embeddings available")
            return []
        
        if not self.indexed_post_ids and not self.indexed_comment_ids:
            logger.warning("No indexed items")
            return []
        
        # Encode query
        query_embedding = self.model.encode([query])
        
        # Calculate cosine similarity
        cosine_scores = np.dot(self.embeddings, query_embedding.T).flatten()
        
        # Get top k indices
        top_k_indices = np.argsort(cosine_scores)[-top_k:][::-1]
        
        # Fetch document details from database
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return []
        
        results = []
        try:
            # We need to map embedding indices back to item IDs
            # Build a mapping of index -> (id, type)
            all_items = []
            for item_id in self.indexed_post_ids:
                all_items.append((item_id, 'post'))
            for item_id in self.indexed_comment_ids:
                all_items.append((item_id, 'comment'))
            
            # Note: This assumes embeddings are in the same order as we added them
            # For production, you'd want a more robust indexing strategy
            
            for idx in top_k_indices:
                if idx >= len(all_items):
                    continue
                
                item_id, item_type = all_items[idx]
                score = float(cosine_scores[idx])
                
                if content_type and item_type != content_type:
                    continue
                
                # Fetch full document from DB
                if item_type == 'post':
                    cursor.execute("""
                        SELECT id, title, selftext, author, subreddit, url, score, num_comments, created_utc
                        FROM posts WHERE id = ?
                    """, (item_id,))
                else:
                    cursor.execute("""
                        SELECT id, body, author, post_id, score, created_utc
                        FROM comments WHERE id = ?
                    """, (item_id,))
                
                row = cursor.fetchone()
                if row:
                    result = {
                        'id': row['id'],
                        'type': item_type,
                        'score': score,
                        'author': row.get('author', 'unknown'),
                        'created_utc': row.get('created_utc'),
                    }
                    
                    if item_type == 'post':
                        result['title'] = row['title']
                        result['content'] = row['selftext']
                        result['subreddit'] = row['subreddit']
                        result['url'] = row['url']
                        result['post_score'] = row['score']
                        result['num_comments'] = row['num_comments']
                    else:
                        result['content'] = row['body']
                        result['post_id'] = row['post_id']
                        result['comment_score'] = row['score']
                    
                    # Add relevance label
                    if score > 0.7:
                        result['relevance'] = 'HIGH'
                    elif score > 0.5:
                        result['relevance'] = 'MEDIUM'
                    else:
                        result['relevance'] = 'LOW'
                    
                    results.append(result)
        finally:
            conn.close()
        
        return results
    
    def get_stats(self) -> dict:
        """Get statistics about the indexed data."""
        return {
            'total_embeddings': len(self.embeddings) if self.embeddings is not None else 0,
            'indexed_posts': len(self.indexed_post_ids),
            'indexed_comments': len(self.indexed_comment_ids),
            'last_update': self.metadata.get('last_update', 'Never'),
            'model': self.model_name,
            'embedding_dim': self.embeddings.shape[1] if self.embeddings is not None and len(self.embeddings.shape) > 1 else 0
        }
    
    def clear_index(self):
        """Clear all indexed embeddings."""
        self.embeddings = np.array([]).reshape(0, 384)
        self.indexed_post_ids = set()
        self.indexed_comment_ids = set()
        self.metadata = {
            'last_update': None,
            'data_hash': None,
            'post_count': 0,
            'comment_count': 0
        }
        self._save_embeddings()
        logger.info("Cleared all indexed embeddings")


# Singleton instance for easy import
_rag_manager: Optional[RAGManager] = None


def get_rag_manager(db_path: str = None) -> RAGManager:
    """Get or create the singleton RAG manager instance."""
    global _rag_manager
    if _rag_manager is None:
        _rag_manager = RAGManager(db_path=db_path or '/workspace/05_Conversational_RAG_Interface/backend/reddit_leads.db')
    return _rag_manager
