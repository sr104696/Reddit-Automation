# Safer analytics loader with optional dependencies removed and portable sqlite/pandas usage
import json
import os
import sqlite3
import pandas as pd

USER_ANALYSIS_FILE = os.environ.get('USER_ANALYSIS_FILE', 'user_analysis.csv')
DB_PATH = os.environ.get('SHARED_DB_PATH', './reddit_automation.db')


def get_user_analysis():
    if os.path.exists(USER_ANALYSIS_FILE):
        df = pd.read_csv(USER_ANALYSIS_FILE)
        if 'main_topics' in df.columns:
            df['main_topics'] = df['main_topics'].apply(lambda x: json.loads(x) if isinstance(x, str) and x else [])
        return df
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH)
        query = '''
        SELECT
            username,
            COALESCE(summary, '') AS summary,
            COALESCE(main_topics, '[]') AS main_topics,
            COALESCE(lead_score, 0) AS lead_score,
            COALESCE(post_count, 0) AS post_count,
            COALESCE(comment_count, 0) AS comment_count,
            account_created,
            link_karma,
            comment_karma,
            total_karma,
            last_activity,
            is_suspended,
            last_updated
        FROM users
        WHERE COALESCE(post_count, 0) + COALESCE(comment_count, 0) > 0
        ORDER BY lead_score DESC
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        df['main_topics'] = df['main_topics'].apply(lambda x: json.loads(x) if isinstance(x, str) and x else [])
        if 'summary' in df.columns:
            df['summary'] = df['summary'].astype(str).str.replace('\n', ' ', regex=False).str.replace('"', "'", regex=False)
        return df
    except Exception:
        return pd.DataFrame()


def update_user_analysis(username, data):
    df = get_user_analysis()
    if df.empty:
        df = pd.DataFrame([data])
    else:
        match_idx = df.index[df['username'] == username].tolist() if 'username' in df.columns else []
        if match_idx:
            for key, value in data.items():
                df.at[match_idx[0], key] = value
        else:
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    if 'main_topics' in df.columns:
        df_save = df.copy()
        df_save['main_topics'] = df_save['main_topics'].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    else:
        df_save = df
    df_save.to_csv(USER_ANALYSIS_FILE, index=False)
    return True
