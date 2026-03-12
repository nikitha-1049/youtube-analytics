import mysql.connector
from youtube_etl import etl_channel
from channel_resolver import resolve_channel_id

DB_CONFIG = {
    "user": "root",
    "password": "YOUR PASSWORD",
    "host": "localhost",
    "database": "youtube_analytics"
}


def ensure_channel_data(query):
    """
    Accepts: channel name / handle / ID
    Returns: validated channel_id
    Downloads only if not already stored.
    """

    # 1️⃣ always resolve to channel_id FIRST
    channel_id = resolve_channel_id(query)

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*) FROM channels WHERE channel_id=%s",
        (channel_id,)
    )

    exists = cur.fetchone()[0]

    cur.close()
    conn.close()

    # 2️⃣ If new → trigger ETL once only
    if not exists:
        print(f"📥 Downloading fresh channel data → {channel_id}")
        etl_channel(channel_id)
    else:
        print(f"✔ Using cached DB data → {channel_id}")


    return channel_id
