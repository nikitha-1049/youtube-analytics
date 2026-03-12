from googleapiclient.discovery import build
from app_secrets.API_KEY import YOUTUBE_API_KEY
import mysql.connector

DB_CONFIG = {
    "user": "root",
    "password": "Nikki66$$",
    "host": "localhost",
    "database": "youtube_analytics"
}

yt = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)


def get_db():
    return mysql.connector.connect(**DB_CONFIG)


def resolve_channel_id(query: str):
    if not query:
        return None
    print("Query",query)

    q = query.strip()
    q_lower = q.lower()

    # 1️⃣ check DB first
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT channel_id FROM channels WHERE LOWER(channel_name)=%s OR channel_id=%s",
        (q_lower, q)
    )
    row = cur.fetchone()

    conn.close()

    if row:
        print("DB CACHE HIT:", row[0])
        return row[0]

    # 2️⃣ block URLs
    if "youtube.com" in q_lower or "youtu.be" in q_lower:
        return None

    # 3️⃣ channel ID
    if q_lower.startswith("uc") and len(q) >= 20:
        return q

    # 4️⃣ handle
    if q.startswith("@"):
        q_lower=q_lower[1:]
        try:
            res = yt.channels().list(
                part="id",
                forHandle=q_lower
            ).execute()

            items = res.get("items", [])
            if items:
                return items[0]["id"]
        except:
            pass

    # 5️⃣ search by name
    try:
        res = yt.search().list(
            part="snippet",
            q=q,
            type="channel",
            maxResults=1
        ).execute()

        items = res.get("items", [])
        if items:
            return items[0]["snippet"]["channelId"]

    except Exception as e:
        print("RESOLVER ERROR:", e)

    return None