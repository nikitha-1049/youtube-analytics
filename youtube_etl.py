from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
from dateutil import parser
import mysql.connector
import isodate
from datetime import datetime
import time

from app_secrets.API_KEY import YOUTUBE_API_KEY
import os
import ssl
import httplib2
# ---- FIX SSL ERRORS ----
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

http = httplib2.Http(timeout=30, ca_certs=None, disable_ssl_certificate_validation=True)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


DB_CONFIG = {
    "user": "root",
    "password": "YOUTUBE PASSWORD",
    "host": "localhost",
    "database": "youtube_analytics"
}


def get_connection():
    return mysql.connector.connect(
        **DB_CONFIG,
        autocommit=True,
        connection_timeout=30
    )


def parse_duration(d):
    try:
        return int(isodate.parse_duration(d).total_seconds())
    except:
        return 0


def etl_channel(channel_id, retries=1):

    yt = build(
    "youtube",
    "v3",
    developerKey=YOUTUBE_API_KEY,
    cache_discovery=False,
    http=http
    )


    for attempt in range(1, retries + 1):
        try:
            print(f"📡 ETL attempt {attempt} for {channel_id}")

            conn = get_connection()
            cur = conn.cursor()

            # -------- CHANNEL DATA --------
            res = yt.channels().list(
                part="snippet,statistics,contentDetails",
                id=channel_id
            ).execute()

            if not res.get("items"):
                raise RuntimeError("Channel not found")

            ch = res["items"][0]
            sn, st = ch["snippet"], ch["statistics"]

            cur.execute("""
                INSERT INTO channels
                (channel_id, channel_name, subscribers, total_views, total_videos, created_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    subscribers = VALUES(subscribers),
                    total_views = VALUES(total_views),
                    total_videos = VALUES(total_videos)
            """, (
                channel_id,
                sn["title"],
                int(st.get("subscriberCount", 0)),
                int(st.get("viewCount", 0)),
                int(st.get("videoCount", 0)),
                parser.parse(sn["publishedAt"]).replace(tzinfo=None)
            ))

            # -------- VIDEOS --------
            playlist = ch["contentDetails"]["relatedPlaylists"]["uploads"]

            vids = []
            token = None

            while True:
                r = yt.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist,
                    maxResults=50,
                    pageToken=token
                ).execute()

                vids += [i["contentDetails"]["videoId"] for i in r["items"]]
                token = r.get("nextPageToken")
                if not token:
                    break

            for i in range(0, len(vids), 50):
                data = yt.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(vids[i:i + 50])
                ).execute()

                for v in data.get("items", []):
                    sn, st, cd = v["snippet"], v.get("statistics", {}), v["contentDetails"]

                    dt = parser.parse(sn["publishedAt"]).replace(tzinfo=None)
                    views = int(st.get("viewCount", 0))
                    likes = int(st.get("likeCount", 0))
                    comments = int(st.get("commentCount", 0))
                    dur = parse_duration(cd["duration"])
                    age = max((datetime.now() - dt).days, 1)
                    eng = (likes + comments) / views if views else 0

                    cur.execute("""
                        INSERT INTO videos
                        (video_id, channel_id, title, published_date, views, likes, comments,
                         duration_sec, duration_category, engagement_rate, views_per_day,
                         title_word_count, year, month, weekday, hour, video_age_days)
                        VALUES
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            views = VALUES(views),
                            likes = VALUES(likes),
                            comments = VALUES(comments),
                            engagement_rate = VALUES(engagement_rate),
                            views_per_day = VALUES(views_per_day)
                    """, (
                        v["id"], channel_id, sn["title"], dt,
                        views, likes, comments, dur,
                        "Short" if dur < 60 else "Medium" if dur < 600 else "Long",
                        round(eng, 4), round(views / age, 2),
                        len(sn["title"].split()),
                        dt.year, dt.month, dt.strftime("%A"), dt.hour, age
                    ))

            conn.close()
            print("✅ ETL complete")
            break

        except Exception as e:
            print("⚠️ ETL error:", e)
            if attempt == retries:
                raise
            time.sleep(3)
def get_channel_profile(youtube, channel_id):
    res = youtube.channels().list(
        part="snippet",
        id=channel_id
    ).execute()

    if not res.get("items"):
        return None

    c = res["items"][0]

    return {
        "channel_name": c["snippet"]["title"],
        "avatar": c["snippet"]["thumbnails"]["high"]["url"]

    }
