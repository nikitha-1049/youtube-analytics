import os
from datetime import datetime
import mysql.connector
import pandas as pd

# ================== DB CONFIG ==================
DB_CONFIG = {
    "user": "root",
    "password": "YOUR PASSWORD",
    "host": "localhost",
    "database": "youtube_analytics",
    "auth_plugin": "mysql_native_password",
    "charset": "utf8mb4"
}

CHANNELS_CSV = os.path.join("cleaned_data", "channels_cleaned.csv")
VIDEOS_CSV = os.path.join("transformed_data", "videos_transformed.csv")
# =================================================


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_channels():
    print(f"\n📂 Loading channels from: {CHANNELS_CSV}")
    df = pd.read_csv(CHANNELS_CSV, encoding="utf-8")

    # Remove emojis and non-ASCII characters
    df["channel_name"] = df["channel_name"].astype(str).str.encode("ascii", "ignore").str.decode("ascii")

    # Convert Date Column
    df["created_date"] = (
        pd.to_datetime(df["publishedAt"], errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df = df.where(pd.notnull(df), None)

    conn = get_connection()
    cursor = conn.cursor()

    # Load existing channels to SKIP duplicates
    cursor.execute("SELECT channel_id FROM channels")
    existing_channels = set([row[0] for row in cursor.fetchall()])
    print(f"🔄 Already in DB: {len(existing_channels)} channels")

    insert_count = 0
    insert_sql = """
        INSERT INTO channels (
            channel_id, channel_name, subscribers, total_views, total_videos, created_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            subscribers = VALUES(subscribers),
            total_views = VALUES(total_views),
            total_videos = VALUES(total_videos),
            created_date = VALUES(created_date);
    """

    for _, row in df.iterrows():
        if row["channel_id"] in existing_channels:
            continue

        cursor.execute(insert_sql, (
            row["channel_id"],
            row["channel_name"],
            int(row["subscribers"]),
            int(row["total_views"]),
            int(row["total_videos"]),
            row["created_date"]
        ))
        insert_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Channels inserted/updated: {insert_count}")


def load_videos():
    print(f"\n📂 Loading videos from: {VIDEOS_CSV}")
    df = pd.read_csv(VIDEOS_CSV, encoding="utf-8")

    # Remove emojis
    df["title"] = df["title"].astype(str).str.encode("ascii", "ignore").str.decode("ascii")

    # Convert date field
    df["published_date"] = (
        pd.to_datetime(df["published_date"], errors="coerce")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df = df.where(pd.notnull(df), None)

    conn = get_connection()
    cursor = conn.cursor()

    # Load existing videos to skip duplicates
    cursor.execute("SELECT video_id FROM videos")
    existing_videos = set([row[0] for row in cursor.fetchall()])
    print(f"🔄 Already in DB: {len(existing_videos)} videos")

    insert_sql = """
        INSERT INTO videos (
            video_id, channel_id, title, published_date,
            views, likes, comments, duration_sec, duration_category,
            engagement_rate, views_per_day, title_word_count,
            year, month, weekday, hour, video_age_days
        )
        VALUES (%s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            views = VALUES(views),
            likes = VALUES(likes),
            comments = VALUES(comments),
            engagement_rate = VALUES(engagement_rate),
            views_per_day = VALUES(views_per_day);
    """

    insert_count = 0

    for _, row in df.iterrows():
        if row["video_id"] in existing_videos:
            continue

        cursor.execute(insert_sql, (
            row["video_id"],
            row["channel_id"],
            row["title"],
            row["published_date"],
            int(row["views"]),
            int(row["likes"]),
            int(row["comments"]),
            int(row["duration_sec"]),
            row["duration_category"],
            float(row["engagement_rate"]),
            float(row["views_per_day"]),
            int(row["title_word_count"]),
            int(row["year"]),
            int(row["month"]),
            row["weekday"],
            int(row["hour"]),
            int(row["video_age_days"]),
        ))
        insert_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"🎯 Videos inserted/updated: {insert_count}")


if __name__ == "__main__":
    load_channels()
    load_videos()

    print("\n🚀 MySQL data load completed successfully!")
