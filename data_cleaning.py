import os
import json
import pandas as pd
from dateutil import parser
import isodate

RAW_FOLDER = "raw_data"
CLEANED_FOLDER = "cleaned_data"

os.makedirs(CLEANED_FOLDER, exist_ok=True)

channels_file = os.path.join(CLEANED_FOLDER, "channels_cleaned.csv")
videos_file = os.path.join(CLEANED_FOLDER, "videos_cleaned.csv")

channels_list = []
videos_list = []

# 🛑 Load existing cleaned channels/videos to avoid duplicates
existing_channels = set()
existing_videos = set()

if os.path.exists(channels_file):
    old_channels_df = pd.read_csv(channels_file)
    if "channel_id" in old_channels_df.columns:
        existing_channels = set(old_channels_df["channel_id"])
        print(f"🔄 Already cleaned channels: {len(existing_channels)}")

if os.path.exists(videos_file):
    old_videos_df = pd.read_csv(videos_file)
    if "video_id" in old_videos_df.columns:
        existing_videos = set(old_videos_df["video_id"])
        print(f"🔄 Already cleaned videos: {len(existing_videos)}")


def parse_duration(duration_str):
    try:
        return int(isodate.parse_duration(duration_str).total_seconds())
    except Exception:
        return None


# ========== PROCESS RAW JSON FILES ==========
for file in os.listdir(RAW_FOLDER):
    if not file.endswith(".json"):
        continue

    filepath = os.path.join(RAW_FOLDER, file)
    print(f"\n📂 Processing: {file}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_channel = data["raw_channel"]
    raw_videos = data["raw_videos"]

    channel_id = raw_channel.get("id")

    # 🛑 Skip if channel is already cleaned
    if channel_id in existing_channels:
        print(f"⏩ Skipped (already cleaned channel): {channel_id}")
        continue

    snippet = raw_channel.get("snippet", {})
    stats = raw_channel.get("statistics", {})

    # ------- CHANNEL ROW -------
    channel_row = {
        "channel_id": channel_id,
        "channel_name": snippet.get("title"),
        "subscribers": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "total_videos": int(stats.get("videoCount", 0)),
        "publishedAt": snippet.get("publishedAt"),
    }

    channels_list.append(channel_row)

    # ------- VIDEO ROWS -------
    for v in raw_videos:
        video_id = v.get("id")

        # 🛑 Skip duplicate videos
        if video_id in existing_videos:
            continue

        sn = v.get("snippet", {})
        st = v.get("statistics", {})
        cd = v.get("contentDetails", {})

        published = sn.get("publishedAt")
        dt = parser.parse(published) if published else None

        # Consistent date/time strings
        if dt:
            pub_date_str = dt.strftime("%Y-%m-%d")    # YYYY-MM-DD
            pub_time_str = dt.strftime("%H:%M:%S")    # HH:MM:SS
            weekday_str = dt.strftime("%A")
        else:
            pub_date_str = None
            pub_time_str = None
            weekday_str = None

        views = int(st.get("viewCount", 0))
        likes = int(st.get("likeCount", 0)) if "likeCount" in st else 0
        comments = int(st.get("commentCount", 0)) if "commentCount" in st else 0

        duration = parse_duration(cd.get("duration"))
        if duration is not None:
            if duration < 60:
                duration_cat = "Short"
            elif duration < 600:
                duration_cat = "Medium"
            else:
                duration_cat = "Long"
        else:
            duration_cat = "Unknown"

        engagement = (likes + comments) / views if views > 0 else 0

        videos_list.append({
            "channel_id": channel_id,
            "channel_name": snippet.get("title"),
            "video_id": video_id,
            "title": sn.get("title"),
            "views": views,
            "likes": likes,
            "comments": comments,
            "published_date": pub_date_str,
            "published_time": pub_time_str,
            "weekday": weekday_str,
            "duration_sec": duration,
            "duration_category": duration_cat,
            "engagement_rate": round(engagement, 4),
        })


# ========== MERGE WITH EXISTING CLEANED DATA (IF ANY) ==========

# Channels
if os.path.exists(channels_file):
    df_old_ch = old_channels_df
    df_new_ch = pd.DataFrame(channels_list)
    df_channels = pd.concat([df_old_ch, df_new_ch], ignore_index=True)
else:
    df_channels = pd.DataFrame(channels_list)

# Videos
if os.path.exists(videos_file):
    df_old_v = old_videos_df
    df_new_v = pd.DataFrame(videos_list)
    df_videos = pd.concat([df_old_v, df_new_v], ignore_index=True)
else:
    df_videos = pd.DataFrame(videos_list)

# ========== SAVE UPDATED RESULTS ==========
df_channels.to_csv(channels_file, index=False)
df_videos.to_csv(videos_file, index=False)

print("\n🎯 CLEANING COMPLETED SUCCESSFULLY!")
print("📁 Updated files:")
print(f"- {channels_file}")
print(f"- {videos_file}")