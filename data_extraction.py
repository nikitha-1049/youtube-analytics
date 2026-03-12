import os
import json
from datetime import datetime
from googleapiclient.discovery import build

# ==================== CONFIG ====================
API_KEY = "AIzaSyCalxosgkvDF6j5B18wuvSYUda5QL5AOlo"   # <-- put your YouTube API key here

KEYWORDS = [
    "education",
    "cricket",
    "programming tutorials"
]

MAX_CHANNELS_PER_KEYWORD = 5    # Number of new channels per keyword

OUTPUT_FOLDER = "raw_data"
# =================================================

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def get_service():
    return build("youtube", "v3", developerKey=API_KEY)


def get_already_extracted_channels():
    """Check raw_data folder and collect channel_ids stored already"""
    extracted_ids = set()

    if not os.path.exists(OUTPUT_FOLDER):
        return extracted_ids

    for filename in os.listdir(OUTPUT_FOLDER):
        if filename.endswith(".json"):
            fpath = os.path.join(OUTPUT_FOLDER, filename)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "channel_id" in data:
                        extracted_ids.add(data["channel_id"])
            except Exception:
                pass

    return extracted_ids


def search_channels_by_keyword(youtube, keyword, already_seen):
    """Search channels using YouTube Search Module (search().list)"""
    collected = []
    next_page_token = None

    while len(collected) < MAX_CHANNELS_PER_KEYWORD:
        req = youtube.search().list(
            part="snippet",
            q=keyword,
            type="channel",
            maxResults=50,
            pageToken=next_page_token,
        )
        res = req.execute()
        items = res.get("items", [])
        if not items:
            break

        for item in items:
            cid = item.get("id", {}).get("channelId")
            if cid and cid not in already_seen:
                already_seen.add(cid)
                collected.append(cid)
                print(f"➕ New channel identified: {cid}  (keyword: {keyword})")
                if len(collected) >= MAX_CHANNELS_PER_KEYWORD:
                    break

        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    return collected


def fetch_channel_details(youtube, channel_id):
    """Fetch channel snippet, statistics, uploads playlist"""
    req = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    res = req.execute()
    items = res.get("items", [])
    if not items:
        raise ValueError("Channel not found: " + channel_id)
    return items[0]


def fetch_video_ids(youtube, playlist_id):
    video_ids = []
    next_token = None

    while True:
        req = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_token
        )
        res = req.execute()

        for item in res.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        next_token = res.get("nextPageToken")
        if not next_token:
            break

    return video_ids


def fetch_videos_data(youtube, video_ids):
    videos = []
    for i in range(0, len(video_ids), 50):
        req = youtube.videos().list(
            part="snippet,contentDetails,statistics,liveStreamingDetails",
            id=",".join(video_ids[i:i + 50])
        )
        videos.extend(req.execute().get("items", []))
    return videos


def save_raw_json(channel_name, payload):
    safe_name = channel_name.replace(" ", "_").replace("/", "_")
    filepath = os.path.join(OUTPUT_FOLDER, f"raw_{safe_name}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)
    print(f"💾 Saved → {filepath}")


if __name__ == "__main__":
    youtube = get_service()

    already_extracted = get_already_extracted_channels()
    print(f"📝 Channels already extracted earlier: {len(already_extracted)}")

    all_discovered_channels = []
    seen_channel_ids = set(already_extracted)

    # 🔍 Search channels for each keyword
    for kw in KEYWORDS:
        print(f"\n🔎 Searching for new channels on keyword: '{kw}' ...")
        new_channels = search_channels_by_keyword(youtube, kw, seen_channel_ids)
        print(f"✔ Newly discovered channels for '{kw}': {len(new_channels)}")

        for cid in new_channels:
            all_discovered_channels.append((cid, kw))

    print(f"\n📊 Total NEW channels to extract: {len(all_discovered_channels)}")

    # 📌 Extract raw data for each new channel
    for cid, kw in all_discovered_channels:
        try:
            print(f"\n📌 Extracting channel: {cid} (keyword: '{kw}') ...")

            ch_data = fetch_channel_details(youtube, cid)
            cname = ch_data["snippet"]["title"]
            playlist_id = ch_data["contentDetails"]["relatedPlaylists"]["uploads"]

            video_ids = fetch_video_ids(youtube, playlist_id)
            print(f"📹 Total Videos Found: {len(video_ids)}")

            videos_data = fetch_videos_data(youtube, video_ids)

            payload = {
                "source_keyword": kw,
                "channel_id": cid,
                "channel_name": cname,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "raw_channel": ch_data,
                "raw_videos": videos_data,
            }

            save_raw_json(cname, payload)

        except Exception as e:
            print(f"❌ Error extracting channel: {cid}")
            print("Reason:", str(e))
            print("➡️ Skipping...")

    print("\n🎯 RAW DATA EXTRACTION FINISHED SUCCESSFULLY ✔")