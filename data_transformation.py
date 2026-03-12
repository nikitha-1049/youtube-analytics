import os
from datetime import datetime
import pandas as pd

CLEANED_FOLDER = "cleaned_data"
TRANSFORM_FOLDER = "transformed_data"

os.makedirs(TRANSFORM_FOLDER, exist_ok=True)

videos_file = os.path.join(CLEANED_FOLDER, "videos_cleaned.csv")
output_file = os.path.join(TRANSFORM_FOLDER, "videos_transformed.csv")

print(f"📂 Reading cleaned data from: {videos_file}")

# ---------- Load cleaned videos ----------
df_clean = pd.read_csv(videos_file)

# ---------- Check already transformed videos ----------
existing_transformed = set()
df_old_transformed = None

if os.path.exists(output_file):
    df_old_transformed = pd.read_csv(output_file)
    if "video_id" in df_old_transformed.columns:
        existing_transformed = set(df_old_transformed["video_id"])
        print(f"🔄 Already transformed videos: {len(existing_transformed)}")

# Filter only NEW videos that are not yet transformed
if "video_id" in df_clean.columns:
    df_new = df_clean[~df_clean["video_id"].isin(existing_transformed)].copy()
else:
    # Fallback: if no video_id (should not happen), transform all
    df_new = df_clean.copy()

if df_new.empty:
    print("✅ No new videos to transform. Keeping existing transformed file as is.")
    # Ensure file exists even on first run with no data
    if not os.path.exists(output_file) and df_old_transformed is not None:
        df_old_transformed.to_csv(output_file, index=False)
    print(f"📁 Current transformed file: {output_file}")
else:
    print(f"🆕 New videos to transform: {len(df_new)}")

    # ----------- Convert Date Columns Safely -----------
    df_new["published_date"] = pd.to_datetime(df_new["published_date"], errors="coerce")

    # If no dates → fill with today (avoids .dt errors)
    df_new["published_date"] = df_new["published_date"].fillna(datetime.now())

    # Published time handling
    df_new["published_time"] = pd.to_datetime(
    df_new["published_time"],
    format="%H:%M:%S",
    errors="coerce"
)
    df_new["hour"] = df_new["published_time"].dt.hour

    # ----------- Numeric Columns Safety -----------
    for col in ["views", "likes", "comments", "duration_sec"]:
        if col in df_new.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce").fillna(0).astype(int)
        else:
            df_new[col] = 0

    # ----------- Time-based Features -----------
    df_new["year"] = df_new["published_date"].dt.year
    df_new["month"] = df_new["published_date"].dt.month
    df_new["weekday"] = df_new["published_date"].dt.day_name()

    today = datetime.now()
    df_new["video_age_days"] = (today - df_new["published_date"]).dt.days
    df_new["video_age_days"] = df_new["video_age_days"].replace(0, 1)

    # ----------- Engagement & Ratios -----------
    df_new["engagement_rate"] = (df_new["likes"] + df_new["comments"]) / df_new["views"].replace(0, 1)
    df_new["like_ratio"] = df_new["likes"] / df_new["views"].replace(0, 1)
    df_new["comment_ratio"] = df_new["comments"] / df_new["views"].replace(0, 1)

    df_new["views_per_day"] = df_new["views"] / df_new["video_age_days"]

    # ----------- Duration Category -----------
    def duration_category(sec):
        try:
            sec = int(sec)
        except (ValueError, TypeError):
            return "Unknown"
        if sec < 60:
            return "Short"
        elif sec < 600:
            return "Medium"
        else:
            return "Long"

    df_new["duration_category"] = df_new["duration_sec"].apply(duration_category)

    # ----------- Title Word Count -----------
    df_new["title_word_count"] = df_new["title"].astype(str).apply(lambda x: len(x.split()))

    # ----------- Merge with old transformed (if any) -----------
    if df_old_transformed is not None:
        df_final = pd.concat([df_old_transformed, df_new], ignore_index=True)
    else:
        df_final = df_new

    # ----------- Save Final Output -----------
    df_final.to_csv(output_file, index=False)

    print("\n🎯 DATA TRANSFORMATION COMPLETED SUCCESSFULLY!")
    print(f"📁 Saved: {output_file}")