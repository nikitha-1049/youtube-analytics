import pandas as pd
import mysql.connector
try:
    from mysql.connector import pooling
except Exception:
    pooling = None
from functools import lru_cache
from datetime import datetime
from data_test import ensure_channel_data

DB_CONFIG = {
    "user": "root",
    "password": "Nikki66$$",
    "host": "localhost",
    "database": "youtube_analytics",
}

_POOL = None

def get_connection():
    global _POOL
    if _POOL is None and pooling:
        try:
            _POOL = pooling.MySQLConnectionPool(
                pool_name="youtube_pool",
                pool_size=5,
                **DB_CONFIG
            )
        except Exception:
            _POOL = None

    if _POOL:
        return _POOL.get_connection()

    # fallback to direct connect
    return mysql.connector.connect(**DB_CONFIG)


# ================= LOAD =================
def load_channel_data(channel_id):
    conn = get_connection()

    ch = pd.read_sql(
        "SELECT channel_id, channel_name, subscribers, total_views, total_videos FROM channels WHERE channel_id=%s",
        conn,
        params=[channel_id]
    )

    df = pd.read_sql(
        "SELECT video_id, title, published_date, views, likes, comments, duration_sec, engagement_rate, views_per_day, weekday, hour, year FROM videos WHERE channel_id=%s",
        conn,
        params=[channel_id]
    )

    conn.close()

    # -------- FIX: never crash dashboard --------
    if ch.empty:
        raise RuntimeError("Channel not found")

    if df.empty:
        df = pd.DataFrame(columns=[
            "video_id","title","published_date","views","likes","comments",
            "duration_sec","engagement_rate","views_per_day",
            "weekday","hour","year"
        ])

    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"])

    return ch, df


@lru_cache(maxsize=100)
def load_data_cached(channel_id):
    return load_channel_data(channel_id)


# ================= KPI =================
def get_kpis(channel_id):
    ch, df = load_data_cached(channel_id)
    monthly_views = df["views_per_day"].mean() * 30 if len(df) else 0
    cpm = 2.5  # dollars per 1000 views
    est_monthly_earnings = (monthly_views / 1000) * cpm

    return {
        "Channel Name": ch.iloc[0]["channel_name"],
        "Subscribers": int(ch.iloc[0]["subscribers"]),
        "Total Views": int(ch.iloc[0]["total_views"]),
        "Total Videos": int(ch.iloc[0]["total_videos"]),
        "Avg Video Length (min)": round((df["duration_sec"].mean() / 60) if len(df) else 0, 2),
        "Avg Engagement Rate": round((df["engagement_rate"].mean() * 100) if len(df) else 0, 2),
        "Avg Views / Day": round(df["views_per_day"].mean() if len(df) else 0, 2),
        "Est. Monthly Earnings ($)": round(est_monthly_earnings, 2)
    }

def get_monthly_report(channel_id, year, month):

    conn = get_connection()

    df = pd.read_sql(
        """
        SELECT 
            title,
            published_date,
            views,
            likes,
            comments
        FROM videos
        WHERE channel_id=%s
        """,
        conn,
        params=[channel_id]
    )
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")
    df = df.dropna(subset=["published_date"])



    conn.close()

    if df.empty:
        return {}

    df["published_date"] = pd.to_datetime(df["published_date"])
    df["month"] = df["published_date"].dt.month
    df["year"] = df["published_date"].dt.year

    month_df = df[(df["year"] == int(year)) & (df["month"] == int(month))]

    if month_df.empty:
        return {"empty": True}

    month_df["engagement"] = (month_df["likes"] + month_df["comments"]) / month_df["views"].replace(0, 1)

    report = {
        "total_videos": int(len(month_df)),
        "total_views": int(month_df["views"].sum()),
        "avg_views": float(month_df["views"].mean()),
        "top_videos": month_df.sort_values("views", ascending=False).head(5)[["title", "views"]].to_dict(orient="records"),
        "worst_videos": month_df.sort_values("views").head(5)[["title", "views"]].to_dict(orient="records"),
        "best_engagement": month_df.sort_values("engagement", ascending=False).head(5)[["title", "engagement"]].to_dict(orient="records"),
    }

    return report

def get_trends(channel_id):
    import pandas as pd
    import mysql.connector

    DB_CONFIG = {
        "user": "root",
        "password": "Nikki66$$",
        "host": "localhost",
        "database": "youtube_analytics"
    }

    conn = mysql.connector.connect(**DB_CONFIG)

    df = pd.read_sql(
        """
        SELECT
            title,
            published_date,
            views,
            likes,
            comments
        FROM videos
        WHERE channel_id=%s
        ORDER BY published_date ASC
        """,
        conn,
        params=[channel_id]
    )

    conn.close()

    if df.empty:
        return {"improving": [], "declining": []}

    df["published_date"] = pd.to_datetime(df["published_date"])
    df = df.sort_values("published_date")

    df["engagement"] = (
        (df["likes"] + df["comments"]) /
        df["views"].replace(0, 1)
    )

    improving = []
    declining = []

    for i in range(1, len(df)):

        prev = df.iloc[i - 1]
        now = df.iloc[i]

        if now["views"] > prev["views"] and now["engagement"] > prev["engagement"]:
            improving.append({
                "title": now["title"],
                "views": int(now["views"]),
                "engagement": round(float(now["engagement"]) * 100, 2)
            })

        if now["views"] < prev["views"] and now["engagement"] < prev["engagement"]:
            declining.append({
                "title": now["title"],
                "views": int(now["views"]),
                "engagement": round(float(now["engagement"]) * 100, 2)
            })

    return {
        "improving": improving[:10],
        "declining": declining[:10]
    }

# ================= GROWTH =================
def get_year_vs_views(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "values": []}

    g = df.groupby("year")["views"].sum().reset_index()
    return {"labels": g["year"].tolist(), "values": g["views"].tolist()}


def get_growth_trend(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "values": []}

    df = df.sort_values("published_date")

    return {
        "labels": df["published_date"].dt.strftime("%Y-%m-%d").tolist(),
        "values": df["views"].tolist(),
    }


def get_daily_performance(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "views": [], "change": []}

    d = (
        df.assign(date=df["published_date"].dt.date)
        .groupby("date")
        .agg(views=("views", "sum"))
        .reset_index()
    )

    d["change"] = d["views"].diff().fillna(0)

    return {
        "labels": d["date"].astype(str).tolist(),
        "views": d["views"].tolist(),
        "change": d["change"].tolist(),
    }


# ================= LIKES vs COMMENTS =================
def get_likes_comments(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"points": []}

    df["likes"] = df["likes"].fillna(0)
    df["comments"] = df["comments"].fillna(0)

    # 🔥 remove videos with no engagement at all
    df = df[(df["likes"] > 0) | (df["comments"] > 0)]

    return {
        "points": [
            {"x": int(l), "y": int(c)}
            for l, c in zip(df["likes"], df["comments"])
        ]
    }


# ================= TOP VIDEOS =================
def get_top_videos(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"titles": [], "views": [], "engagement": []}

    top = df.sort_values("views", ascending=False).head(10)

    return {
    "titles": top["title"].tolist(),
    "views": top["views"].tolist(),
    "engagement": top["engagement_rate"].tolist(),
    "video_ids": top["video_id"].tolist()
}



# ================= POSTING TIME =================
def get_best_posting_day(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "values": []}

    d = df.groupby("weekday")["views_per_day"].mean().reset_index()
    return {"labels": d["weekday"].tolist(), "values": d["views_per_day"].round(2).tolist()}


def get_best_posting_hour(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "values": []}

    d = df.groupby("hour")["views_per_day"].mean().reset_index()

    return {
        "labels": d["hour"].astype(str).tolist(),
        "values": d["views_per_day"].round(2).tolist()
    }


def get_monthly_uploads_vs_views(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "uploads": [], "views": []}

    m = (
        df.groupby(df["published_date"].dt.to_period("M"))
        .agg(uploads=("video_id", "count"), views=("views", "sum"))
        .reset_index()
    )

    return {
        "labels": m["published_date"].astype(str).tolist(),
        "uploads": m["uploads"].tolist(),
        "views": m["views"].tolist(),
    }


# ================= ENGAGEMENT =================
def get_engagement_distribution(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"labels": [], "values": [], "avg_engagement": 0}

    qv = df["views"].quantile(0.75)
    qe = df["engagement_rate"].quantile(0.75)

    def label(row):
        if row["views"] >= qv and row["engagement_rate"] >= qe:
            return "Viral Content"
        if row["engagement_rate"] >= qe:
            return "Low Views + High Engagement"
        if row["views"] >= qv:
            return "High Views + Low Engagement"
        return "Underperforming Content"

    c = df.apply(label, axis=1).value_counts()

    return {
        "labels": c.index.tolist(),
        "values": c.values.tolist(),
        "avg_engagement": round(df["engagement_rate"].mean() * 100, 2)
    }


# ================= CONTENT TRENDS =================
def get_content_trends(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        return {"improving": {"titles": [], "values": []},
                "declining": {"titles": [], "values": []}}

    improving = df.sort_values("views_per_day", ascending=False).head(5)
    declining = df.sort_values("views_per_day").head(5)

    return {
        "improving": {
            "titles": improving["title"].tolist(),
            "values": improving["views_per_day"].round(2).tolist(),
        },
        "declining": {
            "titles": declining["title"].tolist(),
            "values": declining["views_per_day"].round(2).tolist(),
        },
    }


# ================= REALTIME =================
def get_realtime_metrics(channel_id):
    _, df = load_data_cached(channel_id)

    if df.empty:
        print("REALTIME → df is EMPTY")
        return {"views_last_hour": 0, "engagement_24h": 0, "trending_score": 0}

    print("REALTIME → total rows:", len(df))

    df["published_date"] = pd.to_datetime(df["published_date"])

    window = datetime.now() - pd.Timedelta(days=7)

    last = df[df["published_date"] >= window]

    print("REALTIME → rows in window:", len(last))
    print(last[["title", "published_date", "views_per_day", "engagement_rate"]].head(10))

    if last.empty:
        return {"views_last_hour": 0, "engagement_24h": 0, "trending_score": 0}

    views_hour = last["views_per_day"].sum() / (7 * 24)
    engagement = last["engagement_rate"].mean()

    return {
        "views_last_hour": round(views_hour, 2),
        "engagement_24h": round(engagement, 4),
        "trending_score": round(views_hour * engagement, 2),
    }




# ================= RECOMMENDATIONS =================
def get_recommendations(channel_id):
    _, df = load_data_cached(channel_id)

    recs = []

    if not df.empty:
        try:
            best_day = df.groupby("weekday")["views_per_day"].mean().idxmax()
            recs.append(f"Post more on {best_day} for better reach")
        except:
            pass

        try:
            best_hour = df.groupby("hour")["views_per_day"].mean().idxmax()
            recs.append(f"Upload around {best_hour}:00 for best performance")
        except:
            pass

        try:
            avg_len = df["duration_sec"].mean() / 60
            recs.append(f"Ideal video length ~ {round(avg_len,1)} minutes")
        except:
            pass

        declining = df.sort_values("views_per_day").head(3)
        if not declining.empty:
            recs.append("Improve thumbnails & titles for low-performing videos")

    if not recs:
        recs.append("Need more videos for analysis")

    return recs


# ================= UNIFIED =================
def analyze_channel(channel_id):
    """
    Central dashboard aggregator.
    """

    # --- get KPIs first (guaranteed to exist) ---
    kpis = get_kpis(channel_id)

    # Try to detect channel name in KPIs
    channel_name = (
        kpis.get("channel_name")
        or kpis.get("Channel Name")
        or kpis.get("name")
        or "Unknown Channel"
    )

    return {
        "channel_id": channel_id,
        "channel_name": channel_name,

        "kpis": kpis,

        "year_vs_views": get_year_vs_views(channel_id),
        "growth_trend": get_growth_trend(channel_id),
        "daily_performance": get_daily_performance(channel_id),
        "top_videos": get_top_videos(channel_id),
        "likes_comments": get_likes_comments(channel_id),

        "best_posting_day": get_best_posting_day(channel_id),
        "best_posting_hour": get_best_posting_hour(channel_id),

        "monthly_uploads_vs_views": get_monthly_uploads_vs_views(channel_id),
        "engagement_distribution": get_engagement_distribution(channel_id),
        "content_trends": get_content_trends(channel_id),

        "realtime": get_realtime_metrics(channel_id),
        "recommendations": get_recommendations(channel_id),
    }