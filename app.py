import os
import sys
import json


cwd = os.path.abspath(os.path.dirname(__file__))
if cwd in sys.path:
    sys.path.remove(cwd)
try:
    import pandas  # ensure numpy/pandas load without local package shadowing
except Exception as e:
    print("Pre-import pandas error:", e)
sys.path.insert(0, cwd)

from flask import Flask, jsonify, render_template, session, redirect, url_for, request
from channel_resolver import resolve_channel_id
from data_test import ensure_channel_data
from analytics import get_trends
from auth import auth, get_authenticated_service

from analytics import (
    get_kpis,
    get_year_vs_views,
    get_growth_trend,
    get_daily_performance,
    get_top_videos,
    get_best_posting_day,
    get_best_posting_hour,
    get_monthly_uploads_vs_views,
    get_engagement_distribution,
    get_content_trends,
    get_realtime_metrics,
    get_recommendations,
    get_likes_comments,
    analyze_channel,
)

app = Flask(__name__)
app.secret_key = "CHANGE_ME_SECRET"
app.register_blueprint(auth)
# Serve static files with a long max age (clients will cache aggressively)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000  # 1 year

# Optional: enable gzip/deflate compression if Flask-Compress is installed
try:
    from flask_compress import Compress
    Compress(app)
    print("Flask-Compress enabled")
except Exception:
    print("Flask-Compress not installed; skipping response compression")


# ---------------- AUTH GUARD ----------------
def login_required(fn):
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect("/login?error=Please login to continue")
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper


# ---------------- HOME ----------------
@app.route("/")
def landing():
    return render_template("landing.html")

@app.route("/login")
def show_login():
    return render_template("login.html")


@app.route("/session")
def session_user():
    return session.get("user", {})

@app.route("/dashboard")
def dashboard():
    return render_template("index.html")
@app.route("/protected")
@login_required
def protected():
    return render_template("feature.html")



# ---------------- SAFE API WRAPPER ----------------
def safe_api(fn, query):
    try:
        # always resolve first
        channel_id = resolve_channel_id(query)

        if not channel_id:
            resp = jsonify({"error": "Channel not found"})
            resp.headers["Cache-Control"] = "public, max-age=5"
            return resp, 404

        ensure_channel_data(channel_id)
        # call the function and return JSON with a short cache header
        resp = jsonify(fn(channel_id))
        resp.headers["Cache-Control"] = "public, max-age=10"
        return resp

    except Exception as e:
        print("❌ ERROR:", e)
        resp = jsonify({"error": "Failed"})
        resp.headers["Cache-Control"] = "no-store"
        return resp, 500


# ---------------- AI HELPERS ----------------
def generate_with_gemini(prompt, api_key):
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        # Get available models
        models = genai.list_models()

        # Pick the first text model available
        text_model = None
        for m in models:
            if "generateContent" in getattr(m, "supported_generation_methods", []):
                text_model = m.name
                break

        if not text_model:
            raise RuntimeError("No compatible Gemini text model found for this account.")

        print("USING MODEL:", text_model)

        model = genai.GenerativeModel(text_model)
        resp = model.generate_content(prompt)

        if hasattr(resp, "text"):
            return resp.text

        return str(resp)

    except Exception as e:
        print("Gemini generation failed:", e)
        raise


def try_generate(prompt, context=''):
    # Prefer GEMINI, then fallback deterministic response
    gemini_key = os.environ.get('GEMINI_API_KEY')
    try:
        from app_secrets.GEMINI_API_KEY import GEMINI_API_KEY as _gk
        gemini_key = gemini_key or _gk
    except Exception:
        pass

    if gemini_key:
        try:
            return generate_with_gemini(prompt, gemini_key)
        except Exception:
            pass

    # fallback deterministic response
    if context:
        return f"Fallback result based on context: {context}"
    return "Fallback result: no AI available"



# -------- INDIVIDUAL APIs --------
@app.route("/api/kpis/<query>")
def kpis(query): return safe_api(get_kpis, query)

@app.route("/api/year-vs-views/<query>")
def year_vs_views(query): return safe_api(get_year_vs_views, query)

@app.route("/api/growth-trend/<query>")
def growth_trend(query): return safe_api(get_growth_trend, query)

@app.route("/api/daily-performance/<query>")
def daily_performance(query): return safe_api(get_daily_performance, query)

@app.route("/api/top-videos/<query>")
def top_videos(query): return safe_api(get_top_videos, query)

@app.route("/api/best-posting-day/<query>")
def best_posting_day(query): return safe_api(get_best_posting_day, query)

@app.route("/api/best-posting-hour/<query>")
def best_posting_hour(query): return safe_api(get_best_posting_hour, query)

@app.route("/api/monthly-uploads/<query>")
def monthly_uploads(query): return safe_api(get_monthly_uploads_vs_views, query)

@app.route("/api/engagement/<query>")
def engagement(query):
    return safe_api(get_engagement_distribution, query)


# ----------- MY CHANNEL (Google connect option) -----------
@app.route("/api/my-channel")
def my_channel():
    if "credentials" not in session:
        return jsonify({"error": "not_authenticated"}), 401

    try:
        youtube = get_authenticated_service()

        data = youtube.channels().list(
            part="id",
            mine=True
        ).execute()

        if not data["items"]:
            return jsonify({"error": "no_channel"})

        channel_id = data["items"][0]["id"]
        return jsonify({"channel_id": channel_id})

    except Exception as e:
        print("MY CHANNEL ERROR:", e)
        return jsonify({"error": "failed"})


@app.route("/api/content-trends/<query>")
def content_trends(query): return safe_api(get_content_trends, query)


@app.route("/api/realtime/<query>")
def realtime(query): return safe_api(get_realtime_metrics, query)


@app.route("/api/recommendations/<query>")
@login_required
def recommendations(query): return safe_api(get_recommendations, query)


@app.route("/api/trends/<query>")
def trends(query):
    try:
        channel_id = resolve_channel_id(query)
        ensure_channel_data(channel_id)
        return jsonify(get_trends(channel_id))
    except Exception as e:
        print("TRENDS ERROR:", e)
        return jsonify({"error": "failed"}), 500
@app.route("/pricing")
def pricing():
    return render_template("pricing.html")

@app.route('/feature/<name>', methods=['GET', 'POST'])
def feature_page(name):

    public_features = {"daily-ideas"}

    if name not in public_features and "user" not in session:
        return redirect("/login?error=Please login to continue")

    mapping = {
        "daily-ideas": (
            "Daily Video Ideas",
            "Give me 10 short daily video ideas about tech tutorials.",
            "bg-daily-ideas"
        ),
        "ai-title": (
            "Title Ideas",
            "Create 10 catchy YouTube video titles for:",
            "bg-title"
        ),
        "ai-content": (
            "Content Assistant",
            "Create a short script or outline for:",
            "bg-content"
        ),
        "ai-description": (
            "Video Description",
            "Create a YouTube video description for:",
            "bg-description"
        ),
        "ai-channel-names": (
            "Channel Name Ideas",
            "Suggest 10 creative YouTube channel names for:",
            "bg-channel-names"
        ),
    }

    title, example, bg_class = mapping.get(
        name,
        (name.replace("-", " ").title(), "", "bg-default")
    )

    if request.method == "POST":
        prompt = (request.form.get("prompt") or "").strip()
        final_prompt = prompt if prompt else example

        try:
            result = try_generate(final_prompt, context=final_prompt)
        except Exception:
            result = "Something went wrong while generating the content."

        return render_template(
            "feature.html",
            title=title,
            example=example,
            result=result,
            bg_class=bg_class
        )

    return render_template(
        "feature.html",
        title=title,
        example=example,
        bg_class=bg_class
    )

# ---------------- NAV ENDPOINTS ----------------
@app.route("/blogs")
def blogs():
    return render_template("blogs.html")

@app.route("/api/nav/items")
def nav_items():
    # Static, canonical nav items — front-end can request these and render dynamically
    items = [
        {"label": "Home", "href": "/dashboard"},
        {"label": "Blogs", "href": "/blogs"},
        {"label": "Daily ideas", "href": "/feature/daily-ideas"},
        {"label": "AI Title generator", "href": "/feature/ai-title"},
        {"label": "AI Content generator", "href": "/feature/ai-content"},
        {"label": "AI Description generator", "href": "/feature/ai-description"},
        {"label": "AI Channel name suggestions", "href": "/feature/ai-channel-names"},
        {"label": "Pricing", "href": "/pricing"}
    ]
    return jsonify({"items": items})


@app.route("/api/nav/suggest", methods=["POST"])
def nav_suggest():
    # Accepts JSON { "query": "..." }
    body = request.get_json(silent=True) or {}
    query = (body.get("query") or "").strip()

    # Prefer Gemini
    gemini_key = os.environ.get('GEMINI_API_KEY')
    try:
        from app_secrets.GEMINI_API_KEY import GEMINI_API_KEY as _gk
        gemini_key = gemini_key or _gk
    except Exception:
        pass

    if gemini_key:
        try:
            text = generate_with_gemini(f"Suggest up to 6 concise navbar items for a YouTube analytics dashboard given: {query}\nReturn as a newline-separated list.", gemini_key)
            items = [line.strip(' -•\t') for line in text.splitlines() if line.strip()]
            return jsonify({"items": items})
        except Exception as e:
            print('Gemini integration failed:', e)

    # If Gemini failed, fall back to the simple heuristic below

    # Fallback heuristic suggestions
    base = ["Home", "Dashboard", "Reports", "Recommendations", "AI Tools", "Settings", "Help"]
    if query:
        # create a contextual item from query
        candidate = query.title()
        if candidate not in base:
            base.insert(2, candidate)

    return jsonify({"items": base})


@app.route("/api/likes-comments/<query>")
def likes_comments(query):
    try:
        channel_id = resolve_channel_id(query)
        ensure_channel_data(channel_id)
        return jsonify(get_likes_comments(channel_id))
    except:
        return jsonify({"points": []})


@app.route("/api/monthly-report/<query>")
def monthly_report(query):
    try:
        channel_id = resolve_channel_id(query)
        ensure_channel_data(channel_id)

        year = request.args.get("year")
        month = request.args.get("month")

        from analytics import get_monthly_report
        return jsonify(get_monthly_report(channel_id, year, month))

    except Exception as e:
        print("MONTHLY REPORT ERROR:", e)
        return jsonify({"error": "failed"}), 500


# ------------ COMPARISON ------------
@app.route("/api/compare")
@login_required
def compare():
    primary = request.args.get("a")
    secondary = request.args.get("b")

    return jsonify({
        "primary": analyze_channel(resolve_channel_id(primary)),
        "secondary": analyze_channel(resolve_channel_id(secondary))
    })

from googleapiclient.discovery import build
from app_secrets.API_KEY import YOUTUBE_API_KEY

def get_youtube_client():
    return build(
        "youtube",
        "v3",
        developerKey=YOUTUBE_API_KEY,
        cache_discovery=False
    )

from youtube_etl import get_channel_profile
@app.route("/api/channel/<query>")
def api_channel(query):
    youtube = get_youtube_client()

    channel_id = resolve_channel_id(query)

    if not channel_id:
        return jsonify({"error": "Channel not found"}), 404

    data = get_channel_profile(youtube, channel_id)
    return jsonify(data)

# ------------ LOCAL LOGIN SYSTEM ------------
USERS_FILE = os.path.join("app_secrets", "users.json")

def load_users():
    if not os.path.exists(USERS_FILE):
        return {"users": []}
    with open(USERS_FILE, "r") as f:
        return json.load(f)

def save_users(data):
    with open(USERS_FILE, "w") as f:
        json.dump(data, f, indent=4)


@app.route("/local-login", methods=["POST"])
def local_login():
    email = request.form["email"]
    password = request.form["password"]

    data = load_users()

    for u in data["users"]:
        if u["email"] == email and u["password"] == password:
            session["user"] = {
            "name": email.split("@")[0],
            "email": email,
            "picture": None,
            "google": False
            }
            return redirect("/dashboard")



    return redirect("/login?error=Invalid email or password")



@app.route("/local-signup", methods=["POST"])
def local_signup():
    email = request.form["email"]
    password = request.form["password"]

    data = load_users()

    for u in data["users"]:
        if u["email"] == email:
            return redirect("/login?error=User already exists")


    data["users"].append({"email": email, "password": password})
    save_users(data)

    return redirect("/login?success=Account created. Please login.")
@app.route("/debug/models")
def debug_models():
    try:
        from google import genai
        api_key = os.environ.get("GEMINI_API_KEY")

        client = genai.Client(api_key=api_key)

        models = client.models.list()

        names = [getattr(m, "name", str(m)) for m in models]
        print("\nAVAILABLE MODELS:\n", names, "\n")

        return {"models": names}
    except Exception as e:
        print("MODEL LIST ERROR:", e)
        return {"error": str(e)}, 500




if __name__ == "__main__":
    app.run(debug=True)