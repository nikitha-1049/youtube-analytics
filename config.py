import os
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = "tubebuddy_secret"

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Nikki66$$",
    "db": "TubeBuddy"
}

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

FIXED_CHANNEL_ID = "UCq4145N44S36U991mD3h67Q"
