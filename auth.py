from flask import Blueprint, session, redirect, request, url_for
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import os

auth = Blueprint("auth", __name__)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

GOOGLE_CLIENT_SECRETS_FILE = os.path.join("app_secrets", "client_secret.json")

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/youtube.readonly"
]


def creds_to_dict(creds):
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }


# ---------------- LOGIN ----------------
@auth.route("/google-login")
def login():
    flow = Flow.from_client_secrets_file(
        GOOGLE_CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri="http://127.0.0.1:5000/auth/callback"
    )

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )

    session["state"] = state
    return redirect(authorization_url)


# ---------------- CALLBACK ----------------
@auth.route("/auth/callback")
def callback():
    state = session.get("state")

    flow = Flow.from_client_secrets_file(
        GOOGLE_CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        state=state,
        redirect_uri=url_for("auth.callback", _external=True)
    )

    flow.fetch_token(authorization_response=request.url)

    creds = flow.credentials
    session["credentials"] = creds_to_dict(creds)

    oauth_service = build("oauth2", "v2", credentials=creds)
    profile = oauth_service.userinfo().get().execute()

    session["user"] = {
        "name": profile.get("name"),
        "picture": profile.get("picture"),
        "email": profile.get("email"),
        "google": True
    }

    return redirect("/dashboard")


# ---------------- LOGOUT ----------------
@auth.route("/logout")
def logout():
    session.clear()
    return redirect("/")


# ---------------- AUTHENTICATED YOUTUBE SERVICE ----------------
def get_authenticated_service():
    if "credentials" not in session:
        raise RuntimeError("User not authenticated")

    creds = Credentials(**session["credentials"])

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        session["credentials"] = creds_to_dict(creds)

    return build("youtube", "v3", credentials=creds)