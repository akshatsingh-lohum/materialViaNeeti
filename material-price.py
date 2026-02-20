"""
Material Price Tracker - Zoho Cliq Integration
Fetches files from S3 and uploads to Zoho Cliq users.

Environment Variables Required:
- ZOHO_REFRESH_TOKEN: Zoho OAuth refresh token
- ZOHO_CLIENT_ID: Zoho OAuth client ID
- ZOHO_CLIENT_SECRET: Zoho OAuth client secret
- ZOHO_CLIQ_USER_IDS: Comma-separated list of user IDs (e.g., "60019117005,60019117006")
- ZOHO_BOT_NAME: Name of the Zoho Cliq bot
- AWS_S3_BUCKET: S3 bucket name
- AWS_S3_FILE_KEY: S3 file key/path
- AWS_REGION: AWS region (default: ap-south-1)
"""

import os
import json
import tempfile
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# Configuration from Environment Variables
# ============================================================================

ZOHO_REFRESH_TOKEN = os.environ.get("ZOHO_REFRESH_TOKEN")
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_CLIQ_USER_IDS = os.environ.get("ZOHO_CLIQ_USER_IDS", "")  # Comma-separated
ZOHO_BOT_NAME = os.environ.get("ZOHO_BOT_NAME", "Metal Price Tracker")

AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET")
AWS_S3_FILE_KEY = os.environ.get("AWS_S3_FILE_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

# Zoho API endpoints
ZOHO_TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"
ZOHO_CLIQ_FILES_URL = "https://cliq.zoho.in/api/v2/bots/policychatbotv/files"


# ============================================================================
# Zoho OAuth Token Management
# ============================================================================

def get_zoho_access_token() -> str:
    """
    Refresh the Zoho OAuth access token using the refresh token.
    Returns the new access token.
    """
    if not all([ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET]):
        raise ValueError("Missing Zoho OAuth credentials in environment variables")

    payload = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(ZOHO_TOKEN_URL, data=payload, headers=headers)
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data.get("access_token")

    if not access_token:
        raise ValueError(f"Failed to get access token: {token_data}")

    print("✅ Successfully refreshed Zoho access token")
    return access_token


# ============================================================================
# AWS S3 Integration
# ============================================================================

def download_file_from_s3(bucket: str, key: str, local_path: str) -> str:
    """
    Download a file from S3 to a local path.
    Returns the local file path.
    """
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    try:
        s3_client.download_file(bucket, key, local_path)
        print(f"✅ Downloaded file from S3: s3://{bucket}/{key}")
        return local_path
    except ClientError as e:
        raise Exception(f"Failed to download from S3: {e}")


def get_user_ids() -> list:
    """
    Get user IDs from environment variable (comma-separated list).
    """
    if not ZOHO_CLIQ_USER_IDS:
        raise ValueError("No user IDs configured. Set ZOHO_CLIQ_USER_IDS environment variable")

    user_ids = [uid.strip() for uid in ZOHO_CLIQ_USER_IDS.split(",") if uid.strip()]
    if not user_ids:
        raise ValueError("ZOHO_CLIQ_USER_IDS is empty")

    print(f"📋 Using {len(user_ids)} user IDs from environment variable")
    return user_ids


# ============================================================================
# Zoho Cliq File Upload
# ============================================================================

def upload_file_to_zoho_cliq(
    access_token: str,
    file_path: str,
    user_id: str,
    bot_name: str,
    comments: list = None,
) -> dict:
    """
    Upload a file to Zoho Cliq via the bot API.
    """
    url = ZOHO_CLIQ_FILES_URL.format(bot_name=bot_name)
    print(f"   🔗 Request URL: {url}")

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
    }

    if comments is None:
        comments = ["Server image"]

    with open(file_path, "rb") as f:
        files = {
            "file": (os.path.basename(file_path), f),
        }
        data = {
            "comments": json.dumps(comments),
            "user_id": user_id,
            "bot_name": bot_name,
        }

        response = requests.post(url, headers=headers, files=files, data=data)

    if response.status_code == 200:
        print(f"✅ Successfully uploaded file to user {user_id}")
        return response.json()
    else:
        print(f"❌ Failed to upload file to user {user_id}: {response.status_code}")
        print(f"   Response: {response.text}")
        return {"error": response.text, "status_code": response.status_code}


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """
    Main function to orchestrate the file upload process.
    1. Refresh Zoho access token
    2. Download file from S3
    3. Get user IDs from environment variable
    4. Upload file to each user via Zoho Cliq
    """
    print("=" * 60)
    print("🚀 Starting Material Price Tracker - Zoho Cliq Upload")
    print("=" * 60)

    # Step 1: Get Zoho access token
    print("\n📝 Step 1: Refreshing Zoho access token...")
    access_token = get_zoho_access_token()

    # Step 2: Download file from S3
    print("\n📥 Step 2: Downloading file from S3...")
    if not AWS_S3_BUCKET or not AWS_S3_FILE_KEY:
        raise ValueError("AWS_S3_BUCKET and AWS_S3_FILE_KEY must be set")

    # Create a temporary file to store the downloaded content
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(AWS_S3_FILE_KEY)[1]) as tmp_file:
        local_file_path = tmp_file.name

    download_file_from_s3(AWS_S3_BUCKET, AWS_S3_FILE_KEY, local_file_path)

    # Step 3: Get user IDs
    print("\n👥 Step 3: Fetching user IDs...")
    user_ids = get_user_ids()
    print(f"   Found {len(user_ids)} users to notify")

    # Step 4: Upload file to each user
    print(f"\n📤 Step 4: Uploading file to {len(user_ids)} users...")
    results = []
    for user_id in user_ids:
        result = upload_file_to_zoho_cliq(
            access_token=access_token,
            file_path=local_file_path,
            user_id=user_id,
            bot_name=ZOHO_BOT_NAME,
            comments=["Metal Price Update"],
        )
        results.append({"user_id": user_id, "result": result})

    # Cleanup temporary file
    try:
        os.unlink(local_file_path)
        print("\n🧹 Cleaned up temporary file")
    except Exception as e:
        print(f"⚠️  Warning: Could not delete temp file: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("📊 Summary")
    print("=" * 60)
    successful = sum(1 for r in results if "error" not in r.get("result", {}))
    print(f"✅ Successful uploads: {successful}/{len(results)}")
    print("🏁 Done!")

    return results


if __name__ == "__main__":
    main()
