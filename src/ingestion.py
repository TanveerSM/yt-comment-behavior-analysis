import requests
import time

YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/commentThreads"


def fetch_comments(api_key, video_id, page_token=None):
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 100
    }

    if page_token:
        params["pageToken"] = page_token

    try:
        response = requests.get(YOUTUBE_API_URL, params=params)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"API request error: {e}")
        return None


def fetch_all_comments(api_key, video_id):
    page_token = None
    all_items = []

    while True:
        data = fetch_comments(api_key, video_id, page_token)
        if not data:
            break

        items = data.get("items", [])
        all_items.extend(items)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_items


def parse_comment(item, video_id):
    snippet = item["snippet"]["topLevelComment"]["snippet"]

    return {
        "comment_id": item["id"],
        "video_id": video_id,
        "author_id": snippet.get("authorChannelId", {}).get("value"),
        "text": snippet.get("textDisplay", ""),
        "published_at": snippet.get("publishedAt"),
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }