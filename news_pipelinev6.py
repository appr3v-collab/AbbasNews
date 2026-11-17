import os
import re
import json
import time
import sqlite3
import datetime
import requests
import feedparser
import trafilatura
import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

FEEDS = {
    "Technology": [
        "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "https://techcrunch.com/feed/",
    ],
    "World News": [
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://feeds.npr.org/1001/rss.xml",
    ],
}

SQLITE_DB = "news_oltp.db"          # OLTP: Transactional ingestion & deduplication
DUCKDB_DB = "news_olap.duckdb"      # OLAP: Analytical warehouse & reporting
OUTPUT_FILE = "daily_digest.md"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

GEMINI_MODELS = [
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash",
]

EXPECTED_COLUMNS = {
    "url": "TEXT PRIMARY KEY",
    "entry_id": "TEXT",
    "title": "TEXT",
    "author": "TEXT",
    "category": "TEXT",
    "source_feed": "TEXT",
    "feed_title": "TEXT",
    "published_raw": "TEXT",
    "published_iso": "TEXT",
    "updated_raw": "TEXT",
    "thumbnail_url": "TEXT",
    "tags_json": "TEXT",
    "media_json": "TEXT",
    "rss_summary": "TEXT",
    "full_text": "TEXT",
    "snippet": "TEXT",
    "ai_summary": "TEXT",
    "word_count": "INTEGER",
    "raw_entry_json": "TEXT",
    "date_added": "TEXT",
}


# ==========================================
# 1. OLTP LAYER (SQLite): Auto-Migrating Schema
# ==========================================
def init_oltp():
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()

    cols_sql = ", ".join([f"{col} {dtype}" for col, dtype in EXPECTED_COLUMNS.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS articles ({cols_sql})")

    cursor.execute("PRAGMA table_info(articles)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    for col, dtype in EXPECTED_COLUMNS.items():
        if col not in existing_cols:
            clean_dtype = dtype.replace(" PRIMARY KEY", "")
            cursor.execute(f"ALTER TABLE articles ADD COLUMN {col} {clean_dtype}")

    conn.commit()
    return conn


def is_duplicate_oltp(conn, url):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM articles WHERE url = ?", (url,))
    return cursor.fetchone() is not None


def insert_article_oltp(conn, record):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO articles (
            url, entry_id, title, author, category, source_feed, feed_title,
            published_raw, published_iso, updated_raw, thumbnail_url,
            tags_json, media_json, rss_summary, full_text, snippet,
            ai_summary, word_count, raw_entry_json, date_added
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        record,
    )
    conn.commit()


# ==========================================
# 2. OLAP LAYER (DuckDB): Sync & Analytics
# ==========================================
def sync_and_analyze_olap():
    duck_conn = duckdb.connect(DUCKDB_DB)
    duck_conn.execute("INSTALL sqlite; LOAD sqlite;")

    duck_conn.execute(f"""
        CREATE OR REPLACE TABLE fact_articles AS
        SELECT
            url,
            entry_id,
            title,
            author,
            category,
            source_feed,
            feed_title,
            published_raw,
            TRY_CAST(published_iso AS TIMESTAMP) AS published_ts,
            updated_raw,
            thumbnail_url,
            CAST(tags_json AS JSON) AS tags,
            CAST(media_json AS JSON) AS media,
            rss_summary,
            full_text,
            snippet,
            ai_summary,
            CAST(word_count AS INTEGER) AS word_count,
            CAST(raw_entry_json AS JSON) AS raw_entry,
            CAST(date_added AS DATE) AS date_added
        FROM sqlite_scan('{SQLITE_DB}', 'articles')
    """)

    analytics = duck_conn.execute("""
        SELECT
            category,
            COUNT(*) AS total_articles,
            COUNT(*) FILTER (WHERE date_added = CURRENT_DATE) AS today_articles,
            ROUND(AVG(word_count), 1) AS avg_full_words
        FROM fact_articles
        GROUP BY category
        ORDER BY total_articles DESC
    """).fetchall()

    duck_conn.close()
    return analytics


# ==========================================
# 3. EXTRACTION & AI SUMMARIZATION HELPERS
# ==========================================
def serialize_safe(obj):
    if isinstance(obj, time.struct_time):
        return datetime.datetime(*obj[:6], tzinfo=datetime.timezone.utc).isoformat()
    if isinstance(obj, dict):
        return {str(k): serialize_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [serialize_safe(i) for i in obj]
    return obj


def clean_html(raw_html):
    clean = re.sub(r"<.*?>", "", raw_html or "")
    return " ".join(clean.split())


def resolve_url(url):
    if "news.google.com" not in url:
        return url
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return resp.url
    except Exception:
        return url


def extract_full_text(url, fallback_summary):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            text = trafilatura.extract(response.text, include_comments=False, include_tables=False)
            if text:
                clean_text = " ".join(text.split())
                snippet = clean_text[:350] + "..." if len(clean_text) > 350 else clean_text
                return clean_text, snippet
    except Exception:
        pass
    clean_fallback = clean_html(fallback_summary)
    return clean_fallback, clean_fallback


def summarize_with_gemini(title, full_text, fallback_snippet):
    """Generates a 2-bullet summary via header-based Gemini auth with automatic model fallback."""
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key or len(full_text.split()) < 40:
        return fallback_snippet

    req_headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    }
    prompt = (
        f"Summarize this news article in exactly 2 concise, factual bullet points.\n\n"
        f"Headline: {title}\n"
        f"Article: {full_text[:4000]}"
    )
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    for model in GEMINI_MODELS:
        endpoint = "https://generativelanguage.googleapis.com/v1beta/models/" + model + ":generateContent"
        try:
            resp = requests.post(endpoint, headers=req_headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                summary = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                time.sleep(2)
                return summary
            elif resp.status_code == 404:
                continue
            else:
                print(f"  -> Gemini API HTTP {resp.status_code} on {model}; using local snippet.")
                break
        except Exception as e:
            print(f"  -> Gemini summary skipped ({e}); using local snippet.")
            break

    return fallback_snippet


def fetch_feed(feed_url):
    try:
        response = requests.get(feed_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        feed_title = feed.feed.get("title", feed_url)
        print(f"  -> [{feed_title}] Found {len(feed.entries)} entries.")
        return feed_title, feed.entries
    except Exception as e:
        print(f"  -> ERROR fetching {feed_url}: {e}")
        return feed_url, []


# ==========================================
# 4. REPORTING & NOTIFICATIONS
# ==========================================
def write_markdown(compiled_news, total_new, analytics):
    today_str = datetime.date.today().strftime("%B %d, %Y")
    lines = [
        f"# Daily News Compilation — {today_str}",
        f"**New Stories Today:** {total_new}\n",
        "## Warehouse Analytics (DuckDB OLAP)",
        "| Category | New Today | All-Time Total | Avg Article Words |",
        "| :--- | :---: | :---: | :---: |",
    ]

    for category, total_all, today_count, avg_words in analytics:
        lines.append(f"| {category} | {today_count} | {total_all} | {avg_words} |")

    lines.append("\n---\n")

    for category, articles in compiled_news.items():
        if not articles:
            continue
        lines.append(f"## {category}\n")
        for item in articles:
            lines.append(f"### [{item['title']}]({item['link']})")
            meta = f"*Source: {item['feed_title']} | Published: {item['published']}*"
            if item["author"]:
                meta += f" | *By {item['author']}*"
            lines.append(f"{meta}\n")
            if item["tags"]:
                lines.append(f"**Tags:** {', '.join(item['tags'])}\n")
            lines.append(f"{item['summary']}\n")
            lines.append("---\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_summary_message(compiled_news, total_new):
    today_str = datetime.date.today().strftime("%b %d, %Y")
    lines = [f"Daily News Digest ({today_str}) — {total_new} new stories:\n"]
    for category, articles in compiled_news.items():
        if not articles:
            continue
        lines.append(f"*{category}*")
        for item in articles[:3]:
            lines.append(f"- {item['title']}\n  {item['link']}")
        lines.append("")
    return "\n".join(lines).strip()


def send_telegram(compiled_news, total_new):
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("  -> Skipping Telegram (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set).")
        return

    base_url = "https://api.telegram.org/bot" + token
    summary_text = build_summary_message(compiled_news, total_new)
    try:
        msg_resp = requests.post(
            base_url + "/sendMessage",
            data={"chat_id": chat_id, "text": summary_text[:4000], "disable_web_page_preview": True},
            timeout=15,
        )
        msg_resp.raise_for_status()

        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            with open(OUTPUT_FILE, "rb") as doc:
                doc_resp = requests.post(
                    base_url + "/sendDocument",
                    data={"chat_id": chat_id, "caption": "Full Daily News Digest"},
                    files={"document": doc},
                    timeout=20,
                )
                doc_resp.raise_for_status()
        print("Sent digest to Telegram.")
    except Exception as e:
        print(f"  -> ERROR sending Telegram notification: {e}")


def send_discord(compiled_news, total_new):
    webhook_url = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    if not webhook_url:
        print("  -> Skipping Discord (DISCORD_WEBHOOK_URL not set).")
        return

    summary_text = build_summary_message(compiled_news, total_new)
    try:
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            with open(OUTPUT_FILE, "rb") as doc:
                requests.post(
                    webhook_url,
                    data={"content": summary_text[:1900]},
                    files={"file": (OUTPUT_FILE, doc, "text/markdown")},
                    timeout=20,
                ).raise_for_status()
        else:
            requests.post(webhook_url, json={"content": summary_text[:1900]}, timeout=15).raise_for_status()
        print("Sent digest to Discord.")
    except Exception as e:
        print(f"  -> ERROR sending Discord notification: {e}")


def send_notifications(compiled_news, total_new):
    if total_new == 0:
        print("No new stories today; skipping chat notifications.")
        return
    send_telegram(compiled_news, total_new)
    send_discord(compiled_news, total_new)


# ==========================================
# 5. PIPELINE ORCHESTRATION
# ==========================================
def run_pipeline():
    oltp_conn = init_oltp()
    compiled_news = {}
    total_new = 0
    today_iso = datetime.date.today().isoformat()

    for category, urls in FEEDS.items():
        compiled_news[category] = []

        for feed_url in urls:
            print(f"Fetching: {feed_url}")
            feed_title, entries = fetch_feed(feed_url)

            for entry in entries:
                raw_link = entry.get("link", "")
                if not raw_link:
                    continue

                link = resolve_url(raw_link)
                if is_duplicate_oltp(oltp_conn, link):
                    continue

                entry_id = entry.get("id", link)
                title = entry.get("title", "No Title")
                author = entry.get("author", "")
                published_raw = entry.get("published", "")
                updated_raw = entry.get("updated", "")

                pub_parsed = entry.get("published_parsed")
                published_iso = (
                    datetime.datetime(*pub_parsed[:6], tzinfo=datetime.timezone.utc).isoformat()
                    if pub_parsed else ""
                )

                thumbnails = entry.get("media_thumbnail", [])
                thumbnail_url = thumbnails[0].get("url", "") if thumbnails else ""

                tags_list = [t.get("term", "") for t in entry.get("tags", []) if t.get("term")]
                media_list = (
                    entry.get("media_content", [])
                    + entry.get("enclosures", [])
                    + thumbnails
                )

                rss_summary = entry.get("summary", entry.get("description", ""))
                full_text, snippet = extract_full_text(link, rss_summary)
                ai_summary = summarize_with_gemini(title, full_text, snippet)
                word_count = len(full_text.split())
                raw_entry_json = json.dumps(serialize_safe(dict(entry)), ensure_ascii=False)

                insert_article_oltp(
                    oltp_conn,
                    (
                        link, entry_id, title, author, category, feed_url, feed_title,
                        published_raw, published_iso, updated_raw, thumbnail_url,
                        json.dumps(tags_list), json.dumps(serialize_safe(media_list)),
                        clean_html(rss_summary), full_text, snippet, ai_summary,
                        word_count, raw_entry_json, today_iso,
                    ),
                )

                compiled_news[category].append({
                    "title": title,
                    "link": link,
                    "feed_title": feed_title,
                    "author": author,
                    "published": published_raw or "Unknown Date",
                    "tags": tags_list,
                    "summary": ai_summary,
                })
                total_new += 1

    oltp_conn.close()

    print("Syncing OLTP -> OLAP and running DuckDB analytics...")
    analytics = sync_and_analyze_olap()
    write_markdown(compiled_news, total_new, analytics)
    send_notifications(compiled_news, total_new)
    print(f"Done! {total_new} new articles ingested into {SQLITE_DB} and analyzed in {DUCKDB_DB}.")


if __name__ == "__main__":
    run_pipeline()
