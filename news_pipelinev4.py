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

SQLITE_DB = "news_oltp.db"          # OLTP: Stores all structured + raw JSON feed fields
DUCKDB_DB = "news_olap.duckdb"      # OLAP: Analytical warehouse with JSON querying
OUTPUT_FILE = "daily_digest.md"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

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
    "word_count": "INTEGER",
    "raw_entry_json": "TEXT",
    "date_added": "TEXT",
}


# ==========================================
# 1. OLTP LAYER (SQLite): Auto-Migrating Ingestion
# ==========================================
def init_oltp():
    """Initializes SQLite and automatically adds any missing columns via ALTER TABLE."""
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()

    cols_def = ",\n            ".join(f"{col} {dtype}" for col, dtype in EXPECTED_COLUMNS.items())
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS articles (
            {cols_def}
        )
    """)

    cursor.execute("PRAGMA table_info(articles)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    for col_name, col_type in EXPECTED_COLUMNS.items():
        if col_name not in existing_cols:
            safe_type = col_type.replace("PRIMARY KEY", "").strip()
            print(f"Migrating OLTP schema: Adding missing column '{col_name}' ({safe_type})...")
            cursor.execute(f"ALTER TABLE articles ADD COLUMN {col_name} {safe_type}")

    conn.commit()
    return conn


def is_duplicate_oltp(conn, url):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM articles WHERE url = ?", (url,))
    return cursor.fetchone() is not None


def insert_article_oltp(conn, record):
    cursor = conn.cursor()
    col_names = ", ".join(EXPECTED_COLUMNS.keys())
    placeholders = ", ".join(["?"] * len(EXPECTED_COLUMNS))
    cursor.execute(
        f"INSERT OR IGNORE INTO articles ({col_names}) VALUES ({placeholders})",
        record,
    )
    conn.commit()


# ==========================================
# 2. OLAP LAYER (DuckDB): Sync & Analytics
# ==========================================
def sync_and_analyze_olap():
    """Syncs all structured and JSON fields into DuckDB for analytical queries."""
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
# 3. EXTRACTION & NORMALIZATION HELPERS
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
# 4. REPORTING & NOTIFICATIONS (Telegram / Discord)
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
            lines.append(f"{item['snippet']}\n")
            lines.append("---\n")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def send_notifications(compiled_news, total_new):
    """Sends a concise headline summary + attaches daily_digest.md to Telegram and/or Discord."""
    if total_new == 0:
        print("No new stories today; skipping notifications.")
        return

    today_str = datetime.date.today().strftime("%B %d, %Y")
    summary_lines = [f"📰 **Daily News Digest — {today_str}** ({total_new} new stories)\n"]
    for category, articles in compiled_news.items():
        if not articles:
            continue
        summary_lines.append(f"**{category}**")
        for item in articles[:5]:
            summary_lines.append(f"• [{item['title']}]({item['link']})")
        summary_lines.append("")

    message_text = "\n".join(summary_lines)[:1900]

    # --- 1. Telegram Bot Notification (Summary + Document Attachment) ---
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if tg_token and tg_chat_id:
        try:
            msg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
            requests.post(
                msg_url,
                json={
                    "chat_id": tg_chat_id,
                    "text": message_text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            doc_url = f"https://api.telegram.org/bot{tg_token}/sendDocument"
            with open(OUTPUT_FILE, "rb") as f:
                requests.post(
                    doc_url,
                    data={"chat_id": tg_chat_id, "caption": f"Full Digest ({today_str})"},
                    files={"document": (OUTPUT_FILE, f, "text/markdown")},
                    timeout=15,
                )
            print("Sent digest to Telegram.")
        except Exception as e:
            print(f"Telegram notification failed: {e}")

    # --- 2. Discord Webhook Notification (Summary + File Attachment) ---
    discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if discord_webhook:
        try:
            with open(OUTPUT_FILE, "rb") as f:
                requests.post(
                    discord_webhook,
                    data={"content": message_text},
                    files={"file": (OUTPUT_FILE, f, "text/markdown")},
                    timeout=15,
                )
            print("Sent digest to Discord.")
        except Exception as e:
            print(f"Discord notification failed: {e}")


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
                word_count = len(full_text.split())

                raw_entry_json = json.dumps(serialize_safe(dict(entry)), ensure_ascii=False)

                insert_article_oltp(
                    oltp_conn,
                    (
                        link, entry_id, title, author, category, feed_url, feed_title,
                        published_raw, published_iso, updated_raw, thumbnail_url,
                        json.dumps(tags_list), json.dumps(serialize_safe(media_list)),
                        clean_html(rss_summary), full_text, snippet, word_count,
                        raw_entry_json, today_iso,
                    ),
                )

                compiled_news[category].append({
                    "title": title,
                    "link": link,
                    "feed_title": feed_title,
                    "author": author,
                    "published": published_raw or "Unknown Date",
                    "tags": tags_list,
                    "snippet": snippet,
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
