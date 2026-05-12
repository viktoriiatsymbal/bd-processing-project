def prepare_queries(session):
    return {
        "PAGE_DETAILS": session.prepare("""
            SELECT page_id, domain, page_title, user_id, user_name, is_bot, created_at,
                   namespace_id, performer_is_registered
            FROM page_details WHERE page_id = ?
        """),
        "PAGES_BY_USER": session.prepare("""
            SELECT user_id, created_at, page_id, domain, page_title, user_name, is_bot
            FROM pages_by_user WHERE user_id = ? LIMIT ?
        """),
        "PAGES_BY_DOMAIN": session.prepare("""
            SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot
            FROM pages_by_domain
            WHERE domain = ? AND created_at >= ? AND created_at <= ?
            LIMIT ?
        """),
        "LANGUAGE_ACTIVITY_BY_DOMAIN": session.prepare("""
            SELECT domain, minute_start, pages_created, unique_authors, avg_title_length, trend_vs_prev_minute
            FROM language_activity WHERE domain = ? AND minute_start >= ?
        """),
        "HOURLY_REPORT": session.prepare("""
            SELECT domain, hour_start, hour_end, pages_created, unique_authors,
                   bot_pages, human_pages, bot_percent, top_authors
            FROM hourly_activity_by_domain
            WHERE domain = ? AND hour_start >= ? AND hour_start < ?
        """),
        "EDITOR_PATTERNS": session.prepare("""
            SELECT user_id, user_name, pages_created, avg_minutes_between_pages,
                   active_hours, domains_count, dominant_domain, updated_at
            FROM editor_behavior_patterns
        """),
        "DOMAINS_SEEN": session.prepare("""
            SELECT bucket, domain, database, last_seen
            FROM domains_seen
            WHERE bucket = ?
        """),
    }
