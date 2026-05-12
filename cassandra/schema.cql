CREATE KEYSPACE IF NOT EXISTS wiki_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_analytics;

CREATE TABLE IF NOT EXISTS page_details (
    page_id bigint PRIMARY KEY,
    domain text,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    created_at timestamp,
    namespace_id int,
    performer_is_registered boolean
);

CREATE TABLE IF NOT EXISTS pages_by_user (
    user_id bigint,
    created_at timestamp,
    page_id bigint,
    domain text,
    page_title text,
    user_name text,
    is_bot boolean,
    PRIMARY KEY ((user_id), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC, page_id ASC);

CREATE TABLE IF NOT EXISTS pages_by_domain (
    domain text,
    created_at timestamp,
    page_id bigint,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    PRIMARY KEY ((domain), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC, page_id ASC);

CREATE TABLE IF NOT EXISTS language_activity (
    domain text,
    minute_start timestamp,
    pages_created bigint,
    unique_authors bigint,
    avg_title_length double,
    trend_vs_prev_minute double,
    PRIMARY KEY ((domain), minute_start)
) WITH CLUSTERING ORDER BY (minute_start DESC);

CREATE TABLE IF NOT EXISTS bot_activity_metrics (
    domain text,
    minute_start timestamp,
    bot_pages bigint,
    human_pages bigint,
    bot_percent double,
    top_bots text,
    top_humans text,
    PRIMARY KEY ((domain), minute_start)
) WITH CLUSTERING ORDER BY (minute_start DESC);

CREATE TABLE IF NOT EXISTS hourly_activity_by_domain (
    domain text,
    hour_start timestamp,
    hour_end timestamp,
    pages_created bigint,
    unique_authors bigint,
    bot_pages bigint,
    human_pages bigint,
    bot_percent double,
    top_authors text,
    pages_by_namespace text,
    PRIMARY KEY ((domain), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);

CREATE TABLE IF NOT EXISTS editor_behavior_patterns (
    user_id bigint PRIMARY KEY,
    user_name text,
    pages_created bigint,
    avg_minutes_between_pages double,
    active_hours text,
    domains_count bigint,
    dominant_domain text,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS domains_seen (
    bucket text,
    domain text,
    database text,
    last_seen timestamp,
    PRIMARY KEY ((bucket), domain)
);
