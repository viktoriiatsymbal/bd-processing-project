from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    cassandra: str


class PageItem(BaseModel):
    page_id: int
    domain: str
    page_title: str
    user_id: int | None = None
    user_name: str | None = None
    is_bot: bool
    created_at: str


class PagesResponse(BaseModel):
    count: int
    items: list[dict]


class DomainStatsItem(BaseModel):
    domain: str
    pages_last_hour: int
    unique_authors_last_hour: int
    avg_title_length: float | None = None
    latest_minute: str | None = None


class HourlyReportItem(BaseModel):
    domain: str
    hour_start: str
    hour_end: str
    pages_created: int
    unique_authors: int
    bot_pages: int
    human_pages: int
    bot_percent: float
    top_authors: list | str | None = None
