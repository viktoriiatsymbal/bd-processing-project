import json
from datetime import datetime, timedelta

from fastapi import FastAPI, Query, HTTPException

from app.cassandra_client import get_cassandra_client
from app.queries import prepare_queries
from app.helpers import full_hour_range, parse_ts, row_to_dict
from app.schemas import HealthResponse



app = FastAPI(title="Wikipedia Analytics API")

cluster, cassandra_session = get_cassandra_client()
queries = prepare_queries(cassandra_session)



def json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)



@app.get("/health", response_model=HealthResponse)
def health():
    try:
        cassandra_session.execute("SELECT now() FROM system.local")
        cassandra_status = "connected"
    except Exception:
        cassandra_status = "disconnected"

    status = "ok" if cassandra_status == "connected" else "error"

    return {
        "status": status,
        "cassandra": cassandra_status,
    }


@app.get("/api/domains")
def domain_list():
    since = datetime.utcnow() - timedelta(hours=1)

    domain_rows = cassandra_session.execute(
        queries["DOMAINS_SEEN"],
        ["all"],
    )

    result = []

    for domain_row in domain_rows:
        domain = domain_row.domain

        activity_rows = list(
            cassandra_session.execute(
                queries["LANGUAGE_ACTIVITY_BY_DOMAIN"],
                [domain, since],
            )
        )

        pages_last_hour = int(sum(row.pages_created for row in activity_rows))

        unique_authors_last_hour = int(
            sum(row.unique_authors for row in activity_rows)
        )

        if pages_last_hour > 0:
            weighted_title_sum = sum(
                float(row.avg_title_length or 0) * int(row.pages_created)
                for row in activity_rows
            )
            avg_title_length = round(weighted_title_sum / pages_last_hour, 2)
        else:
            avg_title_length = 0.0

        latest_minute = (
            max(row.minute_start for row in activity_rows).isoformat()
            if activity_rows
            else None
        )

        result.append(
            {
                "domain": domain,
                "database": getattr(domain_row, "database", None),
                "last_seen": (
                    domain_row.last_seen.isoformat()
                    if getattr(domain_row, "last_seen", None)
                    else None
                ),
                "pages_last_hour": pages_last_hour,
                "unique_authors_last_hour": unique_authors_last_hour,
                "avg_title_length": avg_title_length,
                "latest_minute": latest_minute,
            }
        )

    result.sort(key=lambda x: x["pages_last_hour"], reverse=True)

    return result


@app.get("/api/users/{user_id}/pages")
def pages_by_user(
    user_id: int,
    limit: int = Query(100, ge=1, le=500),
):
    rows = cassandra_session.execute(
        queries["PAGES_BY_USER"],
        [user_id, limit],
    )
    items = [row_to_dict(row) for row in rows]
    return {"count": len(items), "items": items}


@app.get("/api/pages/{page_id}")
def page_details(page_id: int):
    rows = list(
        cassandra_session.execute(
            queries["PAGE_DETAILS"],
            [page_id],
        )
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Page not found")

    return row_to_dict(rows[0])


@app.get("/api/domains/{domain}/pages")
def pages_by_domain(
    domain: str,
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
    limit: int = Query(100, ge=1, le=500),
):
    start_ts = parse_ts(from_)
    end_ts = parse_ts(to)

    if start_ts > end_ts:
        raise HTTPException(status_code=400, detail="from must be before to")

    rows = cassandra_session.execute(
        queries["PAGES_BY_DOMAIN"],
        [domain.lower(), start_ts, end_ts, limit],
    )

    items = [row_to_dict(row) for row in rows]
    return {"count": len(items), "items": items}


@app.get("/api/reports/hourly")
def hourly_report(
    domain: str = Query(...),
    hours: int = Query(6, ge=1, le=24),
):
    start, end = full_hour_range(hours)
    domain = domain.lower()

    rows = cassandra_session.execute(
        queries["HOURLY_REPORT"],
        [domain, start, end],
    )

    result = [row_to_dict(row) for row in rows]

    return result


@app.get("/api/analytics/editor-patterns")
def editor_patterns(
    min_pages: int = Query(5, ge=1),
    limit: int = Query(100, ge=1, le=500),
):

    rows = cassandra_session.execute(queries["EDITOR_PATTERNS"])

    result = []

    for row in rows:
        item = row_to_dict(row)

        if item["pages_created"] > min_pages:
            result.append(item)

        if len(result) >= limit:
            break

    return result