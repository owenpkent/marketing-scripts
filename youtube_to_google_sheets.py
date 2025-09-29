#!/usr/bin/env python3
"""Daily YouTube analytics export to Google Sheets.

This script fetches channel- and video-level performance metrics from the
YouTube Data API and YouTube Analytics API, then appends the results to the
specified Google Sheet. It is meant to run on a daily cadence (e.g., via cron
or Task Scheduler) after a one-time OAuth consent.

Prerequisites:
- Enable YouTube Data API v3, YouTube Analytics API, and Google Sheets API for
  your Google Cloud project.
- Create an OAuth client ID (Desktop app) and download the
  `client_secret.json` file.
- Install dependencies: `pip install google-auth-oauthlib google-api-python-client`
- Place `client_secret.json` alongside this script or point to it via
  `--client-secret`.
- On first run, the script launches a local server to complete OAuth and stores
  a refresh token in `token.json` (configurable).

Sheets layout suggestions:
- Worksheet `Daily` with columns: Date, Retrieved At, Views, Minutes Watched,
  Average View Duration (sec), Average View Percentage, Likes, Comments,
  Shares, Subscribers Gained, Subscribers Lost, Estimated Revenue,
  Impressions, CTR (%), Total Subscribers, Total Views, Total Videos.
- Worksheet `VideoDaily` with columns: Date, Retrieved At, Video ID, Title,
  Published At, Views, Minutes Watched, Avg View Duration (sec),
  Avg View Percentage, Likes, Comments, Shares, Sub Gain, Sub Lost,
  Impressions, CTR (%).
- Worksheet `TrafficSources` with columns: Date, Retrieved At,
  Traffic Source, Views, Minutes Watched.

Usage example:
    python youtube_to_google_sheets.py --spreadsheet-id <sheet_id>
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
from typing import Any, Dict, Iterable, List, Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

LOGGER = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
DEFAULT_CLIENT_SECRET = "client_secret.json"
DEFAULT_TOKEN_PATH = "token.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export daily YouTube analytics data into Google Sheets."
    )
    parser.add_argument(
        "--spreadsheet-id",
        required=True,
        help="Target Google Spreadsheet ID (found in the sheet URL).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="ISO date (YYYY-MM-DD) to export. Defaults to yesterday in the channel locale.",
    )
    parser.add_argument(
        "--client-secret",
        default=DEFAULT_CLIENT_SECRET,
        help="Path to OAuth client secret JSON file.",
    )
    parser.add_argument(
        "--token",
        default=DEFAULT_TOKEN_PATH,
        help="Path to store OAuth refresh token.",
    )
    parser.add_argument(
        "--daily-range",
        default="Daily!A:Z",
        help="Sheet range for daily channel metrics.",
    )
    parser.add_argument(
        "--video-range",
        default="VideoDaily!A:Z",
        help="Sheet range for per-video metrics.",
    )
    parser.add_argument(
        "--traffic-range",
        default="TrafficSources!A:Z",
        help="Sheet range for traffic source metrics.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--max-video-batches",
        type=int,
        default=10,
        help="Safety cap on analytics paging batches for video metrics.",
    )
    parser.add_argument(
        "--skip-video-metrics",
        action="store_true",
        help="Skip per-video analytics (faster, but less detail).",
    )
    parser.add_argument(
        "--skip-traffic-sources",
        action="store_true",
        help="Skip traffic source breakdown for the day.",
    )
    return parser.parse_args()


def iso_yesterday() -> dt.date:
    today = dt.date.today()
    return today - dt.timedelta(days=1)


def resolve_target_date(date_arg: str | None) -> dt.date:
    if not date_arg:
        return iso_yesterday()
    return dt.date.fromisoformat(date_arg)


def load_credentials(client_secret_path: str, token_path: str) -> Credentials:
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes=SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            LOGGER.info("Refreshing OAuth token...")
            creds.refresh(Request())
        else:
            LOGGER.info("Running OAuth flow to obtain new credentials...")
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())
            LOGGER.info("Stored refreshed token at %s", token_path)
    return creds


def build_services(creds: Credentials):
    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)
    youtube_analytics = build(
        "youtubeAnalytics", "v2", credentials=creds, cache_discovery=False
    )
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return youtube, youtube_analytics, sheets


def get_channel_statistics(youtube) -> Dict[str, Any]:
    response = (
        youtube.channels()
        .list(part="snippet,statistics,contentDetails", mine=True)
        .execute()
    )
    items = response.get("items", [])
    if not items:
        raise RuntimeError("No channels found for the authenticated account.")
    channel = items[0]
    stats = channel.get("statistics", {})
    snippet = channel.get("snippet", {})
    return {
        "channel_id": channel.get("id"),
        "title": snippet.get("title"),
        "subs_total": int(stats.get("subscriberCount", 0)),
        "views_total": int(stats.get("viewCount", 0)),
        "videos_total": int(stats.get("videoCount", 0)),
        "uploads_playlist": channel.get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads"),
    }


def analytics_query(youtube_analytics, start_date: dt.date, end_date: dt.date, **kwargs):
    params = {
        "ids": "channel==MINE",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }
    params.update(kwargs)
    return youtube_analytics.reports().query(**params).execute()


def extract_rows_from_report(report: Dict[str, Any]) -> List[List[Any]]:
    return report.get("rows", []) or []


def get_daily_channel_metrics(youtube_analytics, target_date: dt.date) -> Dict[str, Any]:
    metrics = (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage," \
        "likes,comments,shares,subscribersGained,subscribersLost,estimatedRevenue," \
        "impressions,impressionsClickThroughRate"
    )
    report = analytics_query(
        youtube_analytics,
        start_date=target_date,
        end_date=target_date,
        metrics=metrics,
        dimensions="day",
    )
    rows = extract_rows_from_report(report)
    if not rows:
        LOGGER.warning("No channel metrics returned for %s", target_date)
        return {}
    values = rows[0]
    keys = [
        "day",
        "views",
        "estimated_minutes_watched",
        "avg_view_duration",
        "avg_view_percentage",
        "likes",
        "comments",
        "shares",
        "subs_gained",
        "subs_lost",
        "estimated_revenue",
        "impressions",
        "ctr",
    ]
    return dict(zip(keys, values, strict=False))


def get_video_metrics(
    youtube_analytics,
    youtube,
    target_date: dt.date,
    max_batches: int = 10,
) -> List[Dict[str, Any]]:
    metrics = (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage," \
        "likes,comments,shares,subscribersGained,subscribersLost,impressions," \
        "impressionsClickThroughRate"
    )
    dimensions = "video"
    sort_order = "-views"
    results: List[Dict[str, Any]] = []
    start_index = 1
    batch_count = 0
    video_title_cache: Dict[str, Dict[str, Any]] = {}

    while True:
        batch_count += 1
        if batch_count > max_batches:
            LOGGER.warning(
                "Reached max batches (%s) for video analytics; truncating results.",
                max_batches,
            )
            break
        report = analytics_query(
            youtube_analytics,
            start_date=target_date,
            end_date=target_date,
            metrics=metrics,
            dimensions=dimensions,
            sort=sort_order,
            startIndex=start_index,
        )
        rows = extract_rows_from_report(report)
        if not rows:
            break
        for row in rows:
            video_id = row[0]
            metric_values = row[1:]
            record = {
                "video_id": video_id,
                "views": row[1],
                "estimated_minutes_watched": row[2],
                "avg_view_duration": row[3],
                "avg_view_percentage": row[4],
                "likes": row[5],
                "comments": row[6],
                "shares": row[7],
                "subs_gained": row[8],
                "subs_lost": row[9],
                "impressions": row[10],
                "ctr": row[11],
            }
            results.append(record)
        if len(rows) < report.get("pageInfo", {}).get("resultsPerPage", len(rows)):
            break
        start_index += len(rows)

    if not results:
        return []

    # Fetch video metadata in batches of 50 for titles/publish dates.
    unique_ids = [record["video_id"] for record in results]
    for i in range(0, len(unique_ids), 50):
        chunk = unique_ids[i : i + 50]
        response = (
            youtube.videos()
            .list(part="snippet", id=",".join(chunk))
            .execute()
        )
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            video_title_cache[item["id"]] = {
                "title": snippet.get("title", ""),
                "published_at": snippet.get("publishedAt"),
            }

    for record in results:
        meta = video_title_cache.get(record["video_id"], {})
        record["title"] = meta.get("title", "")
        record["published_at"] = meta.get("published_at")

    return results


def get_traffic_source_metrics(
    youtube_analytics, target_date: dt.date
) -> List[Dict[str, Any]]:
    metrics = "views,estimatedMinutesWatched"
    dimensions = "insightTrafficSourceType"
    report = analytics_query(
        youtube_analytics,
        start_date=target_date,
        end_date=target_date,
        metrics=metrics,
        dimensions=dimensions,
        sort="-views",
    )
    rows = extract_rows_from_report(report)
    if not rows:
        return []
    results = []
    for row in rows:
        results.append(
            {
                "source": row[0],
                "views": row[1],
                "estimated_minutes_watched": row[2],
            }
        )
    return results


def append_rows(
    sheets_service,
    spreadsheet_id: str,
    range_name: str,
    rows: Sequence[Sequence[Any]],
) -> None:
    if not rows:
        return
    request = (
        sheets_service.values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        )
    )
    request.execute()
    LOGGER.info("Appended %d rows to %s", len(rows), range_name)


def make_daily_row(
    target_date: dt.date,
    pulled_at: dt.datetime,
    channel_stats: Dict[str, Any],
    channel_metrics: Dict[str, Any],
) -> List[Any]:
    return [
        target_date.isoformat(),
        pulled_at.isoformat(),
        channel_metrics.get("views"),
        channel_metrics.get("estimated_minutes_watched"),
        channel_metrics.get("avg_view_duration"),
        channel_metrics.get("avg_view_percentage"),
        channel_metrics.get("likes"),
        channel_metrics.get("comments"),
        channel_metrics.get("shares"),
        channel_metrics.get("subs_gained"),
        channel_metrics.get("subs_lost"),
        channel_metrics.get("estimated_revenue"),
        channel_metrics.get("impressions"),
        channel_metrics.get("ctr"),
        channel_stats.get("subs_total"),
        channel_stats.get("views_total"),
        channel_stats.get("videos_total"),
    ]


def make_video_rows(
    target_date: dt.date,
    pulled_at: dt.datetime,
    video_metrics: Iterable[Dict[str, Any]],
) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for metric in video_metrics:
        rows.append(
            [
                target_date.isoformat(),
                pulled_at.isoformat(),
                metric.get("video_id"),
                metric.get("title"),
                metric.get("published_at"),
                metric.get("views"),
                metric.get("estimated_minutes_watched"),
                metric.get("avg_view_duration"),
                metric.get("avg_view_percentage"),
                metric.get("likes"),
                metric.get("comments"),
                metric.get("shares"),
                metric.get("subs_gained"),
                metric.get("subs_lost"),
                metric.get("impressions"),
                metric.get("ctr"),
            ]
        )
    return rows


def make_traffic_rows(
    target_date: dt.date,
    pulled_at: dt.datetime,
    traffic_metrics: Iterable[Dict[str, Any]],
) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for metric in traffic_metrics:
        rows.append(
            [
                target_date.isoformat(),
                pulled_at.isoformat(),
                metric.get("source"),
                metric.get("views"),
                metric.get("estimated_minutes_watched"),
            ]
        )
    return rows


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    target_date = resolve_target_date(args.date)
    pulled_at = dt.datetime.utcnow().replace(microsecond=0)

    LOGGER.info("Running YouTube export for %s", target_date)
    creds = load_credentials(args.client_secret, args.token)

    youtube, youtube_analytics, sheets = build_services(creds)

    try:
        channel_stats = get_channel_statistics(youtube)
        channel_metrics = get_daily_channel_metrics(youtube_analytics, target_date)
        daily_row = make_daily_row(target_date, pulled_at, channel_stats, channel_metrics)
        append_rows(sheets, args.spreadsheet_id, args.daily_range, [daily_row])

        if not args.skip_video_metrics:
            video_metrics = get_video_metrics(
                youtube_analytics,
                youtube,
                target_date,
                max_batches=args.max_video_batches,
            )
            video_rows = make_video_rows(target_date, pulled_at, video_metrics)
            append_rows(sheets, args.spreadsheet_id, args.video_range, video_rows)

        if not args.skip_traffic_sources:
            traffic_metrics = get_traffic_source_metrics(youtube_analytics, target_date)
            traffic_rows = make_traffic_rows(target_date, pulled_at, traffic_metrics)
            append_rows(sheets, args.spreadsheet_id, args.traffic_range, traffic_rows)

    except HttpError as exc:
        LOGGER.error("Google API error: %s", exc)
        raise


if __name__ == "__main__":
    main()
