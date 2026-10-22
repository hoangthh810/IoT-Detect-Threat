#!/usr/bin/env python3
"""Four-page Streamlit presentation layer for Phase 7 dashboard snapshots."""

from __future__ import annotations

import html
import json
import os
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = REPOSITORY_ROOT / ".runtime/dashboard"
DATA_DIR = Path(os.environ.get("DASHBOARD_DATA_DIR", str(DEFAULT_DATA_DIR))).expanduser()
REFRESH_INTERVAL = "2s"
ACTIVE_WINDOW_TAIL_COUNT = 2

STATUS_PRESENTATION = {
    "Normal": {
        "label": "SAFE",
        "message": "Network activity is within the normal range",
        "css_class": "status-safe",
    },
    "Warning": {
        "label": "WARNING",
        "message": "Suspicious activity is present in the current window",
        "css_class": "status-warning",
    },
    "Critical": {
        "label": "CRITICAL",
        "message": "A high concentration of threats has been detected",
        "css_class": "status-critical",
    },
}

REQUIRED_METRIC_FIELDS = {
    "window_start",
    "window_end",
    "event_count",
    "event_rate",
    "normal_count",
    "attack_count",
    "normal_rate",
    "attack_rate",
    "status",
}

DASHBOARD_CSS = """
<style>
    :root {
        --ink: #102039;
        --muted: #68758a;
        --line: #e5eaf1;
        --panel: rgba(255, 255, 255, 0.94);
    }
    .stApp {
        background:
            radial-gradient(circle at 87% 4%, rgba(57, 113, 235, 0.11), transparent 24rem),
            linear-gradient(180deg, #f7f9fc 0%, #f1f4f9 100%);
        color: var(--ink);
    }
    .block-container {
        max-width: 1480px;
        padding-top: 3.75rem;
        padding-bottom: 4rem;
    }
    [data-testid="stSidebar"] {
        background:
            radial-gradient(circle at 10% 0%, rgba(72, 123, 235, 0.24), transparent 19rem),
            #0a1428;
        border-right: 1px solid rgba(255, 255, 255, 0.08);
    }
    [data-testid="stSidebar"] * { color: #dce7fa; }
    [data-testid="stSidebarNav"] a {
        border-radius: 0.65rem;
        margin: 0.18rem 0.7rem;
        padding: 0.68rem 0.8rem;
    }
    [data-testid="stSidebarNav"] a:hover { background: rgba(255, 255, 255, 0.08); }
    [data-testid="stSidebarNav"] a[aria-current="page"] {
        background: linear-gradient(90deg, rgba(48, 104, 231, 0.55), rgba(48, 104, 231, 0.16));
        box-shadow: inset 3px 0 0 #76a2ff;
    }
    .sidebar-brand { padding: 0.55rem 0.65rem 0.2rem 0.65rem; }
    .sidebar-mark {
        display: inline-grid;
        place-items: center;
        width: 2.45rem;
        height: 2.45rem;
        border-radius: 0.75rem;
        background: linear-gradient(145deg, #3d75ed, #214cb5);
        color: white;
        font-weight: 800;
        font-size: 1.05rem;
        box-shadow: 0 10px 26px rgba(24, 70, 174, 0.4);
    }
    .sidebar-title {
        margin-top: 0.8rem;
        color: white;
        font-size: 1rem;
        font-weight: 750;
        letter-spacing: -0.01em;
    }
    .sidebar-subtitle {
        color: #8fa3c2;
        font-size: 0.76rem;
        line-height: 1.45;
        margin-top: 0.15rem;
    }
    .sidebar-footer {
        color: #8295b2;
        font-size: 0.72rem;
        line-height: 1.55;
        padding: 1rem 0.7rem;
        border-top: 1px solid rgba(255, 255, 255, 0.09);
    }
    .page-header { margin-bottom: 1.35rem; }
    .page-eyebrow {
        color: #2764e7;
        font-size: 0.73rem;
        font-weight: 800;
        letter-spacing: 0.13em;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }
    .page-title {
        color: var(--ink);
        font-size: clamp(2rem, 4vw, 3.15rem);
        font-weight: 790;
        letter-spacing: -0.045em;
        line-height: 1.05;
        margin: 0;
    }
    .page-description {
        color: var(--muted);
        font-size: 0.98rem;
        margin-top: 0.55rem;
        max-width: 48rem;
    }
    .section-title {
        color: var(--ink);
        font-size: 1.08rem;
        font-weight: 760;
        letter-spacing: -0.015em;
        margin: 1.55rem 0 0.7rem 0;
    }
    .status-banner {
        position: relative;
        overflow: hidden;
        border: 1px solid;
        border-radius: 1.1rem;
        margin: 0.2rem 0 1.2rem 0;
        padding: 1.35rem 1.55rem 1.3rem 1.55rem;
        box-shadow: 0 14px 38px rgba(20, 35, 60, 0.08);
    }
    .status-banner::after {
        content: "";
        position: absolute;
        width: 13rem;
        height: 13rem;
        border-radius: 50%;
        right: -5.5rem;
        top: -7rem;
        background: currentColor;
        opacity: 0.07;
    }
    .status-kicker {
        font-size: 0.67rem;
        font-weight: 800;
        letter-spacing: 0.13em;
        opacity: 0.72;
    }
    .status-label {
        font-size: 2rem;
        font-weight: 820;
        letter-spacing: -0.025em;
        line-height: 1.1;
        margin-top: 0.28rem;
    }
    .status-message { font-size: 0.92rem; margin-top: 0.35rem; opacity: 0.86; }
    .status-time {
        position: absolute;
        right: 1.45rem;
        bottom: 1.25rem;
        font-size: 0.73rem;
        font-weight: 700;
        opacity: 0.62;
    }
    .status-safe {
        background: linear-gradient(120deg, #edfbf4, #f8fffb);
        border-color: #9bdfbd;
        color: #116b48;
    }
    .status-warning {
        background: linear-gradient(120deg, #fff6e5, #fffaf1);
        border-color: #f1ce8d;
        color: #975b02;
    }
    .status-critical {
        background: linear-gradient(120deg, #ffeded, #fff7f7);
        border-color: #f1aaaa;
        color: #a6242b;
    }
    .status-unknown {
        background: linear-gradient(120deg, #eef2f7, #fafbfd);
        border-color: #ccd5e2;
        color: #46566d;
    }
    [data-testid="stMetric"] {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 0.95rem;
        padding: 1rem 1.05rem;
        box-shadow: 0 9px 28px rgba(20, 35, 60, 0.055);
    }
    [data-testid="stMetricLabel"] { color: #6c788a; font-weight: 670; }
    [data-testid="stMetricValue"] {
        color: #102039;
        font-weight: 770;
        letter-spacing: -0.035em;
        font-size: 1.68rem;
    }
    [data-testid="stVegaLiteChart"] {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 1rem;
        padding: 0.65rem;
        box-shadow: 0 9px 28px rgba(20, 35, 60, 0.045);
    }
    [data-testid="stDataFrame"] {
        background: white;
        border: 1px solid var(--line);
        border-radius: 0.95rem;
        overflow: hidden;
        box-shadow: 0 9px 28px rgba(20, 35, 60, 0.04);
    }
    [data-testid="stExpander"] {
        background: rgba(255, 255, 255, 0.82);
        border: 1px solid var(--line);
        border-radius: 0.85rem;
    }
    .empty-state {
        display: grid;
        place-items: center;
        min-height: 23rem;
        border: 1px dashed #c8d1df;
        border-radius: 1.15rem;
        background: rgba(255, 255, 255, 0.72);
        text-align: center;
        padding: 2rem;
    }
    .empty-icon {
        display: inline-grid;
        place-items: center;
        width: 3.4rem;
        height: 3.4rem;
        margin-bottom: 0.8rem;
        border-radius: 1rem;
        background: #e9effb;
        color: #2764e7;
        font-size: 1.4rem;
        font-weight: 800;
    }
    .empty-title { color: var(--ink); font-size: 1.15rem; font-weight: 760; }
    .empty-copy { color: var(--muted); font-size: 0.88rem; margin-top: 0.35rem; }
    .pipeline {
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 0.65rem;
        margin: 0.55rem 0 1rem 0;
    }
    .pipeline-step {
        position: relative;
        background: white;
        border: 1px solid var(--line);
        border-radius: 0.85rem;
        padding: 0.95rem 0.85rem;
        min-height: 5.4rem;
        box-shadow: 0 8px 22px rgba(20, 35, 60, 0.04);
    }
    .pipeline-step:not(:last-child)::after {
        content: "→";
        position: absolute;
        right: -0.62rem;
        top: 1.8rem;
        z-index: 2;
        color: #8290a5;
        font-weight: 800;
    }
    .pipeline-number {
        color: #2764e7;
        font-size: 0.66rem;
        font-weight: 800;
        letter-spacing: 0.1em;
    }
    .pipeline-name {
        color: var(--ink);
        font-size: 0.9rem;
        font-weight: 740;
        margin-top: 0.28rem;
    }
    .pipeline-meta { color: var(--muted); font-size: 0.72rem; margin-top: 0.18rem; }
    .metadata-note {
        background: #f2f6fd;
        border-left: 3px solid #4b7de6;
        border-radius: 0.5rem;
        color: #51617a;
        font-size: 0.79rem;
        padding: 0.7rem 0.85rem;
        margin: 0.6rem 0 0.85rem 0;
    }
    @media (max-width: 900px) {
        .pipeline { grid-template-columns: 1fr; }
        .pipeline-step:not(:last-child)::after {
            content: "↓";
            right: 1rem;
            top: auto;
            bottom: -0.75rem;
        }
        .status-time { position: static; display: block; margin-top: 0.7rem; }
    }
</style>
"""


def read_snapshot(name: str) -> list[dict[str, Any]]:
    """Read one atomically replaced JSON list without breaking a refresh."""
    try:
        with (DATA_DIR / name).open("r", encoding="utf-8") as handle:
            content = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError, UnicodeError):
        return []
    if not isinstance(content, list):
        return []
    return [item for item in content if isinstance(item, dict)]


def valid_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (row for row in rows if REQUIRED_METRIC_FIELDS.issubset(row)),
        key=lambda row: str(row["window_end"]),
    )


def metrics_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True, errors="coerce")
    frame["window_end"] = pd.to_datetime(frame["window_end"], utc=True, errors="coerce")
    numeric_columns = [
        "event_count",
        "event_rate",
        "normal_count",
        "attack_count",
        "normal_rate",
        "attack_rate",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["window_end", *numeric_columns]).sort_values("window_end")
    frame["attack_percent"] = frame["attack_rate"] * 100.0
    frame["normal_percent"] = frame["normal_rate"] * 100.0
    return frame


def alerts_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in (
        "event_timestamp",
        "kafka_timestamp",
        "topic",
        "partition",
        "offset",
        "alert_type",
        "prediction",
        "prediction_label",
        "probability",
        "scenario",
        "ground_truth",
        "source_label",
    ):
        if column not in frame:
            frame[column] = None
    frame["event_timestamp_parsed"] = pd.to_datetime(
        frame["event_timestamp"], utc=True, errors="coerce"
    )
    return frame.sort_values(
        ["event_timestamp_parsed", "topic", "partition", "offset"],
        ascending=False,
        na_position="last",
    )


def load_dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    return (
        metrics_frame(valid_metrics(read_snapshot("metrics.json"))),
        alerts_frame(read_snapshot("alerts.json")),
    )


def completed_metric_windows(metrics: pd.DataFrame) -> pd.DataFrame:
    """Exclude still-filling event-time windows from operational presentation."""
    if metrics.empty:
        return metrics
    if len(metrics) <= ACTIVE_WINDOW_TAIL_COUNT:
        return metrics.iloc[0:0]
    # A 10-second window sliding every 5 seconds leaves the newest two windows
    # open at any instant. Snapshot timestamps are serialized by Spark, so using
    # their sorted position avoids host/browser timezone assumptions.
    return metrics.iloc[:-ACTIVE_WINDOW_TAIL_COUNT]


def display_time(value: Any) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "Unavailable"
    return timestamp.strftime("%H:%M:%S UTC")


def page_header(eyebrow: str, title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="page-header">
            <div class="page-header-copy">
                <div class="page-eyebrow">{eyebrow}</div>
                <h1 class="page-title">{title}</h1>
                <div class="page-description">{description}</div>
            </div>
            <div class="page-live-pill">
                <span class="live-dot"></span>
                LIVE TELEMETRY · 2S
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(title: str) -> None:
    st.markdown(
        f"""
        <div class="section-heading">
            <div class="section-title">{title}</div>
            <div class="section-line"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state() -> None:
    st.markdown(
        """
        <div class="empty-state">
            <div>
                <div class="empty-icon">↗</div>
                <div class="empty-title">Waiting for realtime traffic</div>
                <div class="empty-copy">Run a scenario to start the live monitor.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_window_warming_state() -> None:
    st.markdown(
        """
        <div class="empty-state">
            <div>
                <div class="empty-icon">◌</div>
                <div class="empty-title">Building the first complete window</div>
                <div class="empty-copy">Live events are arriving. Stable metrics appear after the 10-second detection window closes.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status(latest: pd.Series) -> None:
    status = str(latest["status"])
    presentation = STATUS_PRESENTATION.get(
        status,
        {
            "label": "UNAVAILABLE",
            "message": "The latest analytics status is not recognized",
            "css_class": "status-unknown",
        },
    )
    threat_percent = max(0.0, min(100.0, float(latest["attack_rate"]) * 100.0))
    posture_class = presentation["css_class"].replace("status-", "posture-")
    st.markdown(
        f"""
        <div class="posture-shell {posture_class}">
            <div>
                <div class="posture-kicker">MODEL THREAT POSTURE // LIVE</div>
                <div class="posture-title">{presentation['label']}</div>
                <div class="posture-message">{presentation['message']}</div>
                <div class="posture-meta">
                    <span>WINDOW <strong>{display_time(latest['window_start'])} — {display_time(latest['window_end'])}</strong></span>
                    <span>THREATS <strong>{int(latest['attack_count']):02d}</strong></span>
                    <span>SOURCE <strong>PHASE 7</strong></span>
                </div>
            </div>
            <div class="threat-orbit" style="--gauge: {threat_percent * 3.6:.1f}deg;">
                <div>
                    <div class="orbit-value">{threat_percent:.1f}%</div>
                    <div class="orbit-label">THREAT TRAFFIC</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_cards(cards: list[dict[str, str]]) -> None:
    rendered_cards: list[str] = []
    for index, card in enumerate(cards, start=1):
        label = html.escape(card["label"])
        value = html.escape(card["value"])
        unit = html.escape(card.get("unit", ""))
        delta = html.escape(card.get("delta", "LIVE"))
        meta = html.escape(card.get("meta", "LATEST WINDOW"))
        tone = card.get("tone", "cyan")
        rendered_cards.append(
            f"""
            <div class="ops-metric metric-{tone}">
                <div class="ops-metric-top">
                    <span>{label}</span>
                    <span class="metric-index">0{index}</span>
                </div>
                <div class="ops-metric-value">
                    {value}<span class="ops-metric-unit">{unit}</span>
                </div>
                <div class="ops-metric-foot">
                    <span class="metric-delta">{delta}</span>
                    <span>{meta}</span>
                </div>
            </div>
            """
        )
    metric_markup = f'<div class="metric-grid">{"".join(rendered_cards)}</div>'
    st.markdown(
        "".join(line.strip() for line in metric_markup.splitlines()),
        unsafe_allow_html=True,
    )


def render_primary_metrics(metrics: pd.DataFrame) -> None:
    latest = metrics.iloc[-1]
    previous = metrics.iloc[-2] if len(metrics) > 1 else None
    rate_delta = "FIRST SAMPLE"
    threat_delta = "FIRST SAMPLE"
    if previous is not None:
        rate_delta = f"Δ {latest['event_rate'] - previous['event_rate']:+.1f} EPS"
        threat_delta = f"Δ {(latest['attack_rate'] - previous['attack_rate']) * 100:+.1f} PTS"

    render_metric_cards(
        [
            {
                "label": "Network traffic",
                "value": f"{latest['event_rate']:.1f}",
                "unit": "EVENTS / S",
                "delta": rate_delta,
                "meta": "COMPLETE 10S",
                "tone": "cyan",
            },
            {
                "label": "Threat traffic",
                "value": f"{latest['attack_rate']:.1%}",
                "delta": threat_delta,
                "meta": "MODEL OUTPUT",
                "tone": "red",
            },
            {
                "label": "Events analyzed",
                "value": f"{int(latest['event_count']):,}",
                "delta": "INGESTED",
                "meta": "STABLE WINDOW",
                "tone": "blue",
            },
            {
                "label": "Detected threats",
                "value": f"{int(latest['attack_count']):,}",
                "delta": "PREDICTED ATTACK",
                "meta": "BINARY MODEL",
                "tone": "violet",
            },
        ]
    )


def render_alert_table(alerts: pd.DataFrame, *, limit: int | None = None) -> None:
    if alerts.empty:
        st.markdown(
            """
            <div class="empty-alerts">
                <span>No predicted threats in the current runtime.</span>
                <strong>STREAM CLEAR</strong>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return
    visible = alerts.head(limit) if limit is not None else alerts
    rows: list[str] = []
    for _, alert in visible.iterrows():
        time = html.escape(display_time(alert["event_timestamp_parsed"]).replace(" UTC", ""))
        prediction = html.escape(str(alert["prediction_label"] or "Attack"))
        scenario = html.escape(str(alert["scenario"] or "Unknown"))
        source_label = html.escape(str(alert["source_label"] or "Unknown"))
        rows.append(
            f"""
            <div class="alert-stream-row">
                <span class="alert-time">{time}</span>
                <span class="alert-badge">{prediction.upper()}</span>
                <span class="alert-scenario">{scenario}</span>
                <span class="alert-source">{source_label}</span>
            </div>
            """
        )
    alert_markup = f"""
        <div class="alert-stream">
            <div class="alert-stream-head">
                <span>TIME / UTC</span>
                <span>MODEL</span>
                <span>SCENARIO</span>
                <span>SOURCE LABEL · GROUND TRUTH</span>
            </div>
            {''.join(rows)}
        </div>
        """
    st.markdown(
        "".join(line.strip() for line in alert_markup.splitlines()),
        unsafe_allow_html=True,
    )


def polish_chart(chart: alt.TopLevelMixin) -> alt.TopLevelMixin:
    return (
        chart.properties(background="transparent", padding={"left": 8, "right": 14, "top": 10, "bottom": 6})
        .configure_view(strokeOpacity=0)
        .configure_axis(
            domain=False,
            gridColor="#e2e8f0",
            gridOpacity=0.9,
            gridDash=[3, 4],
            labelColor="#475569",
            labelFont="Ubuntu Sans Mono",
            labelFontSize=12,
            labelPadding=9,
            labelLimit=140,
            tickColor="#cbd5e1",
            tickSize=4,
            titleColor="#475569",
            titleFont="Ubuntu Sans",
            titleFontSize=13,
            titleFontWeight=600,
            titlePadding=16,
        )
        .configure_legend(
            labelColor="#475569",
            labelFont="Ubuntu Sans",
            labelFontSize=12,
            labelPadding=7,
            titleColor="#334155",
            titleFont="Ubuntu Sans Mono",
            titleFontSize=11,
            titleFontWeight=700,
            titlePadding=8,
            symbolSize=120,
            symbolStrokeWidth=4,
            columnPadding=18,
            rowPadding=8,
        )
    )


def event_rate_chart(metrics: pd.DataFrame, *, height: int = 340) -> alt.TopLevelMixin:
    average_rate = float(metrics["event_rate"].mean())
    base = alt.Chart(metrics).encode(
        x=alt.X(
            "window_end:T",
            title=None,
            axis=alt.Axis(
                format="%H:%M:%S",
                tickCount=6,
                labelAngle=0,
                labelOverlap="greedy",
            ),
        ),
        y=alt.Y(
            "event_rate:Q",
            title="Events / second",
            scale=alt.Scale(zero=True, nice=True),
        ),
        tooltip=[
            alt.Tooltip("window_end:T", title="Window end", format="%H:%M:%S"),
            alt.Tooltip("event_rate:Q", title="Events/s", format=".1f"),
            alt.Tooltip("event_count:Q", title="Events", format=","),
            alt.Tooltip("status:N", title="Status"),
        ],
    )
    area = base.mark_area(
        color="#0891b2",
        opacity=0.13,
        interpolate="monotone",
    )
    line = base.mark_line(color="#0891b2", strokeWidth=3.2, interpolate="monotone")
    points = base.mark_point(
        color="#ffffff",
        fill="#0891b2",
        size=72,
        strokeWidth=2.5,
    )
    latest = (
        alt.Chart(metrics.tail(1))
        .mark_point(color="#0891b2", fillOpacity=0, size=240, strokeWidth=2.2)
        .encode(x="window_end:T", y="event_rate:Q")
    )
    average_data = pd.DataFrame(
        {"event_rate": [average_rate], "label": [f"AVG  {average_rate:.1f} EPS"]}
    )
    average_rule = (
        alt.Chart(average_data)
        .mark_rule(color="#64748b", strokeDash=[7, 5], strokeWidth=1.4, opacity=0.7)
        .encode(y="event_rate:Q")
    )
    average_label = (
        alt.Chart(average_data)
        .mark_text(
            align="left",
            baseline="bottom",
            dx=8,
            dy=-5,
            color="#475569",
            font="Ubuntu Sans Mono",
            fontSize=11,
            fontWeight=700,
        )
        .encode(x=alt.value(0), y="event_rate:Q", text="label:N")
    )
    return polish_chart(
        (area + average_rule + line + points + latest + average_label).properties(
            height=height
        )
    )


def threat_rate_chart(metrics: pd.DataFrame, *, height: int = 340) -> alt.TopLevelMixin:
    average_threat = float(metrics["attack_percent"].mean())
    status_scale = alt.Scale(
        domain=["Normal", "Warning", "Critical"],
        range=["#059669", "#d97706", "#e11d48"],
    )
    base = alt.Chart(metrics).encode(
        x=alt.X(
            "window_end:T",
            title=None,
            axis=alt.Axis(
                format="%H:%M:%S",
                tickCount=6,
                labelAngle=0,
                labelOverlap="greedy",
            ),
        ),
        y=alt.Y(
            "attack_percent:Q",
            title="Threat traffic (%)",
            scale=alt.Scale(domain=[0, 100]),
        ),
        tooltip=[
            alt.Tooltip("window_end:T", title="Window end", format="%H:%M:%S"),
            alt.Tooltip("attack_percent:Q", title="Threat traffic", format=".1f"),
            alt.Tooltip("attack_count:Q", title="Threats", format=","),
            alt.Tooltip("status:N", title="Phase 7 status"),
        ],
    )
    area = base.mark_area(color="#e11d48", opacity=0.10, interpolate="monotone")
    line = base.mark_line(color="#e11d48", strokeWidth=3.2, interpolate="monotone")
    points = base.mark_circle(size=100, stroke="#ffffff", strokeWidth=2.5).encode(
        color=alt.Color(
            "status:N",
            title="Phase 7 status",
            scale=status_scale,
            legend=alt.Legend(
                orient="top",
                direction="horizontal",
                title=None,
            ),
        )
    )
    latest = (
        alt.Chart(metrics.tail(1))
        .mark_point(color="#e11d48", fillOpacity=0, size=270, strokeWidth=2.2)
        .encode(x="window_end:T", y="attack_percent:Q")
    )
    average_data = pd.DataFrame(
        {
            "attack_percent": [average_threat],
            "label": [f"AVG  {average_threat:.1f}%"],
        }
    )
    average_rule = (
        alt.Chart(average_data)
        .mark_rule(color="#64748b", strokeDash=[7, 5], strokeWidth=1.4, opacity=0.7)
        .encode(y="attack_percent:Q")
    )
    average_label = (
        alt.Chart(average_data)
        .mark_text(
            align="left",
            baseline="bottom",
            dx=8,
            dy=-5,
            color="#475569",
            font="Ubuntu Sans Mono",
            fontSize=11,
            fontWeight=700,
        )
        .encode(x=alt.value(0), y="attack_percent:Q", text="label:N")
    )
    return polish_chart(
        (area + average_rule + line + points + latest + average_label).properties(
            height=height
        )
    )


def donut_chart(
    data: pd.DataFrame,
    *,
    category: str,
    value: str,
    domain: list[str],
    colors: list[str],
    center_label: str,
    height: int = 340,
) -> alt.TopLevelMixin:
    total = int(data[value].sum())
    enriched = data.copy()
    enriched["Share"] = enriched[value] / total if total else 0.0
    arcs = (
        alt.Chart(enriched)
        .mark_arc(
            innerRadius=76,
            outerRadius=116,
            cornerRadius=8,
            padAngle=0.025,
            stroke="#ffffff",
            strokeWidth=3,
        )
        .encode(
            theta=alt.Theta(f"{value}:Q"),
            color=alt.Color(
                f"{category}:N",
                scale=alt.Scale(domain=domain, range=colors),
                legend=alt.Legend(orient="bottom", direction="horizontal", title=None),
            ),
            tooltip=[
                alt.Tooltip(f"{category}:N", title=category),
                alt.Tooltip(f"{value}:Q", title=value, format=","),
                alt.Tooltip("Share:Q", title="Share", format=".1%"),
            ],
        )
    )
    center = pd.DataFrame(
        {"display": [f"{total:,}"], "label": [center_label.upper()]}
    )
    total_text = (
        alt.Chart(center)
        .mark_text(
            color="#0f172a",
            font="Ubuntu Sans Mono",
            fontSize=28,
            fontWeight=700,
            dy=-7,
        )
        .encode(text="display:N")
    )
    label_text = (
        alt.Chart(center)
        .mark_text(
            color="#94a3b8",
            font="Ubuntu Sans Mono",
            fontSize=11,
            fontWeight=700,
            dy=21,
        )
        .encode(text="label:N")
    )
    return polish_chart(
        (arcs + total_text + label_text).properties(height=height)
    )


def prediction_mix_chart(latest: pd.Series) -> alt.TopLevelMixin:
    mix = pd.DataFrame(
        {
            "Prediction": ["Normal", "Attack"],
            "Events": [latest["normal_count"], latest["attack_count"]],
        }
    )
    return donut_chart(
        mix,
        category="Prediction",
        value="Events",
        domain=["Normal", "Attack"],
        colors=["#2563eb", "#e11d48"],
        center_label="events",
    )


def page_overview() -> None:
    page_header(
        "Operations / Live posture",
        "Command Overview",
        "One live view of network pressure, model decisions, and signals requiring attention.",
    )
    overview_live()


@st.fragment(run_every=REFRESH_INTERVAL)
def overview_live() -> None:
    metrics, alerts = load_dashboard_data()
    if metrics.empty:
        render_empty_state()
        return

    metrics = completed_metric_windows(metrics)
    if metrics.empty:
        render_window_warming_state()
        return

    latest = metrics.iloc[-1]
    render_status(latest)
    render_primary_metrics(metrics)
    section_title("Live signal")
    traffic_column, mix_column = st.columns([1.75, 1], gap="large")
    with traffic_column:
        st.markdown('<div class="chart-kicker">NETWORK TRAFFIC // COMPLETE WINDOWS</div>', unsafe_allow_html=True)
        st.altair_chart(event_rate_chart(metrics, height=340), width="stretch")
    with mix_column:
        st.markdown('<div class="chart-kicker">LATEST PREDICTION MIX</div>', unsafe_allow_html=True)
        st.altair_chart(prediction_mix_chart(latest), width="stretch")
    section_title("Latest threats")
    render_alert_table(alerts, limit=6)


def page_traffic() -> None:
    page_header(
        "Telemetry / Flow analysis",
        "Traffic Matrix",
        "Processing velocity, detection-window volume, and prediction flow across the live buffer.",
    )
    traffic_live()


@st.fragment(run_every=REFRESH_INTERVAL)
def traffic_live() -> None:
    metrics, _alerts = load_dashboard_data()
    if metrics.empty:
        render_empty_state()
        return

    metrics = completed_metric_windows(metrics)
    if metrics.empty:
        render_window_warming_state()
        return

    latest = metrics.iloc[-1]
    render_metric_cards(
        [
            {
                "label": "Current rate",
                "value": f"{latest['event_rate']:.1f}",
                "unit": "EVENTS / S",
                "delta": "ACTIVE",
                "meta": "LAST COMPLETE",
                "tone": "cyan",
            },
            {
                "label": "Peak rate",
                "value": f"{metrics['event_rate'].max():.1f}",
                "unit": "EVENTS / S",
                "delta": "MAX OBSERVED",
                "meta": "RUNTIME",
                "tone": "violet",
            },
            {
                "label": "Latest volume",
                "value": f"{int(latest['event_count']):,}",
                "unit": "EVENTS",
                "delta": "PROCESSED",
                "meta": "COMPLETE 10S",
                "tone": "blue",
            },
            {
                "label": "Windows tracked",
                "value": f"{len(metrics):,}",
                "delta": "BUFFERED",
                "meta": "MAX 60",
                "tone": "green",
            },
        ]
    )

    section_title("Processing rate")
    st.altair_chart(event_rate_chart(metrics, height=390), width="stretch")

    lower_left, lower_right = st.columns(2, gap="large")
    with lower_left:
        section_title("Events per detection window")
        average_volume = float(metrics["event_count"].mean())
        volume_base = alt.Chart(metrics).encode(
            x=alt.X(
                "window_end:T",
                title=None,
                axis=alt.Axis(
                    format="%H:%M:%S",
                    tickCount=5,
                    labelAngle=0,
                    labelOverlap="greedy",
                ),
            ),
            y=alt.Y("event_count:Q", title="Events", scale=alt.Scale(zero=True, nice=True)),
            tooltip=[
                alt.Tooltip("window_end:T", title="Window end", format="%H:%M:%S"),
                alt.Tooltip("event_count:Q", title="Events", format=","),
            ],
        )
        volume_bar = volume_base.mark_bar(
            cornerRadiusTopLeft=6,
            cornerRadiusTopRight=6,
            opacity=0.92,
        ).encode(
            color=alt.Color(
                "event_count:Q",
                scale=alt.Scale(range=["#bfdbfe", "#2563eb"]),
                legend=None,
            )
        )
        volume_average_data = pd.DataFrame(
            {"event_count": [average_volume], "label": [f"AVG  {average_volume:.0f}"]}
        )
        volume_average_rule = (
            alt.Chart(volume_average_data)
            .mark_rule(color="#7c3aed", strokeDash=[7, 5], strokeWidth=1.5)
            .encode(y="event_count:Q")
        )
        volume_average_label = (
            alt.Chart(volume_average_data)
            .mark_text(
                align="left",
                baseline="bottom",
                dx=8,
                dy=-5,
                color="#6d28d9",
                font="Ubuntu Sans Mono",
                fontSize=11,
                fontWeight=700,
            )
            .encode(
                x=alt.value(0),
                y="event_count:Q",
                text="label:N",
            )
        )
        window_chart = (volume_bar + volume_average_rule + volume_average_label).properties(
            height=340
        )
        st.altair_chart(polish_chart(window_chart), width="stretch")

    with lower_right:
        section_title("Prediction volume")
        composition = metrics[["window_end", "normal_count", "attack_count"]].melt(
            id_vars="window_end",
            value_vars=["normal_count", "attack_count"],
            var_name="Prediction",
            value_name="Events",
        )
        composition["Prediction"] = composition["Prediction"].map(
            {"normal_count": "Normal", "attack_count": "Attack"}
        )
        composition_base = alt.Chart(composition).encode(
            x=alt.X(
                "window_end:T",
                title=None,
                axis=alt.Axis(
                    format="%H:%M:%S",
                    tickCount=5,
                    labelAngle=0,
                    labelOverlap="greedy",
                ),
            ),
            y=alt.Y("Events:Q", title="Events", stack=True),
            color=alt.Color(
                "Prediction:N",
                scale=alt.Scale(
                    domain=["Normal", "Attack"], range=["#2563eb", "#e11d48"]
                ),
                legend=alt.Legend(orient="top", direction="horizontal", title=None),
            ),
            tooltip=[
                alt.Tooltip("window_end:T", title="Window end", format="%H:%M:%S"),
                alt.Tooltip("Prediction:N"),
                alt.Tooltip("Events:Q", format=","),
            ],
        )
        composition_area = composition_base.mark_area(
            opacity=0.68,
            interpolate="monotone",
        )
        composition_outline = composition_base.mark_line(
            strokeWidth=2.1,
            interpolate="monotone",
        )
        composition_chart = (composition_area + composition_outline).properties(height=340)
        st.altair_chart(polish_chart(composition_chart), width="stretch")


def page_threats() -> None:
    page_header(
        "Intelligence / Model output",
        "Threat Intelligence",
        "Threat concentration, incident activity, and simulation context from the binary model.",
    )
    threats_live()


@st.fragment(run_every=REFRESH_INTERVAL)
def threats_live() -> None:
    metrics, alerts = load_dashboard_data()
    if metrics.empty:
        render_empty_state()
        return

    metrics = completed_metric_windows(metrics)
    if metrics.empty:
        render_window_warming_state()
        return

    latest = metrics.iloc[-1]
    presentation = STATUS_PRESENTATION.get(str(latest["status"]), {})
    render_metric_cards(
        [
            {
                "label": "Threat traffic",
                "value": f"{latest['attack_rate']:.1%}",
                "delta": "MODEL SHARE",
                "meta": "LAST COMPLETE",
                "tone": "red",
            },
            {
                "label": "Latest threats",
                "value": f"{int(latest['attack_count']):,}",
                "delta": "ATTACK OUTPUT",
                "meta": "BINARY MODEL",
                "tone": "violet",
            },
            {
                "label": "Alerts retained",
                "value": f"{len(alerts):,}",
                "delta": "INCIDENT STREAM",
                "meta": "MAX 100",
                "tone": "amber",
            },
            {
                "label": "Phase 7 status",
                "value": str(presentation.get("label", latest["status"])),
                "delta": "AUTHORITATIVE",
                "meta": "NO UI RULES",
                "tone": "cyan",
            },
        ]
    )

    section_title("Threat concentration")
    st.altair_chart(threat_rate_chart(metrics, height=390), width="stretch")
    st.markdown(
        '<div class="metadata-note">Scenario and source labels are simulation metadata, not attack-family predictions.</div>',
        unsafe_allow_html=True,
    )

    if not alerts.empty:
        label_column, scenario_column = st.columns([1.35, 1], gap="large")
        with label_column:
            section_title("Source-label activity")
            label_counts = (
                alerts["source_label"]
                .fillna("Unknown")
                .value_counts()
                .rename_axis("Source label")
                .reset_index(name="Alerts")
                .head(10)
                .sort_values("Alerts")
            )
            label_base = alt.Chart(label_counts).encode(
                x=alt.X("Alerts:Q", title="Retained alerts", scale=alt.Scale(zero=True, nice=True)),
                y=alt.Y(
                    "Source label:N",
                    title=None,
                    sort=None,
                    axis=alt.Axis(labelLimit=210, labelPadding=10),
                ),
                tooltip=["Source label:N", alt.Tooltip("Alerts:Q", format=",")],
            )
            label_bars = label_base.mark_bar(
                cornerRadiusEnd=6,
                height=20,
            ).encode(
                color=alt.Color(
                    "Alerts:Q",
                    scale=alt.Scale(range=["#fecdd3", "#e11d48"]),
                    legend=None,
                )
            )
            label_values = label_base.mark_text(
                align="left",
                baseline="middle",
                dx=8,
                color="#881337",
                font="Ubuntu Sans Mono",
                fontSize=12,
                fontWeight=700,
            ).encode(text=alt.Text("Alerts:Q", format=","))
            label_chart = (label_bars + label_values).properties(height=350)
            st.altair_chart(polish_chart(label_chart), width="stretch")

        with scenario_column:
            section_title("Scenario context")
            scenario_counts = (
                alerts["scenario"]
                .fillna("Unknown")
                .value_counts()
                .rename_axis("Scenario")
                .reset_index(name="Alerts")
            )
            scenario_chart = donut_chart(
                scenario_counts,
                category="Scenario",
                value="Alerts",
                domain=["NORMAL", "DDOS_BURST", "MIXED"],
                colors=["#059669", "#e11d48", "#7c3aed"],
                center_label="alerts",
                height=350,
            )
            st.altair_chart(scenario_chart, width="stretch")

    section_title("Recent threat alerts")
    render_alert_table(alerts, limit=25)


def page_system() -> None:
    page_header(
        "Infrastructure / Signal path",
        "Pipeline Control",
        "Handoff health, active detection-window state, and raw transport evidence.",
    )
    system_live()


@st.fragment(run_every=REFRESH_INTERVAL)
def system_live() -> None:
    metrics, alerts = load_dashboard_data()
    section_title("Live pipeline")
    st.markdown(
        """
        <div class="pipeline">
            <div class="pipeline-step">
                <div class="pipeline-number">01 · REPLAY</div>
                <div class="pipeline-name">CICIoT2023</div>
                <div class="pipeline-meta">Historical network events</div>
            </div>
            <div class="pipeline-step">
                <div class="pipeline-number">02 · TRANSPORT</div>
                <div class="pipeline-name">Kafka</div>
                <div class="pipeline-meta">iot-network-events</div>
            </div>
            <div class="pipeline-step">
                <div class="pipeline-number">03 · ANALYTICS</div>
                <div class="pipeline-name">Spark Streaming</div>
                <div class="pipeline-meta">Schema + 27 features</div>
            </div>
            <div class="pipeline-step">
                <div class="pipeline-number">04 · INFERENCE</div>
                <div class="pipeline-name">Random Forest</div>
                <div class="pipeline-meta">Normal / Attack</div>
            </div>
            <div class="pipeline-step">
                <div class="pipeline-number">05 · OUTPUT</div>
                <div class="pipeline-name">Dashboard + SQLite</div>
                <div class="pipeline-meta">Live + durable history</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if metrics.empty:
        render_empty_state()
        return

    complete_metrics = completed_metric_windows(metrics)
    if complete_metrics.empty:
        render_window_warming_state()
        return

    latest = complete_metrics.iloc[-1]
    render_metric_cards(
        [
            {
                "label": "Snapshot feed",
                "value": "ONLINE",
                "delta": "2S REFRESH",
                "meta": "JSON HANDOFF",
                "tone": "green",
            },
            {
                "label": "Metric windows",
                "value": f"{len(complete_metrics):,}",
                "delta": "AVAILABLE",
                "meta": "COMPLETE",
                "tone": "cyan",
            },
            {
                "label": "Alert buffer",
                "value": f"{len(alerts):,}",
                "unit": "/ 100",
                "delta": "BOUNDED",
                "meta": "CURRENT RUNTIME",
                "tone": "red",
            },
            {
                "label": "Last update",
                "value": display_time(latest["window_end"]).replace(" UTC", ""),
                "delta": "UTC",
                "meta": "WINDOW END",
                "tone": "violet",
            },
        ]
    )

    section_title("Latest complete detection window")
    window_table = pd.DataFrame(
        [
            {
                "Window start": latest["window_start"],
                "Window end": latest["window_end"],
                "Status": latest["status"],
                "Events": int(latest["event_count"]),
                "Event rate": float(latest["event_rate"]),
                "Normal": int(latest["normal_count"]),
                "Attack": int(latest["attack_count"]),
                "Threat traffic": float(latest["attack_rate"]),
            }
        ]
    )
    st.dataframe(
        window_table,
        hide_index=True,
        width="stretch",
        column_config={
            "Window start": st.column_config.DatetimeColumn(format="HH:mm:ss"),
            "Window end": st.column_config.DatetimeColumn(format="HH:mm:ss"),
            "Event rate": st.column_config.NumberColumn(format="%.1f events/s"),
            "Threat traffic": st.column_config.NumberColumn(format="percent"),
        },
    )

    with st.expander("Metric snapshot records"):
        technical_metrics = metrics[
            [
                "window_start",
                "window_end",
                "status",
                "event_count",
                "event_rate",
                "normal_count",
                "attack_count",
                "attack_rate",
            ]
        ].sort_values("window_end", ascending=False)
        st.dataframe(technical_metrics, hide_index=True, width="stretch", height=360)

    with st.expander("Kafka alert metadata"):
        if alerts.empty:
            st.info("No alert metadata in the current runtime.")
        else:
            technical_alerts = alerts[
                [
                    "event_timestamp",
                    "kafka_timestamp",
                    "topic",
                    "partition",
                    "offset",
                    "prediction",
                    "probability",
                ]
            ]
            st.dataframe(technical_alerts, hide_index=True, width="stretch", height=360)


def render_sidebar_navigation(pages: list[st.Page]) -> None:
    with st.sidebar:
        st.markdown(
            """
            <div class="sidebar-brand">
                <div class="sidebar-logo-row">
                    <div class="sidebar-mark">S//</div>
                    <div>
                        <div class="sidebar-title">SENTINEL / IOT</div>
                        <div class="sidebar-subtitle">Threat Operations Console</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown('<div class="nav-label">COMMAND MODULES</div>', unsafe_allow_html=True)
        st.page_link(pages[0], label="Command Overview", icon=":material/grid_view:", width="stretch")
        st.page_link(pages[1], label="Traffic Matrix", icon=":material/ssid_chart:", width="stretch")
        st.page_link(pages[2], label="Threat Intelligence", icon=":material/shield:", width="stretch")
        st.page_link(pages[3], label="Pipeline Control", icon=":material/account_tree:", width="stretch")
        st.markdown(
            """
            <div class="sidebar-health">
                <div class="sidebar-health-row">
                    <span class="live-dot"></span>
                    SYSTEM LINK ACTIVE
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="sidebar-footer">
                ENGINE // RANDOM FOREST · FROZEN<br>
                SIGNAL // PHASE 7 AUTHORITATIVE<br>
                SYNC // 2 SECOND INTERVAL
            </div>
            """,
            unsafe_allow_html=True,
        )


def main() -> None:
    st.set_page_config(
        page_title="IoT Threat Detection Monitor",
        page_icon="🛡️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)
    premium_css = Path(__file__).with_name("premium.css").read_text(encoding="utf-8")
    st.markdown(f"<style>{premium_css}</style>", unsafe_allow_html=True)
    pages = [
        st.Page(
            page_overview,
            title="Overview",
            icon=":material/dashboard:",
            url_path="overview",
            default=True,
        ),
        st.Page(
            page_traffic,
            title="Traffic Analytics",
            icon=":material/monitoring:",
            url_path="traffic",
        ),
        st.Page(
            page_threats,
            title="Threat Intelligence",
            icon=":material/security:",
            url_path="threats",
        ),
        st.Page(
            page_system,
            title="System Details",
            icon=":material/hub:",
            url_path="system",
        ),
    ]
    navigation = st.navigation(pages, position="hidden")
    render_sidebar_navigation(pages)
    navigation.run()


main()
