"""
Trade Surveillance Compliance Dashboard
========================================
Reads live alerts from the Kafka 'alerts' topic (time-based seek to today's
messages) and gold-layer Parquet aggregates from MinIO. Auto-refreshes every
30 seconds via streamlit-autorefresh.

Sections
--------
1. Summary row  — total alerts, spoofing, wash trading, unique traders flagged
2. Live alerts  — filterable by type / symbol / severity; colour-coded rows
3. High-risk traders  — ranked by alert count + cancel rate
4. Asset risk overview — stacked bar chart of alerts per symbol
5. Trader deep dive    — per-trader alert list + gold-layer activity stats
"""

import json
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable
from minio import Minio
from minio.error import S3Error
from streamlit_autorefresh import st_autorefresh

# ── Page config (must be the very first Streamlit call) ───────────────────────
st.set_page_config(
    page_title="Trade Surveillance",
    page_icon="⚠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_SERVERS      = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_ALERTS_TOPIC = os.environ.get("KAFKA_ALERTS_TOPIC",      "alerts")
MINIO_ENDPOINT     = os.environ.get("MINIO_ENDPOINT",          "minio:9000")
MINIO_ACCESS_KEY   = os.environ.get("MINIO_ACCESS_KEY",        "minio_admin")
MINIO_SECRET_KEY   = os.environ.get("MINIO_SECRET_KEY",        "minio_secret_123")
MINIO_BUCKET       = os.environ.get("MINIO_BUCKET",            "surveillance-lake")

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── global ── */
.stApp { background-color: #0E1117; }
html, body, [class*="css"] { font-family: "Inter", "Segoe UI", monospace; }

/* ── top-bar ── */
.dash-header {
    display: flex; align-items: center; gap: 14px;
    padding: 6px 0 20px; border-bottom: 1px solid #1E2736; margin-bottom: 20px;
}
.dash-title   { color: #E2E8F0; font-size: 1.45rem; font-weight: 700; margin: 0; line-height: 1.2; }
.dash-sub     { color: #6B7A8D; font-size: 0.78rem; margin: 2px 0 0; }
.live-dot     {
    width: 9px; height: 9px; border-radius: 50%;
    background: #00FF88; flex-shrink: 0;
    box-shadow: 0 0 6px #00FF88;
    animation: pulse 2s ease-in-out infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

/* ── metric cards ── */
[data-testid="stMetric"] {
    background: #131A27;
    border: 1px solid #1E2736;
    border-radius: 10px;
    padding: 18px 20px !important;
}
[data-testid="stMetric"] label {
    color: #6B7A8D !important; font-size: 0.72rem !important;
    text-transform: uppercase; letter-spacing: .1em;
}
[data-testid="stMetricValue"] {
    color: #E2E8F0 !important; font-size: 2.1rem !important; font-weight: 800 !important;
}

/* ── section headings ── */
.sec-head {
    color: #00D4FF; font-size: 0.78rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: .14em;
    border-left: 3px solid #00D4FF; padding-left: 10px;
    margin: 24px 0 10px;
}

/* ── severity pill inside markdown ── */
.pill-HIGH   { background:rgba(255,75,75,.15);  color:#FF4B4B; border:1px solid #FF4B4B; }
.pill-MEDIUM { background:rgba(255,165,0,.15);  color:#FFA500; border:1px solid #FFA500; }
.pill-LOW    { background:rgba(255,215,0,.12);  color:#FFD700; border:1px solid #FFD700; }
.pill-HIGH, .pill-MEDIUM, .pill-LOW {
    padding: 1px 8px; border-radius: 4px; font-size: .72rem; font-weight: 600;
}

/* ── tables ── */
[data-testid="stDataFrame"] { border: 1px solid #1E2736 !important; border-radius: 8px; }

/* ── divider ── */
hr { border-color: #1E2736 !important; margin: 22px 0; }

/* ── hide chrome ── */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh (30 s) ───────────────────────────────────────────────────────
refresh_count = st_autorefresh(interval=30_000, key="surveillance_autorefresh")

# ── Header ────────────────────────────────────────────────────────────────────
now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
st.markdown(f"""
<div class="dash-header">
    <div class="live-dot"></div>
    <div>
        <p class="dash-title">⚠&nbsp; Trade Surveillance</p>
        <p class="dash-sub">Updated {now_utc} &nbsp;·&nbsp; auto-refresh every 30 s &nbsp;·&nbsp; cycle #{refresh_count}</p>
    </div>
</div>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# Data loading
# ════════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def _minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


@st.cache_data(ttl=30, show_spinner="Polling Kafka alerts…")
def load_alerts() -> pd.DataFrame:
    """Read today's alerts from Kafka using timestamp-based offset seeking."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS,
            group_id=None,                   # read-only, no group offsets committed
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: v.decode("utf-8"),
            consumer_timeout_ms=5_000,
            request_timeout_ms=12_000,
            connections_max_idle_ms=15_000,
        )

        partition_ids = consumer.partitions_for_topic(KAFKA_ALERTS_TOPIC)
        if not partition_ids:
            consumer.close()
            return pd.DataFrame()

        tps = [TopicPartition(KAFKA_ALERTS_TOPIC, p) for p in partition_ids]
        consumer.assign(tps)

        # Seek to today's UTC midnight so we only read today's messages
        today_ms = int(
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp() * 1_000
        )
        offsets = consumer.offsets_for_times({tp: today_ms for tp in tps})
        for tp, meta in offsets.items():
            if meta:
                consumer.seek(tp, meta.offset)
            else:
                consumer.seek_to_end(tp)   # no messages yet at/after midnight

        rows = []
        for msg in consumer:
            try:
                rows.append(json.loads(msg.value))
            except json.JSONDecodeError:
                pass
        consumer.close()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "detected_at" in df.columns:
            df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True, errors="coerce")
        if "cancel_rate" in df.columns:
            df["cancel_rate"] = pd.to_numeric(df["cancel_rate"], errors="coerce")
        return df

    except (NoBrokersAvailable, KafkaError):
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_gold(_minio: Minio, table: str) -> pd.DataFrame:
    """Read today's gold-layer Parquet from MinIO."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path  = f"gold/{table}/{today}/part.parquet"
    try:
        resp = _minio.get_object(MINIO_BUCKET, path)
        return pd.read_parquet(BytesIO(resp.read()))
    except (S3Error, Exception):
        return pd.DataFrame()


def _severity(row: dict) -> str:
    if row.get("alert_type") == "WASH_TRADING":
        return "HIGH"
    cr = row.get("cancel_rate") or 0
    try:
        cr = float(cr)
    except (TypeError, ValueError):
        cr = 0
    if cr >= 0.95:
        return "HIGH"
    if cr >= 0.85:
        return "MEDIUM"
    return "LOW"


def add_severity(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["severity"] = df.apply(_sev_row, axis=1)
    return df


def _sev_row(row) -> str:
    return _severity(row.to_dict())


# ── Fetch everything ──────────────────────────────────────────────────────────
minio_client    = _minio_client()
alerts_raw      = load_alerts()
gold_trader     = load_gold(minio_client, "trader_daily_activity")
gold_asset      = load_gold(minio_client, "asset_hourly_summary")

alerts_df = alerts_raw.copy()
if not alerts_df.empty:
    alerts_df["severity"] = alerts_df.apply(_sev_row, axis=1)


# ════════════════════════════════════════════════════════════════════════════════
# § 1  Summary row
# ════════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="sec-head">Summary — Today</p>', unsafe_allow_html=True)

total   = len(alerts_df)
n_spoof = int((alerts_df["alert_type"] == "SPOOFING").sum())    if not alerts_df.empty else 0
n_wash  = int((alerts_df["alert_type"] == "WASH_TRADING").sum()) if not alerts_df.empty else 0

unique_traders: set = set()
if not alerts_df.empty:
    for col in ("trader_id", "trader_id_1", "trader_id_2"):
        if col in alerts_df.columns:
            unique_traders.update(alerts_df[col].dropna().astype(str))

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Alerts Today",    f"{total:,}")
c2.metric("Spoofing",              f"{n_spoof:,}")
c3.metric("Wash Trading",          f"{n_wash:,}")
c4.metric("Unique Traders Flagged", f"{len(unique_traders):,}")

st.markdown("---")


# ════════════════════════════════════════════════════════════════════════════════
# § 2  Live alerts table
# ════════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="sec-head">Live Alerts Feed</p>', unsafe_allow_html=True)

if alerts_df.empty:
    st.info("No alerts today. Kafka broker may still be starting, or Flink jobs are not yet running.")
else:
    # ── Filter bar ──────────────────────────────────────────────────────────
    fc1, fc2, fc3, _pad = st.columns([1, 1, 1, 3])

    type_opts  = ["ALL"] + sorted(alerts_df["alert_type"].dropna().unique())
    sev_opts   = ["ALL", "HIGH", "MEDIUM", "LOW"]
    sym_opts   = ["ALL"] + (sorted(alerts_df["symbol"].dropna().unique())
                            if "symbol" in alerts_df.columns else [])

    sel_type = fc1.selectbox("Alert type", type_opts, key="f_type")
    sel_sev  = fc2.selectbox("Severity",   sev_opts,  key="f_sev")
    sel_sym  = fc3.selectbox("Symbol",     sym_opts,  key="f_sym")

    flt = alerts_df.copy()
    if sel_type != "ALL":
        flt = flt[flt["alert_type"] == sel_type]
    if sel_sev != "ALL":
        flt = flt[flt["severity"] == sel_sev]
    if sel_sym != "ALL" and "symbol" in flt.columns:
        flt = flt[flt["symbol"] == sel_sym]

    if "detected_at" in flt.columns:
        flt = flt.sort_values("detected_at", ascending=False)

    # ── Build display frame ─────────────────────────────────────────────────
    want_cols = [
        "detected_at", "alert_type", "severity", "symbol",
        "trader_id", "trader_id_1", "trader_id_2",
        "cancel_rate", "cancel_count", "total_orders",
        "match_count", "avg_price_diff_pct",
    ]
    show_cols = [c for c in want_cols if c in flt.columns]
    disp = flt[show_cols].head(500).copy()

    if "detected_at" in disp.columns:
        disp["detected_at"] = disp["detected_at"].dt.strftime("%H:%M:%S")
    if "cancel_rate" in disp.columns:
        disp["cancel_rate"] = disp["cancel_rate"].apply(
            lambda x: f"{x:.1%}" if pd.notna(x) else "—"
        )
    if "avg_price_diff_pct" in disp.columns:
        disp["avg_price_diff_pct"] = disp["avg_price_diff_pct"].apply(
            lambda x: f"{float(x):.3f}%" if pd.notna(x) else "—"
        )

    # ── Row colouring ────────────────────────────────────────────────────────
    SEV_BG = {
        "HIGH":   "background-color: rgba(255, 75,  75, 0.10)",
        "MEDIUM": "background-color: rgba(255,165,  0, 0.10)",
        "LOW":    "background-color: rgba(255,215,  0, 0.08)",
    }

    def _row_style(row):
        bg = SEV_BG.get(row.get("severity", ""), "")
        return [bg] * len(row)

    styled = disp.style.apply(_row_style, axis=1)

    st.caption(f"{len(flt):,} alerts match filters — showing latest 500")
    st.dataframe(styled, use_container_width=True, height=360, hide_index=True)

st.markdown("---")


# ════════════════════════════════════════════════════════════════════════════════
# § 3  High-risk traders  |  § 4  Asset risk chart
# ════════════════════════════════════════════════════════════════════════════════
left_col, right_col = st.columns(2, gap="large")

# ── High-risk traders ─────────────────────────────────────────────────────────
with left_col:
    st.markdown('<p class="sec-head">High-Risk Traders</p>', unsafe_allow_html=True)

    if alerts_df.empty:
        st.info("No data yet.")
    else:
        rows_acc: list[dict] = []

        # SPOOFING: single trader_id per alert
        spoof = alerts_df[alerts_df.get("alert_type", pd.Series(dtype=str)) == "SPOOFING"] \
            if "alert_type" in alerts_df.columns else pd.DataFrame()
        if not spoof.empty and "trader_id" in spoof.columns:
            for tid, grp in spoof.groupby("trader_id"):
                rows_acc.append({
                    "trader_id":       str(tid),
                    "alert_count":     len(grp),
                    "alert_types":     "SPOOFING",
                    "avg_cancel_rate": float(grp["cancel_rate"].mean())
                                       if "cancel_rate" in grp.columns else None,
                    "latest_alert":    grp["detected_at"].max()
                                       if "detected_at" in grp.columns else None,
                })

        # WASH_TRADING: two traders per alert
        wash = alerts_df[alerts_df.get("alert_type", pd.Series(dtype=str)) == "WASH_TRADING"] \
            if "alert_type" in alerts_df.columns else pd.DataFrame()
        if not wash.empty:
            for side in ("trader_id_1", "trader_id_2"):
                if side not in wash.columns:
                    continue
                for tid, grp in wash.groupby(side):
                    rows_acc.append({
                        "trader_id":       str(tid),
                        "alert_count":     len(grp),
                        "alert_types":     "WASH_TRADING",
                        "avg_cancel_rate": None,
                        "latest_alert":    grp["detected_at"].max()
                                           if "detected_at" in grp.columns else None,
                    })

        if not rows_acc:
            st.info("No trader-level data yet.")
        else:
            risk = (
                pd.DataFrame(rows_acc)
                .groupby("trader_id", as_index=False)
                .agg(
                    alert_count     =("alert_count",     "sum"),
                    alert_types     =("alert_types",     lambda s: " + ".join(sorted(set(s)))),
                    avg_cancel_rate =("avg_cancel_rate", "mean"),
                    latest_alert    =("latest_alert",    "max"),
                )
                .sort_values("alert_count", ascending=False)
                .head(25)
            )

            risk["avg_cancel_rate"] = risk["avg_cancel_rate"].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) else "—"
            )
            if pd.api.types.is_datetime64_any_dtype(risk["latest_alert"]):
                risk["latest_alert"] = risk["latest_alert"].dt.strftime("%H:%M:%S")

            risk.columns = ["Trader", "Alerts", "Types", "Avg Cancel Rate", "Last Alert"]
            st.dataframe(risk, use_container_width=True, hide_index=True, height=380)

# ── Asset risk bar chart ──────────────────────────────────────────────────────
with right_col:
    st.markdown('<p class="sec-head">Asset Risk Overview</p>', unsafe_allow_html=True)

    if alerts_df.empty or "symbol" not in alerts_df.columns:
        st.info("No data yet.")
    else:
        sym_counts = (
            alerts_df
            .groupby(["symbol", "alert_type"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
        )

        fig = px.bar(
            sym_counts,
            x="symbol",
            y="count",
            color="alert_type",
            barmode="stack",
            color_discrete_map={"SPOOFING": "#FF4B4B", "WASH_TRADING": "#FF8C00"},
            template="plotly_dark",
            labels={"count": "Alerts", "symbol": "Symbol", "alert_type": "Type"},
        )
        fig.update_layout(
            plot_bgcolor="#131A27",
            paper_bgcolor="#131A27",
            font=dict(color="#C5CDD8", size=12),
            title=None,
            legend=dict(
                orientation="h", y=-0.22, x=0,
                font=dict(size=11), bgcolor="rgba(0,0,0,0)",
            ),
            margin=dict(l=0, r=0, t=10, b=50),
            xaxis=dict(gridcolor="#1E2736", title=None),
            yaxis=dict(gridcolor="#1E2736"),
            bargap=0.28,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Secondary mini-chart: hourly alert volume
        if "detected_at" in alerts_df.columns:
            hourly = alerts_df.copy()
            hourly["hour"] = hourly["detected_at"].dt.hour
            hourly_counts = (
                hourly.groupby(["hour", "alert_type"], as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )
            fig2 = px.area(
                hourly_counts,
                x="hour",
                y="count",
                color="alert_type",
                color_discrete_map={"SPOOFING": "#FF4B4B", "WASH_TRADING": "#FF8C00"},
                template="plotly_dark",
                labels={"count": "Alerts", "hour": "Hour (UTC)", "alert_type": "Type"},
            )
            fig2.update_layout(
                plot_bgcolor="#131A27",
                paper_bgcolor="#131A27",
                font=dict(color="#C5CDD8", size=11),
                title=dict(text="Hourly Volume", font=dict(size=12, color="#6B7A8D"), x=0),
                showlegend=False,
                margin=dict(l=0, r=0, t=30, b=30),
                xaxis=dict(gridcolor="#1E2736", dtick=2),
                yaxis=dict(gridcolor="#1E2736"),
            )
            st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")


# ════════════════════════════════════════════════════════════════════════════════
# § 5  Trader deep dive
# ════════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="sec-head">Trader Deep Dive</p>', unsafe_allow_html=True)

if alerts_df.empty:
    st.info("No alert data available for deep dive.")
else:
    all_tids: set = set()
    for col in ("trader_id", "trader_id_1", "trader_id_2"):
        if col in alerts_df.columns:
            all_tids.update(alerts_df[col].dropna().astype(str))

    if not all_tids:
        st.info("No traders found in today's alerts.")
    else:
        selected = st.selectbox(
            "Select trader", sorted(all_tids), key="deep_dive_sel"
        )

        # Mask: any column containing this trader
        mask = pd.Series(False, index=alerts_df.index)
        for col in ("trader_id", "trader_id_1", "trader_id_2"):
            if col in alerts_df.columns:
                mask |= alerts_df[col].astype(str) == selected

        ta = alerts_df[mask].copy()

        dd_left, dd_right = st.columns([3, 2], gap="large")

        with dd_left:
            st.markdown(f"**Alerts for `{selected}`** — {len(ta):,} today")

            if not ta.empty:
                if "detected_at" in ta.columns:
                    ta = ta.sort_values("detected_at", ascending=False)
                    ta["detected_at"] = ta["detected_at"].dt.strftime("%H:%M:%S")
                if "cancel_rate" in ta.columns:
                    ta["cancel_rate"] = ta["cancel_rate"].apply(
                        lambda x: f"{x:.1%}" if pd.notna(x) else "—"
                    )
                if "avg_price_diff_pct" in ta.columns:
                    ta["avg_price_diff_pct"] = ta["avg_price_diff_pct"].apply(
                        lambda x: f"{float(x):.3f}%" if pd.notna(x) else "—"
                    )

                def _sev_bg(row):
                    bg = SEV_BG.get(row.get("severity", ""), "")
                    return [bg] * len(row)

                st.dataframe(
                    ta.head(200).style.apply(_sev_bg, axis=1),
                    use_container_width=True, hide_index=True,
                )

        with dd_right:
            st.markdown("**Daily Activity (gold layer)**")

            if gold_trader.empty or "trader_id" not in gold_trader.columns:
                st.caption("Gold-layer data not yet available — check silver-gold-writer logs.")
            else:
                act = gold_trader[gold_trader["trader_id"] == selected]
                if act.empty:
                    st.caption(f"No gold-layer activity found for `{selected}` today.")
                else:
                    for _, row in act.iterrows():
                        sym = row.get("symbol", "?")
                        st.markdown(f"**{sym}**")
                        m1, m2 = st.columns(2)
                        m1.metric("Orders",  f"{int(row.get('total_orders',  0)):,}")
                        m2.metric("Cancels", f"{int(row.get('total_cancels', 0)):,}")
                        cr = row.get("cancel_rate")
                        m1.metric("Cancel Rate", f"{cr:.1%}" if pd.notna(cr) else "—")
                        m2.metric("Trades", f"{int(row.get('total_trades', 0)):,}")
                        avg_sz = row.get("avg_order_size")
                        avg_ttc = row.get("avg_time_to_cancel")
                        m1.metric("Avg Order Size", f"{avg_sz:.1f}" if pd.notna(avg_sz) else "—")
                        m2.metric("Avg Time-to-Cancel", f"{avg_ttc:.0f}s" if pd.notna(avg_ttc) else "—")
                        st.markdown("<hr style='margin:10px 0;border-color:#1E2736'>",
                                    unsafe_allow_html=True)
