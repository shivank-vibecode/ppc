#!/usr/bin/env python3
"""
AppsFlyer CSV parser for the App Paid Marketing Analyst skill.

Reads CSV exports from /Users/shivank/Desktop/ASO/Client/sources/ and outputs
a structured JSON summary for the agent to reason over.

Expected CSV types (detected by column headers):
  - Campaign performance by Date: columns [Date, Campaign, ...]
  - Campaign performance by Media Source: columns [Media source, Campaign, ...]
  - Media source performance by Date: columns [Date, Media source, ...]
"""

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

SOURCES_DIR = Path("/Users/shivank/Desktop/ASO/Client/sources")
APPSFLYER_SUBDIR = SOURCES_DIR / "Appsflyer"

METRIC_COLS = {
    "installs": "Installs appsflyer",
    "signups": "Unique users ltv days cumulative appsflyer signup",
    "real_accounts": "Unique users ltv days cumulative appsflyer wallet_created",
    "ftd": "Unique users ltv days cumulative appsflyer first_time_deposit",
    "ftt": "Unique users ltv days cumulative appsflyer first_time_trade",
    "total_attributions": "Total attributions appsflyer",
}


def safe_int(val):
    if val is None or val == "":
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def extract_metrics(row):
    return {key: safe_int(row.get(col, 0)) for key, col in METRIC_COLS.items()}


def compute_rates(metrics):
    installs = metrics.get("installs", 0)
    if installs == 0:
        return {
            "signup_rate": 0,
            "real_account_rate": 0,
            "ftd_rate": 0,
            "ftt_rate": 0,
            "dau_quality_score": 0,
        }
    signup_r = metrics["signups"] / installs
    ra_r = metrics["real_accounts"] / installs
    ftd_r = metrics["ftd"] / installs
    ftt_r = metrics["ftt"] / installs
    dau_q = 0.1 * signup_r + 0.2 * ra_r + 0.3 * ftd_r + 0.4 * ftt_r
    return {
        "signup_rate": round(signup_r, 4),
        "real_account_rate": round(ra_r, 4),
        "ftd_rate": round(ftd_r, 4),
        "ftt_rate": round(ftt_r, 4),
        "dau_quality_score": round(dau_q, 4),
    }


def detect_csv_type(headers):
    has_date = "Date" in headers
    has_source = "Media source" in headers
    has_campaign = "Campaign" in headers

    if has_date and has_campaign and not has_source:
        return "campaign_by_date"
    if has_source and has_campaign and not has_date:
        return "campaign_by_source"
    if has_date and has_source and not has_campaign:
        return "source_by_date"
    if has_date and has_source and has_campaign:
        return "campaign_by_date_and_source"
    return "unknown"


def read_csv_file(filepath):
    rows = []
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def aggregate_metrics(records):
    agg = {k: 0 for k in METRIC_COLS}
    for r in records:
        for k in METRIC_COLS:
            agg[k] += r[k]
    return agg


def process_campaign_by_date(rows):
    daily = defaultdict(lambda: defaultdict(list))
    for row in rows:
        date = row.get("Date", "")
        campaign = row.get("Campaign", "Unknown")
        metrics = extract_metrics(row)
        daily[date][campaign].append(metrics)

    daily_totals = {}
    campaign_daily = defaultdict(dict)
    for date in sorted(daily.keys()):
        day_agg = {k: 0 for k in METRIC_COLS}
        for campaign, records in daily[date].items():
            camp_agg = aggregate_metrics(records)
            campaign_daily[campaign][date] = {**camp_agg, **compute_rates(camp_agg)}
            for k in METRIC_COLS:
                day_agg[k] += camp_agg[k]
        daily_totals[date] = {**day_agg, **compute_rates(day_agg)}

    campaign_totals = {}
    for campaign, dates in campaign_daily.items():
        agg = {k: 0 for k in METRIC_COLS}
        for d_metrics in dates.values():
            for k in METRIC_COLS:
                agg[k] += d_metrics[k]
        campaign_totals[campaign] = {**agg, **compute_rates(agg)}

    return {
        "daily_totals": daily_totals,
        "campaign_totals": dict(
            sorted(campaign_totals.items(), key=lambda x: x[1]["installs"], reverse=True)
        ),
    }


def process_campaign_by_source(rows):
    source_campaigns = defaultdict(list)
    for row in rows:
        source = row.get("Media source", "Unknown")
        campaign = row.get("Campaign", "Unknown")
        metrics = extract_metrics(row)
        source_campaigns[source].append({"campaign": campaign, **metrics})

    result = {}
    for source, campaigns in source_campaigns.items():
        source_agg = aggregate_metrics(campaigns)
        campaign_list = []
        for c in campaigns:
            camp_metrics = {k: c[k] for k in METRIC_COLS}
            campaign_list.append({
                "campaign": c["campaign"],
                **camp_metrics,
                **compute_rates(camp_metrics),
            })
        campaign_list.sort(key=lambda x: x["installs"], reverse=True)
        result[source] = {
            "totals": {**source_agg, **compute_rates(source_agg)},
            "campaigns": campaign_list,
        }

    return result


def process_source_by_date(rows):
    daily_source = defaultdict(lambda: defaultdict(lambda: {k: 0 for k in METRIC_COLS}))
    for row in rows:
        date = row.get("Date", "")
        source = row.get("Media source", "Unknown")
        metrics = extract_metrics(row)
        for k in METRIC_COLS:
            daily_source[date][source][k] += metrics[k]

    result = {}
    for date in sorted(daily_source.keys()):
        sources = {}
        for source, metrics in daily_source[date].items():
            sources[source] = {**metrics, **compute_rates(metrics)}
        result[date] = sources

    source_totals = defaultdict(lambda: {k: 0 for k in METRIC_COLS})
    for date_data in daily_source.values():
        for source, metrics in date_data.items():
            for k in METRIC_COLS:
                source_totals[source][k] += metrics[k]

    source_totals_with_rates = {}
    for source, metrics in source_totals.items():
        source_totals_with_rates[source] = {**metrics, **compute_rates(metrics)}

    return {
        "daily_by_source": result,
        "source_totals": dict(
            sorted(source_totals_with_rates.items(), key=lambda x: x[1]["installs"], reverse=True)
        ),
    }


def compute_rolling_averages(daily_totals, window=7):
    dates = sorted(daily_totals.keys())
    rolling = {}
    for i, date in enumerate(dates):
        start = max(0, i - window + 1)
        window_dates = dates[start : i + 1]
        window_data = [daily_totals[d] for d in window_dates]
        n = len(window_data)
        avg = {}
        for k in METRIC_COLS:
            avg[k] = round(sum(d[k] for d in window_data) / n, 1)
        avg.update(compute_rates(avg))
        rolling[date] = avg
    return rolling


def compute_day_over_day(daily_totals):
    dates = sorted(daily_totals.keys())
    deltas = {}
    for i in range(1, len(dates)):
        today = daily_totals[dates[i]]
        yesterday = daily_totals[dates[i - 1]]
        delta = {}
        for k in METRIC_COLS:
            prev = yesterday[k]
            curr = today[k]
            delta[k] = curr - prev
            if prev > 0:
                delta[f"{k}_pct_change"] = round((curr - prev) / prev * 100, 1)
            else:
                delta[f"{k}_pct_change"] = None
        for rate_key in ["signup_rate", "real_account_rate", "ftd_rate", "ftt_rate"]:
            prev_rate = yesterday.get(rate_key, 0)
            curr_rate = today.get(rate_key, 0)
            delta[rate_key] = round(curr_rate - prev_rate, 4)
        deltas[dates[i]] = delta
    return deltas


def compute_source_mix(source_by_date_result):
    daily = source_by_date_result.get("daily_by_source", {})
    mix = {}
    for date in sorted(daily.keys()):
        total_installs = sum(s["installs"] for s in daily[date].values())
        if total_installs == 0:
            continue
        day_mix = {}
        for source, metrics in daily[date].items():
            day_mix[source] = round(metrics["installs"] / total_installs * 100, 1)
        mix[date] = day_mix
    return mix


def identify_zero_ftt_campaigns(campaign_by_source_result, min_installs=50):
    flagged = []
    for source, data in campaign_by_source_result.items():
        for c in data["campaigns"]:
            if c["installs"] >= min_installs and c["ftt"] == 0:
                flagged.append({
                    "source": source,
                    "campaign": c["campaign"],
                    "installs": c["installs"],
                    "signups": c["signups"],
                    "real_accounts": c["real_accounts"],
                    "signup_rate": c["signup_rate"],
                    "real_account_rate": c["real_account_rate"],
                })
    flagged.sort(key=lambda x: x["installs"], reverse=True)
    return flagged


def identify_anomalies(daily_totals, rolling_averages):
    anomalies = []
    dates = sorted(daily_totals.keys())
    for date in dates:
        if date not in rolling_averages:
            continue
        avg = rolling_averages[date]
        actual = daily_totals[date]
        if avg["installs"] > 0:
            ratio = actual["installs"] / avg["installs"]
            if ratio > 2.0:
                anomalies.append({
                    "date": date,
                    "type": "volume_spike",
                    "detail": f"Installs {actual['installs']} vs 7d avg {avg['installs']:.0f} ({ratio:.1f}x)",
                })
            elif ratio < 0.5:
                anomalies.append({
                    "date": date,
                    "type": "volume_drop",
                    "detail": f"Installs {actual['installs']} vs 7d avg {avg['installs']:.0f} ({ratio:.1f}x)",
                })
        for rate_key in ["signup_rate", "real_account_rate", "ftd_rate", "ftt_rate"]:
            avg_rate = avg.get(rate_key, 0)
            actual_rate = actual.get(rate_key, 0)
            if avg_rate > 0:
                change = (actual_rate - avg_rate) / avg_rate
                if abs(change) > 0.10:
                    direction = "increased" if change > 0 else "decreased"
                    anomalies.append({
                        "date": date,
                        "type": f"{rate_key}_anomaly",
                        "detail": f"{rate_key} {direction} {abs(change)*100:.1f}% vs 7d avg "
                                  f"(actual: {actual_rate:.4f}, avg: {avg_rate:.4f})",
                    })
    return anomalies


def main():
    if not SOURCES_DIR.exists():
        print(json.dumps({"error": f"Sources directory not found: {SOURCES_DIR}"}))
        sys.exit(1)

    csv_files = sorted(SOURCES_DIR.glob("**/*.csv"))
    if APPSFLYER_SUBDIR.exists():
        csv_files.extend(sorted(APPSFLYER_SUBDIR.glob("*.csv")))
    if not csv_files:
        print(json.dumps({"error": f"No CSV files found in {SOURCES_DIR}"}))
        sys.exit(1)

    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "sources_dir": str(SOURCES_DIR),
            "files_processed": [],
        },
        "campaign_by_date": None,
        "campaign_by_source": None,
        "source_by_date": None,
        "rolling_averages_7d": None,
        "day_over_day_deltas": None,
        "source_mix_pct": None,
        "zero_ftt_campaigns": None,
        "anomalies": None,
    }

    for csv_file in csv_files:
        try:
            rows = read_csv_file(csv_file)
        except Exception as e:
            output["metadata"]["files_processed"].append({
                "file": csv_file.name,
                "status": "error",
                "error": str(e),
            })
            continue

        if not rows:
            output["metadata"]["files_processed"].append({
                "file": csv_file.name,
                "status": "empty",
            })
            continue

        csv_type = detect_csv_type(list(rows[0].keys()))
        output["metadata"]["files_processed"].append({
            "file": csv_file.name,
            "type": csv_type,
            "rows": len(rows),
            "status": "ok",
        })

        if csv_type == "campaign_by_date":
            output["campaign_by_date"] = process_campaign_by_date(rows)
        elif csv_type == "campaign_by_source":
            output["campaign_by_source"] = process_campaign_by_source(rows)
        elif csv_type == "source_by_date":
            output["source_by_date"] = process_source_by_date(rows)

    if output["campaign_by_date"] and "daily_totals" in output["campaign_by_date"]:
        dt = output["campaign_by_date"]["daily_totals"]
        output["rolling_averages_7d"] = compute_rolling_averages(dt)
        output["day_over_day_deltas"] = compute_day_over_day(dt)

    if output["source_by_date"]:
        output["source_mix_pct"] = compute_source_mix(output["source_by_date"])

    if output["campaign_by_source"]:
        output["zero_ftt_campaigns"] = identify_zero_ftt_campaigns(output["campaign_by_source"])

    if output["campaign_by_date"] and output["rolling_averages_7d"]:
        output["anomalies"] = identify_anomalies(
            output["campaign_by_date"]["daily_totals"],
            output["rolling_averages_7d"],
        )

    last_7_dates = sorted(output["campaign_by_date"]["daily_totals"].keys())[-7:] if output["campaign_by_date"] else []
    if last_7_dates:
        output["summary"] = {
            "period": f"{last_7_dates[0]} to {last_7_dates[-1]}",
            "latest_date": last_7_dates[-1],
            "last_7_days": last_7_dates,
        }

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
