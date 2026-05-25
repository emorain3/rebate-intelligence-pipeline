"""
Silver -> Gold ETL: reads silver CSV facts/dims and writes business-ready
gold tables to data/gold/*.parquet and data/gold/csv_exports/*.csv (overwritten
each run) for rebate monitoring, decline detection, and reconciliation.
"""

from __future__ import annotations

import hashlib
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
def _project_root() -> Path:
    here = Path(__file__).resolve().parent
    for d in [here, *here.parents]:
        if (d / "data" / "silver").is_dir():
            return d
    return here.parent


PROJECT_ROOT = _project_root()
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_EXPORT_DIR = SILVER_DIR / "csv_exports"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_CSV_EXPORT_DIR = GOLD_DIR / "csv_exports"

AFFILIATE_PATH = SILVER_EXPORT_DIR / "silver_affiliate.csv"
REBATE_PATH = SILVER_EXPORT_DIR / "silver_rebate.csv"
PARTNER_PATH = SILVER_EXPORT_DIR / "silver_partner.csv"

# POC tier rates by paint_sponsor tier id (placeholder percentages)
PAINT_SPONSOR_TIER_RATES: dict[Any, float] = {
    1: 0.05,
    2: 0.07,
    3: 0.10,
    4: 0.12,
    5: 0.15,
}

DECLINE_THRESHOLD = 0.30
PARTIAL_FEED_THRESHOLD = 0.80
EXCEPTIONS_CSV = SILVER_EXPORT_DIR / "pipeline_exceptions.csv"


def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


def make_row_id(row: pd.Series | dict[str, Any], source: str) -> str:
    """Stable row_id from rebate composite key, else affiliate/period keys, else uuid."""
    if isinstance(row, pd.Series):
        data = row.to_dict()
    else:
        data = row

    parts: list[str] = []
    for key in ("transaction_id", "partner_id", "memo", "net_amount", "netamount"):
        val = data.get(key)
        if val is not None and not (isinstance(val, float) and np.isnan(val)) and str(val).strip() != "":
            parts.append(str(val).strip())

    if not parts:
        for key in ("affiliate_id", "rebate_month", "partner_id"):
            val = data.get(key)
            if val is not None and not (isinstance(val, float) and np.isnan(val)) and str(val).strip() != "":
                parts.append(str(val).strip())

    if parts:
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
        return f"{source}_{digest}"
    return str(uuid.uuid4())


def log_exception(
    row: pd.Series,
    *,
    source: str,
    error_type: str,
    severity: str,
    field: str,
    detected_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one exception record matching the pipeline exception schema."""
    ts = detected_at or datetime.now()
    return {
        "row_id": make_row_id(row, source),
        "source": source,
        "error_type": error_type,
        "severity": severity,
        "field": field,
        "detected_at": ts,
        "status": "open",
    }


def write_exceptions_csv(exceptions: list[dict[str, Any]], append: bool = True) -> None:
    """Write or append structured exception rows to the shared exceptions log."""
    if not exceptions:
        return
    SILVER_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(exceptions)
    new_df["detected_at"] = pd.to_datetime(new_df["detected_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    if append and EXCEPTIONS_CSV.is_file():
        existing = pd.read_csv(EXCEPTIONS_CSV)
        out = pd.concat([existing, new_df], ignore_index=True)
    else:
        out = new_df

    out.to_csv(EXCEPTIONS_CSV, index=False, encoding="utf-8")
    log(f"Wrote {len(new_df)} exception row(s) -> {EXCEPTIONS_CSV} ({'appended' if append else 'overwritten'})")


def detect_volume_outliers(
    monthly_summary: pd.DataFrame,
    rebate: pd.DataFrame,
    exceptions: list[dict[str, Any]],
    detected_at: datetime,
) -> pd.DataFrame:
    """
    Flag affiliates whose monthly rebate total is outside the IQR fence
    for that period; set bad_data_flag on matching rebate rows.
    """
    if monthly_summary.empty or rebate.empty:
        return rebate

    out = rebate.copy()
    if "bad_data_flag" not in out.columns:
        out["bad_data_flag"] = False

    for period, group in monthly_summary.groupby("rebate_month", dropna=False):
        amounts = group["total_rebate_amount"].astype(float)
        if len(amounts) < 4:
            continue
        q1 = float(amounts.quantile(0.25))
        q3 = float(amounts.quantile(0.75))
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        outlier_mask = (group["total_rebate_amount"].astype(float) < low) | (
            group["total_rebate_amount"].astype(float) > high
        )
        for _, summary_row in group.loc[outlier_mask].iterrows():
            exceptions.append(
                log_exception(
                    summary_row,
                    source="rebate",
                    error_type="volume_outlier",
                    severity="MEDIUM",
                    field="total_rebate_amount",
                    detected_at=detected_at,
                )
            )
            aff_id = summary_row["affiliate_id"]
            period_mask = (out["affiliate_id"] == aff_id) & (
                out["rebate_month"].astype(str) == str(period)
            )
            out.loc[period_mask, "bad_data_flag"] = True

    flagged = int(out["bad_data_flag"].sum())
    if flagged:
        log(f"[rebate] Flagged {flagged} row(s) as volume_outlier (retained in processing).")
    return out


def detect_silent_shops(
    affiliate: pd.DataFrame,
    rebate: pd.DataFrame,
    latest_month: str | None,
    exceptions: list[dict[str, Any]],
    detected_at: datetime,
) -> None:
    """Log affiliates in the master list with zero rebate activity in the current period."""
    if latest_month is None or affiliate.empty:
        return

    active_latest = set(
        rebate.loc[rebate["rebate_month"].astype(str) == str(latest_month), "affiliate_id"].dropna()
    )
    silent = affiliate.loc[~affiliate["affiliate_id"].isin(active_latest)]
    for idx in silent.index:
        row = affiliate.loc[idx].copy()
        row["rebate_month"] = str(latest_month)
        exceptions.append(
            log_exception(
                row,
                source="affiliate",
                error_type="silent_shop",
                severity="LOW",
                field="affiliate_id",
                detected_at=detected_at,
            )
        )
    if len(silent):
        log(f"[affiliate] Logged {len(silent)} silent_shop exception(s) for period {latest_month}.")


# ---------------------------------------------------------------------------
# Silver schema alignment (handles both canonical snake_case and raw exports)
# ---------------------------------------------------------------------------
def standardize_affiliate(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "affiliate_id" not in out.columns and "id" in out.columns:
        out = out.rename(columns={"id": "affiliate_id"})
    if "affiliate_name" not in out.columns:
        for alt in ("companyname", "company_name", "name"):
            if alt in out.columns:
                out = out.rename(columns={alt: "affiliate_name"})
                break
    if "parent_id" not in out.columns and "parent" in out.columns:
        out = out.rename(columns={"parent": "parent_id"})
    if "parent_id" not in out.columns:
        out["parent_id"] = pd.NA
    out["affiliate_id"] = pd.to_numeric(out["affiliate_id"], errors="coerce").astype("Int64")
    if "unified_parent_id" not in out.columns:
        out["unified_parent_id"] = np.where(
            out["parent_id"].notna(),
            pd.to_numeric(out["parent_id"], errors="coerce").astype("Int64"),
            out["affiliate_id"],
        )
    else:
        out["unified_parent_id"] = pd.to_numeric(out["unified_parent_id"], errors="coerce").astype("Int64")
    if "paint_sponsor" not in out.columns:
        out["paint_sponsor"] = pd.NA
    if "affiliate_name" not in out.columns:
        out["affiliate_name"] = "Unknown"
    return out


def standardize_rebate(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "net_amount" not in out.columns:
        for alt in ("netamount", "net_amt", "amount"):
            if alt in out.columns:
                out = out.rename(columns={alt: "net_amount"})
                break
    if "net_amount" not in out.columns:
        out["net_amount"] = np.nan
    else:
        out["net_amount"] = pd.to_numeric(out["net_amount"], errors="coerce")
    for alt, canon in (("trandate", "tran_date"), ("tran_date", "tran_date")):
        if canon not in out.columns and alt in out.columns:
            out = out.rename(columns={alt: "tran_date"})
    if "tran_date" in out.columns:
        out["tran_date"] = pd.to_datetime(out["tran_date"], errors="coerce")
    if "rebate_date" in out.columns:
        out["rebate_date"] = pd.to_datetime(out["rebate_date"], errors="coerce")
    for col in ("affiliate_id", "partner_id"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    if "rebate_month" not in out.columns or out["rebate_month"].isna().all():
        if "rebate_date" in out.columns:
            out["rebate_month"] = out["rebate_date"].dt.strftime("%Y-%m")
        elif "tran_date" in out.columns:
            out["rebate_month"] = out["tran_date"].dt.strftime("%Y-%m")
        else:
            out["rebate_month"] = pd.NA
    return out


def standardize_partner(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "partner_id" in out.columns:
        out["partner_id"] = pd.to_numeric(out["partner_id"], errors="coerce").round().astype("Int64")
    if "partner_name" not in out.columns:
        for alt in ("name", "partnername"):
            if alt in out.columns:
                out = out.rename(columns={alt: "partner_name"})
                break
    if "partner_name" not in out.columns:
        out["partner_name"] = pd.NA
    return out


def _gross_sales_column(df: pd.DataFrame) -> pd.Series:
    for c in ("gross_sales", "grosssales", "gross_amount", "sales_amount", "total_sales"):
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series(np.nan, index=df.index)


def max_rebate_month(months: pd.Series) -> str | None:
    s = months.dropna().astype(str).unique().tolist()
    if not s:
        return None
    return max(s, key=lambda x: pd.Period(x, freq="M"))


def months_between_month_strings(later: str, earlier: str | None) -> float | Any:
    if earlier is None or (isinstance(earlier, float) and np.isnan(earlier)):
        return pd.NA
    try:
        pe = pd.Period(str(earlier), freq="M")
        pl = pd.Period(str(later), freq="M")
        return float((pl - pe).n)
    except Exception:
        return pd.NA


# ---------------------------------------------------------------------------
# 1) Affiliate monthly summary
# ---------------------------------------------------------------------------
def build_affiliate_monthly_summary(
    affiliate: pd.DataFrame,
    rebate: pd.DataFrame,
) -> pd.DataFrame:
    if rebate.empty:
        return pd.DataFrame(
            columns=[
                "affiliate_id",
                "unified_parent_id",
                "rebate_month",
                "affiliate_name",
                "transaction_count",
                "total_rebate_amount",
                "avg_rebate_amount",
                "distinct_partner_count",
            ]
        )

    aff = affiliate[
        ["affiliate_id", "unified_parent_id", "affiliate_name"]
    ].drop_duplicates(subset=["affiliate_id"], keep="first")

    base = rebate.dropna(subset=["affiliate_id", "rebate_month"]).merge(
        aff,
        on="affiliate_id",
        how="left",
    )
    base["unified_parent_id"] = base["unified_parent_id"].fillna(base["affiliate_id"])
    base["affiliate_name"] = base["affiliate_name"].fillna("Unknown")

    g = (
        base.groupby(["affiliate_id", "unified_parent_id", "rebate_month"], dropna=False)
        .agg(
            affiliate_name=("affiliate_name", "first"),
            transaction_count=("net_amount", "size"),
            total_rebate_amount=("net_amount", "sum"),
            avg_rebate_amount=("net_amount", "mean"),
            distinct_partner_count=("partner_id", pd.Series.nunique),
        )
        .reset_index()
    )
    return g


# ---------------------------------------------------------------------------
# 2) Missing recent activity
# ---------------------------------------------------------------------------
def build_missing_recent_activity(
    affiliate: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    latest_month: str | None,
) -> pd.DataFrame:
    cols = [
        "affiliate_id",
        "affiliate_name",
        "unified_parent_id",
        "last_active_month",
        "months_since_last_activity",
        "activity_status",
        "status",
    ]
    if latest_month is None or affiliate.empty:
        return pd.DataFrame(columns=cols)

    aff = affiliate[
        ["affiliate_id", "affiliate_name", "unified_parent_id"]
    ].drop_duplicates(subset=["affiliate_id"], keep="first")

    active_latest = set(
        monthly_summary.loc[monthly_summary["rebate_month"].astype(str) == str(latest_month), "affiliate_id"].dropna()
    )

    last_active = (
        monthly_summary.dropna(subset=["rebate_month"])
        .groupby("affiliate_id")["rebate_month"]
        .apply(lambda s: max_rebate_month(s.astype(str)))
        .rename("last_active_month")
    )

    out = aff.merge(last_active, on="affiliate_id", how="left")
    out["months_since_last_activity"] = [
        months_between_month_strings(str(latest_month), lm if pd.notna(lm) else None)
        for lm in out["last_active_month"]
    ]

    missing_latest = ~out["affiliate_id"].isin(active_latest)
    out = out.loc[missing_latest].copy()

    def classify(row: pd.Series) -> str:
        if pd.isna(row["last_active_month"]):
            return "Never Active"
        return "Dormant"

    out["activity_status"] = out.apply(classify, axis=1)
    out["status"] = "Missing Recent Activity"
    return out[cols]


# ---------------------------------------------------------------------------
# 3) Decline flags
# ---------------------------------------------------------------------------
def build_decline_flags(
    affiliate: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    latest_month: str | None,
) -> pd.DataFrame:
    cols = [
        "affiliate_id",
        "affiliate_name",
        "latest_month",
        "latest_amount",
        "historical_avg",
        "decline_pct",
        "status",
    ]
    if latest_month is None or monthly_summary.empty:
        return pd.DataFrame(columns=cols)

    aff = affiliate[["affiliate_id", "affiliate_name"]].drop_duplicates("affiliate_id", keep="first")
    m = monthly_summary.copy()
    m["rebate_month"] = m["rebate_month"].astype(str)

    latest_amt = (
        m.loc[m["rebate_month"] == str(latest_month), ["affiliate_id", "total_rebate_amount"]]
        .groupby("affiliate_id", as_index=False)["total_rebate_amount"]
        .sum()
        .rename(columns={"total_rebate_amount": "latest_amount"})
    )

    prior = m.loc[m["rebate_month"] != str(latest_month), ["affiliate_id", "rebate_month", "total_rebate_amount"]]
    prior_avg = (
        prior.groupby("affiliate_id", as_index=False)["total_rebate_amount"]
        .mean()
        .rename(columns={"total_rebate_amount": "historical_avg"})
    )

    merged = latest_amt.merge(prior_avg, on="affiliate_id", how="inner").merge(aff, on="affiliate_id", how="left")
    merged["affiliate_name"] = merged["affiliate_name"].fillna("Unknown")

    hist = merged["historical_avg"].astype(float)
    lat = merged["latest_amount"].astype(float)
    flag = (hist > 0) & (lat < DECLINE_THRESHOLD * hist)

    merged["decline_pct"] = np.where(hist > 0, (hist - lat) / hist * 100.0, np.nan)
    merged = merged.loc[flag].copy()
    merged["latest_month"] = str(latest_month)
    merged["status"] = "Decline Alert"
    return merged[
        [
            "affiliate_id",
            "affiliate_name",
            "latest_month",
            "latest_amount",
            "historical_avg",
            "decline_pct",
            "status",
        ]
    ]


# ---------------------------------------------------------------------------
# 4) Partner feed health
# ---------------------------------------------------------------------------
def build_partner_feed_health(rebate: pd.DataFrame, partner: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "partner_id",
        "partner_name",
        "rebate_month",
        "transaction_count",
        "total_rebate_amount",
        "distinct_affiliate_count",
        "partner_hist_avg_transactions",
        "is_complete_feed",
        "feed_health_status",
    ]
    if rebate.empty:
        return pd.DataFrame(columns=cols)

    g = (
        rebate.dropna(subset=["partner_id", "rebate_month"])
        .groupby(["partner_id", "rebate_month"], dropna=False)
        .agg(
            transaction_count=("net_amount", "size"),
            total_rebate_amount=("net_amount", "sum"),
            distinct_affiliate_count=("affiliate_id", pd.Series.nunique),
        )
        .reset_index()
    )

    p = partner[["partner_id", "partner_name"]].drop_duplicates("partner_id", keep="first")
    g = g.merge(p, on="partner_id", how="left")
    g["partner_name"] = g["partner_name"].fillna("Unknown")

    hist_avg: list[float | Any] = []
    complete: list[bool] = []
    status: list[str] = []

    for _, row in g.iterrows():
        pid = row["partner_id"]
        rm = str(row["rebate_month"])
        peer = g.loc[(g["partner_id"] == pid) & (g["rebate_month"].astype(str) != rm), "transaction_count"]
        h = float(peer.mean()) if len(peer) else np.nan
        hist_avg.append(h)
        txn = float(row["transaction_count"])
        if np.isnan(h) or h <= 0:
            complete.append(True)
            status.append("Complete")
        elif txn < PARTIAL_FEED_THRESHOLD * h:
            complete.append(False)
            status.append("Possible Partial Feed.")
        else:
            complete.append(True)
            status.append("Complete")

    g["partner_hist_avg_transactions"] = hist_avg
    g["is_complete_feed"] = complete
    g["feed_health_status"] = status
    return g[cols]


# ---------------------------------------------------------------------------
# 5) KPI summary (single row)
# ---------------------------------------------------------------------------
def build_kpi_summary(
    affiliate: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    rebate: pd.DataFrame,
    decline_flags: pd.DataFrame,
    latest_month: str | None,
) -> pd.DataFrame:
    total_affiliates = int(affiliate["affiliate_id"].nunique()) if not affiliate.empty else 0

    if latest_month is None or monthly_summary.empty:
        active_latest = 0
    else:
        active_latest = int(
            monthly_summary.loc[monthly_summary["rebate_month"].astype(str) == str(latest_month), "affiliate_id"]
            .nunique()
        )

    inactive_latest = max(total_affiliates - active_latest, 0)
    decline_alert_count = int(len(decline_flags))

    if latest_month is None or rebate.empty:
        total_latest_month_rebates = 0.0
    else:
        total_latest_month_rebates = float(
            rebate.loc[rebate["rebate_month"].astype(str) == str(latest_month), "net_amount"].sum()
        )

    row = {
        "total_affiliates": total_affiliates,
        "active_latest_month": active_latest,
        "inactive_latest_month": inactive_latest,
        "decline_alert_count": decline_alert_count,
        "total_latest_month_rebates": total_latest_month_rebates,
        "latest_rebate_month": latest_month,
        "as_of_timestamp": pd.Timestamp.now(),
    }
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# 6) Rebate reconciliation
# ---------------------------------------------------------------------------
def build_rebate_reconciliation(rebate: pd.DataFrame, affiliate: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "affiliate_id",
        "partner_id",
        "rebate_month",
        "expected_amount",
        "actual_amount",
        "gap_amount",
    ]
    if rebate.empty:
        return pd.DataFrame(columns=cols)

    aff = affiliate[["affiliate_id", "paint_sponsor"]].drop_duplicates("affiliate_id", keep="first")
    out = rebate.merge(aff, on="affiliate_id", how="left")

    tier_rate = out["paint_sponsor"].map(PAINT_SPONSOR_TIER_RATES).astype(float)
    unknown = tier_rate.isna() & out["paint_sponsor"].notna()
    if unknown.any():
        log(
            f"WARNING: {unknown.sum()} row(s) use unknown paint_sponsor tier; "
            "expected_amount set using default 5% for POC."
        )
    tier_rate = tier_rate.fillna(PAINT_SPONSOR_TIER_RATES.get(1, 0.05))

    gross = _gross_sales_column(out)
    if gross.isna().all():
        log("WARNING: gross_sales not found on rebate; expected_amount will be NaN.")

    out["expected_amount"] = gross * tier_rate
    out["actual_amount"] = out["net_amount"]
    out["gap_amount"] = out["expected_amount"] - out["actual_amount"]

    return out[
        [
            "affiliate_id",
            "partner_id",
            "rebate_month",
            "expected_amount",
            "actual_amount",
            "gap_amount",
        ]
    ]


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------
def read_silver_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        log(f"WARNING: Missing silver file {path}; using empty DataFrame.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    for col in ("rebate_date", "tran_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=False)

    log(f"Loaded {len(df)} row(s) from {path}")
    return df


def write_gold_parquet(df: pd.DataFrame, name: str) -> None:
    """Overwrite gold Parquet at data/gold/<name> (replaces any prior file)."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    out = GOLD_DIR / name
    df.to_parquet(out, index=False, engine="pyarrow")
    log(f"Wrote {len(df)} row(s) -> {out}")


def write_gold_csv_export(df: pd.DataFrame, name: str) -> None:
    GOLD_CSV_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = GOLD_CSV_EXPORT_DIR / name
    df.to_csv(out, index=False, encoding="utf-8")
    log(f"Wrote {len(df)} row(s) -> {out}")


def write_gold_table(df: pd.DataFrame, stem: str) -> None:
    """Write gold Parquet to data/gold/ and CSV to data/gold/csv_exports/."""
    write_gold_parquet(df, f"{stem}.parquet")
    write_gold_csv_export(df, f"{stem}.csv")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log("Starting silver -> gold ETL.")
    try:
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        aff_raw = read_silver_csv(AFFILIATE_PATH)
        reb_raw = read_silver_csv(REBATE_PATH)
        par_raw = read_silver_csv(PARTNER_PATH)

        affiliate = standardize_affiliate(aff_raw)
        rebate = standardize_rebate(reb_raw)
        partner = standardize_partner(par_raw)
        if "bad_data_flag" not in rebate.columns:
            rebate["bad_data_flag"] = False
        else:
            rebate["bad_data_flag"] = rebate["bad_data_flag"].fillna(False).astype(bool)

        latest_month = max_rebate_month(rebate["rebate_month"]) if "rebate_month" in rebate.columns else None
        log(f"Latest rebate_month in silver: {latest_month!r}")

        detected_at = datetime.now()
        gold_exceptions: list[dict[str, Any]] = []

        monthly = build_affiliate_monthly_summary(affiliate, rebate)
        rebate = detect_volume_outliers(monthly, rebate, gold_exceptions, detected_at)
        detect_silent_shops(affiliate, rebate, latest_month, gold_exceptions, detected_at)
        write_exceptions_csv(gold_exceptions, append=True)

        write_gold_table(monthly, "gold_affiliate_monthly_summary")

        missing = build_missing_recent_activity(affiliate, monthly, latest_month)
        write_gold_table(missing, "gold_missing_recent_activity")

        declines = build_decline_flags(affiliate, monthly, latest_month)
        write_gold_table(declines, "gold_decline_flags")

        partner_health = build_partner_feed_health(rebate, partner)
        write_gold_table(partner_health, "gold_partner_feed_health")

        kpis = build_kpi_summary(affiliate, monthly, rebate, declines, latest_month)
        write_gold_table(kpis, "gold_kpi_summary")

        recon = build_rebate_reconciliation(rebate, affiliate)
        write_gold_table(recon, "gold_rebate_reconciliation")

        log("Silver -> gold ETL completed successfully.")
        return 0

    except Exception as exc:
        log(f"ERROR: {exc!r}")
        log(traceback.format_exc())
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
