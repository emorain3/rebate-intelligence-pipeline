"""
Bronze → Silver ETL: reads synthetic bronze CSVs from data/bronze/,
applies cleaning rules, writes CSV and Parquet to data/silver/ (overwrites
prior files), and writes structured exceptions to data/silver/csv_exports/pipeline_exceptions.csv.
"""

from __future__ import annotations

import hashlib
import re
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
    """Resolve repo root (folder that contains data/bronze), even when this file lives under scripts/."""
    here = Path(__file__).resolve().parent
    for d in [here, *here.parents]:
        if (d / "data" / "bronze").is_dir():
            return d
    return here.parent


PROJECT_ROOT = _project_root()
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_AFFILIATE_CSV = BRONZE_DIR / "synthetic_dim_affiliate.csv"
BRONZE_PARTNER_CSV = BRONZE_DIR / "synthetic_dim_partner.csv"
BRONZE_REBATE_CSV = BRONZE_DIR / "synthetic_fact_rebate.csv"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "csv_exports"
EXCEPTIONS_CSV = SILVER_DIR / "pipeline_exceptions.csv"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def to_snake_case(name: Any) -> str:
    """Convert a column label to snake_case lowercase."""
    s = str(name).strip()
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [to_snake_case(c) for c in out.columns]
    return out


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )
    return out


def make_row_id(row: pd.Series | dict[str, Any], source: str) -> str:
    """Stable row_id from rebate composite key, else dimension keys, else uuid."""
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
        for key in ("affiliate_id", "partner_id"):
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


def record_exceptions(
    df: pd.DataFrame,
    mask: pd.Series,
    exceptions: list[dict[str, Any]],
    *,
    source: str,
    error_type: str,
    severity: str,
    field: str,
    detected_at: datetime,
) -> None:
    """Set bad_data_flag and append structured exception rows for matching indices."""
    if not mask.any():
        return
    df.loc[mask, "bad_data_flag"] = True
    for idx in df.index[mask]:
        exceptions.append(
            log_exception(
                df.loc[idx],
                source=source,
                error_type=error_type,
                severity=severity,
                field=field,
                detected_at=detected_at,
            )
        )


def write_exceptions_csv(exceptions: list[dict[str, Any]]) -> None:
    """Overwrite exceptions file for this run (idempotent batch output)."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    if not exceptions:
        if EXCEPTIONS_CSV.exists():
            EXCEPTIONS_CSV.unlink()
            log(f"Removed stale {EXCEPTIONS_CSV.name} (no exceptions this run).")
        return
    out = pd.DataFrame(exceptions)
    out["detected_at"] = pd.to_datetime(out["detected_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    out.to_csv(EXCEPTIONS_CSV, index=False, encoding="utf-8")
    log(f"Wrote {len(out)} exception row(s) to {EXCEPTIONS_CSV}")


def write_silver_table(df: pd.DataFrame, stem: str) -> None:
    """Write silver CSV and Parquet (overwrites existing files at same paths)."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = SILVER_DIR / f"{stem}.csv"
    parquet_path = SILVER_DIR / f"{stem}.parquet"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log(f"Wrote {len(df)} row(s) -> {csv_path} and {parquet_path}")


# ---------------------------------------------------------------------------
# Affiliate
# ---------------------------------------------------------------------------
def transform_affiliate(
    df: pd.DataFrame,
    exceptions: list[dict[str, Any]],
    detected_at: datetime,
) -> pd.DataFrame:
    """Returns silver_affiliate with bad_data_flag; appends exceptions in place."""
    source = "affiliate"
    df = df.rename(columns={
        "trandate": "tran_date",
        "netamount": "net_amount",
    })
    df = trim_string_columns(df)
    df["bad_data_flag"] = False

    if "affiliate_id" not in df.columns:
        log(f"WARNING [{source}]: expected column affiliate_id missing after rename.")
        return df

    null_mask = df["affiliate_id"].isna() | (df["affiliate_id"].astype(str).str.strip() == "")
    record_exceptions(
        df,
        null_mask,
        exceptions,
        source=source,
        error_type="null_affiliate_id",
        severity="HIGH",
        field="affiliate_id",
        detected_at=detected_at,
    )
    if null_mask.any():
        log(f"[{source}] Flagged {null_mask.sum()} row(s) with null/empty affiliate_id.")

    before = len(df)
    df = df.drop_duplicates(subset=["affiliate_id"], keep="first")
    if before != len(df):
        log(f"[{sheet}] Removed {before - len(df)} duplicate affiliate_id row(s) (kept first).")

    if "parent_id" not in df.columns:
        df["parent_id"] = pd.NA

    df["unified_parent_id"] = np.where(df["parent_id"].notna(), df["parent_id"], df["affiliate_id"])

    if "affiliate_name" in df.columns:
        an = df["affiliate_name"]
        df["normalized_name"] = np.where(
            an.notna(),
            an.astype(str).str.lower().str.strip(),
            pd.NA,
        )
    else:
        df["normalized_name"] = pd.NA
        log(f"WARNING [{source}]: affiliate_name not found; normalized_name set to NA.")

    return df


# ---------------------------------------------------------------------------
# Partner
# ---------------------------------------------------------------------------
def transform_partner(
    df: pd.DataFrame,
    exceptions: list[dict[str, Any]],
    detected_at: datetime,
) -> pd.DataFrame:
    source = "partner"
    df = df.rename(columns={
        "trandate": "tran_date",
        "netamount": "net_amount",
    })
    df = trim_string_columns(df)
    df["bad_data_flag"] = False

    if "partner_id" not in df.columns:
        log(f"WARNING [{source}]: expected column partner_id missing after rename.")
        return df

    null_mask = df["partner_id"].isna() | (df["partner_id"].astype(str).str.strip() == "")
    record_exceptions(
        df,
        null_mask,
        exceptions,
        source=source,
        error_type="null_partner_id",
        severity="HIGH",
        field="partner_id",
        detected_at=detected_at,
    )
    if null_mask.any():
        log(f"[{source}] Flagged {null_mask.sum()} row(s) with null/empty partner_id.")

    before = len(df)
    df = df.drop_duplicates(subset=["partner_id"], keep="first")
    if before != len(df):
        log(f"[{source}] Removed {before - len(df)} duplicate partner_id row(s) (kept first).")

    return df


# ---------------------------------------------------------------------------
# Rebate
# ---------------------------------------------------------------------------
def _coerce_rebate_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("rebate_date", "tran_date"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", dayfirst=False)
    if "net_amount" in out.columns:
        out["net_amount"] = (
            out["net_amount"]
            .astype("string")
            .str.replace(r"[$,\s]", "", regex=True)
            .replace({"": pd.NA})
        )
        out["net_amount"] = pd.to_numeric(out["net_amount"], errors="coerce")
    return out


def transform_rebate(
    df: pd.DataFrame,
    exceptions: list[dict[str, Any]],
    detected_at: datetime,
    valid_partner_ids: set[str],
) -> pd.DataFrame:
    source = "rebate"
    df = df.rename(columns={
        "trandate": "tran_date",
        "netamount": "net_amount",
        "transaction_date": "tran_date",
    })
    df = trim_string_columns(df)
    df["bad_data_flag"] = False

    # Important: Keep IDs as strings through validation so numeric coercion doesn't
    # turn legitimate values into NaN and get them incorrectly flagged.
    for col in ("affiliate_id", "partner_id"):
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype("string").str.strip()

    null_aff_mask = df["affiliate_id"].isna() | (df["affiliate_id"] == "")
    record_exceptions(
        df,
        null_aff_mask,
        exceptions,
        source=source,
        error_type="null_affiliate_id",
        severity="HIGH",
        field="affiliate_id",
        detected_at=detected_at,
    )

    null_partner_mask = df["partner_id"].isna() | (df["partner_id"] == "")
    record_exceptions(
        df,
        null_partner_mask,
        exceptions,
        source=source,
        error_type="null_partner_id",
        severity="HIGH",
        field="partner_id",
        detected_at=detected_at,
    )

    raw_tran = df["tran_date"].copy() if "tran_date" in df.columns else None
    raw_rebate = df["rebate_date"].copy() if "rebate_date" in df.columns else None
    df = _coerce_rebate_types(df)

    if raw_tran is not None:
        had_tran = raw_tran.notna() & (raw_tran.astype(str).str.strip() != "")
        bad_tran = had_tran & df["tran_date"].isna()
        record_exceptions(
            df,
            bad_tran,
            exceptions,
            source=source,
            error_type="bad_date",
            severity="HIGH",
            field="tran_date",
            detected_at=detected_at,
        )

    if raw_rebate is not None and "rebate_date" in df.columns:
        had_rebate = raw_rebate.notna() & (raw_rebate.astype(str).str.strip() != "")
        bad_rebate = had_rebate & df["rebate_date"].isna()
        record_exceptions(
            df,
            bad_rebate,
            exceptions,
            source=source,
            error_type="bad_date",
            severity="HIGH",
            field="rebate_date",
            detected_at=detected_at,
        )

    if "net_amount" not in df.columns:
        df["net_amount"] = pd.NA
    negative_mask = df["net_amount"].notna() & (df["net_amount"] < 0)
    record_exceptions(
        df,
        negative_mask,
        exceptions,
        source=source,
        error_type="negative_amount",
        severity="MEDIUM",
        field="net_amount",
        detected_at=detected_at,
    )

    if valid_partner_ids:
        has_partner = df["partner_id"].notna() & (df["partner_id"] != "")
        unknown_partner = has_partner & ~df["partner_id"].isin(valid_partner_ids)
        record_exceptions(
            df,
            unknown_partner,
            exceptions,
            source=source,
            error_type="unrecognized_partner",
            severity="MEDIUM",
            field="partner_id",
            detected_at=detected_at,
        )

    flagged = int(df["bad_data_flag"].sum())
    if flagged:
        log(f"[{source}] Flagged {flagged} row(s) with data quality exceptions (retained in output).")

    if df.empty:
        df = df.assign(
            rebate_month=pd.Series(dtype="string"),
            tran_month=pd.Series(dtype="string"),
            load_timestamp=pd.Series(dtype="datetime64[ns]"),
        )
        return df

    before_dedupe = len(df)
    df = df.drop_duplicates(keep="first")
    if before_dedupe != len(df):
        log(f"[{sheet}] Removed {before_dedupe - len(df)} fully identical duplicate row(s).")

    if "rebate_date" in df.columns:
        df["rebate_month"] = df["rebate_date"].dt.strftime("%Y-%m")
    else:
        df["rebate_month"] = pd.NA

    if "tran_date" in df.columns:
        df["tran_month"] = df["tran_date"].dt.strftime("%Y-%m")
    else:
        df["tran_month"] = pd.NA

    df["load_timestamp"] = pd.Timestamp.now()

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log("Starting bronze -> silver ETL.")
    try:
        bronze_inputs = {
            "Affiliate": BRONZE_AFFILIATE_CSV,
            "Partner": BRONZE_PARTNER_CSV,
            "Rebate": BRONZE_REBATE_CSV,
        }
        missing = [label for label, path in bronze_inputs.items() if not path.is_file()]
        if missing:
            for label in missing:
                log(f"ERROR: Bronze file not found: {bronze_inputs[label]}")
            return 1

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        detected_at = datetime.now()
        all_exceptions: list[dict[str, Any]] = []

        # Partner (read first for rebate FK validation)
        log(f"Reading CSV: {BRONZE_PARTNER_CSV}")
        par = pd.read_csv(BRONZE_PARTNER_CSV)
        par.columns = [to_snake_case(c) for c in par.columns]
        silver_par = transform_partner(par, all_exceptions, detected_at)
        valid_partner_ids = {
            str(pid).strip()
            for pid in silver_par["partner_id"].dropna().tolist()
            if str(pid).strip() != ""
        }
        write_silver_table(silver_par, "silver_partner")

        # Affiliate
        log(f"Reading CSV: {BRONZE_AFFILIATE_CSV}")
        aff = pd.read_csv(BRONZE_AFFILIATE_CSV)
        aff.columns = [to_snake_case(c) for c in aff.columns]
        silver_aff = transform_affiliate(aff, all_exceptions, detected_at)
        write_silver_table(silver_aff, "silver_affiliate")

        # Rebate
        log(f"Reading CSV: {BRONZE_REBATE_CSV}")
        reb = pd.read_csv(BRONZE_REBATE_CSV)
        reb.columns = [to_snake_case(c) for c in reb.columns]
        silver_reb = transform_rebate(reb, all_exceptions, detected_at, valid_partner_ids)
        write_silver_table(silver_reb, "silver_rebate")

        write_exceptions_csv(all_exceptions)

        log("ETL completed successfully.")
        return 0

    except Exception as exc:
        log(f"ERROR: ETL failed: {exc!r}")
        log(traceback.format_exc())
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
