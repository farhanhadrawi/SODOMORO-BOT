import os
from datetime import date
from html import escape  # optional, berguna kalau mau log aman

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from dateutil import parser as dateparser
import re

load_dotenv()
SHEET_ID = os.getenv("SHEET_ID")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# -----------------------------
# CACHING CLIENT & WORKSHEET
# -----------------------------
_gc = None
_ws = None

def _get_client():
    global _gc
    if _gc:
        return _gc
    creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    _gc = gspread.authorize(creds)
    return _gc

def get_ws(worksheet_name: str | None = None):
    """
    Dapatkan worksheet.
    Default: 'Order MODOROSO'.
    Bisa pilih worksheet lain dengan nama tab: get_ws("NamaTab").
    """
    global _ws
    default_name = "Order MODOROSO"

    # kalau _ws sudah diset dan user tidak minta tab lain → pakai cache
    if _ws and worksheet_name is None:
        return _ws

    gc = _get_client()
    sh = gc.open_by_key(SHEET_ID)

    # kalau user kasih nama, pakai itu; kalau tidak, pakai default_name
    ws = sh.worksheet(worksheet_name or default_name)

    if worksheet_name is None:
        _ws = ws
    return ws


# -----------------------------
# UTIL UMUM
# -----------------------------
def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _is_done(status: str) -> bool:
    """
    True jika status dianggap selesai (Complete/Cancel).
    Tangani variasi umum: "Complete", "Completed (PS)", "Cancel", "Canceled", "Cancelled".
    """
    s = _norm(status)
    if s.startswith("complete"):
        return True
    if s in {"cancel", "canceled", "cancelled"}:
        return True
    return False

def _to_date(cell: str | None) -> date | None:
    """
    Ubah isi sel tanggal menjadi objek date.
    Mengembalikan None jika tidak valid/placeholder.
    """
    if not cell:
        return None
    s = str(cell).strip()
    if not s or s in {"-", "0"}:
        return None
    try:
        # dayfirst=True agar '02/01/2024' terbaca 2 Jan 2024 (konteks ID)
        d = dateparser.parse(s, dayfirst=True, fuzzy=True).date()
        if d.year < 1971:
            return None
        return d
    except Exception:
        return None

def _sort_by_date(rows: list[dict], key: str = "ORDER_DATE") -> list[dict]:
    """Urutkan list dict berdasarkan kolom tanggal (terlama ke terbaru)."""
    return sorted(rows, key=lambda r: _to_date(r.get(key, "")) or date.min)


# -----------------------------
# FUNGSI FITUR
# -----------------------------
def find_order(order_key: str):
    """
    Cari order berdasarkan kolom ORDER_ID atau No SC.
    Return dict atau None.
    """
    ws = get_ws()
    rows = ws.get_all_values()
    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}

    if "order_id" not in header or "no sc" not in header:
        raise RuntimeError("Kolom 'ORDER_ID' atau 'No SC' tidak ditemukan di sheet.")

    idx_orderid = header["order_id"]
    idx_nosc = header["no sc"]

    for r in rows[1:]:
        val_orderid = r[idx_orderid].strip() if len(r) > idx_orderid else ""
        val_nosc = r[idx_nosc].strip() if len(r) > idx_nosc else ""

        if order_key.strip() in (val_orderid, val_nosc):
            return {
                "ORDER_ID": val_orderid,
                "NO_SC": val_nosc,
                "STATUS_DO": r[header.get("status do", idx_orderid)],
                "CUSTOMER_NAME": r[header.get("customer_name", idx_orderid)],
                "JENIS_ORDER": r[header.get("jenis order", idx_orderid)],
                "ORDER_DATE": r[header.get("order_date", idx_orderid)],
            }
    return None


def search_by_name(query: str, limit: int = 50):
    """
    Cari order berdasarkan CUSTOMER_NAME (case-insensitive, substring).
    Return: list[dict] maksimal `limit`.
    """
    ws = get_ws()
    rows = ws.get_all_values()
    if not rows:
        return []

    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}
    required = ["customer_name", "order_id", "no sc", "status do", "jenis order", "order_date"]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Kolom hilang di sheet: {', '.join(missing)}")

    idx_name = header["customer_name"]
    idx_order = header["order_id"]
    idx_nosc = header["no sc"]
    idx_status = header["status do"]
    idx_jenis = header["jenis order"]
    idx_date = header["order_date"]

    q = _norm(query)
    results = []
    for r in rows[1:]:
        name = r[idx_name].strip() if len(r) > idx_name else ""
        if q in name.lower():
            results.append({
                "CUSTOMER_NAME": name,
                "ORDER_ID": r[idx_order] if len(r) > idx_order else "",
                "NO_SC": r[idx_nosc] if len(r) > idx_nosc else "",
                "STATUS_DO": r[idx_status] if len(r) > idx_status else "",
                "JENIS_ORDER": r[idx_jenis] if len(r) > idx_jenis else "",
                "ORDER_DATE": r[idx_date] if len(r) > idx_date else "",
            })
            if len(results) >= limit:
                break
    return results


def list_not_done(keyword: str | None = None, limit: int = 2000):
    """
    Ambil order yang Status DO-nya BUKAN Complete/Cancel (case-insensitive).
    Jika `keyword` diisi, filter juga yang mengandung keyword di CUSTOMER_NAME / ORDER_ID / No SC.
    Return: list[dict] tersortir tanggal (terlama→terbaru), maksimal `limit`.
    """
    ws = get_ws()
    rows = ws.get_all_values()
    if not rows:
        return []

    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}
    required = ["order_id", "no sc", "status do", "jenis order", "order_date", "customer_name"]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Kolom hilang di sheet: {', '.join(missing)}")

    idx_name   = header["customer_name"]
    idx_order  = header["order_id"]
    idx_nosc   = header["no sc"]
    idx_status = header["status do"]
    idx_jenis  = header["jenis order"]
    idx_date   = header["order_date"]

    q = _norm(keyword) if keyword else None
    out = []
    for r in rows[1:]:
        status = r[idx_status] if len(r) > idx_status else ""
        if _is_done(status):
            continue

        name = r[idx_name] if len(r) > idx_name else ""
        order_id = r[idx_order] if len(r) > idx_order else ""
        no_sc = r[idx_nosc] if len(r) > idx_nosc else ""

        if q:
            hay = f"{name} {order_id} {no_sc}".lower()
            if q not in hay:
                continue

        out.append({
            "CUSTOMER_NAME": name,
            "ORDER_ID": order_id,
            "NO_SC": no_sc,
            "STATUS_DO": status,
            "JENIS_ORDER": r[idx_jenis] if len(r) > idx_jenis else "",
            "ORDER_DATE": r[idx_date] if len(r) > idx_date else "",
        })
        if len(out) >= limit:
            break

    return _sort_by_date(out)


def list_pending_in_range(start: date | None, end: date | None, limit: int = 2000):
    """
    Ambil order pending (Status DO ≠ Complete/Cancel) dengan ORDER_DATE di [start, end] (inklusif).
    Jika start atau end None → tanpa batas di sisi itu.
    Return: list[dict] tersortir tanggal (terlama→terbaru), maksimal `limit`.
    """
    ws = get_ws()
    rows = ws.get_all_values()
    if not rows:
        return []

    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}
    required = ["order_id", "no sc", "status do", "jenis order", "order_date", "customer_name"]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Kolom hilang di sheet: {', '.join(missing)}")

    idx_name   = header["customer_name"]
    idx_order  = header["order_id"]
    idx_nosc   = header["no sc"]
    idx_status = header["status do"]
    idx_jenis  = header["jenis order"]
    idx_date   = header["order_date"]

    out = []
    for r in rows[1:]:
        status = r[idx_status] if len(r) > idx_status else ""
        if _is_done(status):
            continue

        d = _to_date(r[idx_date] if len(r) > idx_date else "")
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue

        out.append({
            "CUSTOMER_NAME": r[idx_name] if len(r) > idx_name else "",
            "ORDER_ID": r[idx_order] if len(r) > idx_order else "",
            "NO_SC": r[idx_nosc] if len(r) > idx_nosc else "",
            "STATUS_DO": status,
            "JENIS_ORDER": r[idx_jenis] if len(r) > idx_jenis else "",
            "ORDER_DATE": r[idx_date] if len(r) > idx_date else "",
        })
        if len(out) >= limit:
            break

    return _sort_by_date(out)


def list_pending_in_month(year: int, month: int, limit: int = 2000):
    """
    Ambil order pending di bulan (year, month) tertentu.
    Return: list[dict] tersortir tanggal (terlama→terbaru), maksimal `limit`.
    """
    from calendar import monthrange
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return list_pending_in_range(start, end, limit=limit)

def list_pending(
    keyword: str | None = None,
    start: date | None = None,
    end: date | None = None,
    year: int | None = None,
    month: int | None = None,
    branch: str | None = None,          # NEW: filter DATEL/Branch
    limit: int = 2000
):
    """
    Ambil order pending (Status DO ≠ Complete/Cancel).
    Filter opsional:
      - branch/DATEL (nama persis di kolom 'branch' / 'datel' jika ada)
      - keyword (CUSTOMER_NAME / ORDER_ID / No SC)
      - rentang tanggal (start–end) atau bulan (year, month)
    """
    from calendar import monthrange
    if year and month:
        start = date(year, month, 1)
        end = date(year, month, monthrange(year, month)[1])

    ws = get_ws()
    rows = ws.get_all_values()
    if not rows:
        return []

    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}
    required = ["order_id", "no sc", "status do", "jenis order", "order_date", "customer_name"]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Kolom hilang di sheet: {', '.join(missing)}")

    # cari kolom branch/datel jika ada
    idx_branch = None
    for cand in ("branch", "datel"):
        if cand in header:
            idx_branch = header[cand]
            break

    idx_name   = header["customer_name"]
    idx_order  = header["order_id"]
    idx_nosc   = header["no sc"]
    idx_status = header["status do"]
    idx_jenis  = header["jenis order"]
    idx_date   = header["order_date"]

    q = (keyword or "").strip().lower() or None
    want_branch = None
    if branch:
        # normalisasi: buang spasi & lowercase agar "MUARO JAMBI" == "muarojambi"
        want_branch = re.sub(r"\s+", "", branch.strip().lower())

    out = []
    for r in rows[1:]:
        status = r[idx_status] if len(r) > idx_status else ""
        if _is_done(status):
            continue

        # filter branch kalau kolomnya ada & user minta
        if idx_branch is not None and want_branch:
            val = r[idx_branch] if len(r) > idx_branch else ""
            norm = re.sub(r"\s+", "", val.strip().lower())
            if norm != want_branch:
                continue

        d = _to_date(r[idx_date] if len(r) > idx_date else "")
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue

        name = r[idx_name] if len(r) > idx_name else ""
        order_id = r[idx_order] if len(r) > idx_order else ""
        no_sc = r[idx_nosc] if len(r) > idx_nosc else ""

        if q:
            hay = f"{name} {order_id} {no_sc}".lower()
            if q not in hay:
                continue

        out.append({
            "CUSTOMER_NAME": name,
            "ORDER_ID": order_id,
            "NO_SC": no_sc,
            "STATUS_DO": status,
            "JENIS_ORDER": r[idx_jenis] if len(r) > idx_jenis else "",
            "ORDER_DATE": r[idx_date] if len(r) > idx_date else "",
        })
        if len(out) >= limit:
            break

    return _sort_by_date(out)


# Map kolom yang kita butuhkan dari sheet raw
RAW_SHEET_NAME = "Order MODOROSO"
COL_DATEL = "branch"
COL_STATUS = "status do"         # pastikan sama persis seperti header di sheet
COL_JENIS = "jenis order"        # MO/DO/RO/SO/PDA/CO/CN/AS/MIGRATE
COL_ORDER_DATE = "order_date"

_JENIS_LIST = ["MO","DO","RO","SO","PDA","CO","CN","AS","MIGRATE"]

def _parse_date(cell: str | None) -> date | None:
    if not cell:
        return None
    s = str(cell).strip()
    if not s or s in {"-", "0"}:
        return None
    try:
        d = dateparser.parse(s, dayfirst=True, fuzzy=True).date()
        if d.year < 1971:
            return None
        return d
    except Exception:
        return None
    
def _normalize_branch(name: str) -> str:
    return re.sub(r"\s+", "", (name or "").strip()).lower()


def summarize_orders(branch: str | None = None, start: date | None = None, end: date | None = None):
    """
    Ringkas data dari sheet raw 'Order MODOROSO'.
    Filter:
      - branch (DATEL) = None berarti semua datel
      - tanggal (ORDER_DATE) di [start, end] inklusif; kalau None → tanpa batas
    Return:
      {
        "per_status": {status: total, ...},                    # semua status (Complete/Cancel termasuk)
        "per_status_by_jenis": {status: {jenis: n, ...}, ...}, # pivot status x jenis (MO/DO/...)
        "totals_by_jenis": {jenis: total, ...},
        "grand_total": N
      }
    """
    ws = get_ws(RAW_SHEET_NAME)
    rows = ws.get_all_values()
    if not rows:
        return {"per_status": {}, "per_status_by_jenis": {}, "totals_by_jenis": {}, "grand_total": 0}

    header = {h.strip().lower(): i for i, h in enumerate(rows[0])}
    required = [COL_DATEL, COL_STATUS, COL_JENIS, COL_ORDER_DATE]
    missing = [c for c in required if c not in header]
    if missing:
        raise RuntimeError(f"Kolom hilang di sheet '{RAW_SHEET_NAME}': {', '.join(missing)}")

    i_datel = header[COL_DATEL]
    i_status = header[COL_STATUS]
    i_jenis = header[COL_JENIS]
    i_date = header[COL_ORDER_DATE]

    per_status: dict[str,int] = {}
    per_status_by_jenis: dict[str,dict[str,int]] = {}
    totals_by_jenis: dict[str,int] = {j: 0 for j in _JENIS_LIST}
    grand_total = 0

    want_branch = _normalize_branch(branch) if branch else None

    for r in rows[1:]:
        datel = r[i_datel].strip() if len(r) > i_datel else ""
        datel_norm = _normalize_branch(datel)
        if want_branch and datel_norm != want_branch:
            continue

        d = _parse_date(r[i_date] if len(r) > i_date else "")
        if start and (not d or d < start):
            continue
        if end and (not d or d > end):
            continue

        status = (r[i_status].strip() if len(r) > i_status else "") or "(blank)"
        jenis = (r[i_jenis].strip().upper() if len(r) > i_jenis else "")
        if jenis not in _JENIS_LIST:
            jenis = "(OTHER)"

        # akumulasi
        per_status[status] = per_status.get(status, 0) + 1
        per_status_by_jenis.setdefault(status, {})
        per_status_by_jenis[status][jenis] = per_status_by_jenis[status].get(jenis, 0) + 1

        if jenis in totals_by_jenis:
            totals_by_jenis[jenis] += 1
        else:
            totals_by_jenis[jenis] = totals_by_jenis.get(jenis, 0) + 1

        grand_total += 1

    return {
        "per_status": dict(sorted(per_status.items(), key=lambda x: (-x[1], x[0]))),
        "per_status_by_jenis": per_status_by_jenis,
        "totals_by_jenis": totals_by_jenis,
        "grand_total": grand_total,
    }