import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from sheets import find_order, search_by_name, list_pending_in_range, list_pending_in_month, summarize_orders, list_pending
from html import escape
from telegram.constants import ParseMode
from datetime import datetime, date, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo
import asyncio
from calendar import monthrange
import re

def _highlight(text: str, query: str) -> str:
    if not text or not query:
        return escape(text or "")
    t, q = text, query
    i = t.lower().find(q.lower())
    if i == -1:
        return escape(t)
    return escape(t[:i]) + "<b>" + escape(t[i:i+len(q)]) + "</b>" + escape(t[i+len(q):])

def _format_item(i: int, d: dict, query: str) -> str:
    name = _highlight(d.get("CUSTOMER_NAME",""), query)
    order_id = escape(d.get("ORDER_ID",""))
    no_sc = escape(d.get("NO_SC",""))
    status_do = escape(d.get("STATUS_DO",""))
    jenis = escape(d.get("JENIS_ORDER",""))
    tgl = escape(d.get("ORDER_DATE",""))
    last_upd_raw = d.get("LAST_UPDATED_DATE","")

    # hitung Age & Stale (kalender hari)
    od = _parse_date_str(d.get("ORDER_DATE",""))
    lu = _parse_date_str(last_upd_raw)
    today = _today_id()
    age = _days_between(od, today)         # ORDER_DATE -> today
    stale = _days_between(lu or od, today) # LAST_UPDATED_DATE -> today; fallback ke ORDER_DATE

    # tampilkan raw last updated jika ada
    last_upd = escape(last_upd_raw) if last_upd_raw else "-"

    return (
        f"<b>{i}. {name}</b>\n"
        f"      <b>ORDER_ID:</b> <code>{order_id}</code> | <b>No SC:</b> <code>{no_sc}</code>\n"
        f"      <b>Status DO:</b> {status_do} | <b>Jenis Order:</b> {jenis}\n"
        f"      <b>Tgl Order:</b> {tgl} | <b>Last Updated:</b> {last_upd}\n"
        f"      <b>Umur Order:</b> {age} | <b>Lama Tidak Update:</b> {stale}"
    )


def _get_admin_ids() -> list[int]:
    raw = os.getenv("ADMIN_CHAT_IDS", "")  # nama variabel ENV pakai huruf besar
    ids: list[int] = []
    for part in raw.replace("\n", ",").split(","):
        s = part.strip()
        if not s:
            continue
        # terima angka negatif (grup/channel) dan positif (user)
        try:
            ids.append(int(s))
        except ValueError:
            pass
    return ids

def _parse_date_str(s: str) -> date | None:
    if not s:
        return None
    try:
        from dateutil import parser as dateparser
        # asumsikan input bisa "YYYY-MM-DD", "DD/MM/YYYY", dll.
        return dateparser.parse(str(s), dayfirst=True, fuzzy=True).date()
    except Exception:
        return None

def _days_between(a: date | None, b: date | None) -> str:
    """Return selisih hari sebagai string 'Xd' atau '-' jika tidak valid."""
    if not a or not b:
        return "-"
    try:
        d = (b - a).days
        return f"{d}d" if d >= 0 else "-"
    except Exception:
        return "-"

def _today_id() -> date:
    # supaya konsisten zona waktu Indonesia
    return datetime.now(ZoneInfo("Asia/Jakarta")).date()

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        " Halo! Saya adalah Bot Monitoring Order.\n\n"
        "<b>Panduan Perintah:</b>\n"
        "• /order <ORDER_ID atau No SC>\n"
        "   ➝ Cek detail status order.\n"
        "   Contoh: <code>/order 1000353626</code>\n\n"
        "• /search <nama customer>\n"
        "   ➝ Cari order berdasarkan nama customer.\n"
        "   Contoh: <code>/search budi</code>\n\n"
        "• /pending\n"
        "   ➝ Lihat daftar order pending (Status ≠ Complete/Cancel).\n"
        "   Contoh:\n"
        "   <b>/pending 01-07-2025 15-08-2025</b> → filter rentang tanggal (DD-MM-YYYY)\n"
        "   <b>/pending 01-07-2025</b> → filter dari tanggal (DD-MM-YYYY) - hari ini \n"
        "   <b>/pending 2025-08</b> → filter bulan (YYYY-MM)\n"
        "   <b>/pending JAMBI 2025-08</b> → branch + bulan (YYYY-MM)\n"
        "   <b>/pending SUNGAI PENUH 01-07-2025</b> → branch + tanggal (DD-MM-YYYY)\n"
        "• /summarybranch [DATEL] [YYYY-MM]\n"
        "   ➝ Ringkasan per status & jenis order.\n"
        "   Contoh: <code>/summarybranch JAMBI 2025-08</code>\n\n"
        "Semua data diambil langsung dari Google Sheets (Order MODOROSO)."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def order_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Contoh: /order 1000353626 atau /order SC1000353626",
                                        parse_mode=ParseMode.HTML)
        return

    order_key = context.args[0]
    try:
        data = find_order(order_key)
    except Exception as e:
        await update.message.reply_text(f"Error membaca sheet: {escape(str(e))}",
                                        parse_mode=ParseMode.HTML)
        return

    if not data:
        await update.message.reply_text(f"Order <code>{escape(order_key)}</code> tidak ditemukan.",
                                        parse_mode=ParseMode.HTML)
        return

    msg = (
        f"<b>ORDER_ID:</b> <code>{escape(data['ORDER_ID'])}</code>\n"
        f"<b>No SC:</b> <code>{escape(data['NO_SC'])}</code>\n"
        f"<b>Status DO:</b> {escape(data['STATUS_DO'])}\n"
        f"<b>Customer:</b> {escape(data['CUSTOMER_NAME'])}\n"
        f"<b>Jenis Order:</b> {escape(data['JENIS_ORDER'])}\n"
        f"<b>Order Date:</b> {escape(data['ORDER_DATE'])}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Contoh: /search telkom atau /search budi",
                                        parse_mode=ParseMode.HTML)
        return

    query = " ".join(context.args).strip()
    try:
        results = search_by_name(query, limit=50)
    except Exception as e:
        await update.message.reply_text(f"Error membaca sheet: {escape(str(e))}",
                                        parse_mode=ParseMode.HTML)
        return

    if not results:
        await update.message.reply_text(f"Tidak ada hasil untuk: <code>{escape(query)}</code>",
                                        parse_mode=ParseMode.HTML)
        return

    # dedup dan batasi
    seen, dedup = set(), []
    for r in results:
        key = (r.get("ORDER_ID",""), r.get("NO_SC",""))
        if key in seen: continue
        seen.add(key); dedup.append(r)

    shown, more = dedup[:10], max(0, len(dedup)-10)

    # pecah pesan jika panjang mendekati limit
    chunks, buf = [], ""
    for i, d in enumerate(shown, 1):
        line = _format_item(i, d, query)
        if len(buf) + len(line) + 2 > 3800:
            chunks.append(buf); buf = line
        else:
            buf = (buf + ("\n\n" if buf else "")) + line
    if buf: chunks.append(buf)

    for c in chunks:
        await update.message.reply_text(c, parse_mode=ParseMode.HTML)

    if more:
        await update.message.reply_text(
            f"Menampilkan 10 hasil pertama. Ada <b>+{more}</b> hasil lain.\n"
            f"Coba persempit: <code>/search {escape(query)} jambi</code>",
            parse_mode=ParseMode.HTML
        )


async def pending_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args

    # DEFAULT: tampilkan cara penggunaan
    if not args:
        help_text = (
            "Cara menggunakan <b>/pending</b>:\n"
            "<b>/pending 01-07-2025 15-08-2025</b> → filter rentang tanggal (DD-MM-YYYY)\n"
            "<b>/pending 01-07-2025</b> → filter dari tanggal (DD-MM-YYYY) - hari ini \n"
            "<b>/pending 2025-08</b> → filter bulan (YYYY-MM)\n"
            "<b>/pending JAMBI 2025-08</b> → branch + bulan (YYYY-MM)\n"
            "<b>/pending SUNGAI PENUH 01-07-2025</b> → branch + tanggal (DD-MM-YYYY)\n"
        )
        await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)
        return

    branch_tokens = []
    keyword = None
    start = None
    end = None
    year = None
    month = None

    # Parsing argumen fleksibel:
    i = 0
    while i < len(args):
        a = args[i].strip()

        # keyword opsional pakai prefix kw:
        if a.lower().startswith("kw:"):
            keyword = a[3:].strip()
            i += 1
            continue

        # cek bulan: YYYY-MM atau YYYY/MM
        if re.match(r"^\d{4}[-/]\d{2}$", a):
            y, m = a.replace("/", "-").split("-")
            year, month = int(y), int(m)
            i += 1
            continue

        # cek tanggal lengkap
        d = _parse_date_arg(a)
        if d:
            if not start:
                start = d
            elif not end:
                end = d
            else:
                # kalau user kasih >2 tanggal, sisanya kita anggap bagian dari nama branch
                branch_tokens.append(a)
            i += 1
            continue

        # fallback: dianggap bagian dari nama branch (boleh multi-kata)
        branch_tokens.append(a)
        i += 1

    # normalisasi rentang
    if start and not end:
        end = date.today()
    if start and end and end < start:
        start, end = end, start

    branch = " ".join(branch_tokens).strip() if branch_tokens else None

    try:
        results = list_pending(
            keyword=keyword,
            start=start,
            end=end,
            year=year,
            month=month,
            branch=branch,   # <= kirim ke sheets
            limit=2000
        )
    except Exception as e:
        await update.message.reply_text(
            f"Error membaca sheet: <code>{escape(str(e))}</code>",
            parse_mode=ParseMode.HTML
        )
        return

    if not results:
        title = "<b>Daftar Pending</b>\n"
        if branch: title += f"Branch: <b>{escape(branch)}</b>\n"
        if keyword: title += f"Keyword: <code>{escape(keyword)}</code>\n"
        if year and month: title += f"Bulan: {year}-{month:02d}\n"
        if start: title += f"Periode: {start} – {end}\n"
        title += "Tidak ada order pending sesuai filter."
        await update.message.reply_text(title, parse_mode=ParseMode.HTML)
        return

    # Dedup hasil (ORDER_ID, NO_SC)
    seen, dedup = set(), []
    for r in results:
        key = (r.get("ORDER_ID",""), r.get("NO_SC",""))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    # Header ringkas
    title = "<b>Daftar Pending</b>\n"
    if branch: title += f"Branch: <b>{escape(branch)}</b>\n"
    if keyword: title += f"Keyword: <code>{escape(keyword)}</code>\n"
    if year and month: title += f"Bulan: {year}-{month:02d}\n"
    if start: title += f"Periode: {start} – {end}\n"

    # Pecah pesan bila panjang
    buf, chunks = title, []
    for i, d in enumerate(dedup, 1):
        line = _format_item(i, d, keyword or "")
        if len(buf) + len(line) + 2 > 3500:
            chunks.append(buf); buf = line
        else:
            buf = (buf + ("\n\n" if buf else "")) + line
    if buf:
        chunks.append(buf)

    for c in chunks:
        await update.message.reply_text(c, parse_mode=ParseMode.HTML)


def _parse_date_arg(s: str) -> date | None:
    """Parse argumen tanggal user jadi date (YYYY-MM-DD, DD/MM/YYYY, 2-Jan-24, dll.)."""
    try:
        from dateutil import parser as dateparser
        return dateparser.parse(s, dayfirst=True, fuzzy=True).date()
    except Exception:
        return None

async def pending_date_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Contoh:\n"
            "<code>/pendingdate 2024-01-01 2024-01-31</code>\n"
            "<code>/pendingdate 01/01/2024</code>",
            parse_mode=ParseMode.HTML
        )
        return

    start = _parse_date_arg(context.args[0])
    end = _parse_date_arg(context.args[1]) if len(context.args) >= 2 else date.today()

    if not start:
        await update.message.reply_text("Tanggal <b>start</b> tidak valid.", parse_mode=ParseMode.HTML)
        return
    if end and end < start:
        start, end = end, start

    results = list_pending_in_range(start, end, limit=2000)  # ambil banyak
    if not results:
        await update.message.reply_text(
            f"Tidak ada order pending pada rentang "
            f"<code>{escape(str(start))}</code> – <code>{escape(str(end))}</code>.",
            parse_mode=ParseMode.HTML
        )
        return

    # dedup
    seen, dedup = set(), []
    for r in results:
        key = (r.get("ORDER_ID",""), r.get("NO_SC",""))
        if key in seen: continue
        seen.add(key); dedup.append(r)

    title = (f"<b>Pending (Status ≠ Complete/Cancel)</b>\n"
             f"Rentang: <code>{escape(str(start))}</code> – <code>{escape(str(end))}</code>\n")
    buf, chunks = title, []
    for i, d in enumerate(dedup, 1):
        line = _format_item(i, d, "")
        if len(buf) + len(line) + 2 > 3500:  # pecah pesan
            chunks.append(buf); buf = line
        else:
            buf = (buf + ("\n\n" if buf else "")) + line
    if buf: chunks.append(buf)

    for c in chunks:
        await update.message.reply_text(c, parse_mode=ParseMode.HTML)


async def pending_month_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Contoh: <code>/pendingmonth 2024-01</code>",
                                        parse_mode=ParseMode.HTML)
        return

    raw = context.args[0].strip()
    y, m = None, None
    try:
        if "-" in raw or "/" in raw:
            sep = "-" if "-" in raw else "/"
            a, b = raw.split(sep)
            if len(a) == 4:  # YYYY-MM
                y, m = int(a), int(b)
            else:            # MM-YYYY
                m, y = int(a), int(b)
        else:
            d = _parse_date_arg(raw)
            if d:
                y, m = d.year, d.month
    except Exception:
        pass

    if not y or not m or not (1 <= m <= 12):
        await update.message.reply_text("Format bulan tidak valid. Contoh: <code>/pendingmonth 2024-01</code>",
                                        parse_mode=ParseMode.HTML)
        return

    results = list_pending_in_month(y, m, limit=2000)
    from calendar import month_name
    label = f"{month_name[m]} {y}"

    if not results:
        await update.message.reply_text(
            f"Tidak ada order pending pada bulan <b>{escape(label)}</b>.",
            parse_mode=ParseMode.HTML
        )
        return

    # dedup
    seen, dedup = set(), []
    for r in results:
        key = (r.get("ORDER_ID",""), r.get("NO_SC",""))
        if key in seen: continue
        seen.add(key); dedup.append(r)

    title = f"<b>Pending (Status ≠ Complete/Cancel)</b>\nBulan: <b>{escape(label)}</b>\n"
    buf, chunks = title, []
    for i, d in enumerate(dedup, 1):
        line = _format_item(i, d, "")
        if len(buf) + len(line) + 2 > 3500:
            chunks.append(buf); buf = line
        else:
            buf = (buf + ("\n\n" if buf else "")) + line
    if buf: chunks.append(buf)

    for c in chunks:
        await update.message.reply_text(c, parse_mode=ParseMode.HTML)


async def send_pending_last2months(context: ContextTypes.DEFAULT_TYPE):
    # ambil target (DM, grup biasa, atau topic forum)
    raw_targets = _get_targets()
    if not raw_targets:
        return
    # de-dup biar tidak kirim dua kali ke target yang sama
    seen = set()
    targets: list[tuple[int, int | None]] = []
    for t in raw_targets:
        if t not in seen:
            seen.add(t)
            targets.append(t)

    today = date.today()
    # awal dua bulan lalu (kalender)
    y, m = today.year, today.month
    if m > 1:
        start_y, start_m = y, m - 1
    else:
        start_y, start_m = y - 1, m + 10
    start = date(start_y, start_m, 1)
    end = today

    # ambil data (non-blocking)
    try:
        results = await asyncio.to_thread(list_pending_in_range, start, end, 5000)
    except Exception as e:
        err = (
            f"<b>Ringkasan Pending – 2 Bulan Terakhir</b>\n"
            f"Rentang: <code>{start}</code> – <code>{end}</code>\n"
            f"Gagal membaca data: <code>{escape(str(e))}</code>"
        )
        for chat_id, thread_id in targets:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=err,
                    parse_mode=ParseMode.HTML,
                    message_thread_id=thread_id
                )
            except Exception:
                pass
            await asyncio.sleep(0.25)
        return

    header_msg = (
        f"<b>Ringkasan Pending – 2 Bulan Terakhir</b>\n"
        f"Rentang: <code>{start}</code> – <code>{end}</code>\n"
        f"Total: <b>{len(results)}</b>"
    )

    # kirim header ke semua target (hormati topic/thread)
    for chat_id, thread_id in targets:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=header_msg,
                parse_mode=ParseMode.HTML,
                message_thread_id=thread_id
            )
        except Exception as e:
            print(f"[WARN] gagal kirim header ke {chat_id} (thread={thread_id}): {e}")
        await asyncio.sleep(0.25)

    if not results:
        return

    # dedup data
    seen_keys, dedup = set(), []
    for r in results:
        key = (r.get("ORDER_ID", ""), r.get("NO_SC", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        dedup.append(r)

    # pecah agar aman dari limit Telegram
    buf, chunks = "", []
    for i, d in enumerate(dedup, 1):
        line = _format_item(i, d, "")
        if len(buf) + len(line) + 2 > 3500:
            chunks.append(buf); buf = line
        else:
            buf = (buf + ("\n\n" if buf else "")) + line
    if buf:
        chunks.append(buf)

    # kirim detail ke semua target
    for c in chunks:
        for chat_id, thread_id in targets:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=c,
                    parse_mode=ParseMode.HTML,
                    message_thread_id=thread_id
                )
            except Exception as e:
                print(f"[WARN] gagal kirim detail ke {chat_id} (thread={thread_id}): {e}")
            await asyncio.sleep(0.35)




async def summary_branch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /summarybranch [NAMA_BRANCH] [YYYY-MM]
    Contoh:
      /summarybranch              -> semua branch, bulan ini
      /summarybranch JAMBI        -> hanya DATEL JAMBI, bulan ini
      /summarybranch JAMBI 2025-08 -> JAMBI, Agustus 2025
    """
    args = context.args or []
    branch = None
    year = None
    month = None

    if len(args) >= 1:
        # kalau argumen terlihat seperti YYYY-MM taruh ke bulan
        a0 = args[0].strip()
        if len(a0) in (7, 10) and ("-" in a0 or "/" in a0):
            ym = a0.replace("/", "-").split("-")
            if len(ym) >= 2 and ym[0].isdigit() and ym[1].isdigit():
                year, month = int(ym[0]), int(ym[1])
        else:
            branch = a0

    if len(args) >= 2 and (year is None or month is None):
        a1 = args[1].strip()
        ym = a1.replace("/", "-").split("-")
        if len(ym) >= 2 and ym[0].isdigit() and ym[1].isdigit():
            year, month = int(ym[0]), int(ym[1])

    # default: bulan ini
    today = date.today()
    y = year or today.year
    m = month or today.month
    start = date(y, m, 1)
    end = date(y, m, monthrange(y, m)[1])

    try:
        res = summarize_orders(branch=branch, start=start, end=end)
    except Exception as e:
        await update.message.reply_text(
            f"Gagal membaca data: <code>{escape(str(e))}</code>", parse_mode=ParseMode.HTML
        )
        return

    per_status = res["per_status"]
    grand = res["grand_total"]
    by_jenis = res["totals_by_jenis"]

    if not grand:
        target = branch or "SEMUA BRANCH"
        await update.message.reply_text(
            f"Tidak ada data untuk <b>{escape(target)}</b> pada {y}-{m:02d}.",
            parse_mode=ParseMode.HTML
        )
        return

    title = f"<b>SUMMARY MTD {y}-{m:02d}</b>\n"
    if branch:
        title += f"Branch: <b>{escape(branch)}</b>\n"
    else:
        title += "Branch: <b>SEMUA</b>\n"
    title += f"Periode: <code>{start}</code> – <code>{end}</code>\n\n"

    # daftar status (semua, termasuk Complete & Cancel)
    lines = [title]
    for k, v in per_status.items():
        lines.append(f"{escape(k)}: <b>{v}</b>")

    # total per jenis
    jenis_line = " | ".join([f"{j}: {by_jenis.get(j,0)}" for j in ["MO","DO","RO","SO","PDA","CO","CN","AS","MIGRATE"]])
    lines.append(f"\n<b>Total per Jenis</b>\n{escape(jenis_line)}")
    lines.append(f"\nTOTAL: <b>{grand}</b>")

    # pecah jika kepanjangan
    text = "\n".join(lines)
    if len(text) > 3500:
        # kirim header dulu, lanjut sisanya bertahap
        head, rest = title, "\n".join(lines[1:])
        await update.message.reply_text(head, parse_mode=ParseMode.HTML)
        # potong-potong 3000 char
        chunk = ""
        for ln in rest.split("\n"):
            if len(chunk) + len(ln) + 1 > 3000:
                await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
                chunk = ln
            else:
                chunk = (chunk + "\n" + ln) if chunk else ln
        if chunk:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

def _get_targets() -> list[tuple[int, int | None]]:
    """
    Baca target dari ENV:
      ADMIN_TARGETS (direkomendasikan), fallback ke ADMIN_CHAT_IDS.
    Format:
      -1001234567890           -> kirim ke chat (tanpa topic)
      -1001234567890:12345     -> kirim ke topic id 12345 di chat tsb
    Pisahkan dengan koma atau baris baru.
    """
    raw = os.getenv("ADMIN_TARGETS", "") or os.getenv("ADMIN_CHAT_IDS", "")
    targets: list[tuple[int, int | None]] = []
    for part in raw.replace("\n", ",").split(","):
        s = part.strip()
        if not s:
            continue
        if ":" in s:
            a, b = s.split(":", 1)
            try:
                targets.append((int(a.strip()), int(b.strip())))
            except ValueError:
                pass
        else:
            try:
                targets.append((int(s), None))
            except ValueError:
                pass
    return targets


async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg  = update.effective_message
    await update.message.reply_text(
        f"<b>Chat ID:</b> <code>{chat.id}</code>\n"
        f"<b>Topic ID (message_thread_id):</b> <code>{getattr(msg, 'message_thread_id', None)}</code>",
        parse_mode=ParseMode.HTML
    )

async def debug_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = _get_targets()
    if not items:
        return await update.message.reply_text(
            "Tidak ada target. Cek ENV ADMIN_TARGETS / ADMIN_CHAT_IDS.",
            parse_mode=ParseMode.HTML
        )
    lines = [f"{i+1}. chat_id={cid}, thread_id={tid}" for i, (cid, tid) in enumerate(items)]
    await update.message.reply_text("<b>Targets:</b>\n" + "\n".join(lines), parse_mode=ParseMode.HTML)


async def sendtopic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /sendtopic <chat_id> <thread_id> <teks...>
    Contoh:
      /sendtopic -1002719604219 5 Halo dari bot
    """
    if len(context.args) < 3:
        return await update.message.reply_text(
            "Format: /sendtopic <chat_id> <thread_id> <teks...>",
            parse_mode=ParseMode.HTML
        )
    try:
        chat_id = int(context.args[0])
        thread_id = int(context.args[1])
    except ValueError:
        return await update.message.reply_text("chat_id / thread_id harus angka.", parse_mode=ParseMode.HTML)
    text = " ".join(context.args[2:])
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"TEST TOPIC ✅\n{escape(text)}",
            parse_mode=ParseMode.HTML,
            message_thread_id=thread_id
        )
        await update.message.reply_text("Terkirim ✔️", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(
            f"Gagal kirim: <code>{escape(str(e))}</code>", parse_mode=ParseMode.HTML
        )



import logging
logging.basicConfig(level=logging.INFO)
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled exception", exc_info=context.error)


def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("order", order_cmd))
    app.add_handler(CommandHandler("search", search_cmd))
    app.add_handler(CommandHandler("pending", pending_cmd))
    app.add_handler(CommandHandler("pendingdate", pending_date_cmd))
    app.add_handler(CommandHandler("pendingmonth", pending_month_cmd))
    app.add_handler(CommandHandler("summarybranch", summary_branch_cmd))
    # di main():
    app.add_handler(CommandHandler("whereami", whereami))
    app.add_error_handler(on_error)
    # di main():
    app.add_handler(CommandHandler("debugtargets", debug_targets))
    # di main():
    app.add_handler(CommandHandler("sendtopic", sendtopic))

    jakarta = ZoneInfo("Asia/Jakarta")
    app.job_queue.run_daily(
        send_pending_last2months,
        time=dtime(hour=10, minute=15, tzinfo=jakarta),
        name="daily_pending_last2months",
    )

    # tes sekali setelah start
    app.job_queue.run_once(send_pending_last2months, when=20)

    app.run_polling()


if __name__ == "__main__":
    main()
