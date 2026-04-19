"""
Smiles Worker — Railway
- 16 hubs agrupados por región
- Búsquedas en Google Flights en paralelo (10 simultáneas) para acelerar
- Smiles via @smileshelperbot con rate limit
- Jobs asíncronos para no timeoutear
"""

import os
import re
import uuid
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from aiohttp import web
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

try:
    from fast_flights import FlightData, Passengers, get_flights
    FAST_FLIGHTS_AVAILABLE = True
except ImportError:
    FAST_FLIGHTS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smiles-worker")

# ── Config ──
API_ID     = int(os.environ["TG_API_ID"])
API_HASH   = os.environ["TG_API_HASH"]
SESSION    = os.environ.get("TG_SESSION", "smiles_session")
BOT_USER   = os.environ.get("SMILES_BOT", "smileshelperbot")
WORKER_KEY = os.environ.get("WORKER_SECRET", "change-me")
PORT       = int(os.environ.get("PORT", "8080"))

SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION, API_ID, API_HASH)

smiles_lock = asyncio.Lock()
LAST_SMILES_TS = [0.0]
MIN_SMILES_INTERVAL = 30

# ══════════════════════════════════════════════════════
# 16 HUBS agrupados por región
# ══════════════════════════════════════════════════════
HUBS = {
    # 🌎 Sudamérica
    "SCL": {"name": "Santiago",    "region": "sudamerica"},
    "GRU": {"name": "São Paulo",   "region": "sudamerica"},
    "LIM": {"name": "Lima",        "region": "sudamerica"},
    "BOG": {"name": "Bogotá",      "region": "sudamerica"},
    "PTY": {"name": "Panamá",      "region": "sudamerica"},
    # 🇺🇸 Norteamérica
    "MIA": {"name": "Miami",       "region": "norteamerica"},
    "JFK": {"name": "Nueva York",  "region": "norteamerica"},
    "LAX": {"name": "Los Ángeles", "region": "norteamerica"},
    "MEX": {"name": "CDMX",        "region": "norteamerica"},
    # 🇪🇺 Europa
    "MAD": {"name": "Madrid",      "region": "europa"},
    "LIS": {"name": "Lisboa",      "region": "europa"},
    "FCO": {"name": "Roma",        "region": "europa"},
    "AMS": {"name": "Ámsterdam",   "region": "europa"},
    # 🌍 Medio Oriente
    "DOH": {"name": "Doha",        "region": "medio_oriente"},
    "DXB": {"name": "Dubai",       "region": "medio_oriente"},
    "IST": {"name": "Estambul",    "region": "medio_oriente"},
}
HUB_CODES = list(HUBS.keys())

# Paralelismo: cuántas búsquedas simultáneas a Google Flights
MAX_CONCURRENT_GF = 8

JOBS = {}

# Cache in-memory de resultados de fast-flights
# key: (origin, dest, date, return_date or "") -> (timestamp, results)
GF_CACHE = {}
GF_CACHE_TTL = 1800  # 30 min
GF_CACHE_LOCK = asyncio.Lock()


# ═══════════════════════════════════════════════
# Smiles bot
# ═══════════════════════════════════════════════
def extract_entity_links(message) -> list:
    links = []
    text = message.message or ""
    for ent in (message.entities or []):
        if isinstance(ent, MessageEntityTextUrl):
            visible = text[ent.offset : ent.offset + ent.length]
            links.append({"text": visible, "url": ent.url})
        elif isinstance(ent, MessageEntityUrl):
            visible = text[ent.offset : ent.offset + ent.length]
            links.append({"text": visible, "url": visible})
    return links


async def query_smiles_bot(origin: str, dest: str, date: str, days: int = 7):
    async with smiles_lock:
        now = asyncio.get_event_loop().time()
        elapsed = now - LAST_SMILES_TS[0]
        if elapsed < MIN_SMILES_INTERVAL:
            await asyncio.sleep(MIN_SMILES_INTERVAL - elapsed)

        command = f"{origin} {dest} {date} d{days}"
        log.info(f"→ Smiles: {command}")

        collected = []
        got = asyncio.Event()

        @client.on(events.NewMessage(from_users=BOT_USER))
        async def handler(event):
            msg = event.message.message or ""
            if len(msg) < 80 and any(w in msg.lower() for w in ["buscando", "procesando", "espera"]):
                return
            collected.append((msg, extract_entity_links(event.message)))
            got.set()

        try:
            await client.send_message(BOT_USER, command)
            try:
                await asyncio.wait_for(got.wait(), timeout=60)
            except asyncio.TimeoutError:
                return "", []
            await asyncio.sleep(3)
        finally:
            client.remove_event_handler(handler)
            LAST_SMILES_TS[0] = asyncio.get_event_loop().time()

        all_text = "\n\n".join(m[0] for m in collected)
        all_links = []
        for _, l in collected:
            all_links.extend(l)
        return all_text, all_links


def parse_smiles_response(text: str, links: list) -> list:
    if not text:
        return []

    date_to_urls = {}
    for link in links:
        t = link["text"].strip()
        m = re.match(r"^(\d{1,2})/(\d{1,2})$", t)
        if m and "smiles.com.ar" in link["url"].lower():
            key = f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"
            date_to_urls.setdefault(key, []).append(link["url"])

    results = []
    pattern = re.compile(
        r"✈️?\s*(\d{1,2})/(\d{1,2}).*?:\s*([\d.]+)\s*\+\s*(\d+)K/\$?([\d.]+K?)\s*"
        r"([A-Za-zÁ-ú\s]+?)\s*,\s*([A-Z]+)\s*,\s*(\d+)\s*escala",
        re.IGNORECASE,
    )

    year_match = re.search(r"(\d{4})-\d{2}-\d{2}", text)
    year = int(year_match.group(1)) if year_match else datetime.now().year

    date_link_idx = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        m = pattern.search(line)
        if not m:
            continue
        day, month, miles_s, ars_k, _, airline, cabin, stops = m.groups()
        miles = int(miles_s.replace(".", "").replace(",", ""))
        ars = int(ars_k) * 1000
        dur_m = re.search(r"🕐?\s*(\d+)\s*hs", line)
        duration_hs = int(dur_m.group(1)) if dur_m else None

        date_str = f"{year}-{int(month):02d}-{int(day):02d}"
        date_key = f"{int(day):02d}/{int(month):02d}"
        urls = date_to_urls.get(date_key, [])
        idx = date_link_idx.get(date_key, 0)
        booking_url = urls[idx] if idx < len(urls) else (urls[0] if urls else None)
        date_link_idx[date_key] = idx + 1

        results.append({
            "date": date_str, "day_month": date_key,
            "miles": miles, "ars": ars,
            "airline": airline.strip(), "cabin": cabin.strip(),
            "stops": int(stops), "duration_hs": duration_hs,
            "booking_url": booking_url, "raw": line[:200],
        })

    results.sort(key=lambda r: r["miles"])
    return results


# ═══════════════════════════════════════════════
# Google Flights via fast-flights (con paralelismo)
# ═══════════════════════════════════════════════
_gf_semaphore = None

def _gf_search_sync(origin, dest, date):
    if not FAST_FLIGHTS_AVAILABLE:
        return []
    try:
        result = get_flights(
            flight_data=[FlightData(date=date, from_airport=origin, to_airport=dest)],
            trip="one-way", seat="economy",
            passengers=Passengers(adults=1),
            fetch_mode="fallback",
        )
        out = []
        for f in (getattr(result, "flights", []) or []):
            price_str = getattr(f, "price", "") or ""
            digits = "".join(c for c in str(price_str) if c.isdigit())
            if not digits:
                continue
            price = int(digits)
            if price < 50:
                continue
            airline = getattr(f, "name", "") or "?"
            stops_v = getattr(f, "stops", 0)
            if isinstance(stops_v, str):
                sd = "".join(c for c in stops_v if c.isdigit())
                stops_n = int(sd) if sd else (0 if "nonstop" in stops_v.lower() else 1)
            else:
                stops_n = int(stops_v) if stops_v else 0
            out.append({
                "origin": origin, "dest": dest, "date": date,
                "price": price, "airline": airline, "stops": stops_n,
                "duration": getattr(f, "duration", "") or "",
                "dep_time": getattr(f, "departure", "") or "",
                "arr_time": getattr(f, "arrival", "") or "",
            })
        return sorted(out, key=lambda x: x["price"])
    except Exception as e:
        log.warning(f"fast-flights {origin}→{dest} {date}: {e}")
        return []


def _gf_search_sync_rt(origin, dest, outbound, return_date):
    if not FAST_FLIGHTS_AVAILABLE:
        return []
    try:
        result = get_flights(
            flight_data=[
                FlightData(date=outbound, from_airport=origin, to_airport=dest),
                FlightData(date=return_date, from_airport=dest, to_airport=origin),
            ],
            trip="round-trip", seat="economy",
            passengers=Passengers(adults=1),
            fetch_mode="fallback",
        )
        out = []
        for f in (getattr(result, "flights", []) or []):
            price_str = getattr(f, "price", "") or ""
            digits = "".join(c for c in str(price_str) if c.isdigit())
            if not digits:
                continue
            if int(digits) < 50:
                continue
            stops_v = getattr(f, "stops", 0)
            if isinstance(stops_v, str):
                sd = "".join(c for c in stops_v if c.isdigit())
                stops_n = int(sd) if sd else 0
            else:
                stops_n = int(stops_v) if stops_v else 0
            out.append({
                "origin": origin, "dest": dest, "date": outbound,
                "return_date": return_date,
                "price": int(digits),
                "airline": getattr(f, "name", "") or "?",
                "stops": stops_n,
                "duration": getattr(f, "duration", "") or "",
            })
        return sorted(out, key=lambda x: x["price"])
    except Exception as e:
        log.warning(f"RT {origin}→{dest}: {e}")
        return []


async def gf_search(origin, dest, date, rt_return=None):
    """Wrapped en semáforo + cache TTL 30min."""
    global _gf_semaphore
    if _gf_semaphore is None:
        _gf_semaphore = asyncio.Semaphore(MAX_CONCURRENT_GF)

    cache_key = (origin, dest, date, rt_return or "")
    now = asyncio.get_event_loop().time()

    async with GF_CACHE_LOCK:
        hit = GF_CACHE.get(cache_key)
        if hit and (now - hit[0]) < GF_CACHE_TTL:
            return hit[1]

    async with _gf_semaphore:
        loop = asyncio.get_event_loop()
        if rt_return:
            res = await loop.run_in_executor(None, _gf_search_sync_rt, origin, dest, date, rt_return)
        else:
            res = await loop.run_in_executor(None, _gf_search_sync, origin, dest, date)

    async with GF_CACHE_LOCK:
        GF_CACHE[cache_key] = (asyncio.get_event_loop().time(), res)
        # Limpieza oportunista: remover entradas vencidas si el cache creció
        if len(GF_CACHE) > 2000:
            cutoff = asyncio.get_event_loop().time() - GF_CACHE_TTL
            for k in [k for k, v in GF_CACHE.items() if v[0] < cutoff]:
                GF_CACHE.pop(k, None)

    return res


# ═══════════════════════════════════════════════
# Combos (split ticket) — con paralelismo
# ═══════════════════════════════════════════════
def parse_time_str(s: str) -> int:
    s = (s or "").strip().upper()
    m = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", s)
    if not m:
        return -1
    h = int(m.group(1)); mn = int(m.group(2)); ap = m.group(3)
    if ap == "PM" and h != 12: h += 12
    elif ap == "AM" and h == 12: h = 0
    return h * 60 + mn


def estimate_layover_hours(d1: str, arr: str, d2: str, dep: str) -> float:
    try:
        dt1 = datetime.strptime(d1, "%Y-%m-%d")
        dt2 = datetime.strptime(d2, "%Y-%m-%d")
    except ValueError:
        return 0.0
    am = parse_time_str(arr); dm = parse_time_str(dep)
    if am < 0 or dm < 0:
        return max(24.0 * (dt2 - dt1).days, 0.0)
    minutes = ((dt2 - dt1).days * 24 * 60) + (dm - am)
    return minutes / 60.0


def get_sample_dates_for_month(year_month: str, samples=None) -> list:
    if samples is None:
        samples = [10, 20]
    try:
        year, month = map(int, year_month.split("-"))
    except ValueError:
        return []
    dates = []
    for day in samples:
        try:
            d = datetime(year, month, day)
            if d > datetime.now() + timedelta(days=2):
                dates.append(d.strftime("%Y-%m-%d"))
        except ValueError:
            pass
    return dates


def _best_combo_per_hub_date(legs1_by_hd, legs2_by_hd, date,
                               min_layover_h, max_layover_h):
    """
    Dada una fecha y los dicts por (hub, date), devuelve el mejor combo
    por cada hub (leg1 + leg2) que respete el layover.
    Retorna lista de dicts: {hub, leg1, leg2, total, layover_hours}.
    """
    out = []
    for hub in HUB_CODES:
        leg1s = sorted(legs1_by_hd.get((hub, date), []), key=lambda x: x["price"])[:5]
        leg2s = sorted(legs2_by_hd.get((hub, date), []), key=lambda x: x["price"])[:5]
        best = None
        for l1 in leg1s:
            for l2 in leg2s:
                layover = estimate_layover_hours(
                    l1["date"], l1.get("arr_time", ""),
                    l2["date"], l2.get("dep_time", ""),
                )
                if layover < min_layover_h or layover > max_layover_h:
                    continue
                total = l1["price"] + l2["price"]
                if best is None or total < best["total"]:
                    best = {
                        "hub": hub,
                        "leg1": l1, "leg2": l2,
                        "total": total,
                        "layover_hours": round(layover, 1),
                    }
        if best:
            out.append(best)
    return out


async def run_combo_search(job_id: str, origin: str, final_dest: str,
                            year_month: str, min_layover_h: int, max_layover_h: int,
                            include_smiles: bool,
                            min_stay_days: int = 14, max_stay_days: int = 21):
    """
    Round-trip combo. Busca en paralelo:
      - Ida: EZE→hub→dest, por varias fechas y cada hub
      - Vuelta: dest→hub→EZE, por varias fechas (outbound + stay) y cada hub
      - Directo ida+vuelta de referencia
    Combina permitiendo hubs distintos en ida y vuelta.
    """
    job = JOBS[job_id]
    job["status"] = "running"

    out_dates = get_sample_dates_for_month(year_month)
    if not out_dates:
        job["status"] = "done"
        job["progress"] = "✅ Sin fechas válidas"
        job["result"] = {"error": "Sin fechas futuras en ese mes", "combos": []}
        return

    # Muestreo de stay: min, mid, max (si max-min pequeño usa solo uno)
    if max_stay_days - min_stay_days <= 2:
        stay_samples = [min_stay_days]
    elif max_stay_days - min_stay_days <= 6:
        stay_samples = [min_stay_days, max_stay_days]
    else:
        stay_samples = [min_stay_days, (min_stay_days + max_stay_days) // 2, max_stay_days]

    # Fechas de vuelta únicas (out + stay)
    ret_dates_set = set()
    out_to_rets = {}  # out_date -> [ret_date, ...]
    for od in out_dates:
        rets = []
        for s in stay_samples:
            rd = (datetime.strptime(od, "%Y-%m-%d") + timedelta(days=s)).strftime("%Y-%m-%d")
            ret_dates_set.add(rd)
            rets.append((s, rd))
        out_to_rets[od] = rets
    ret_dates = sorted(ret_dates_set)

    # Armar tareas
    tasks = []
    desc = []  # (kind, date, hub)

    # Directo ida (referencia): 1 por out_date
    for od in out_dates:
        tasks.append(gf_search(origin, final_dest, od))
        desc.append(("direct_out", od, None))

    # Directo vuelta (referencia): 1 por ret_date
    for rd in ret_dates:
        tasks.append(gf_search(final_dest, origin, rd))
        desc.append(("direct_back", rd, None))

    # Ida: EZE→hub + hub→dest por cada out_date y hub
    for od in out_dates:
        for hub in HUB_CODES:
            tasks.append(gf_search(origin, hub, od))
            desc.append(("out_leg1", od, hub))
            for offset in [0, 1]:
                nd = (datetime.strptime(od, "%Y-%m-%d") + timedelta(days=offset)).strftime("%Y-%m-%d")
                tasks.append(gf_search(hub, final_dest, nd))
                desc.append(("out_leg2", od, hub))

    # Vuelta: dest→hub + hub→EZE por cada ret_date y hub
    for rd in ret_dates:
        for hub in HUB_CODES:
            tasks.append(gf_search(final_dest, hub, rd))
            desc.append(("back_leg1", rd, hub))
            for offset in [0, 1]:
                nd = (datetime.strptime(rd, "%Y-%m-%d") + timedelta(days=offset)).strftime("%Y-%m-%d")
                tasks.append(gf_search(hub, origin, nd))
                desc.append(("back_leg2", rd, hub))

    total = len(tasks)
    log.info(f"Job {job_id}: {total} búsquedas (ida+vuelta) en paralelo ({MAX_CONCURRENT_GF} simultáneas)")
    job["progress"] = f"🔍 {total} búsquedas en paralelo..."

    results_indexed = [None] * total
    completed = [0]

    async def run_one(i, coro):
        try:
            results_indexed[i] = await coro
        finally:
            completed[0] += 1
            if completed[0] % 10 == 0 or completed[0] == total:
                job["progress"] = f"🔍 {completed[0]}/{total} búsquedas completadas..."

    await asyncio.gather(*[run_one(i, t) for i, t in enumerate(tasks)])

    # Organizar
    direct_out_by_date = {}
    direct_back_by_date = {}
    out_leg1_by_hd = {}
    out_leg2_by_hd = {}
    back_leg1_by_hd = {}
    back_leg2_by_hd = {}

    for i, (kind, date, hub) in enumerate(desc):
        res = results_indexed[i] or []
        if kind == "direct_out":
            direct_out_by_date.setdefault(date, []).extend(res)
        elif kind == "direct_back":
            direct_back_by_date.setdefault(date, []).extend(res)
        elif kind == "out_leg1":
            out_leg1_by_hd.setdefault((hub, date), []).extend(res)
        elif kind == "out_leg2":
            out_leg2_by_hd.setdefault((hub, date), []).extend(res)
        elif kind == "back_leg1":
            back_leg1_by_hd.setdefault((hub, date), []).extend(res)
        elif kind == "back_leg2":
            back_leg2_by_hd.setdefault((hub, date), []).extend(res)

    # Combinar
    job["progress"] = "🧩 Combinando tramos ida + vuelta..."

    # Mejor combo de ida por (out_date, hub)
    outbound_combos_by_date = {}  # out_date -> [combo ida]
    for od in out_dates:
        outbound_combos_by_date[od] = _best_combo_per_hub_date(
            out_leg1_by_hd, out_leg2_by_hd, od, min_layover_h, max_layover_h
        )

    # Mejor combo de vuelta por (ret_date, hub)
    return_combos_by_date = {}  # ret_date -> [combo vuelta]
    for rd in ret_dates:
        return_combos_by_date[rd] = _best_combo_per_hub_date(
            back_leg1_by_hd, back_leg2_by_hd, rd, min_layover_h, max_layover_h
        )

    # Directos ida/vuelta por fecha
    best_direct_out_by_date = {
        od: min(flights, key=lambda x: x["price"])
        for od, flights in direct_out_by_date.items() if flights
    }
    best_direct_back_by_date = {
        rd: min(flights, key=lambda x: x["price"])
        for rd, flights in direct_back_by_date.items() if flights
    }

    # Armar round-trip combos: mejor ida + mejor vuelta (por pares out/ret válidos)
    all_combos = []
    for od in out_dates:
        out_list = outbound_combos_by_date.get(od, [])
        if not out_list:
            continue
        for stay, rd in out_to_rets[od]:
            ret_list = return_combos_by_date.get(rd, [])
            if not ret_list:
                continue
            # Permitimos hubs distintos → best out × best back independientes
            best_out = min(out_list, key=lambda c: c["total"])
            best_back = min(ret_list, key=lambda c: c["total"])

            # Además probamos todos los pares para no perder combos
            # donde un hub "caro de ida" sea "barato de vuelta"
            for o in out_list[:5]:
                for b in ret_list[:5]:
                    total = o["total"] + b["total"]
                    all_combos.append({
                        "hub_out":  o["hub"],
                        "hub_back": b["hub"],
                        "hub_out_name":  HUBS[o["hub"]]["name"],
                        "hub_back_name": HUBS[b["hub"]]["name"],
                        "out_leg1": o["leg1"], "out_leg2": o["leg2"],
                        "back_leg1": b["leg1"], "back_leg2": b["leg2"],
                        "out_layover_hours":  o["layover_hours"],
                        "back_layover_hours": b["layover_hours"],
                        "total_price": total,
                        "out_date": od,
                        "return_date": rd,
                        "stay_days": stay,
                    })

    # Dedupe por (hub_out, hub_back, out_date, return_date) quedándonos con el más barato
    uniq = {}
    for c in all_combos:
        k = (c["hub_out"], c["hub_back"], c["out_date"], c["return_date"])
        if k not in uniq or c["total_price"] < uniq[k]["total_price"]:
            uniq[k] = c
    all_combos = sorted(uniq.values(), key=lambda c: c["total_price"])
    top_combos = all_combos[:10]

    # Directo round-trip de referencia (mejor combinación out+back directos)
    best_direct_rt = None
    for od in out_dates:
        if od not in best_direct_out_by_date:
            continue
        for stay, rd in out_to_rets[od]:
            if rd not in best_direct_back_by_date:
                continue
            total = best_direct_out_by_date[od]["price"] + best_direct_back_by_date[rd]["price"]
            if best_direct_rt is None or total < best_direct_rt["total_price"]:
                best_direct_rt = {
                    "out":    best_direct_out_by_date[od],
                    "back":   best_direct_back_by_date[rd],
                    "total_price": total,
                    "out_date": od, "return_date": rd, "stay_days": stay,
                }

    # Smiles opcional (solo mejor combo — 4 tramos)
    smiles_data = {"direct_out": None, "direct_back": None, "legs": {}}
    if include_smiles and top_combos:
        best = top_combos[0]
        od, rd = best["out_date"], best["return_date"]
        ho, hb = best["hub_out"], best["hub_back"]

        async def safe_smiles(o, d, date):
            try:
                raw, links = await query_smiles_bot(o, d, date, 7)
                return parse_smiles_response(raw, links)[:3]
            except Exception as e:
                log.warning(f"Smiles {o}→{d} {date}: {e}")
                return []

        job["progress"] = f"🎫 Smiles directo ida {origin}→{final_dest}..."
        smiles_data["direct_out"] = await safe_smiles(origin, final_dest, od)

        job["progress"] = f"🎫 Smiles directo vuelta {final_dest}→{origin}..."
        smiles_data["direct_back"] = await safe_smiles(final_dest, origin, rd)

        job["progress"] = f"🎫 Smiles ida {origin}→{ho}..."
        out_l1 = await safe_smiles(origin, ho, od)

        job["progress"] = f"🎫 Smiles ida {ho}→{final_dest}..."
        out_l2 = await safe_smiles(ho, final_dest, od)

        job["progress"] = f"🎫 Smiles vuelta {final_dest}→{hb}..."
        back_l1 = await safe_smiles(final_dest, hb, rd)

        job["progress"] = f"🎫 Smiles vuelta {hb}→{origin}..."
        back_l2 = await safe_smiles(hb, origin, rd)

        smiles_data["legs"] = {
            "out":  {"hub": ho, "leg1": out_l1,  "leg2": out_l2},
            "back": {"hub": hb, "leg1": back_l1, "leg2": back_l2},
        }

    job["status"] = "done"
    job["progress"] = "✅ Completado"
    job["result"] = {
        "origin": origin, "final_dest": final_dest, "year_month": year_month,
        "sample_dates": out_dates,
        "return_dates": ret_dates,
        "min_stay_days": min_stay_days, "max_stay_days": max_stay_days,
        "direct_rt": best_direct_rt,
        "combos": top_combos,
        "smiles": smiles_data,
        "hubs_tried": [{"code": h, **HUBS[h]} for h in HUB_CODES],
        "total_combos_found": len(all_combos),
    }
    log.info(f"Job {job_id} done: {len(top_combos)} combos RT de {len(all_combos)} analizados")


# ═══════════════════════════════════════════════
# Direct search (también paralelo)
# ═══════════════════════════════════════════════
async def run_direct_search(job_id: str, origin: str, dest: str, year_month: str,
                             threshold: int, trip_type: str,
                             min_days: int, max_days: int):
    job = JOBS[job_id]
    job["status"] = "running"

    if trip_type == "1":
        min_days = max(3, min_days)
        max_days = max(min_days, max_days)
        if max_days - min_days <= 2:
            stay_options = [min_days]
        else:
            stay_options = [min_days, (min_days + max_days) // 2, max_days]
        outbound_dates = get_sample_dates_for_month(year_month, [7, 17, 27])
    else:
        stay_options = [None]
        outbound_dates = get_sample_dates_for_month(year_month, [3, 10, 17, 24])

    # Armar todas las búsquedas en paralelo
    tasks = []
    meta = []
    for outbound in outbound_dates:
        for stay in stay_options:
            if stay is not None:
                ret_date = (datetime.strptime(outbound, "%Y-%m-%d") + timedelta(days=stay)).strftime("%Y-%m-%d")
                tasks.append(gf_search(origin, dest, outbound, rt_return=ret_date))
                meta.append({"date": outbound, "return_date": ret_date, "stay": stay})
            else:
                tasks.append(gf_search(origin, dest, outbound))
                meta.append({"date": outbound, "return_date": None, "stay": None})

    total = len(tasks)
    job["progress"] = f"🔍 {total} búsquedas en paralelo..."
    completed = [0]

    async def run_one(i, coro):
        try:
            return await coro
        finally:
            completed[0] += 1
            job["progress"] = f"🔍 {completed[0]}/{total}..."

    all_res = await asyncio.gather(*[run_one(i, t) for i, t in enumerate(tasks)])

    results = []
    for i, flights in enumerate(all_res):
        if not flights:
            continue
        best = min(flights, key=lambda x: x["price"])
        if best["price"] <= threshold:
            best["return_date"] = meta[i]["return_date"]
            best["stay_days"] = meta[i]["stay"]
            results.append(best)

    # Dedupe
    unique = {}
    for r in results:
        key = (r["date"], r.get("return_date") or "")
        if key not in unique or r["price"] < unique[key]["price"]:
            unique[key] = r
    results = sorted(unique.values(), key=lambda r: r["price"])

    job["status"] = "done"
    job["progress"] = "✅ Completado"
    job["result"] = {
        "origin": origin, "dest": dest, "year_month": year_month,
        "threshold": threshold, "trip_type": trip_type,
        "results": results, "count": len(results),
    }


# ═══════════════════════════════════════════════
# HTTP endpoints
# ═══════════════════════════════════════════════
def _auth(request):
    key = request.headers.get("X-Worker-Key") or request.query.get("key")
    return key == WORKER_KEY


async def handle_smiles_search(request: web.Request):
    if not _auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)
    origin = request.query.get("origin", "").upper()
    dest   = request.query.get("dest",   "").upper()
    date   = request.query.get("date",   "")
    days   = int(request.query.get("days", "7"))
    if not (origin and dest and date):
        return web.json_response({"error": "origin, dest, date requeridos"}, status=400)
    try:
        raw, links = await query_smiles_bot(origin, dest, date, days)
        results = parse_smiles_response(raw, links)
        return web.json_response({
            "origin": origin, "dest": dest, "date": date, "days": days,
            "results": results, "raw": raw[:3000],
            "at": datetime.now().isoformat(),
        })
    except FloodWaitError as e:
        return web.json_response({"error": f"Flood wait {e.seconds}s"}, status=429)
    except Exception as e:
        log.exception("Error")
        return web.json_response({"error": str(e)}, status=500)


async def handle_direct_start(request: web.Request):
    if not _auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)
    dest       = request.query.get("dest", "").upper()
    year_month = request.query.get("month", "")
    threshold  = int(request.query.get("threshold", "1500"))
    trip_type  = request.query.get("trip_type", "2")
    min_days   = int(request.query.get("min_days", "14"))
    max_days   = int(request.query.get("max_days", "21"))

    if not dest or not year_month:
        return web.json_response({"error": "dest y month requeridos"}, status=400)

    job_id = str(uuid.uuid4())[:12]
    JOBS[job_id] = {"status": "queued", "progress": "Iniciando...",
                    "result": None, "started_at": datetime.now().isoformat()}
    asyncio.create_task(run_direct_search(
        job_id, "EZE", dest, year_month, threshold, trip_type, min_days, max_days
    ))
    return web.json_response({"job_id": job_id, "status": "queued"})


async def handle_combo_start(request: web.Request):
    if not _auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)
    dest       = request.query.get("dest", "").upper()
    year_month = request.query.get("month", "")
    min_lo     = int(request.query.get("min_layover", "6"))
    max_lo     = int(request.query.get("max_layover", "24"))
    smiles     = request.query.get("smiles", "true").lower() == "true"
    min_days   = int(request.query.get("min_days", "14"))
    max_days   = int(request.query.get("max_days", "21"))

    if not dest or not year_month:
        return web.json_response({"error": "dest y month requeridos"}, status=400)
    if min_days < 1: min_days = 1
    if max_days < min_days: max_days = min_days

    job_id = str(uuid.uuid4())[:12]
    JOBS[job_id] = {"status": "queued", "progress": "Iniciando...",
                    "result": None, "started_at": datetime.now().isoformat()}
    asyncio.create_task(run_combo_search(
        job_id, "EZE", dest, year_month, min_lo, max_lo, smiles, min_days, max_days
    ))
    return web.json_response({"job_id": job_id, "status": "queued"})


async def handle_job_status(request: web.Request):
    if not _auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)
    job_id = request.query.get("job_id", "")
    job = JOBS.get(job_id)
    if not job:
        return web.json_response({"error": "Job no encontrado"}, status=404)
    return web.json_response({
        "job_id":   job_id,
        "status":   job["status"],
        "progress": job["progress"],
        "result":   job["result"] if job["status"] == "done" else None,
    })


async def handle_health(request: web.Request):
    return web.json_response({
        "status": "ok", "connected": client.is_connected(),
        "fast_flights": FAST_FLIGHTS_AVAILABLE,
        "hubs": len(HUB_CODES),
        "concurrent_gf": MAX_CONCURRENT_GF,
        "jobs": len(JOBS),
        "cache_size": len(GF_CACHE),
        "time": datetime.now().isoformat(),
    })


async def handle_root(request: web.Request):
    return web.Response(
        text=f"Smiles Worker OK — {len(HUB_CODES)} hubs, {MAX_CONCURRENT_GF} búsquedas paralelas",
        content_type="text/plain",
    )


async def cleanup_old_jobs():
    while True:
        await asyncio.sleep(600)
        now = datetime.now()
        remove = [jid for jid, job in JOBS.items()
                  if (now - datetime.fromisoformat(job["started_at"])).total_seconds() > 3600]
        for jid in remove:
            JOBS.pop(jid, None)


async def main():
    log.info(f"fast-flights: {FAST_FLIGHTS_AVAILABLE}")
    log.info(f"Hubs: {len(HUB_CODES)} ({', '.join(HUB_CODES)})")
    log.info(f"Paralelismo GF: {MAX_CONCURRENT_GF}")
    log.info("Conectando a Telegram...")
    await client.start()
    me = await client.get_me()
    log.info(f"✅ Conectado como: {me.first_name}")

    app = web.Application()
    app.router.add_get("/",              handle_root)
    app.router.add_get("/health",        handle_health)
    app.router.add_get("/search",        handle_smiles_search)
    app.router.add_get("/direct/start",  handle_direct_start)
    app.router.add_get("/combo/start",   handle_combo_start)
    app.router.add_get("/combo/status",  handle_job_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"🌐 Port {PORT}")
    asyncio.create_task(cleanup_old_jobs())
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
