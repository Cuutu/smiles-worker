"""
Smiles Worker — Railway
- Consulta @smileshelperbot para vuelos con millas
- Busca combinaciones de tramos (split ticket) via fast-flights
- Maneja cola de trabajos asíncrona porque las búsquedas combinadas tardan varios minutos
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

# fast-flights para Google Flights sin API key
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

# ── Rate limiting Smiles ──
smiles_lock = asyncio.Lock()
LAST_SMILES_TS = [0.0]
MIN_SMILES_INTERVAL = 30

# ── Hubs de conexión ──
HUBS = ["SCL", "GRU", "MAD"]

# ── Job storage (en memoria) ──
JOBS = {}  # job_id → {status, progress, result, started_at}


# ═══════════════════════════════════════════════════════════
# Smiles bot (con extracción de links)
# ═══════════════════════════════════════════════════════════

def extract_entity_links(message) -> list:
    links = []
    text = message.message or ""
    entities = message.entities or []
    for ent in entities:
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


# ═══════════════════════════════════════════════════════════
# Google Flights (fast-flights) - wrapped en executor
# ═══════════════════════════════════════════════════════════

def _gf_search_sync(origin, dest, date):
    """Sincrónico, corre en thread executor."""
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
            airline = getattr(f, "name", "") or "?"
            stops_v = getattr(f, "stops", 0)
            if isinstance(stops_v, str):
                sd = "".join(c for c in stops_v if c.isdigit())
                stops_n = int(sd) if sd else (0 if "nonstop" in stops_v.lower() else 1)
            else:
                stops_n = int(stops_v) if stops_v else 0
            duration = getattr(f, "duration", "") or ""
            dep_time = getattr(f, "departure", "") or ""
            arr_time = getattr(f, "arrival", "") or ""
            out.append({
                "origin": origin, "dest": dest, "date": date,
                "price": price, "airline": airline, "stops": stops_n,
                "duration": duration, "dep_time": dep_time, "arr_time": arr_time,
            })
        return sorted(out, key=lambda x: x["price"])
    except Exception as e:
        log.warning(f"fast-flights error {origin}→{dest} {date}: {e}")
        return []


async def gf_search(origin, dest, date):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _gf_search_sync, origin, dest, date)


# ═══════════════════════════════════════════════════════════
# Búsqueda combinada (split ticket)
# ═══════════════════════════════════════════════════════════

def parse_time_str(s: str) -> int:
    """Convierte '10:30 AM' o '14:25' a minutos del día. Devuelve -1 si no puede."""
    s = (s or "").strip().upper()
    m = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", s)
    if not m:
        return -1
    h = int(m.group(1))
    mn = int(m.group(2))
    ap = m.group(3)
    if ap == "PM" and h != 12:
        h += 12
    elif ap == "AM" and h == 12:
        h = 0
    return h * 60 + mn


def estimate_layover_hours(leg1_date: str, leg1_arr: str,
                             leg2_date: str, leg2_dep: str) -> float:
    """Estima horas entre llegada del tramo 1 y salida del tramo 2."""
    try:
        d1 = datetime.strptime(leg1_date, "%Y-%m-%d")
        d2 = datetime.strptime(leg2_date, "%Y-%m-%d")
    except ValueError:
        return 0.0
    arr_min = parse_time_str(leg1_arr)
    dep_min = parse_time_str(leg2_dep)
    if arr_min < 0 or dep_min < 0:
        # No pudimos parsear, asumimos mínimo día completo si fechas distintas
        return max(24.0 * (d2 - d1).days, 0.0)
    minutes = ((d2 - d1).days * 24 * 60) + (dep_min - arr_min)
    return minutes / 60.0


async def run_combo_search(job_id: str, origin: str, final_dest: str,
                            outbound_date: str, min_layover_h: int, max_layover_h: int,
                            include_smiles: bool):
    """
    Busca tramos EZE→hub + hub→destino para cada hub, combina y filtra
    por tiempo de layover razonable. Opcionalmente consulta Smiles.
    """
    job = JOBS[job_id]
    job["status"] = "running"
    job["progress"] = "Buscando tramos directos..."

    all_combos = []
    total_steps = 1 + len(HUBS) * 2 + (len(HUBS) + 1 if include_smiles else 0)
    step = 0

    def bump(msg):
        nonlocal step
        step += 1
        job["progress"] = f"[{step}/{total_steps}] {msg}"

    # 1. Vuelo directo (benchmark)
    bump(f"Buscando vuelo directo {origin}→{final_dest}")
    direct = await gf_search(origin, final_dest, outbound_date)
    direct_best = min(direct, key=lambda f: f["price"]) if direct else None

    # 2. Por cada hub, buscar los dos tramos
    hub_results = {}
    for hub in HUBS:
        bump(f"Buscando {origin}→{hub}")
        leg1 = await gf_search(origin, hub, outbound_date)

        # Para el segundo tramo, probar llegada el mismo día y al día siguiente
        leg2_all = []
        for offset in [0, 1]:
            next_date = (datetime.strptime(outbound_date, "%Y-%m-%d")
                         + timedelta(days=offset)).strftime("%Y-%m-%d")
            bump(f"Buscando {hub}→{final_dest} el {next_date}")
            leg2 = await gf_search(hub, final_dest, next_date)
            leg2_all.extend(leg2)

        hub_results[hub] = {"leg1": leg1, "leg2": leg2_all}

    # 3. Combinar
    job["progress"] = "Combinando tramos..."
    for hub, legs in hub_results.items():
        for l1 in legs["leg1"][:5]:  # top 5 más baratos de cada pierna
            for l2 in legs["leg2"][:5]:
                layover_h = estimate_layover_hours(
                    l1["date"], l1.get("arr_time", ""),
                    l2["date"], l2.get("dep_time", ""),
                )
                if layover_h < min_layover_h or layover_h > max_layover_h:
                    continue
                total_price = l1["price"] + l2["price"]
                all_combos.append({
                    "hub": hub,
                    "leg1": l1,
                    "leg2": l2,
                    "total_price": total_price,
                    "layover_hours": round(layover_h, 1),
                })

    all_combos.sort(key=lambda c: c["total_price"])
    top_combos = all_combos[:6]  # top 6 para no inflar respuesta

    # 4. Smiles (opcional)
    smiles_data = {"direct": None, "legs": {}}
    if include_smiles:
        job["progress"] = "Consultando Smiles vuelo directo..."
        try:
            raw, links = await query_smiles_bot(origin, final_dest, outbound_date, 7)
            smiles_data["direct"] = parse_smiles_response(raw, links)[:3]
        except Exception as e:
            log.warning(f"Smiles direct error: {e}")

        for hub in HUBS:
            # Solo consultamos Smiles si hay combo viable con ese hub
            if not any(c["hub"] == hub for c in top_combos):
                continue
            job["progress"] = f"Consultando Smiles {origin}→{hub}..."
            try:
                raw, links = await query_smiles_bot(origin, hub, outbound_date, 7)
                leg1_smiles = parse_smiles_response(raw, links)[:3]
            except Exception as e:
                leg1_smiles = []
                log.warning(f"Smiles {origin}→{hub}: {e}")

            job["progress"] = f"Consultando Smiles {hub}→{final_dest}..."
            try:
                raw, links = await query_smiles_bot(hub, final_dest, outbound_date, 7)
                leg2_smiles = parse_smiles_response(raw, links)[:3]
            except Exception as e:
                leg2_smiles = []
                log.warning(f"Smiles {hub}→{final_dest}: {e}")

            smiles_data["legs"][hub] = {
                "leg1": leg1_smiles,
                "leg2": leg2_smiles,
            }

    # 5. Resultado final
    job["status"] = "done"
    job["progress"] = "✅ Completado"
    job["result"] = {
        "origin": origin,
        "final_dest": final_dest,
        "outbound_date": outbound_date,
        "direct": direct_best,
        "direct_all": direct[:5],
        "combos": top_combos,
        "smiles": smiles_data,
        "hubs_tried": HUBS,
        "total_combos_found": len(all_combos),
    }
    log.info(f"Job {job_id} completado: {len(top_combos)} combos")


# ═══════════════════════════════════════════════════════════
# HTTP endpoints
# ═══════════════════════════════════════════════════════════

def _auth(request):
    key = request.headers.get("X-Worker-Key") or request.query.get("key")
    return key == WORKER_KEY


async def handle_search(request: web.Request):
    """Búsqueda Smiles directa (como antes)."""
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


async def handle_combo_start(request: web.Request):
    """Inicia búsqueda combinada, devuelve job_id para polling."""
    if not _auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    origin     = request.query.get("origin", "EZE").upper()
    final_dest = request.query.get("dest", "").upper()
    date       = request.query.get("date", "")
    min_layover = int(request.query.get("min_layover", "6"))
    max_layover = int(request.query.get("max_layover", "24"))
    include_smiles = request.query.get("smiles", "true").lower() == "true"

    if not final_dest or not date:
        return web.json_response({"error": "dest y date requeridos"}, status=400)

    job_id = str(uuid.uuid4())[:12]
    JOBS[job_id] = {
        "status": "queued", "progress": "Iniciando...",
        "result": None, "started_at": datetime.now().isoformat(),
    }

    # Arrancar en background
    asyncio.create_task(run_combo_search(
        job_id, origin, final_dest, date, min_layover, max_layover, include_smiles
    ))

    return web.json_response({"job_id": job_id, "status": "queued"})


async def handle_combo_status(request: web.Request):
    """Consultar estado de un job."""
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
        "status":    "ok",
        "connected": client.is_connected(),
        "fast_flights": FAST_FLIGHTS_AVAILABLE,
        "hubs":      HUBS,
        "jobs":      len(JOBS),
        "time":      datetime.now().isoformat(),
    })


async def handle_root(request: web.Request):
    return web.Response(text="Smiles Worker OK", content_type="text/plain")


async def cleanup_old_jobs():
    """Limpia jobs viejos cada 10 min."""
    while True:
        await asyncio.sleep(600)
        now = datetime.now()
        to_remove = []
        for jid, job in JOBS.items():
            try:
                started = datetime.fromisoformat(job["started_at"])
                if (now - started).total_seconds() > 3600:  # 1 hora
                    to_remove.append(jid)
            except Exception:
                pass
        for jid in to_remove:
            JOBS.pop(jid, None)
        log.info(f"Cleanup: removidos {len(to_remove)} jobs viejos")


async def main():
    log.info(f"fast-flights disponible: {FAST_FLIGHTS_AVAILABLE}")
    log.info("Conectando a Telegram...")
    await client.start()
    me = await client.get_me()
    log.info(f"✅ Conectado como: {me.first_name}")

    app = web.Application()
    app.router.add_get("/",             handle_root)
    app.router.add_get("/health",       handle_health)
    app.router.add_get("/search",       handle_search)
    app.router.add_get("/combo/start",  handle_combo_start)
    app.router.add_get("/combo/status", handle_combo_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"🌐 Escuchando en puerto {PORT}")

    asyncio.create_task(cleanup_old_jobs())
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
