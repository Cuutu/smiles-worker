"""
Smiles Worker — Railway
Expone una API HTTP que recibe búsquedas y las consulta al bot @smileshelperbot
usando tu cuenta personal via Telethon.
"""

import os
import re
import asyncio
import logging
from datetime import datetime
from aiohttp import web
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# ── Logging ──
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

# ── Rate limiting ──
search_lock = asyncio.Lock()
LAST_SEARCH_TS = [0.0]
MIN_INTERVAL   = 30


async def query_smiles_bot(origin: str, dest: str, date: str, days: int = 7) -> str:
    """
    Manda un mensaje al bot y espera todos los mensajes de respuesta.
    El bot suele mandar primero "Buscando..." y después el resultado (puede ser 1 o varios).
    """
    async with search_lock:
        # Rate limit
        now = asyncio.get_event_loop().time()
        elapsed = now - LAST_SEARCH_TS[0]
        if elapsed < MIN_INTERVAL:
            wait = MIN_INTERVAL - elapsed
            log.info(f"Rate limit: esperando {wait:.1f}s")
            await asyncio.sleep(wait)

        command = f"{origin} {dest} {date} d{days}"
        log.info(f"→ Enviando a @{BOT_USER}: {command}")

        collected = []
        got_real_message = asyncio.Event()

        @client.on(events.NewMessage(from_users=BOT_USER))
        async def handler(event):
            msg = event.message.message or ""
            # Ignorar "Buscando las mejores ofertas..." u otros de estado
            is_status = len(msg) < 80 and (
                "buscando" in msg.lower() or
                "procesando" in msg.lower() or
                "espera" in msg.lower()
            )
            if is_status:
                log.info(f"  [status] {msg[:60]}")
                return
            collected.append(msg)
            got_real_message.set()
            log.info(f"  ← mensaje real ({len(msg)} chars)")

        try:
            await client.send_message(BOT_USER, command)

            # Esperar primer mensaje real (máx 60s)
            try:
                await asyncio.wait_for(got_real_message.wait(), timeout=60)
            except asyncio.TimeoutError:
                log.warning("Timeout esperando primera respuesta")
                return ""

            # Esperar 3 segundos más por si hay más mensajes
            await asyncio.sleep(3)

        finally:
            client.remove_event_handler(handler)
            LAST_SEARCH_TS[0] = asyncio.get_event_loop().time()

        return "\n\n".join(collected)


def parse_smiles_response(text: str) -> list:
    """
    Parsea el formato real de @smileshelperbot:

    ✈️07/10: 107500 + 63K/$163K Avianca,ECO,1 escalas,🕐20hs,💺4👣1
    ✈️11/10: 111000 + 63K/$163K Avianca,ECO,1 escalas,🕐19hs,💺9👣1

    Devuelve lista estructurada.
    """
    if not text:
        return []

    results = []

    # Regex para cada línea de vuelo
    # ✈️DD/MM: MILLAS + ARSK/$USDK AEROLÍNEA,CLASE,N escalas,🕐Xhs,...
    pattern = re.compile(
        r"✈️?\s*"                           # avión (opcional el emoji)
        r"(\d{1,2})/(\d{1,2})"              # DD/MM
        r".*?:\s*"                          # : después de fecha
        r"([\d.]+)"                         # millas (puede tener puntos)
        r"\s*\+\s*"                         # +
        r"(\d+)K"                           # ARS en K
        r"/\$?([\d.]+K?)"                   # USD o valor alternativo
        r"\s*"
        r"([A-Za-zÁ-ú\s]+?)"                # aerolínea
        r"\s*,\s*"
        r"([A-Z]+)"                         # clase (ECO, EJE, etc)
        r"\s*,\s*"
        r"(\d+)\s*escala",                  # N escalas
        re.IGNORECASE,
    )

    # Intentar extraer año del texto (si el bot lo pone en el header "EZE MIA 2026-10-07")
    year_match = re.search(r"(\d{4})-\d{2}-\d{2}", text)
    year = int(year_match.group(1)) if year_match else datetime.now().year

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        match = pattern.search(line)
        if not match:
            continue

        day, month, miles_str, ars_k, usd_or_alt, airline, cabin, stops = match.groups()

        # Normalizar millas (pueden venir como "107500" o "107.500")
        miles = int(miles_str.replace(".", "").replace(",", ""))

        # Convertir ARS_K a número real (63K → 63000)
        ars = int(ars_k) * 1000

        # Intentar extraer duración
        duration_match = re.search(r"🕐?\s*(\d+)\s*hs", line)
        duration_hs = int(duration_match.group(1)) if duration_match else None

        # Formatear fecha YYYY-MM-DD
        date_str = f"{year}-{int(month):02d}-{int(day):02d}"

        results.append({
            "date":        date_str,
            "day_month":   f"{int(day):02d}/{int(month):02d}",
            "miles":       miles,
            "ars":         ars,
            "airline":     airline.strip(),
            "cabin":       cabin.strip(),
            "stops":       int(stops),
            "duration_hs": duration_hs,
            "raw":         line[:200],
        })

    # Ordenar por millas (más baratos arriba)
    results.sort(key=lambda r: r["miles"])
    return results


# ── HTTP endpoints ──
async def handle_search(request: web.Request):
    key = request.headers.get("X-Worker-Key") or request.query.get("key")
    if key != WORKER_KEY:
        return web.json_response({"error": "Unauthorized"}, status=401)

    origin = request.query.get("origin", "").upper()
    dest   = request.query.get("dest",   "").upper()
    date   = request.query.get("date",   "")
    days   = int(request.query.get("days", "7"))

    if not (origin and dest and date):
        return web.json_response({"error": "origin, dest, date son obligatorios"}, status=400)

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return web.json_response({"error": "date debe ser YYYY-MM-DD"}, status=400)

    try:
        raw = await query_smiles_bot(origin, dest, date, days)
        results = parse_smiles_response(raw)
        return web.json_response({
            "origin":  origin,
            "dest":    dest,
            "date":    date,
            "days":    days,
            "results": results,
            "raw":     raw[:3000],
            "at":      datetime.now().isoformat(),
        })
    except FloodWaitError as e:
        return web.json_response({"error": f"Flood wait {e.seconds}s"}, status=429)
    except Exception as e:
        log.exception("Error en búsqueda")
        return web.json_response({"error": str(e)}, status=500)


async def handle_health(request: web.Request):
    return web.json_response({
        "status":    "ok",
        "connected": client.is_connected(),
        "time":      datetime.now().isoformat(),
    })


async def handle_root(request: web.Request):
    return web.Response(
        text="Smiles Worker OK. /search?origin=EZE&dest=NRT&date=2026-07-15&days=7&key=SECRET",
        content_type="text/plain",
    )


async def main():
    log.info("Conectando a Telegram...")
    await client.start()
    me = await client.get_me()
    log.info(f"✅ Conectado como: {me.first_name} (@{me.username})")

    app = web.Application()
    app.router.add_get("/",        handle_root)
    app.router.add_get("/health",  handle_health)
    app.router.add_get("/search",  handle_search)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"🌐 HTTP server escuchando en puerto {PORT}")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
