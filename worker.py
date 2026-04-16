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

# ── Logging ──
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smiles-worker")

# ── Config ──
API_ID     = int(os.environ["TG_API_ID"])
API_HASH   = os.environ["TG_API_HASH"]
SESSION    = os.environ.get("TG_SESSION", "smiles_session")
BOT_USER   = os.environ.get("SMILES_BOT", "smileshelperbot")
WORKER_KEY = os.environ.get("WORKER_SECRET", "change-me")  # para evitar abuso
PORT       = int(os.environ.get("PORT", "8080"))

# Usa StringSession si hay session string, sino archivo local
from telethon.sessions import StringSession
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION, API_ID, API_HASH)

# ── Búsqueda con rate limiting ──
# Un solo lock para todas las búsquedas (evita spam al bot)
search_lock = asyncio.Lock()
LAST_SEARCH_TS = [0.0]
MIN_INTERVAL   = 30  # segundos entre búsquedas


async def query_smiles_bot(origin: str, dest: str, date: str, days: int = 7) -> str:
    """
    Manda un mensaje a @smileshelperbot y espera la respuesta.
    Formato: "EZE NRT 2026-07-15 d7"
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

        # Enviar mensaje
        sent = await client.send_message(BOT_USER, command)

        # Esperar respuesta (máximo 60s)
        response_text = None

        async def wait_for_reply():
            nonlocal response_text
            future = asyncio.Future()

            @client.on(events.NewMessage(from_users=BOT_USER))
            async def handler(event):
                # Ignorar mensajes "Procesando..." típicos
                msg = event.message.message or ""
                if len(msg) < 50 and ("procesando" in msg.lower() or "buscando" in msg.lower()):
                    return
                if not future.done():
                    future.set_result(msg)

            try:
                response_text = await asyncio.wait_for(future, timeout=60)
            finally:
                client.remove_event_handler(handler)

        try:
            await wait_for_reply()
        except asyncio.TimeoutError:
            log.warning("Timeout esperando respuesta del bot")
            response_text = None

        LAST_SEARCH_TS[0] = asyncio.get_event_loop().time()
        return response_text or ""


def parse_smiles_response(text: str) -> list:
    """
    Parsea la respuesta del bot a una lista estructurada.
    El formato exacto depende del bot, así que hacemos un parseo genérico:
    buscamos líneas con millas + precio + fecha + aerolínea.
    """
    if not text:
        return []

    results = []
    lines = text.split("\n")

    # Heurística: buscar líneas que tengan números de millas + fecha
    # Formato típico del bot: "✈️ 2026-07-15 | 40.000 millas + USD 120 | Aerolíneas Argentinas"
    flight_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2}).*?([\d.,]+)\s*mill?a?s?(?:.*?(\d+)\s*(?:USD|\$))?",
        re.IGNORECASE,
    )

    current_block = []
    for line in lines:
        line = line.strip()
        if not line:
            if current_block:
                block_text = " ".join(current_block)
                match = flight_pattern.search(block_text)
                if match:
                    date    = match.group(1)
                    miles   = int(re.sub(r"[.,]", "", match.group(2)))
                    usd     = int(match.group(3)) if match.group(3) else 0
                    results.append({
                        "date":    date,
                        "miles":   miles,
                        "usd":     usd,
                        "raw":     block_text[:300],
                    })
                current_block = []
            continue
        current_block.append(line)

    # Último bloque
    if current_block:
        block_text = " ".join(current_block)
        match = flight_pattern.search(block_text)
        if match:
            date    = match.group(1)
            miles   = int(re.sub(r"[.,]", "", match.group(2)))
            usd     = int(match.group(3)) if match.group(3) else 0
            results.append({
                "date":  date,
                "miles": miles,
                "usd":   usd,
                "raw":   block_text[:300],
            })

    # Si no parseó nada, devolver el texto crudo como un solo "resultado"
    if not results and text:
        results.append({"date": None, "miles": 0, "usd": 0, "raw": text[:2000]})

    return results


# ── HTTP endpoints ──
async def handle_search(request: web.Request):
    # Auth
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
        text="Smiles Worker OK. Usar /search?origin=EZE&dest=NRT&date=2026-07-15&days=7&key=SECRET",
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

    # Mantener corriendo
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
