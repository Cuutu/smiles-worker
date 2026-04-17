"""
Smiles Worker — Railway
Expone una API HTTP que recibe búsquedas y las consulta al bot @smileshelperbot
usando tu cuenta personal via Telethon.

IMPORTANTE: Este worker extrae los HYPERLINKS embebidos en cada fecha del mensaje
del bot (que son los que apuntan directo a Smiles.com.ar con el vuelo precargado).
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
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

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


def extract_entity_links(message) -> list:
    """
    Extrae todos los links embebidos en un mensaje de Telegram.
    Devuelve lista de dicts con offset, length, url y el texto visible.
    """
    links = []
    text = message.message or ""
    entities = message.entities or []

    for ent in entities:
        if isinstance(ent, MessageEntityTextUrl):
            # Hyperlink con texto visible diferente a la URL (ej "05/07" → smiles.com.ar/...)
            visible_text = text[ent.offset : ent.offset + ent.length]
            links.append({
                "text":   visible_text,
                "url":    ent.url,
                "offset": ent.offset,
            })
        elif isinstance(ent, MessageEntityUrl):
            # URL visible en el texto
            visible_text = text[ent.offset : ent.offset + ent.length]
            links.append({
                "text":   visible_text,
                "url":    visible_text,
                "offset": ent.offset,
            })
    return links


async def query_smiles_bot(origin: str, dest: str, date: str, days: int = 7):
    """
    Manda un mensaje al bot y devuelve:
      - texto plano concatenado
      - lista de links (de todos los mensajes recibidos)
    """
    async with search_lock:
        now = asyncio.get_event_loop().time()
        elapsed = now - LAST_SEARCH_TS[0]
        if elapsed < MIN_INTERVAL:
            wait = MIN_INTERVAL - elapsed
            log.info(f"Rate limit: esperando {wait:.1f}s")
            await asyncio.sleep(wait)

        command = f"{origin} {dest} {date} d{days}"
        log.info(f"→ Enviando a @{BOT_USER}: {command}")

        collected_messages = []  # lista de (texto, links)
        got_real_message = asyncio.Event()

        @client.on(events.NewMessage(from_users=BOT_USER))
        async def handler(event):
            msg = event.message.message or ""
            is_status = len(msg) < 80 and (
                "buscando" in msg.lower() or
                "procesando" in msg.lower() or
                "espera" in msg.lower()
            )
            if is_status:
                log.info(f"  [status] {msg[:60]}")
                return

            links = extract_entity_links(event.message)
            collected_messages.append((msg, links))
            got_real_message.set()
            log.info(f"  ← mensaje real ({len(msg)} chars, {len(links)} links)")

        try:
            await client.send_message(BOT_USER, command)
            try:
                await asyncio.wait_for(got_real_message.wait(), timeout=60)
            except asyncio.TimeoutError:
                log.warning("Timeout esperando primera respuesta")
                return "", []
            # Esperar 3s por si el bot manda más mensajes
            await asyncio.sleep(3)
        finally:
            client.remove_event_handler(handler)
            LAST_SEARCH_TS[0] = asyncio.get_event_loop().time()

        # Concatenar todo
        all_text = "\n\n".join(m[0] for m in collected_messages)
        all_links = []
        for _, links in collected_messages:
            all_links.extend(links)

        return all_text, all_links


def parse_smiles_response(text: str, links: list) -> list:
    """
    Parsea las líneas del formato:
      ✈️07/10: 107500 + 63K/$163K Avianca,ECO,1 escalas,🕐20hs
    Y mapea cada línea al link correspondiente (por la fecha DD/MM).
    """
    if not text:
        return []

    # Armar un índice fecha → link
    # Los links del bot tienen texto visible tipo "07/10" y URL de smiles.com.ar
    date_to_url = {}
    for link in links:
        # El texto visible suele ser DD/MM
        text_clean = link["text"].strip()
        m = re.match(r"^(\d{1,2})/(\d{1,2})$", text_clean)
        if m and "smiles.com.ar" in link["url"].lower():
            key = f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"
            # Si hay múltiples vuelos el mismo día, el bot pone links distintos;
            # guardamos solo el primero que encontremos para esa fecha (el más barato)
            if key not in date_to_url:
                date_to_url[key] = link["url"]

    log.info(f"Links indexados por fecha: {len(date_to_url)}")

    results = []

    pattern = re.compile(
        r"✈️?\s*"
        r"(\d{1,2})/(\d{1,2})"
        r".*?:\s*"
        r"([\d.]+)"
        r"\s*\+\s*"
        r"(\d+)K"
        r"/\$?([\d.]+K?)"
        r"\s*"
        r"([A-Za-zÁ-ú\s]+?)"
        r"\s*,\s*"
        r"([A-Z]+)"
        r"\s*,\s*"
        r"(\d+)\s*escala",
        re.IGNORECASE,
    )

    year_match = re.search(r"(\d{4})-\d{2}-\d{2}", text)
    year = int(year_match.group(1)) if year_match else datetime.now().year

    # Index para manejar múltiples vuelos el mismo día
    date_link_idx = {}  # "DD/MM" → índice actual
    # Re-indexar TODOS los links por fecha (no solo el primero)
    date_to_all_urls = {}
    for link in links:
        text_clean = link["text"].strip()
        m = re.match(r"^(\d{1,2})/(\d{1,2})$", text_clean)
        if m and "smiles.com.ar" in link["url"].lower():
            key = f"{int(m.group(1)):02d}/{int(m.group(2)):02d}"
            date_to_all_urls.setdefault(key, []).append(link["url"])

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        match = pattern.search(line)
        if not match:
            continue

        day, month, miles_str, ars_k, usd_or_alt, airline, cabin, stops = match.groups()

        miles = int(miles_str.replace(".", "").replace(",", ""))
        ars   = int(ars_k) * 1000

        duration_match = re.search(r"🕐?\s*(\d+)\s*hs", line)
        duration_hs = int(duration_match.group(1)) if duration_match else None

        date_str   = f"{year}-{int(month):02d}-{int(day):02d}"
        date_key   = f"{int(day):02d}/{int(month):02d}"

        # Buscar el link correspondiente para esta fecha
        # Si hay varios links la misma fecha, avanzar al siguiente
        urls_for_date = date_to_all_urls.get(date_key, [])
        idx = date_link_idx.get(date_key, 0)
        booking_url = urls_for_date[idx] if idx < len(urls_for_date) else (
            urls_for_date[0] if urls_for_date else None
        )
        date_link_idx[date_key] = idx + 1

        results.append({
            "date":        date_str,
            "day_month":   date_key,
            "miles":       miles,
            "ars":         ars,
            "airline":     airline.strip(),
            "cabin":       cabin.strip(),
            "stops":       int(stops),
            "duration_hs": duration_hs,
            "booking_url": booking_url,
            "raw":         line[:200],
        })

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
        raw, links = await query_smiles_bot(origin, dest, date, days)
        results = parse_smiles_response(raw, links)
        return web.json_response({
            "origin":    origin,
            "dest":      dest,
            "date":      date,
            "days":      days,
            "results":   results,
            "raw":       raw[:3000],
            "links_n":   len(links),
            "at":        datetime.now().isoformat(),
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
