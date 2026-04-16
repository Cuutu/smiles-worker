# 🎫 Smiles Worker

Worker de Python + Telethon que le habla a **@smileshelperbot** usando tu cuenta personal de Telegram. Expone una API HTTP que tu webapp puede consumir.

---

## 🚀 Setup paso a paso

### PASO 1 — Obtener credenciales de Telegram

1. Andá a **https://my.telegram.org/apps** y logueate con tu número
2. Click en **"API development tools"**
3. Creá una nueva app con cualquier nombre (ej: "smiles-worker")
4. Anotá **`api_id`** (número) y **`api_hash`** (string largo)

### PASO 2 — Generar el SESSION STRING (en tu compu)

Esto se hace **una sola vez** y genera una clave de sesión que Railway va a usar.

```bash
# En tu compu, terminal:
pip install telethon
python generate_session.py
```

Te va a pedir:
- `TG_API_ID` → el número del paso 1
- `TG_API_HASH` → el hash del paso 1
- Tu número de teléfono (con +54 adelante)
- El código que te llega a Telegram
- Si tenés 2FA, tu password

Al final te imprime un **SESSION STRING**. Copialo, lo vamos a usar en el paso 4.

> ⚠️ **GUARDÁ ESE STRING EN SECRETO**. Quien lo tenga puede loguearse como vos.

### PASO 3 — Crear el repo en GitHub

1. En GitHub, creá un repo **privado** llamado `smiles-worker`
2. Subí estos 4 archivos al repo:
   - `worker.py`
   - `requirements.txt`
   - `railway.json`
   - `README.md`

### PASO 4 — Deploy en Railway

1. Andá a **https://railway.app** y creá cuenta con GitHub
2. Click en **"New Project" → "Deploy from GitHub repo"**
3. Seleccioná el repo `smiles-worker`
4. Una vez creado, andá a **Variables** y agregá:

| Variable | Valor |
|----------|-------|
| `TG_API_ID` | el `api_id` del paso 1 |
| `TG_API_HASH` | el `api_hash` del paso 1 |
| `TG_SESSION_STRING` | el SESSION STRING del paso 2 |
| `WORKER_SECRET` | una clave random tuya (ej: `xK9p2mQ8vL4nR7`) |
| `SMILES_BOT` | `smileshelperbot` (opcional, es el default) |

5. En **Settings → Networking**, click en **"Generate Domain"** para obtener una URL pública tipo `smiles-worker-production.up.railway.app`

6. **Anotá esa URL**, la vamos a usar en Vercel.

### PASO 5 — Probar que funciona

Abrí en el navegador:

```
https://TU-URL.railway.app/search?origin=EZE&dest=NRT&date=2026-07-15&days=7&key=TU_WORKER_SECRET
```

Si todo anda bien, te devuelve un JSON con los resultados de Smiles 🎉

> ⚠️ Tiene rate limit de 30 segundos entre búsquedas para no spammear al bot.

---

## 🆘 Problemas comunes

**"Unauthorized"** → el `WORKER_SECRET` no coincide con el parámetro `key`

**"Timeout esperando respuesta"** → el bot puede estar lento, esperá unos minutos

**Railway dice "Crashed"** → mirá los logs, probablemente el SESSION STRING está mal pegado

**Mi cuenta de Telegram fue desconectada** → eso significa que Telegram te detectó uso sospechoso. Bajá la frecuencia, esperá 24hs y volvé a generar el session string.

---

## 💰 Costos

Railway tiene un plan gratuito con **$5 USD de crédito/mes**. Este worker consume aprox **$0.50-1 USD/mes**, así que gratis.
