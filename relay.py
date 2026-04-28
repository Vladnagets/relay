"""
relay.py — WebSocket Relay Server
Поддержка: авторизация по токену, комнаты, graceful reconnect
Запуск:    python relay.py
Env vars:  PORT=8765  AUTH_TOKEN=changeme123
"""

import asyncio
import websockets
import json
import os
import hashlib
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("relay")

AUTH_TOKEN  = os.environ.get("AUTH_TOKEN", "changeme123")
TOKEN_HASH  = hashlib.sha256(AUTH_TOKEN.encode()).hexdigest()
PORT        = int(os.environ.get("PORT", 8765))

# ── Хранилище комнат ──────────────────────────────────────────────────────────
# rooms[room_name] = {"agent": ws, "client": ws}
rooms: dict[str, dict] = {}

# ── Утилиты ───────────────────────────────────────────────────────────────────
async def safe_send(ws, data: dict | str):
    try:
        msg = json.dumps(data) if isinstance(data, dict) else data
        await ws.send(msg)
    except Exception:
        pass

def get_room(name: str) -> dict:
    if name not in rooms:
        rooms[name] = {"agent": None, "client": None}
    return rooms[name]

def peer_role(role: str) -> str:
    return "client" if role == "agent" else "agent"

# ── Обработчик подключения ────────────────────────────────────────────────────
async def handler(websocket):
    ip = websocket.remote_address[0]

    # ── Первое сообщение: рукопожатие ────────────────────────────────────────
    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=15)
    except asyncio.TimeoutError:
        log.warning(f"[{ip}] Таймаут рукопожатия")
        await safe_send(websocket, {"status": "error", "reason": "handshake timeout"})
        return

    # Поддержка старого формата (просто строка "agent"/"client")
    try:
        hello = json.loads(raw)
        role  = hello.get("role", "")
        token = hello.get("token", "")
        room  = hello.get("room", "default")
    except (json.JSONDecodeError, AttributeError):
        # Устаревший формат без JSON
        role  = raw.strip()
        token = TOKEN_HASH   # auto-auth для старых клиентов
        room  = "default"

    # Нормализация
    role = role.lower()
    room = (room or "default").strip()[:64]

    if role not in ("agent", "client"):
        await safe_send(websocket, {"status": "error", "reason": "unknown role"})
        return

    # ── Проверка токена ───────────────────────────────────────────────────────
    if token != TOKEN_HASH:
        log.warning(f"[{ip}] Неверный токен для комнаты '{room}' role={role}")
        await safe_send(websocket, {"status": "error", "reason": "invalid token"})
        return

    # ── Регистрация в комнате ─────────────────────────────────────────────────
    r = get_room(room)

    # Если предыдущее соединение в этой роли ещё висит — закрываем
    if r[role] is not None and r[role] is not websocket:
        old_ws = r[role]
        try:
            await old_ws.close(1001, "replaced by new connection")
        except Exception:
            pass

    r[role] = websocket
    log.info(f"[{ip}] +{role.upper()} → комната '{room}'  (всего комнат: {len(rooms)})")

    await safe_send(websocket, {"status": "ok", "room": room, "role": role})

    # Уведомляем пир о появлении нас
    peer = r[peer_role(role)]
    if peer:
        await safe_send(peer, {"type": "peer_connected", "role": role})

    # ── Проксирование сообщений ───────────────────────────────────────────────
    try:
        async for message in websocket:
            target_role = peer_role(role)
            target_ws   = r.get(target_role)

            if target_ws is None:
                # Пир ещё не подключён — просто дропаем кадр (не блокируем)
                continue

            try:
                await target_ws.send(message)
            except Exception:
                r[target_role] = None   # пир отвалился
                log.info(f"[relay] Пир {target_role.upper()} в '{room}' отвалился")

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"[{ip}] Ошибка: {e}")
    finally:
        # Очистка
        if r.get(role) is websocket:
            r[role] = None
        log.info(f"[{ip}] -{role.upper()} ← комната '{room}'")

        # Уведомляем пира об отключении
        peer = r.get(peer_role(role))
        if peer:
            await safe_send(peer, {"type": "peer_disconnected", "role": role})

        # Удаляем пустую комнату
        if r["agent"] is None and r["client"] is None:
            rooms.pop(room, None)
            log.info(f"[relay] Комната '{room}' закрыта")

# ── Точка входа ───────────────────────────────────────────────────────────────
async def main():
    log.info(f"Relay-сервер запущен на порту {PORT}")
    log.info(f"Токен: {'(из ENV)' if 'AUTH_TOKEN' in os.environ else AUTH_TOKEN}")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=30,
        max_size=50 * 1024 * 1024,   # 50 МБ — для больших файлов
    ):
        log.info("Relay готов к приёму соединений. Ctrl+C для остановки.")
        await asyncio.Future()   # работаем вечно

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Relay остановлен")
