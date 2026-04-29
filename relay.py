import asyncio
import websockets
import os

clients = {}

async def handler(websocket):
    role = await websocket.recv()
    clients[role] = websocket
    print(f"[+] Подключился: {role}")

    try:
        async for message in websocket:
            target = "client" if role == "agent" else "agent"
            if target in clients:
                await clients[target].send(message)
    except:
        pass
    finally:
        clients.pop(role, None)
        print(f"[-] Отключился: {role}")

async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"Relay-сервер запущен на порту {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

asyncio.run(main())
