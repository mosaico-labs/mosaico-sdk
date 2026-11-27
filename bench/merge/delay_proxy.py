import asyncio
import sys
import time

LISTEN, TARGET, DELAY_MS = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
DELAY = DELAY_MS / 2000


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    queue: asyncio.Queue[tuple[float, bytes]] = asyncio.Queue()

    async def send() -> None:
        while True:
            due, chunk = await queue.get()
            if not chunk:
                break
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            writer.write(chunk)
            await writer.drain()
        writer.close()

    sender = asyncio.create_task(send())
    try:
        while True:
            chunk = await reader.read(1 << 20)
            queue.put_nowait((time.monotonic() + DELAY, chunk))
            if not chunk:
                break
    except ConnectionError:
        queue.put_nowait((0.0, b""))
    await sender


async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    server_reader, server_writer = await asyncio.open_connection("127.0.0.1", TARGET)
    await asyncio.gather(
        pipe(client_reader, server_writer), pipe(server_reader, client_writer), return_exceptions=True
    )


async def main() -> None:
    server = await asyncio.start_server(handle, "127.0.0.1", LISTEN)
    print("ready", flush=True)
    async with server:
        await server.serve_forever()


asyncio.run(main())
