import asyncio
import unittest

from apister.brokers.redis_broker import RedisBroker, _Resp


class RedisProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_and_pull_against_fake_server(self) -> None:
        published: list[tuple[bytes, bytes]] = []

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            conn = _Resp(reader, writer)
            try:
                while True:
                    message = await conn.read()
                    if not isinstance(message, list) or not message:
                        break
                    kind = message[0]
                    if kind == b"PUBLISH":
                        published.append((message[1], message[2]))
                        writer.write(b":1\r\n")
                        await writer.drain()
                        continue
                    if kind == b"SUBSCRIBE":
                        channel = message[1]
                        writer.write(_subscribe_reply(b"subscribe", channel, 1))
                        await writer.drain()
                        continue
                    if kind == b"PSUBSCRIBE":
                        pattern = message[1]
                        writer.write(_subscribe_reply(b"psubscribe", pattern, 1))
                        writer.write(_pmessage(pattern, b"news/sports", b"hello"))
                        await writer.drain()
                        continue
            except (asyncio.CancelledError, ConnectionError, OSError, EOFError):
                return
            finally:
                writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        sockets = server.sockets or []
        port = sockets[0].getsockname()[1]
        broker = RedisBroker(f"redis://127.0.0.1:{port}/0")
        try:
            await broker.connect()
            await broker.subscribe(["news/#"])
            await broker.publish("news/sports", b"hello")
            incoming = await asyncio.wait_for(broker.pull(), timeout=2)
            self.assertEqual(incoming.topic, "news/sports")
            self.assertEqual(incoming.payload, b"hello")
            self.assertEqual(published, [(b"news/sports", b"hello")])
        finally:
            await broker.close()
            server.close()
            await server.wait_closed()


def _subscribe_reply(kind: bytes, channel: bytes, count: int) -> bytes:
    return (
        f"*3\r\n${len(kind)}\r\n".encode()
        + kind
        + b"\r\n"
        + f"${len(channel)}\r\n".encode()
        + channel
        + f"\r\n:{count}\r\n".encode()
    )


def _pmessage(pattern: bytes, channel: bytes, payload: bytes) -> bytes:
    return (
        b"*4\r\n$8\r\npmessage\r\n"
        + f"${len(pattern)}\r\n".encode()
        + pattern
        + b"\r\n"
        + f"${len(channel)}\r\n".encode()
        + channel
        + b"\r\n"
        + f"${len(payload)}\r\n".encode()
        + payload
        + b"\r\n"
    )
