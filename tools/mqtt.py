"""A small MQTT 3.1.1 client: connect, and publish at QoS 1.

What the riders' devices need from a broker and nothing more, in the standard
library like every tool here. publish() returns once the broker has
acknowledged the message, so a feeder that has published a report knows the
broker holds it.
"""

import socket
import struct


def _string(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def _length(n):
    out = bytearray()
    while True:
        digit, n = n % 128, n // 128
        out.append(digit | (0x80 if n else 0))
        if not n:
            return bytes(out)


class Client:
    def __init__(self, host, port, client_id, keepalive=120):
        self.sock = socket.create_connection((host, port), timeout=30)
        self.next_id = 1
        # A clean session: a device keeps nothing at the broker but the
        # retained status it publishes.
        self._send(0x10, _string("MQTT") + bytes([4, 0x02]) + struct.pack("!H", keepalive)
                   + _string(client_id))
        kind, body = self._read()
        if kind != 0x20 or len(body) < 2 or body[1] != 0:
            raise ConnectionError(f"the broker refused the connection: {body!r}")

    def publish(self, topic, payload, retain=False):
        """Publish at QoS 1 and wait for the broker's acknowledgement."""
        pid = self.next_id
        self.next_id = self.next_id % 65535 + 1
        if isinstance(payload, str):
            payload = payload.encode()
        self._send(0x32 | (1 if retain else 0), _string(topic) + struct.pack("!H", pid) + payload)
        while True:
            kind, body = self._read()
            if kind == 0x40 and struct.unpack("!H", body[:2])[0] == pid:
                return

    def close(self):
        try:
            self._send(0xE0, b"")
        finally:
            self.sock.close()

    def _send(self, header, body):
        self.sock.sendall(bytes([header]) + _length(len(body)) + body)

    def _read(self):
        first = self._exact(1)[0]
        n, shift = 0, 0
        while True:
            b = self._exact(1)[0]
            n |= (b & 0x7F) << shift
            if not b & 0x80:
                break
            shift += 7
        return first & 0xF0, self._exact(n)

    def _exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("the broker closed the connection")
            buf += chunk
        return buf
