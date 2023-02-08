from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
import asyncio

class Action(IntEnum):
    READ = 0,
    WRITE = 1,

@dataclass
class Request:
    action: Action
    key: bytes
    value: Optional[bytes]

    async def write(self, w: asyncio.StreamWriter):
        msg = bytearray()
        msg.append(self.action)
        msg.extend(str(len(self.key)).encode())
        msg.append(0)
        msg.extend(self.key)
        if self.value is not None:
            assert(self.action == Action.WRITE)
            msg.extend(str(len(self.value)).encode())
            msg.append(0)
            msg.extend(self.value)
        else:
            msg.append(0)
        w.write(msg)
        await w.drain()

    @classmethod
    async def read(cls, r: asyncio.StreamReader):
        code = (await r.readexactly(1))[0]
        action = Action(code)
        key_len = await r.readuntil(b'\0')
        assert(len(key_len) > 1)
        key_len = int(key_len[:-1])
        key = bytes(await r.readexactly(key_len))
        value = None
        value_len = await r.readuntil(b'\0')
        if len(value_len) > 1:
            value_len = int(value_len[:-1])
            value = bytes(await r.readexactly(value_len))
        return cls(
            action = action,
            key = key,
            value = value,
        )


class Status(IntEnum):
    OK = 0,
    NOT_LEADER = 1,
    INTERNAL_ERROR = 2,

@dataclass
class Response:
    status: Status
    value: Optional[bytes]

    async def write(self, w: asyncio.StreamWriter):
        msg = bytearray()
        msg.append(self.status)
        if self.status == Status.OK:
           if self.value is not None:
                msg.extend(str(len(self.value)).encode())
                msg.append(0)
                msg.extend(self.value)
           else:
                msg.append(0)
        w.write(msg)
        await w.drain()

    @classmethod
    async def read(cls, r: asyncio.StreamReader):
        code = (await r.readexactly(1))[0]
        status = Status(code)
        value = None
        if status == Status.OK:
            value_len = await r.readuntil(b'\0')
            if len(value_len) > 1:
                value_len = int(value_len[:-1])
                value = bytes(await r.readexactly(value_len))
        return cls(
                status = status,
                value = value,
        )
        