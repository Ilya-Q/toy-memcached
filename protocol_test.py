import unittest
import asyncio
import protocol
import os

class RoundtripTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async def echo(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            w.write(await r.read())
            w.close()
            await w.wait_closed()
        self._server = await asyncio.start_unix_server(echo, "test.sock")

    async def _roundtrip(self, obj: protocol.Request | protocol.Response):
        r,w = await asyncio.open_unix_connection("test.sock")
        await obj.write(w)
        w.write_eof()
        got = await obj.read(r)
        w.close()
        await w.wait_closed()
        self.assertEqual(obj, got)
    
    async def test_read_request(self):
        return await self._roundtrip(protocol.Request(
            action=protocol.Action.READ,
            key=b'test',
            value=None,
        ))

    async def test_write_request(self):
        return await self._roundtrip(protocol.Request(
            action=protocol.Action.WRITE,
            key=b'test',
            value=b'test',
        ))

    async def test_empty_read(self):
        return await self._roundtrip(protocol.Request(
            action=protocol.Action.READ,
            key=b'',
            value=None,
        ))

    async def test_empty_write(self):
        return await self._roundtrip(protocol.Request(
            action=protocol.Action.WRITE,
            key=b'',
            value=b'',
        ))

    async def test_delete_request(self):
        return await self._roundtrip(protocol.Request(
            action=protocol.Action.WRITE,
            key=b'test',
            value=None,
        ))

    async def test_ok_response(self):
        return await self._roundtrip(protocol.Response(
            status=protocol.Status.OK,
            value=b'test',
        ))

    async def test_error_response(self):
        return await self._roundtrip(protocol.Response(
            status = protocol.Status.INTERNAL_ERROR,
            value=None
        ))

    async def test_not_leader_response(self):
        return await self._roundtrip(protocol.Response(
            status = protocol.Status.NOT_LEADER,
            value=None
        ))

    async def test_empty_ok(self):
        return await self._roundtrip(protocol.Response(
            status=protocol.Status.OK,
            value=None,
        ))

    async def asyncTearDown(self):
        self._server.close()
        await self._server.wait_closed()

    def tearDown(self):
        os.remove("test.sock")

if __name__ == "__main__":
    unittest.main()
