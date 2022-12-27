import asyncio
import socket
import random
from string import ascii_letters
from typing import Tuple, Union, Text


Address = Tuple[str, int]


class BroadcastProtocol(asyncio.DatagramProtocol):
    """
    Basic broadcast protocol.
    """
    def __init__(self, target: Address, message: str, *, loop: asyncio.AbstractEventLoop=None, data_received=None):
        self.message = message  # Broadcast message
        self.target = target  # Broadcast addres + port
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.data_received = data_received # callback to receive the broadcasts

    def connection_made(self, transport: asyncio.transports.DatagramTransport):
        print("Connection made.")
        self.transport = transport
        sock = transport.get_extra_info("socket")  # type: socket.socket
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast(self.message)

    def datagram_received(self, data: Union[bytes, Text], addr: Address):
        if(self.data_received):
            self.data_received(data.decode())
        print("Data received:", data, addr)

    def broadcast(self, message):
        """
        Sends broadcast message.

        Args:
            message -- Message to send via broadcast protocol
        Returns:
            None
        """
        print("Sending broadcast message: ", message)
        self.transport.sendto(message.encode(), self.target)
        #self.loop.call_later(5, self.broadcast, message)

    def close(self):
        self.transport.close()
        print("Connection closed")


async def start_broadcast_protocol(bind, port, broadcast_addr, broadcast_port, broadcast_msg, data_received):
    """
    A coroutine which starts broadcast protocol.

    Args:
        bind -- IP adress of the local machine
        port -- Port number to be opened on the local machine
        broadcast_addr -- Broadcast adress
        broadcast_port -- Broadcast port
        broadcast_msg -- Broadcast message
        data_received -- callback

    Returns:
        -- create_datagram_endpoint coroutine
    """
    loop = asyncio.get_running_loop()
    broadcast = BroadcastProtocol((broadcast_addr, broadcast_port), broadcast_msg, loop=loop, data_received=data_received)
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: broadcast, local_addr=(bind, port), allow_broadcast=True)
    return broadcast


# def main(bind='0.0.0.0', port=9000, broadcast_addr='127.0.0.1', broadcast_port=9999, broadcast_msg='Hello, EU13'):
#     """
#     Test function.
#     """
#     print("Starting broadcast protocol...")
#     loop = asyncio.get_running_loop()
#     coro = start_broadcast_protocol(bind, port, broadcast_addr, broadcast_port, broadcast_msg)
#     transport, _ = loop.run_until_complete(coro)

#     print("Broadcast protocol is running...")
#     try:
#         loop.run_forever()
#     except KeyboardInterrupt:
#         pass
#     finally:
#         print("Closing broadcast protocol...")
#         transport.close()
#         loop.close()


# if __name__ == '__main__':
#     main()