import asyncio
from broadcast import start_broadcast_protocol

async def main():
    await start_broadcast_protocol(
        bind='127.0.0.1', 
        port=9000, 
        broadcast_addr='192.168.1.255', 
        broadcast_port=10001, 
        broadcast_msg='This is a broadcast message')

if __name__ == '__main__':
    asyncio.run(main())

