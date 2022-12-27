import asyncio
from broadcast import start_broadcast_protocol

broadcast_protocol = None

def broadcast_received(data):
    print("woorks" + data)

async def send_test_messages():
    for i in range(3):
        broadcast_protocol.broadcast("test " + str(i))
        await asyncio.sleep(1)
    broadcast_protocol.close()

async def main():
    global broadcast_protocol
    broadcast_protocol, _, _ = await start_broadcast_protocol(
        bind='0.0.0.0', 
        port=9000, 
        broadcast_addr='192.168.178.255', 
        broadcast_port=9000, 
        broadcast_msg='This is a broadcast message',
        data_received=broadcast_received)
    asyncio.create_task(send_test_messages())
    await asyncio.sleep(5)


if __name__ == '__main__':
    asyncio.run(main())

