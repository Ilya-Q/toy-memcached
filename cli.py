#!/usr/bin/env python

import asyncio
import cmd
import threading
import logging
import argparse
import client
import readline

parser = argparse.ArgumentParser(
    prog = "toy-memcached-cli",
    description= "a simple client for toy-memcached"
)

# run the event loop in a background thread
# because the main thread will be runnning the cmdloop
loop = asyncio.new_event_loop()
loop_thread = threading.Thread(target=loop.run_forever, name='event_loop', daemon=True)

async def make_client(args):
    return client.Client(
        discovery_host=args.host,
        discovery_port=args.port
    )

parser.add_argument('--host', default="255.255.255.255")
parser.add_argument('--port', default="1338", type=int)
parser.add_argument('--timeout', type=float, default=3.0)
parser.add_argument('--loglevel', default="INFO")

args = parser.parse_args()
logging.basicConfig(level=args.loglevel)

logger = logging.getLogger(__name__)

loop_thread.start()
f = asyncio.run_coroutine_threadsafe(make_client(args), loop)
c = f.result(args.timeout)

class Cli(cmd.Cmd):
    prompt = "toy-memcached>"
    def do_set(self, arg: str):
        key, value = arg.split(maxsplit=2)
        f = asyncio.run_coroutine_threadsafe(c.set(key.encode(), value.encode()), loop)
        try:
            res = f.result(args.timeout)
            print("OK")
        except Exception:
            logger.exception("Error while setting a key")
    
    def do_get(self, arg: str):
        f = asyncio.run_coroutine_threadsafe(c.get(arg.encode()), loop)
        try:
            res = f.result(args.timeout)
            print(f"OK: {res}")
        except Exception:
            logger.exception("Error while getting a key")

    def do_del(self, arg: str):
        f = asyncio.run_coroutine_threadsafe(c.set(arg.encode(), None), loop)
        try:
            res = f.result(args.timeout)
            print("OK")
        except Exception:
            logger.exception("Error while deleting a key")

    def do_discover(self, arg: str):
        f = asyncio.run_coroutine_threadsafe(c.discover(), loop)
        print("OK")

    def do_cluster(self, arg: str):
        print(f"Leader: {c.leader}")
        print(f"Followers: {c.followers.values()}")

Cli().cmdloop()