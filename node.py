#!/usr/bin/env python

import asyncio
import storage
import logging
import argparse

parser = argparse.ArgumentParser(
    prog = "toy-memcached",
    description= "a simple replicated key-value storage"
)

parser.add_argument('--host', default="0.0.0.0")
parser.add_argument('--port', default="1337", type=int)
parser.add_argument('--leader', action=argparse.BooleanOptionalAction)
parser.add_argument('--discovery_host', default="255.255.255.255")
parser.add_argument('--discovery_port', default="1338", type=int)
parser.add_argument('--loglevel', default="INFO")

args = parser.parse_args()
daemon = storage.StorageNode(
    host = args.host,
    port = args.port,
    discovery_host = args.discovery_host,
    discovery_port = args.discovery_port,
    is_leader = args.leader,
)
logging.basicConfig(level=args.loglevel)
asyncio.run(daemon.start())