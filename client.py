import asyncio
import json
import protocol
import middleware
import uuid
import logging
import random
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

Node = Tuple[str, Dict]

class Client:
    def __init__(self, discovery_host="255.255.255.255", discovery_port=1338):
        self.discovery_host = discovery_host
        self.discovery_port = discovery_port
        self.leader: Optional[Node] = None
        self.followers: Dict[str, Node] = {}
        self._t = asyncio.create_task(self.discover_forever(), name="discovery")
    
    async def discover_forever(self, interval = 10.0):
        while True:
            disco_task = asyncio.create_task(self.discover())
            await asyncio.sleep(interval)
            disco_task.cancel()
            try:
                await disco_task
            except asyncio.CancelledError:
                pass

    async def discover(self):
        loop = asyncio.get_running_loop()
        tr, pr = await loop.create_datagram_endpoint(
            lambda: middleware.DiscoveryProtocol(),
            remote_addr=(self.discovery_host, self.discovery_port),
            allow_broadcast=True
        )
        try:
            while True:
                host, info = await pr.q.get()
                logger.debug(f"Found node {json.dumps(info)} at {host}")
                # TODO: check if there was a different node at this host:port pair previously
                # or maybe just rebuild the cluster from scratch?
                if info["is_leader"]:
                    self.leader = (host, info)
                else:
                    self.followers[info["id"]] = (host, info)
        finally:
            tr.close()

    async def _issue_req(self, req: protocol.Request, node: Optional[Node]):
        if node is None:
            # TODO: make exceptions more specific
            raise Exception("No node available to serve this request")
        host, info = node
        try:

            r, w = await asyncio.open_connection(host, info["port"])
        except ConnectionRefusedError as e:
            # no one's listening
            logger.error(f"Node {node['id']} is no longer available at {host}:{info['port']}")
            self._retire(node["id"])
            # TODO: try again with another node?
            raise(e)
        await req.write(w)
        resp = await protocol.Response.read(r)
        if resp.status == protocol.Status.OK:
            return resp.value # might be None, that's OK
        elif resp.status == protocol.Status.INTERNAL_ERROR:
            # TODO: mark the node as faulty?
            logger.error(f"Node {info['id']} failed to serve this request")
            raise Exception("Internal node error")
        elif resp.status == protocol.Status.NOT_LEADER:
            logger.error(f"Node {info['id']} is not the leader anymore")
            # TODO: trigger a rediscovery?
            self._retire(info["id"])
            raise Exception("Leader gone")

    async def get(self, key: bytes, latest=False):
        req = protocol.Request(protocol.Action.READ, key, None)
        node = self.leader if latest or len(self.followers) == 0 else random.choice(list(self.followers.values()))
        return await self._issue_req(req, node)

    async def set(self, key: bytes, value: bytes):
        req = protocol.Request(protocol.Action.WRITE, key, value)
        node = self.leader
        return await self._issue_req(req, node)

    def _retire(self, id: str):
        if id in self.followers:
            del self.followers["id"]
        elif self.leader is not None:
            _, info = self.leader
            if info["id"] == id:
                self.leader = None
