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
                prev_node = self._get_node_at(host, info["port"])
                if prev_node is not None:
                    _, prev_info = prev_node
                    if prev_info["id"] != info["id"] or prev_info["is_leader"] != info["is_leader"]:
                        logger.info(f"replacing node {json.dumps(prev_info)} at {host}:{info['port']} with {json.dumps(info)}")
                        self._retire(prev_info["id"])
                else:
                    logger.info(f"Node {json.dumps(info)} newly discovered")

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
        logger.debug(f"issueing request to leader: {info['is_leader']}")
        try:
            r, w = await asyncio.open_connection(host, info["port"])
        except Exception as e:
            # no one's listening
            logger.error(f"Node {info['id']} is no longer available at {host}:{info['port']}")
            self._retire(info["id"])
            # TODO: try again with another node?
            return await self._issue_req(req, self._get_node_for_request(req))
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
            del self.followers[id]
        elif self.leader is not None:
            _, info = self.leader
            if info["id"] == id:
                self.leader = None

    def _get_node_at(self, host, port):
        if self.leader is not None:
            h, info = self.leader
            if h == host and info["port"] == port:
                return self.leader
        for (h, info) in self.followers.values():
            if h == host and info["port"] == port:
                return (h, info)
        return None
    
    def _get_node_for_request(self, req: protocol.Request, latest=False):
        if req.action == protocol.Action.READ:
            return self.leader if latest or len(self.followers) == 0 else random.choice(list(self.followers.values()))
        elif req.action == protocol.Action.WRITE:
            return self.leader
        else:
            raise(Exception("Request Action was undefined"))