import asyncio
import uuid
from typing import Callable, Optional, Protocol, Set
import json
import logging

logger = logging.getLogger(__name__)

class Describable(Protocol):
    def describe(self) -> dict:
        ...

class DiscoverabilityProtocol(asyncio.DatagramProtocol):
    def __init__(self, discoverable: Describable, group_ids: Optional[Set[str]] = None):
        self.discoverable = discoverable
        self.group_ids = group_ids
    def connection_made(self, transport):
        self.transport = transport
    def datagram_received(self, data, addr):
        try:
            req = json.loads(data)
        except Exception as e:
            logger.exception(f"Couldn't parse discovery request")
            raise e
        group_id = req.get("group_id")
        if (group_id is not None 
            and self.group_ids is not None
            and group_id not in self.group_ids):
            return
        req_id = req.get("req_id")
        info = self.discoverable.describe()
        try:
            msg = json.dumps({"req_id":req_id, "info":info})
            msg = msg.encode()
        except Exception as e:
            logger.exception(f"Couldn't serialize discovery response")
            raise e
        self.transport.sendto(msg, addr)
        #self.transport.close()

class DiscoveryProtocol(asyncio.DatagramProtocol):
        def __init__(self, group_id: Optional[str] = None):
            self.q = asyncio.Queue()
            self.req_id = str(uuid.uuid4())
            self.group_id = group_id
        def connection_made(self, transport):
            self.transport = transport
            req = {}
            req["req_id"] = self.req_id
            if self.group_id is not None:
                req["group_id"] = self.group_id
            self.transport.sendto(json.dumps(req).encode())
        def datagram_received(self, data, addr):
            try:
                response = json.loads(data)
            except Exception as e:
                logger.exception("Couldn't decode discovery response")
                raise e
            req_id = response.get("req_id")
            if req_id != self.req_id:
                return
            host, _ = addr
            self.q.put_nowait((host, response["info"]))


