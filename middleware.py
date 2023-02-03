import asyncio
from enum import Enum
import uuid
from typing import Callable, List, Optional, Protocol, Set, Tuple, Dict
import json
import logging

logger = logging.getLogger(__name__)

class Describable(Protocol):
    def describe(self) -> dict:
        ...

# even before the ring is fully built we have to listen for election messages, in this case (neighbour_unknown=True) just store them, another instance will process them
class ElectionProtocol(asyncio.DatagramProtocol):
    @staticmethod
    def from_listener(listener):
        return ElectionProtocol(listener.id, listener.describable, False, False, listener.saved_messages)

    def __init__(self, id:str, describable:Describable, initiator=False, neighbour_unknown=False, saved_messages=[]):
        self.id = id
        self.describable = describable
        self.participating = initiator
        self.leader = None
        self.neighbour_unknown = neighbour_unknown
        self.saved_messages = saved_messages
    
    def get_saved_messages(self) -> List[bytes]:
        if not self.neighbour_unknown:
            raise Exception("neighbour was known so there could not be any messages")
        return self.saved_messages

    def _send_election_message(self, other_id: str):
        id_to_send = None
        if other_id == None or other_id < self.id:
            id_to_send = self.id
        else:
            id_to_send = other_id
        message = json.dumps({"type": "ELECTION","id":id_to_send}).encode()
        if self.neighbour_unknown:
            # we dont know where to send our messages yet, another instance will do it for us
            self.saved_messages.append(message)
        else:
            self.transport.sendto(message)
        self.participating = True

    def connection_made(self, transport) -> None:
        self.transport = transport
        # if our neighbour is known and we were given saved_messages we have to send them out 
        if not self.neighbour_unknown and self.saved_messages != None:
            for message in self.saved_messages:
                self.transport.sendto(message)
            logger.debug(f"sending {len(self.saved_messages)} buffered messsage")
        if self.participating:
            self._send_election_message(None)

    async def get_leader(self) -> str:
        while self.leader == None:
            await asyncio.sleep(0.5)
        return self.leader

    def _handle_win(self, leader):
        leader_host, leader_info = leader
        leader_id = leader_info['id']
        logger.info(f"Node {leader_id} won the election and is now the leader")
        if self.leader == None:
            self.transport.sendto(json.dumps({"type": "VICTORY","id":leader_id, "leader_host":leader_host, "leader_info": leader_info}).encode())
            self.leader = leader

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data)
        except Exception as e:
            logger.exception(f"Couldn't parse election message")
            raise e
        incoming_id = message.get("id")
        messageType = message.get("type")
        if messageType == "ELECTION":
            logger.debug(f"got an incoming election message with id {incoming_id}")
            if incoming_id == self.id:
                # we have won !
                leader_info = self.describable.describe()
                leader_info["is_leader"] = True
                self._handle_win((self.describable.host, leader_info))
            elif not self.participating or incoming_id > self.id:
                self._send_election_message(incoming_id)
        elif messageType == "VICTORY":
            leader_info = message["leader_info"]
            # using the self-reported "0.0.0.0" address leads to errors while replicating so we replace it once when it is seen
            leader_host = message["leader_host"] if message["leader_host"] != "0.0.0.0" else addr[0]
            self._handle_win((leader_host, leader_info))

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


