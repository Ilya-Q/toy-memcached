import json
import asyncio
import logging
import protocol
import uuid
import middleware
import dataclasses
from collections import ChainMap
from typing import Optional, Dict, Tuple, List

logger = logging.getLogger(__name__)

# TODO: probably a good idea to move all of this groupview business to middleware.py

@dataclasses.dataclass
class ElectionParticipant:
    id: str
    host: str

@dataclasses.dataclass
class Follower:
    id: str
    clock: int
    host: str
    _q: asyncio.Queue = dataclasses.field(default_factory=asyncio.Queue)
    _ready: bool = dataclasses.field(default=False)
    _t: Optional[asyncio.Task] = dataclasses.field(default=None)

class GroupView:
    def __init__(self, clock):
        self.followers: Dict[str, Follower] = {}
        self.max_clock: int = clock # how far ahead is the most up-to-date replica?
        self.min_clock: int = clock # how far behind is the least up-to-date replica?
    def add_candidate(self, id, clock, host):
        f = Follower(
            id,
            clock,
            host,
        )
        self.followers[id] = f
    def drop_follower(self, id):
        f = self.followers[id]
        assert(f._t is None)
        del self.followers[id]
    def replicate(self, msg: protocol.Request):
        self.max_clock += 1
        for f in self.followers.values():
            f._q.put_nowait(msg)

    def update_min_clock(self) -> int:
        self.min_clock = min(f.clock for f in self.followers.values())
        return self.min_clock

    async def do_replication(self, id, stream):
        f = self.followers[id]
        assert(f._t is None and type(f._q) != type("hi"))
        f._t = asyncio.create_task(self._queue_worker(f, stream), name=f"replication:{id}")
        try:
            await f._t
        except Exception as e:
            logger.error(f"Replication to {id} failed: {e}")
            ex = f._t.exception()
            assert(ex is not None)
            logger.error(f"Replication task failed with {ex}")
            r, w = stream 
            w.close()
            f._t = None
            self.drop_follower(id)


    async def _queue_worker(self, f: Follower, stream: Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        r, w = stream
        while True:
            msg: Optional[protocol.Request] = None
            try:
                msg = await asyncio.wait_for(f._q.get(), 3.0) # TODO: configurable heartbeat interval
            except asyncio.TimeoutError:
                # nothing to replicate
                pass
            w.write(b'%b\n' % str(self.update_min_clock()).encode())
            await w.drain()
            if msg is not None:
                next_clock = f.clock + 1
                w.write(b'%b\n' % str(next_clock).encode())
                await w.drain()
                await msg.write(w)
            else:
                w.write(b'\n')
                await w.drain()
            ack_line = await r.readline()
            assert(ack_line == b'ACK\n')
            if msg is not None:
                f.clock = next_clock

        

class StorageNode:
    election_port = 4000
    def __init__(self, *, node_id: Optional[str] = None, is_leader=True, host="0.0.0.0", port=1337, discovery_host="255.255.255.255", discovery_port=1338, replication_port=1444):
        if node_id is None:
            self.id = str(uuid.uuid4())
            logger.warning("UUID generated randomly")
        else:
            self.id = node_id
        self.store: Optional[ChainMap] = None
        self.election_ring : List[ElectionParticipant] = [ElectionParticipant(self.id, host)]
        # TODO: figure out if we should be the leader or not automatically
        self.is_leader: bool = False
        self.host: str = host
        self.port: int = port
        self.replication_port: int = replication_port
        self.discovery_host: str = discovery_host
        self.discovery_port: str = discovery_port
        # unfortunately there's no such thing as a UDP server in asyncio
        # but we still need to keep a reference to the transport and protocol somewhere so they don't get collected
        self.discoverability_pair: Optional[Tuple[asyncio.Transport, asyncio.Protocol]] = None
        self.server: Optional[asyncio.Server] = None
        self.replication_server: Optional[asyncio.Server] = None
        self.replication_t: Optional[asyncio.Task] = None
        self.groupview: Optional[GroupView] = None
        self.clock: Optional[int] = None
        self.follower_replication_stream : Tuple[asyncio.StreamReader, asyncio.StreamWriter] = None
        # these should probably be abstracted away in some way
        self.unstable: Optional[List[protocol.Request]] = None
        self.unstable_clock: Optional[int] = None
        self.first_start = True

    def describe(self):
        return {
            "id": str(self.id),
            "is_leader": self.is_leader,
            "host": self.host,
            "port": self.port,
            "replication_port": self.replication_port,
            "clock": self.clock
        }

    async def _setup_election_listener(self):
        loop = asyncio.get_running_loop()
        return await loop.create_datagram_endpoint(
                    lambda: middleware.ElectionProtocol(self.id,self, True, True, []),
                    local_addr=(self.host, StorageNode.election_port),
                    allow_broadcast=True
                )
    
    # execute LeLann-Chang-Roberts (LCR) algorithm to find our new leader
    async def _hold_election(self, listener, timeout = 10.0):
        # find the neighbour on our right-hand side
        next_node: ElectionParticipant = None
        i = -1
        for i, participant in enumerate(self.election_ring):
            if participant.id == self.id: 
                next_node = self.election_ring[0] if i == len(self.election_ring)-1 else self.election_ring[i+1]
        logger.debug(f"right-hand neighbour is {next_node.id}")
        # we only really need the ring to find our neighbour so it should be reset now for the next election
        self.election_ring : List[ElectionParticipant] = [ElectionParticipant(self.id, self.host)]

        # send and listen for election messages
        loop = asyncio.get_running_loop()
        tr, pr = await loop.create_datagram_endpoint(
                    lambda: middleware.ElectionProtocol.from_listener(listener),
                    remote_addr=(next_node.host, StorageNode.election_port),
                    local_addr=(self.host, StorageNode.election_port),
                    allow_broadcast=True
                )

        t = asyncio.create_task(pr.get_leader())
        try:
            await asyncio.wait_for(t, timeout=timeout)
        except Exception as e:
            logger.warn(f"The election ran into a timeout")
        finally:
            tr.close()
        return t.result()
    

    # TODO: use custom protocols, right now the nodes are flooding each other
    async def _find_leader_and_build_election_ring(self, timeout: float) -> Tuple[str,Dict]:
        loop = asyncio.get_running_loop()
        tr, pr = await loop.create_datagram_endpoint(
                    lambda: middleware.DiscoveryProtocol(describable=self),
                    remote_addr=(self.discovery_host, self.discovery_port),
                    allow_broadcast=True
                )
        
        _, discoverability_pr = self.discoverability_pair
        discoverability_pr.setQueue(pr.q)

        async def wait():
            while True:
                host, info = await pr.q.get()
                if (info["id"] != self.id and len(list(filter(lambda participant: participant.id == info["id"], self.election_ring))) == 0):
                    self.election_ring.append(ElectionParticipant(info["id"], host))
                    logger.info(f"added node {info['id']} to election participants")
                if info.get("is_leader"):
                    logger.info(f"leader is {info['id']}")
                    return host, info
        t = asyncio.create_task(wait())
        leader = None
        try:
            await asyncio.wait_for(t, timeout=timeout)
            leader = t.result()
            logger.info(f"Node with id {leader[0]} is leader")
            self.election_ring = [ElectionParticipant(self.id, self.host)]
        except Exception as e:
            self.election_ring.sort(key=lambda participant: participant.id)
            logger.info(f"No leader found, election ring contains the following participants: {self.election_ring}")
        finally:
            discoverability_pr.setQueue(None)
            tr.close()
        return leader

    async def _start_announcing(self):
        loop = asyncio.get_running_loop()
        self.discoverability_pair = await loop.create_datagram_endpoint(
            lambda: middleware.DiscoverabilityProtocol(describable=self),
            local_addr=(self.host, self.discovery_port),
            allow_broadcast=True
        )
    


    async def start(self):
        logger.info(f"Starting server {self.id} as {'leader' if self.is_leader else 'follower'}")
        if self.discoverability_pair == None:
            await self._start_announcing()
        logger.info(f"Looking for existing leader")
        # listen for election messages, other nodes might start the real process before we do but we must not miss any messages
        listener_transport, listener = await self._setup_election_listener()
        leader = await self._find_leader_and_build_election_ring(5.0)
        listener_transport.close()
        await asyncio.sleep(2)
        if leader is None:
            leader = await self._hold_election(listener)
            if leader is None:
                logger.error("I can not live without a leader...")
                return

        leader_host, leader_info = leader
        self.is_leader = leader_info["id"] == self.id 
        if not self.is_leader:
            self.clock = None
            n_tries = 0
            while n_tries < 7:
                try:
                    self.follower_replication_stream = await asyncio.open_connection(leader_host, leader_info["replication_port"])
                    await self._attach(self.follower_replication_stream)
                    logger.info(f"replicating from {leader_host}:{leader_info['replication_port']} succeeded")
                    break
                except Exception as e:
                    logger.error(f"replicating from {leader_host}:{leader_info['replication_port']} failed")
                    logger.error(f"e: {e}")
                    n_tries += 1
                    await asyncio.sleep(1)

        else:
            if self.first_start:
                self.store = ChainMap()
                self.clock = 0
            self.groupview = GroupView(clock=self.clock)
            self.replication_server = await asyncio.start_server(self._handle_replication, self.host, self.replication_port)
            logger.info(f"Started replication server on {self.host}:{self.replication_port}")
        self.first_start = False
        if self.server == None:
            self.server = await asyncio.start_server(self._handle_req, self.host, self.port)
            logger.info(f"Listening on {self.host}:{self.port}")
            await self.server.serve_forever()

    async def _attach(self, stream: Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        r, w = stream
        w.write(b"%b\n" % json.dumps(self.describe()).encode())
        await w.drain()
        await self._read_snapshot(r)
        self.unstable_clock = self.clock
        self.unstable = []
        w.write(b"ACK\n")
        await w.drain()
        # TODO: detect the failure of the leader by catching an exception from this task
        self.replication_t = asyncio.create_task(self._replicate_from(stream))
        self.replication_t.add_done_callback(self.follower_replication_failed_handler)
    
    async def _replicate_from(self, stream: Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        r, w = stream
        while True:
            line = await r.readline()
            min_clock = int(line)
            clock_diff = min_clock - self.unstable_clock
            assert(clock_diff >= 0)
            if clock_diff > 0:
                logging.debug(f"About to prune unstable messages: {self.unstable}")
            del self.unstable[:clock_diff]
            logging.debug(f"Unstable messages after pruning: {self.unstable}")
            self.unstable_clock = min_clock
            next_clock_line = await r.readline()
            if next_clock_line != b'\n':
                next_clock = int(next_clock_line)
                assert(next_clock <= self.clock + 1)
                req = await protocol.Request.read(r)
                if next_clock <= self.clock:
                    logger.info(f"Dropping duplicate message with clock={next_clock} < our clock {self.clock}")
                else:
                    await self.do_req(req, replication=True)
                    self.unstable.append(req)
            w.write(b'ACK\n')
            await w.drain()

    def follower_replication_failed_handler(self, _):
        logger.warn("replication failed")
        logger.error(self.replication_t.exception())
        r, w = self.follower_replication_stream
        w.close()
        asyncio.create_task(self.start())

    async def _handle_req(self, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        try:
            req = await protocol.Request.read(r)
            resp = await self.do_req(req, replication=False)
        except Exception as e:
            logging.error(f"Error while processing request: {str(e)}")
            resp = protocol.Response(
                status = protocol.Status.INTERNAL_ERROR,
                value = None
            )
        finally:
            await resp.write(w)
            w.close()
            await w.wait_closed()

    async def _read_snapshot(self, r: asyncio.StreamReader):
        assert(not self.is_leader)
        assert(self.clock is None)
        clock_line = await r.readline()
        self.clock = int(clock_line)
        snap_len = int(await r.readline())
        store = ChainMap()
        for i in range(snap_len):
            key_len = int(await r.readline())
            key = await r.readexactly(key_len)
            value_len = int(await r.readline())
            value = await r.readexactly(value_len)
            store[key] = value
        self.store = store

    async def _write_snapshot(self, w: asyncio.StreamWriter):
        snapshot = self.store
        self.store = snapshot.new_child()
        w.write(b'%b\n%b\n' % (
            str(self.clock).encode(),
            str(len(snapshot)).encode()
        ))
        await w.drain()
        for k, v in snapshot.items():
            w.write(b'%b\n%b%b\n%b' % (
                str(len(k)).encode(),
                k,
                str(len(v)).encode(),
                v
            ))
            await w.drain()

    async def _handle_replication(self, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        peer = w.get_extra_info("peername")
        intro_line = await r.readline()
        intro = json.loads(intro_line)
        logger.info(f"New follower request from {peer}: {intro_line}")
        self.groupview.add_candidate(id=intro["id"], host=peer[0], clock=self.clock)
        try:
            await self._write_snapshot(w)
            ack_line = await r.readline()
            assert(ack_line == b'ACK\n')
        except Exception as e:
            logger.exception(f"Error while transmitting snapshot to {intro['id']}")
            self.groupview.drop_follower(intro["id"])
            raise e
        logger.info(f"Snapshot sent successfully, starting replication to {peer}")
        await self.groupview.do_replication(intro["id"], stream=(r,w))

    async def do_req(self, req: protocol.Request, replication: bool):
        resp = protocol.Response(status=protocol.Status.OK, value=None)
        if req.action == protocol.Action.READ:
            resp.value = self.store.get(req.key)
        elif req.action == protocol.Action.WRITE:
            if replication or self.is_leader:
                self.clock += 1
                self.store[req.key] = req.value
                if self.is_leader:
                    self.groupview.replicate(req)
            else:
                return protocol.Response(status=protocol.Status.NOT_LEADER, value=None)
        else:
            raise ValueError(f"Unsupported action {str(req.action)}")
        return resp