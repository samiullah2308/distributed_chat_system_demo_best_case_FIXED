"""
ds_node.py - Demo-ready server node for the Distributed Chat System.

This file is intentionally simple and verbose for a university demo.
It demonstrates the required Distributed Systems concepts:

1) Dynamic discovery of hosts:
   Nodes announce themselves via UDP multicast + UDP broadcast.
   No node addresses are hardcoded.

2) Crash fault tolerance:
   The leader sends heartbeat messages through the ring.
   If followers stop receiving heartbeats, they start a new election.

3) Election / Voting exactly as described in the project form:
   Every node has a unique server node ID.
   When the current leader becomes unavailable, the active node with the highest ID becomes leader.
   Election messages are forwarded only to the right neighbor in a logical ring.

4) Simple chat service:
   Only the current leader accepts chat clients.
   Followers reply NOT_LEADER, so clients reconnect to the leader.

5) Basic message replication:
   The leader forwards chat messages through the server ring.
   Followers store the replicated messages in memory.

Run examples:
    python ds_node.py --id 1
    python ds_node.py --id 2
    python ds_node.py --id 3

Only Python standard library is used.
"""

import argparse
import base64
import socket
import struct
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

MULTICAST_GROUP = "239.1.1.1"
DISCOVERY_PORT = 50000

HELLO_INTERVAL = 1.0
NODE_TIMEOUT = 4.0
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 6.0
STATUS_INTERVAL = 4.0

NODE_PORT_BASE = 6000
CLIENT_PORT_BASE = 7000

BIND_HOST = "0.0.0.0"
BROADCAST_ADDRESS = "255.255.255.255"


def now() -> float:
    return time.time()


def get_lan_ip() -> str:
    """Return the LAN IP address used for outgoing traffic."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"
    finally:
        s.close()


def log(node_id: int, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] node {node_id}: {msg}", flush=True)


def safe_b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def safe_unb64(text: str) -> str:
    return base64.b64decode(text.encode("ascii")).decode("utf-8", errors="replace")


class Node:
    def __init__(self, node_id: int, host: Optional[str] = None):
        self.node_id = node_id
        self.host = host or get_lan_ip()
        self.node_port = NODE_PORT_BASE + node_id
        self.client_port = CLIENT_PORT_BASE + node_id

        self.nodes_lock = threading.Lock()
        self.nodes: Dict[int, Dict[str, object]] = {}

        self.state_lock = threading.Lock()
        self.leader_id: Optional[int] = None
        self.participant = False
        self.last_heartbeat = now()
        self.heartbeat_seq = 0

        self.clients_lock = threading.Lock()
        self.clients: List[socket.socket] = []

        self.history_lock = threading.Lock()
        self.chat_history: List[Tuple[str, str]] = []
        self.seen_replica_ids = set()
        self.message_counter = 0

        self.running = True

        self._register_self()

    def _register_self(self) -> None:
        with self.nodes_lock:
            self.nodes[self.node_id] = {
                "host": self.host,
                "node_port": self.node_port,
                "client_port": self.client_port,
                "last": now(),
            }

    def role(self) -> str:
        with self.state_lock:
            return "LEADER" if self.leader_id == self.node_id else "FOLLOWER"

    def is_leader(self) -> bool:
        with self.state_lock:
            return self.leader_id == self.node_id

    def active_node_ids(self) -> List[int]:
        cutoff = now() - NODE_TIMEOUT
        with self.nodes_lock:
            result = []
            for nid, info in self.nodes.items():
                if nid == self.node_id or float(info["last"]) >= cutoff:
                    result.append(nid)
        return sorted(set(result))

    def active_ring_string(self) -> str:
        return " -> ".join(str(x) for x in self.active_node_ids())

    def right_neighbor_id(self) -> Optional[int]:
        ids = self.active_node_ids()
        if len(ids) <= 1 or self.node_id not in ids:
            return None
        idx = ids.index(self.node_id)
        return ids[(idx + 1) % len(ids)]

    def mark_node_dead(self, nid: int) -> None:
        if nid == self.node_id:
            return
        with self.nodes_lock:
            if nid in self.nodes:
                self.nodes[nid]["last"] = 0.0

    def send_to_node(self, nid: int, line: str) -> bool:
        with self.nodes_lock:
            info = self.nodes.get(nid)
        if not info:
            return False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.5)
            s.connect((str(info["host"]), int(info["node_port"])))
            s.sendall((line + "\n").encode("utf-8"))
            s.close()
            return True
        except OSError:
            self.mark_node_dead(nid)
            return False

    def send_to_right(self, line: str) -> bool:
        """Forward one ring message to the next reachable right neighbor."""
        ids = self.active_node_ids()
        if len(ids) <= 1:
            return False
        if self.node_id not in ids:
            return False

        start = ids.index(self.node_id)
        for step in range(1, len(ids)):
            nid = ids[(start + step) % len(ids)]
            if self.send_to_node(nid, line):
                log(self.node_id, f"ring send to node {nid}: {line}")
                return True
            log(self.node_id, f"ring neighbor node {nid} not reachable, skipping")
        return False

    # Dynamic discovery

    def discovery_sender(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        while self.running:
            with self.state_lock:
                leader = self.leader_id if self.leader_id is not None else -1
            # HELLO|node_id|host|node_port|client_port|role|leader_id
            msg = f"HELLO|{self.node_id}|{self.host}|{self.node_port}|{self.client_port}|{self.role()}|{leader}"
            data = msg.encode("utf-8")
            try:
                sock.sendto(data, (MULTICAST_GROUP, DISCOVERY_PORT))
                sock.sendto(data, (BROADCAST_ADDRESS, DISCOVERY_PORT))
            except OSError:
                pass
            time.sleep(HELLO_INTERVAL)
        sock.close()

    def discovery_listener(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.bind(("", DISCOVERY_PORT))
        try:
            mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except OSError:
            pass
        sock.settimeout(1.0)

        while self.running:
            try:
                data, _ = sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                continue

            parts = data.decode("utf-8", errors="ignore").strip().split("|")
            if len(parts) != 7 or parts[0] != "HELLO":
                continue
            try:
                nid = int(parts[1])
                host = parts[2]
                node_port = int(parts[3])
                client_port = int(parts[4])
                announced_leader = int(parts[6])
            except ValueError:
                continue
            if nid == self.node_id:
                continue

            first_time = False
            with self.nodes_lock:
                first_time = nid not in self.nodes
                self.nodes[nid] = {
                    "host": host,
                    "node_port": node_port,
                    "client_port": client_port,
                    "last": now(),
                }

            if first_time:
                log(self.node_id, f"DISCOVERY found node {nid} at {host}:{node_port}; ring now [{self.active_ring_string()}]")

            # Learn leader passively from discovery messages if available.
            if announced_leader > 0:
                with self.state_lock:
                    if self.leader_id is None:
                        self.leader_id = announced_leader
                        self.last_heartbeat = now()

            # If a stronger node appears, run a fresh ring election.
            with self.state_lock:
                current_leader = self.leader_id
            if current_leader is None or nid > current_leader:
                self.start_election(f"new node {nid} discovered")

        sock.close()

    # Node-to-node TCP server

    def node_server(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((BIND_HOST, self.node_port))
        srv.listen(20)
        srv.settimeout(1.0)
        log(self.node_id, f"node TCP port open on {self.host}:{self.node_port}")

        while self.running:
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_node_connection, args=(conn,), daemon=True).start()
        srv.close()

    def handle_node_connection(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(3.0)
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(2048)
                if not chunk:
                    return
                data += chunk
            line = data.decode("utf-8", errors="ignore").strip()
        except OSError:
            return
        finally:
            try:
                conn.close()
            except OSError:
                pass
        self.handle_ring_message(line)

    def handle_ring_message(self, line: str) -> None:
        parts = line.split("|", 3)
        kind = parts[0] if parts else ""

        if kind == "ELECTION" and len(parts) >= 2:
            try:
                candidate = int(parts[1])
            except ValueError:
                return
            self.handle_election(candidate)
            return

        if kind == "LEADER" and len(parts) >= 2:
            try:
                leader = int(parts[1])
            except ValueError:
                return
            self.handle_leader(leader)
            return

        if kind == "HEARTBEAT" and len(parts) >= 3:
            try:
                leader = int(parts[1])
                seq = int(parts[2])
            except ValueError:
                return
            self.handle_heartbeat(leader, seq)
            return

        if kind == "REPLICA":
            self.handle_replica(line)
            return

    # LCR-style ring election

    def start_election(self, reason: str) -> None:
        active = self.active_node_ids()
        if len(active) == 1:
            self.become_leader("single active node")
            return

        with self.state_lock:
            if self.participant:
                return
            self.participant = True
            self.leader_id = None

        log(self.node_id, f"ELECTION started ({reason}); sending own id {self.node_id} to right neighbor")
        ok = self.send_to_right(f"ELECTION|{self.node_id}")
        if not ok:
            self.become_leader("no reachable right neighbor")

    def handle_election(self, candidate: int) -> None:
        log(self.node_id, f"ELECTION received candidate {candidate}")

        if candidate > self.node_id:
            with self.state_lock:
                self.participant = True
                self.leader_id = None
            self.send_to_right(f"ELECTION|{candidate}")
            return

        if candidate < self.node_id:
            with self.state_lock:
                already_participant = self.participant
                if not self.participant:
                    self.participant = True
                    self.leader_id = None
            if already_participant:
                log(self.node_id, f"ELECTION discarded lower candidate {candidate}; already participant")
            else:
                log(self.node_id, f"ELECTION replaces candidate {candidate} with own id {self.node_id}")
                ok = self.send_to_right(f"ELECTION|{self.node_id}")
                if not ok:
                    # Demo robustness: this can happen when a node receives an election
                    # message before its discovery table is fully populated. In that case
                    # the node with the higher id is active and may safely announce itself.
                    self.become_leader("higher id but no reachable right neighbor yet")
            return

        # candidate == own id means the id travelled around the whole ring.
        self.become_leader("own election id returned")
        self.send_to_right(f"LEADER|{self.node_id}")

    def become_leader(self, reason: str) -> None:
        with self.state_lock:
            changed = self.leader_id != self.node_id
            self.leader_id = self.node_id
            self.participant = False
            self.last_heartbeat = now()
        if changed:
            log(self.node_id, f"I AM LEADER ({reason})")

    def handle_leader(self, leader: int) -> None:
        with self.state_lock:
            changed = self.leader_id != leader
            self.leader_id = leader
            self.participant = False
            self.last_heartbeat = now()
        if changed:
            log(self.node_id, f"LEADER announcement: node {leader} is leader")

        if leader != self.node_id:
            self.send_to_right(f"LEADER|{leader}")
        else:
            log(self.node_id, "LEADER announcement completed full ring")

    # Heartbeat and fault tolerance

    def heartbeat_loop(self) -> None:
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            if self.is_leader():
                self.heartbeat_seq += 1
                if len(self.active_node_ids()) > 1:
                    self.send_to_right(f"HEARTBEAT|{self.node_id}|{self.heartbeat_seq}")
                log(self.node_id, f"HEARTBEAT sent through ring seq={self.heartbeat_seq}")
            else:
                with self.state_lock:
                    leader = self.leader_id
                    elapsed = now() - self.last_heartbeat
                if leader is None:
                    self.start_election("no leader known")
                elif elapsed > HEARTBEAT_TIMEOUT:
                    log(self.node_id, f"CRASH DETECTED: no heartbeat from leader {leader} for {elapsed:.1f}s")
                    with self.state_lock:
                        self.leader_id = None
                        self.participant = False
                    self.start_election("leader heartbeat timeout")

    def handle_heartbeat(self, leader: int, seq: int) -> None:
        with self.state_lock:
            self.leader_id = leader
            self.participant = False
            self.last_heartbeat = now()
        log(self.node_id, f"HEARTBEAT received from leader {leader} seq={seq}")

        if leader != self.node_id:
            self.send_to_right(f"HEARTBEAT|{leader}|{seq}")
        else:
            log(self.node_id, f"HEARTBEAT seq={seq} completed full ring")

    # Chat client server

    def client_server(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((BIND_HOST, self.client_port))
        srv.listen(20)
        srv.settimeout(1.0)
        log(self.node_id, f"client TCP port open on {self.host}:{self.client_port}")

        while self.running:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        srv.close()

    def handle_client(self, conn: socket.socket, addr) -> None:
        if not self.is_leader():
            with self.state_lock:
                leader = self.leader_id if self.leader_id is not None else -1
            try:
                conn.sendall(f"NOT_LEADER|{leader}\n".encode("utf-8"))
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
            return

        try:
            conn.sendall(f"WELCOME|{self.node_id}\n".encode("utf-8"))
        except OSError:
            conn.close()
            return

        with self.clients_lock:
            self.clients.append(conn)
        log(self.node_id, f"CLIENT connected from {addr}")

        buf = b""
        try:
            while self.running and self.is_leader():
                chunk = conn.recv(2048)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    text = raw.decode("utf-8", errors="replace").strip()
                    if text:
                        self.accept_chat_message(text)
        except OSError:
            pass
        finally:
            with self.clients_lock:
                if conn in self.clients:
                    self.clients.remove(conn)
            try:
                conn.close()
            except OSError:
                pass
            log(self.node_id, f"CLIENT disconnected from {addr}")

    def accept_chat_message(self, text: str) -> None:
        self.message_counter += 1
        msg_id = f"{self.node_id}-{self.message_counter}"
        with self.history_lock:
            self.chat_history.append((msg_id, text))
            self.chat_history = self.chat_history[-30:]
            self.seen_replica_ids.add(msg_id)

        log(self.node_id, f"CHAT from client: {text}")
        self.broadcast_to_clients(text)

        if len(self.active_node_ids()) > 1:
            self.send_to_right(f"REPLICA|{self.node_id}|{msg_id}|{safe_b64(text)}")

    def broadcast_to_clients(self, text: str) -> None:
        dead = []
        with self.clients_lock:
            for c in self.clients:
                try:
                    c.sendall((text + "\n").encode("utf-8"))
                except OSError:
                    dead.append(c)
            for c in dead:
                if c in self.clients:
                    self.clients.remove(c)

    def handle_replica(self, line: str) -> None:
        # REPLICA|origin_leader_id|message_id|base64_text
        parts = line.split("|", 3)
        if len(parts) != 4:
            return
        try:
            origin_leader = int(parts[1])
        except ValueError:
            return
        msg_id = parts[2]
        try:
            text = safe_unb64(parts[3])
        except Exception:
            return

        if origin_leader == self.node_id:
            log(self.node_id, f"REPLICA {msg_id} completed full ring")
            return

        with self.history_lock:
            is_new = msg_id not in self.seen_replica_ids
            if is_new:
                self.seen_replica_ids.add(msg_id)
                self.chat_history.append((msg_id, text))
                self.chat_history = self.chat_history[-30:]

        if is_new:
            log(self.node_id, f"REPLICA stored message {msg_id}: {text}")
        self.send_to_right(line)

    def status_loop(self) -> None:
        while self.running:
            time.sleep(STATUS_INTERVAL)
            with self.clients_lock:
                client_count = len(self.clients)
            with self.history_lock:
                history_count = len(self.chat_history)
            with self.state_lock:
                leader = self.leader_id if self.leader_id is not None else "?"
            right = self.right_neighbor_id()
            log(
                self.node_id,
                f"STATUS role={self.role()} leader={leader} ring=[{self.active_ring_string()}] right={right} clients={client_count} replicated_messages={history_count}",
            )

    def startup_election_later(self) -> None:
        time.sleep(2.5)
        with self.state_lock:
            leader = self.leader_id
        if leader is None:
            self.start_election("startup")

    def run(self) -> None:
        log(self.node_id, "starting Distributed Chat Server Node")
        log(self.node_id, f"LAN address announced to others: {self.host}")
        log(self.node_id, f"node port={self.node_port}, client port={self.client_port}")

        threads = [
            self.discovery_sender,
            self.discovery_listener,
            self.node_server,
            self.client_server,
            self.heartbeat_loop,
            self.status_loop,
            self.startup_election_later,
        ]
        for target in threads:
            threading.Thread(target=target, daemon=True).start()

        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            log(self.node_id, "stopping node")
            self.running = False
            time.sleep(0.5)


def parse_args():
    parser = argparse.ArgumentParser(description="Distributed Chat System server node")
    parser.add_argument("--id", type=int, required=True, help="Unique node id, e.g. 1, 2, 3")
    parser.add_argument("--host", default=None, help="Optional LAN IP to announce. Usually not needed.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.id <= 0:
        print("ERROR: --id must be a positive integer", file=sys.stderr)
        sys.exit(1)
    Node(args.id, args.host).run()
