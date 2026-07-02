"""
ds_client.py - Demo-ready chat client for the Distributed Chat System.

The client has no hardcoded server address.
It discovers available server nodes via UDP multicast/broadcast.
It tries to connect to the current leader.
If the leader crashes, the TCP connection breaks and the client reconnects.

Run:
    python ds_client.py

Only Python standard library is used.
"""

import queue
import socket
import struct
import threading
import time
from typing import Dict, Optional, Tuple

MULTICAST_GROUP = "239.1.1.1"
DISCOVERY_PORT = 50000
DISCOVERY_LISTEN_TIME = 3.0
RECONNECT_DELAY = 2.0


def discover_nodes(seconds: float = DISCOVERY_LISTEN_TIME) -> Dict[int, Dict[str, object]]:
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

    found: Dict[int, Dict[str, object]] = {}
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            data, _ = sock.recvfrom(2048)
        except socket.timeout:
            continue
        except OSError:
            break
        parts = data.decode("utf-8", errors="ignore").strip().split("|")
        # HELLO|node_id|host|node_port|client_port|role|leader_id
        if len(parts) != 7 or parts[0] != "HELLO":
            continue
        try:
            nid = int(parts[1])
            host = parts[2]
            client_port = int(parts[4])
            role = parts[5]
            leader = int(parts[6])
        except ValueError:
            continue
        found[nid] = {
            "host": host,
            "client_port": client_port,
            "role": role,
            "leader": leader,
        }
    sock.close()
    return found


def connect_to_leader() -> Optional[socket.socket]:
    nodes = discover_nodes()
    if not nodes:
        print("[client] no nodes discovered yet")
        return None

    visible = {
        nid: f"{info['host']}:{info['client_port']} role={info['role']} leader={info['leader']}"
        for nid, info in nodes.items()
    }
    print(f"[client] DISCOVERY found nodes: {visible}", flush=True)

    leader_ids = [int(info["leader"]) for info in nodes.values() if int(info["leader"]) > 0]
    preferred = []
    if leader_ids:
        # Most nodes should announce the same leader. Use the most frequent leader id.
        leader = max(set(leader_ids), key=leader_ids.count)
        if leader in nodes:
            preferred.append(leader)

    # Fallback: try all discovered nodes, highest id first.
    for nid in sorted(nodes.keys(), reverse=True):
        if nid not in preferred:
            preferred.append(nid)

    for nid in preferred:
        info = nodes[nid]
        host = str(info["host"])
        port = int(info["client_port"])
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((host, port))
            data = b""
            while b"\n" not in data:
                chunk = s.recv(256)
                if not chunk:
                    break
                data += chunk
            reply = data.decode("utf-8", errors="replace").strip()
            if reply.startswith("WELCOME"):
                print(f"[client] CONNECTED to leader node {nid} at {host}:{port}", flush=True)
                s.settimeout(None)
                return s
            print(f"[client] node {nid} replied {reply}; trying next node", flush=True)
            s.close()
        except OSError:
            continue
    return None


def receive_loop(sock: socket.socket, stop: Dict[str, bool]) -> None:
    buf = b""
    while not stop["stop"]:
        try:
            chunk = sock.recv(2048)
        except OSError:
            break
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                print(text, flush=True)
    stop["stop"] = True


def stdin_reader(input_queue: "queue.Queue[Optional[str]]", stop: Dict[str, bool]) -> None:
    while not stop["stop"]:
        try:
            line = input()
            input_queue.put(line)
        except EOFError:
            input_queue.put(None)
            break


def main() -> None:
    print("=== Distributed Chat Client ===", flush=True)
    username = ""
    while not username.strip():
        try:
            username = input("Enter your username: ").strip()
        except KeyboardInterrupt:
            print("\n[client] bye")
            return

    print("[client] searching for leader via dynamic discovery...", flush=True)

    while True:
        sock = connect_to_leader()
        if sock is None:
            print(f"[client] no leader available; retrying in {RECONNECT_DELAY}s", flush=True)
            try:
                time.sleep(RECONNECT_DELAY)
            except KeyboardInterrupt:
                print("\n[client] bye")
                return
            continue

        stop = {"stop": False}
        threading.Thread(target=receive_loop, args=(sock, stop), daemon=True).start()
        input_queue: "queue.Queue[Optional[str]]" = queue.Queue()
        threading.Thread(target=stdin_reader, args=(input_queue, stop), daemon=True).start()

        print("[client] type a message and press Enter. Crash the leader to see reconnect.", flush=True)
        try:
            while not stop["stop"]:
                try:
                    line = input_queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                if line is None:
                    stop["stop"] = True
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    sock.sendall(f"{username}: {line}\n".encode("utf-8"))
                except OSError:
                    stop["stop"] = True
                    break
        except KeyboardInterrupt:
            print("\n[client] bye")
            try:
                sock.close()
            except OSError:
                pass
            return

        print("[client] connection lost; rediscovering leader...", flush=True)
        try:
            sock.close()
        except OSError:
            pass
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    main()
