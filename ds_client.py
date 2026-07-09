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

# Netzwerkdaten für die dynamische Suche nach Server-Nodes.
MULTICAST_GROUP = "239.1.1.1"
DISCOVERY_PORT = 50000
DISCOVERY_LISTEN_TIME = 3.0
RECONNECT_DELAY = 2.0


def discover_nodes(seconds: float = DISCOVERY_LISTEN_TIME) -> Dict[int, Dict[str, object]]:
    # Erstellt einen UDP-Socket, der HELLO-Nachrichten der Server empfängt.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass
    sock.bind(("", DISCOVERY_PORT))
    try:
        # Der Client tritt der Multicast-Gruppe bei und kann dadurch Server-Ankündigungen empfangen.
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError:
        pass
    sock.settimeout(1.0)

    # Hier werden alle während des Suchzeitraums gefundenen Nodes gespeichert.
    found: Dict[int, Dict[str, object]] = {}
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            data, _ = sock.recvfrom(2048)
        except socket.timeout:
            continue
        except OSError:
            break

        # Zerlegt die HELLO-Nachricht in ihre einzelnen Felder.
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

        # Speichert nur die Informationen, die der Client für die Verbindung benötigt.
        found[nid] = {
            "host": host,
            "client_port": client_port,
            "role": role,
            "leader": leader,
        }
    sock.close()
    return found


def connect_to_leader() -> Optional[socket.socket]:
    # Startet zuerst die dynamische Suche nach erreichbaren Nodes.
    nodes = discover_nodes()
    if not nodes:
        print("[client] no nodes discovered yet")
        return None

    # Gibt die gefundenen Nodes mit Rolle und bekannter Leader-ID aus.
    visible = {
        nid: f"{info['host']}:{info['client_port']} role={info['role']} leader={info['leader']}"
        for nid, info in nodes.items()
    }
    print(f"[client] DISCOVERY found nodes: {visible}", flush=True)

    # Bevorzugt die Leader-ID, die von den meisten gefundenen Nodes angekündigt wird.
    leader_ids = [int(info["leader"]) for info in nodes.values() if int(info["leader"]) > 0]
    preferred = []
    if leader_ids:
        # Most nodes should announce the same leader. Use the most frequent leader id.
        leader = max(set(leader_ids), key=leader_ids.count)
        if leader in nodes:
            preferred.append(leader)

    # Fallback: try all discovered nodes, highest id first.
    # Falls der angekündigte Leader nicht erreichbar ist, werden die übrigen Nodes getestet.
    for nid in sorted(nodes.keys(), reverse=True):
        if nid not in preferred:
            preferred.append(nid)

    # Versucht die Nodes der Reihe nach per TCP zu erreichen.
    for nid in preferred:
        info = nodes[nid]
        host = str(info["host"])
        port = int(info["client_port"])
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3.0)
            s.connect((host, port))
            data = b""

            # Wartet auf die erste Antwort des Servers: WELCOME oder NOT_LEADER.
            while b"\n" not in data:
                chunk = s.recv(256)
                if not chunk:
                    break
                data += chunk
            reply = data.decode("utf-8", errors="replace").strip()

            # Nur der aktuelle Leader akzeptiert die Verbindung dauerhaft.
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
    # Läuft in einem eigenen Thread und empfängt Chat-Nachrichten vom Leader.
    buf = b""
    while not stop["stop"]:
        try:
            chunk = sock.recv(2048)
        except OSError:
            break
        if not chunk:
            break
        buf += chunk

        # Eine vollständige Nachricht endet mit einem Zeilenumbruch.
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                print(text, flush=True)

    # Signalisiert dem Hauptprogramm, dass die Verbindung beendet wurde.
    stop["stop"] = True


def stdin_reader(input_queue: "queue.Queue[Optional[str]]", stop: Dict[str, bool]) -> None:
    # Liest Benutzereingaben in einem separaten Thread, damit Empfang und Eingabe parallel möglich sind.
    while not stop["stop"]:
        try:
            line = input()
            input_queue.put(line)
        except EOFError:
            input_queue.put(None)
            break


def main() -> None:
    print("=== Distributed Chat Client ===", flush=True)

    # Fragt so lange nach einem Namen, bis eine gültige Eingabe vorhanden ist.
    username = ""
    while not username.strip():
        try:
            username = input("Enter your username: ").strip()
        except KeyboardInterrupt:
            print("\n[client] bye")
            return

    print("[client] searching for leader via dynamic discovery...", flush=True)

    # Diese Schleife sorgt dafür, dass der Client nach einem Verbindungsverlust erneut sucht.
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

        # Gemeinsames Stop-Signal für Empfangs- und Eingabe-Thread.
        stop = {"stop": False}
        threading.Thread(target=receive_loop, args=(sock, stop), daemon=True).start()

        # Die Queue übergibt Benutzereingaben sicher vom Eingabe-Thread an die Hauptschleife.
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
                    # Sendet Benutzername und Nachricht über die bestehende TCP-Verbindung an den Leader.
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

        # Nach einem Verbindungsverlust wird der alte Socket geschlossen und die Discovery neu gestartet.
        print("[client] connection lost; rediscovering leader...", flush=True)
        try:
            sock.close()
        except OSError:
            pass
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    # Startpunkt des Programms, wenn die Datei direkt ausgeführt wird.
    main()
