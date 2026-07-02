# Distributed Chat System - Prototype

This project implements a distributed chat system as a prototype for the Distributed Systems module.

The system demonstrates the following distributed systems concepts:

* Dynamic discovery of server nodes
* Logical ring topology between server nodes
* Leader election based on unique node IDs
* Communication with left/right ring neighbors
* Crash fault tolerance through heartbeat-based failure detection
* Automatic client reconnection after leader failure
* Demonstration on at least two physical machines

## Files

* `ds_node.py` = server node
* `ds_client.py` = chat client

The implementation only uses the Python standard library.
No external Python packages are required.

## System Overview

The distributed chat system consists of multiple server nodes and one or more chat clients.

The server nodes discover each other dynamically using UDP multicast/broadcast. Based on the discovered nodes, each server builds a logical ring ordered by the unique server node IDs.

Only the current leader accepts client connections. Followers reject client connections with a `NOT_LEADER` response. The client then continues searching until it reaches the current leader.

The leader receives chat messages from connected clients and broadcasts them to all connected clients.

## Leader Election

Leader election is based on unique server node IDs.

The active node with the highest ID becomes the leader.

The election process is demonstrated through ring communication. When an election starts, a node sends an election message to its right neighbor. The message is forwarded through the ring. During this process, the highest active node ID is determined.

When the winning node receives the election result, it declares itself as leader and sends a leader announcement through the ring.

Example:

* Active nodes: 1, 2, 3 → Leader = Node 3
* Node 3 crashes → Active nodes: 1, 2 → New leader = Node 2
* Node 2 crashes → Active node: 1 → New leader = Node 1

## Crash Fault Tolerance

The system detects leader failures using heartbeat messages.

The leader sends heartbeat messages through the ring. If followers stop receiving heartbeats, they assume that the leader has crashed and start a new election.

When the leader crashes, connected clients lose their TCP connection. The client detects this connection loss and automatically starts a new discovery process. After a new leader has been elected, the client reconnects to the new leader.

This demonstrates basic crash fault tolerance and failover behavior.

## Local Test on One Machine

Open four terminals in the project folder.

Terminal 1:

```powershell
py ds_node.py --id 1
```

Terminal 2:

```powershell
py ds_node.py --id 2
```

Terminal 3:

```powershell
py ds_node.py --id 3
```

Terminal 4:

```powershell
py ds_client.py
```

Expected behavior:

* The nodes discover each other.
* The logical ring is built as `1 -> 2 -> 3`.
* Node 3 becomes the leader.
* The client connects to leader Node 3.
* Chat messages are sent through the leader.

## Demo on Two Physical Machines

The demo can also be executed on two physical machines in the same network.

If the university Wi-Fi blocks multicast or broadcast traffic, both laptops can be connected to the same mobile hotspot.

### Laptop A

Terminal 1:

```powershell
py ds_node.py --id 1
```

Terminal 2:

```powershell
py ds_node.py --id 2
```

Terminal 3:

```powershell
py ds_client.py
```

### Laptop B

Terminal 1:

```powershell
py ds_node.py --id 3
```

Terminal 2:

```powershell
py ds_client.py
```

## Windows Firewall Rules

On both laptops, run PowerShell as Administrator and execute:

```powershell
New-NetFirewallRule -DisplayName "DS Chat TCP" -Direction Inbound -Protocol TCP -LocalPort 6001-6010,7001-7010 -Action Allow
New-NetFirewallRule -DisplayName "DS Chat UDP Discovery" -Direction Inbound -Protocol UDP -LocalPort 50000 -Action Allow
```

These rules allow the server nodes and clients to communicate over TCP and allow UDP-based discovery.

## Demonstration Scenario

The following steps can be shown during the project demonstration:

1. Start Node 1, Node 2, and Node 3.
2. Show that the nodes discover each other dynamically.
3. Show the logical ring structure, for example `ring=[1 -> 2 -> 3]`.
4. Show that Node 3 becomes the leader because it has the highest active node ID.
5. Start two clients and send chat messages.
6. Stop Node 3 with `Ctrl + C`.
7. Show that the failure is detected.
8. Show that a new election is started.
9. Show that Node 2 becomes the new leader.
10. Show that the clients reconnect automatically to the new leader.

## Architecture Explanation

Our project is a distributed chat system. Several server nodes discover each other dynamically using UDP multicast and broadcast. From the discovered nodes, each server builds the same logical ring ordered by node ID.

The leader election uses ring communication. When an election starts, a node sends an election message only to its right neighbor. The message travels through the ring. The highest active node ID wins. When the winning node receives the election result, it declares itself as leader and sends a leader announcement through the ring.

Only the leader accepts chat clients. Followers reject clients with `NOT_LEADER`, so the client keeps searching until it reaches the leader. The leader receives chat messages and broadcasts them to connected clients.

For crash fault tolerance, the leader sends heartbeat messages through the ring. If followers stop receiving heartbeats, they assume that the leader crashed and start a new election. The client notices the broken connection and automatically reconnects to the new leader.

## Limitations

This system is a university prototype and is not intended for production use.

The focus of the implementation is to demonstrate core distributed systems concepts such as dynamic discovery, ring-based leader election, heartbeat-based failure detection, failover, and basic client reconnection.

Possible limitations are:

* Messages are stored only in memory.
* There is no persistent database.
* There is no authentication or encryption.
* The system focuses on crash fault tolerance, not Byzantine fault tolerance.
* The implementation is optimized for demonstration and explanation, not for large-scale production deployment.
