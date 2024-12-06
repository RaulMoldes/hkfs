# HKFS - A Toy Hadoop Cluster Written in Rust

HKFS is a simple, toy version of the Hadoop Distributed File System (HDFS) written in **Rust**. It simulates a basic NameNode and DataNodes communication system using heartbeats. The project consists of two main components:
- **NameNode**: The master node that manages DataNodes and tracks their health.
- **DataNodes**: The worker nodes that send periodic heartbeats to the NameNode to report their status.

This project aims to replicate some of the basic functionalities of Hadoop, such as node monitoring and fault tolerance, but in a simpler form.

---

## Features

- **NameNode**: 
  - Tracks the health of DataNodes by receiving periodic heartbeats.
  - Marks DataNodes as inactive if no heartbeat is received within a set period.

- **DataNodes**: 
  - Send heartbeats periodically to the NameNode to report their health.
  - Disconnect and reconnect if there are issues with the connection.

- **Configurable Parameters**: 
  - The port for the NameNode and the heartbeat timeout duration can be easily configured.

---

## Project Structure

- **`namenode.rs`**: Contains the logic for the NameNode, including accepting incoming heartbeat connections from DataNodes.
- **`datanode.rs`**: Contains the logic for the DataNodes that periodically send heartbeats to the NameNode.
- **`Cargo.toml`**: The configuration file for the Rust project, including dependencies and project settings.

## Usage
- Start the namenode: 
```bash
  cargo run --bin namenode
```
- Start as many datanodes as you want:
```bash
  cargo run --bin datanode
```
- Use the API provided on port 8080:
**Datanode status**
```bash
  curl http://127.0.0.1:8080/status
```
**Store a block api**
```bash
curl -X POST "http://127.0.0.1:8080/storeblock" -H "Content-Type: application/json" -d '{"block_id": "1"}
```