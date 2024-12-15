# HKFS - A Toy Hadoop Cluster Written in Rust

HKFS is a simple, toy version of the Hadoop Distributed File System (HDFS) written in **Rust**. It simulates a basic NameNode and DataNodes communication system using heartbeats. The project consists of two main components:

- **NameNode**: The master node that manages DataNodes and tracks their health.
- **DataNodes**: The worker nodes that send periodic heartbeats to the NameNode to report their status.

This project aims to replicate some of the basic functionalities of Hadoop, such as node monitoring and fault tolerance, but in a simpler form.

It includes a custom protocol implemented over a TCP layer. The namenode and datanodes communicate using TCP Sockets.

Features and characteristics:

* **Load Balancing**: The namenode acts as a load balancer as well as a master node. It implements the **Round Robin** Load balancing algorithm, cyclically iterating over the datanodes to store messages.

* **Replication**:  HKFS is able to store messages on a fault-tolearant way, by replicating the each message to at least three datanodes. The REPLICATION_FACTOR parameter is used to handle this.

* **Sharding or partitioning**: The namenode partitions each message in partitions of 1KB (This size is not configurable for now). As mentioned before, each partition is replicated across the number of datanodes specified by the user. Additionally, inside each datanode, the message is serialized into bytes and further partitioned into partitions of 24B to allow parallel loading into memory.

* **Datanode health monitoring**: The namenode monitors the status of the datanodes, marking them as inactive when a certain time passes without receiving hearbeats from a certain datanode.

* **Graceful shutdown**: When the main process is terminated, all the child processes (datanodes) are also killed.
---

# Project Objectives

HKFS is an educational project whose main purpose is to teach and explore fundamental concepts of distributed systems, specifically in the context of distributed file systems like HDFS (Hadoop Distributed File System). Through a simplified implementation of a distributed file system, this project aims to provide a basic understanding of various aspects of the architecture and operation of distributed systems, such as node management, fault tolerance, and load balancing.

## Project Structure

- **`namenode/namenode.rs`**: Contains the logic for the NameNode, including accepting incoming heartbeat connections from DataNodes.
- **`datanode/datanode.rs`**: Contains the logic for the DataNodes that periodically send heartbeats to the NameNode.
- **`Cargo.toml`**: The configuration file for the Rust project, including dependencies and project settings.

## Usage
- Start a namenode and N datanodes with R replication factor:
```bash
  chmod +x hkfs.sh
  ./hkfs.sh <N_DATANODES> <REPLICATION_FACTOR>
```

**Datanode status monitoring**
```bash
 ./status.sh
```
**Store a block api**
```bash
./storemessage.sh <id> <message>
```
**Read a block api**
```bash
./readmessage.sh <id>
```

## Contributions


Thank you for your interest in contributing to hkfs! To ensure a smooth and efficient contribution process, please follow these guidelines:

1. **Fork the repository**: Fork the hkfs repository to your GitHub account.

2. **Clone the repository**: Clone the forked repository to your local machine.
  ```sh
  git clone https://github.com/RaulMoldes/hkfs.git
  ```

3. **Create a branch**: Create a new branch to work on your contribution.
  ```sh
  git checkout -b your-branch-name
  ```

4. **Make changes**: Make the necessary changes to the code. Be sure to follow the project's style guides and conventions.

5. **Testing**: Run the existing tests to ensure your changes do not break anything. If necessary, add new tests to cover your changes.

6. **Commits**: Commit your changes with clear and descriptive messages.
  ```sh
  git add .
  git commit -m "Description of the changes"
  ```

7. **Push**: Push your changes to your forked repository on GitHub.
  ```sh
  git push origin your-branch-name
  ```

8. **Pull Request**: Open a pull request to the main repository. Provide a clear description of your changes and any additional context that might be helpful.

I appreciate your contributions and look forward to working with you!

# Author

* Raúl Moldes Castillo
* raul.moldes.work@gmail.com
* @RaulMoldes