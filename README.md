# Distributed Algorithms

This repository contains Java implementations of various distributed algorithms, with a focus on snapshot and election algorithms. These algorithms are run within a simulated network environment created by leveraging the [Akka framework](https://akka.io/).

## Features

- **Graph Structure Integration**: This project integrates with graph structures defined by `.dot` files, which are generated using the [NetGameSim application](https://github.com/0x1DOCD00D/NetGameSim). NetGameSim is an experimental platform designed to create large-scale random graphs and their perturbations, providing a visual and structural foundation for network simulation within this project.
- Simulation of network nodes as Akka actors, which interact in a distributed system to demonstrate algorithm behaviors.
- Implementation of the following algorithms:
- - Snapshot algorithms:
    - - Chandy-Lamport algorithm
    - - Lai-Yang algorithm

## Project Structure

- `src/main/java`: Contains the source code for the project.
    - `snapshot_algorithms`: Implementation of snapshot algorithms for distributed systems.
        - `chandy_lamport`: The Chandy-Lamport algorithm for distributed snapshots.
        - `lai_yang`: The Lai-Yang algorithm for consistent global snapshots.
    - `util`: Utility classes supporting algorithm functionality.
        - `GraphParser`: Parses .dot files from the `resources/graph/` directory to create a graph of actors, embodying the network topology for the simulation.
    - `resources`: Holds configuration settings and graph definitions.
        - `graph`: Directory containing the primary `.dot` file (e.g., `NetGraph.dot`) that represents the network graph used for the simulation.

- `src/test/java`: Test suites for the source code.
    - `snapshot_algorithms`: Test cases for snapshot algorithms.
    - `util`: Tests for utility classes to ensure accurate parsing and functionality.
        - `resources/graph`: Contains multiple `.dot` files used for component testing of graph parsing and actor system simulation.


### Prerequisites

- Java JDK version 21.0.2
- Maven version 3.9.6

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/obukte/Distributed-Algorithms.git
    ```
2. Navigate to the project directory:
    ```bash
    cd Distributed-Algorithms
    ```
3. Build the project with Maven:
    ```bash
    mvn clean install
    ```
