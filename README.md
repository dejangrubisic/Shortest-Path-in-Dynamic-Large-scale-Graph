# Shortest Path in a Dynamic Large-scale Graph

Real-time shortest-path search over a graph whose edge weights keep changing, built on the Lambda Architecture with Apache Spark, Kafka, and HDFS — fully containerized with Docker Compose.

*Master's thesis project (Faculty of Technical Sciences, Novi Sad). See [Thesis.pdf](Thesis.pdf), [Paper.pdf](Paper.pdf), and [MasterPresentation.pptx](MasterPresentation.pptx).*

## Problem

Classic shortest-path algorithms assume a static graph: change one edge weight and the whole computation has to be redone. In real systems — road traffic, network routing, logistics — weights change constantly and the graph is too large for a single machine, so recomputing from scratch on every update is both too slow and too expensive. The system must keep answering "what is the best path right now?" while updates keep streaming in.

## Solution

The system implements the **Lambda Architecture**: an exact-but-slow batch layer and a fast approximate speed layer working on the same data.

- **Batch layer** (`spark_project/batch.py`, PySpark): reads the graph from HDFS and runs an iterative MapReduce search that expands paths from the start node forward and from the end node backward simultaneously. Each node keeps only its top-3 cheapest partial paths from each side, which bounds the state per iteration. Connected forward/backward paths are merged, ranked, and written back to HDFS.
- **Speed layer** (`spark_project/real_time.py`, Spark Streaming): consumes edge-weight updates from Kafka, patches the current set of best paths in place, and immediately publishes the new best path — no full recomputation. When a fresh batch result lands (signaled via `SIGUSR1`), buffered updates are replayed on top of it.
- **Ingestion** (`hdfs_writer/`): consumes edge events from Kafka and maintains the graph (adjacency with in/out neighbors) as JSON in HDFS.
- **Generator** (`gen/gen.py`): simulates a dynamic graph by streaming random or user-defined edge-weight changes into Kafka.
- **Visualization** (`plot/plot_dash.py`, Plotly Dash): live web dashboard that draws the graph, lets you pick start/end nodes, and highlights the current shortest path in red as edges change.

The sample graph is built from San Francisco bike-share station data (`hdfs_writer/preprocess.py`, `hdfs_writer/data.json`).

## Benefit

- Path queries stay responsive under continuous change: the streaming layer reacts to each edge update within its 2-second micro-batch window instead of waiting for a full batch run.
- Batch results periodically restore exactness, so the fast approximation never drifts far from the true optimum.
- Everything runs as Docker containers (Spark master + worker, HDFS namenode + datanode, Kafka, Zookeeper, and the four Python services), so the whole cluster starts with one command and scales by adding workers.

## How to run

Requires Docker and Docker Compose.

```bash
# 1. Build and start the cluster (Zookeeper, Kafka, HDFS, Spark, and app containers)
docker-compose up --build

# 2. Start the batch + real-time Spark jobs
docker exec -it spark-master python main.py

# 3. Start the edge-weight generator
docker exec -it gen python gen.py            # random updates every few seconds
# or target a specific edge:
docker exec -it gen python gen.py <start_id> <end_id> <increment>
```

Then open the dashboard at **http://localhost:9998**, choose start and end nodes from the dropdowns, and press **Submit**. The shortest path is drawn in red and updates live as edge weights change.

Useful UIs:

- HDFS NameNode — http://localhost:50070
- Spark master — http://localhost:8080

## Tech stack

Python 2.7 · Apache Spark 2.1 (PySpark + Spark Streaming) · Apache Kafka · Hadoop HDFS 2.8 · Plotly Dash · Docker Compose

## Repository structure

```
├── docker-compose.yml     # Full cluster definition
├── hadoop.env             # Hadoop/HDFS configuration
├── spark_project/         # Batch (batch.py) and streaming (real_time.py) Spark jobs
├── hdfs_writer/           # Kafka -> HDFS graph ingestion + CSV preprocessing
├── gen/                   # Edge-weight update generator
├── plot/                  # Live Dash visualization
├── Thesis.pdf             # Master's thesis
├── Paper.pdf              # Conference paper
└── MasterPresentation.pptx
```
