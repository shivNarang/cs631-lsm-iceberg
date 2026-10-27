Project Proposal: LSM Tree Indexing in Apache Iceberg Course: CS631 - Implementation Techniques for Relational Database Systems

Team Members:

Shiv Narang (23b1075)

Kaushal Vijayvergia (23b0901)

Kritn Gupta (23b0948)

Project Goals Apache Iceberg is a high-performance open table format for massive analytic datasets, typically relying on min-max filtering and partition pruning for query optimization. However, under high-frequency streaming ingestion or heavy UPSERT/DELETE workloads, the default Copy-on-Write (CoW) or Merge-on-Read (MoR) strategies can lead to performance degradation and the "small file problem."

Our project aims to introduce Log-Structured Merge (LSM) Tree principles into Apache Iceberg's indexing architecture. The primary goals are:

Enhanced Read Performance: Implement a multi-level indexing structure that accelerates point lookups and highly selective range queries, outperforming standard min/max metadata filtering.

Implementation Plan We plan to implement this system through the following phased approach:

1. Phase 1: Architecture Study and Baseline Setup

Analyze Apache Iceberg's core specification, specifically the metadata tree (manifest lists and manifest files) and the Puffin index format.

Set up a baseline environment using Apache Spark or Trino to query standard Iceberg tables and establish baseline metrics.

2. Phase 2: LSM Data Structure Design

MemTable Integration: Design an in-memory buffer to hold active, sorted records before they are committed to storage.

SSTable Mapping: Determine how flushed data will map to Iceberg's underlying data files (e.g., writing tightly sorted Parquet files that act as SSTables).

3. Phase 3: Core Implementation and Query Engine Integration
Write Path: Modify the ingestion pipeline to route writes through the MemTable and trigger flushes based on size or time thresholds, registering these flushes in Iceberg's manifest tree.

Read Path: Implement a custom reader that queries the MemTable and the hierarchical on-disk files simultaneously, resolving the latest state of a record.

Compaction Job: Develop a background service to merge overlapping files, purging deleted records and optimizing the tree structure.

4. Phase 4: Benchmarking and Evaluation

Run standardized workloads (such as YCSB or a modified TPC-H) against our implementation.

Compare our LSM-backed Iceberg tables against vanilla Iceberg tables on metrics including write latency, read latency for point/range queries, and write amplification.`