# Paradis
This directory hosts the maelstrom framework code to avoid any discrepancies caused by different maelstrom versions.
The subdirectory "paradis" contains our primitive implementation of the QuePaxa protocol.
It contains:
- main.go: The quepaxa protocol implementation
- paradis-bin: The compiled binary to run the tests
- run_test: The script to execute the tests

## Steps to run the tests:

cd paradis

./run_test --scenario <normal|latency|partition>

The test will run the linear kv workload on 5 nodes with a rate of 100 operations per second.
The scenario indicates normal working conditions or high network latency or network partition.

### Note:
To facilitate easy setup and configuration, we have commented out the Redis implementation and have presented a local key-value store implementation.
