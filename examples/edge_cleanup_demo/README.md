# Edge Cleanup Bug Demonstration

This directory contains scripts to demonstrate the edge cleanup bug in ezmsg's GraphServer.

## The Bug

When a publisher disconnects unexpectedly (e.g., process crash, SIGKILL), the edge in the DAG remains even though the publisher client is removed from `GraphServer.clients`.

## The Fix: `ezmsg prune`

Run `ezmsg prune` to remove orphan edges (edges where the source topic has no active publisher):

```bash
ezmsg prune
```

This is useful during development when processes get killed unexpectedly.

## Setup

You'll need 2 terminal windows.

### Terminal 1: Start the GraphServer and run the receiver

```bash
cd /path/to/ezmsg/examples/edge_cleanup_demo
source ../../.venv/bin/activate
ezmsg start
python long_running_receiver.py
```

This starts a DebugLog unit connected to a Sink, creating an initial edge in the graph.

### Terminal 2: Check graph, run & kill transient publisher, check graph again

```bash
cd /path/to/ezmsg/examples/edge_cleanup_demo
source ../../.venv/bin/activate

# Check the initial graph state - should show 1 edge: "{RECEIVER URI}" -> "{SINK URI}"
ezmsg mermaid -c -n

# Run the transient publisher that intercepts Ctrl+C and calls os._exit(1)
python transient_publisher_crash.py

# Check the graph again - Additional node and edge remain even though publisher process has exited
ezmsg mermaid -c -n
```

## Comparison: Proper Cleanup

For comparison, `transient_publisher.py` exits gracefully when you press Ctrl+C:

```bash
# After the crash test, restart with a fresh GraphServer:
ezmsg shutdown
ezmsg start

# In Terminal 1: restart the receiver
python long_running_receiver.py

# In Terminal 2: run the proper cleanup version
python transient_publisher.py
# Press Ctrl+C to exit gracefully
ezmsg mermaid -c -n
# Edge is properly removed because ez.run()'s finally block called revert()
```

## Solution

```bash
ezmsg prune
```
