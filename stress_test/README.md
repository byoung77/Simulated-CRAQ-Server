# CRAQ Stress Test

This directory contains a deterministic stress test for the CRAQ (Chain Replication with Apportioned Queries) implementation.

The goal of this script is not benchmarking, but rather to demonstrate and verify the behavior of the system under interleaved reads and writes to the same key.

---

## Purpose

This test suite explores the behavior of the CRAQ system under realistic concurrency patterns, including:

- Overlapping reads and writes
- Multiple writes to the same key (version stacking)
- Interleaved reads and writes across different servers in the chain
- Visibility of intermediate committed states
- Eventual consistency after propagation

---

## How to Run

This test depends on the CRAQ implementation files:

- switch.go
- CRAQServers.go

Run the test using:

go run CRAQ_Stress_Test.go switch.go CRAQServers.go

---

## Output

The test writes a detailed log to:

Log_Files/User_Log.txt

Each log entry is timestamped and includes:

Write replies:
    Write Reply:
        Key = k, Value = v, Version = n

Read replies:
    Read Reply from Server i:
        Key = k, Value = v, Version = n

---

## Structure of the Test

The script runs a sequence of increasingly complex scenarios:

1. Bulk Initialization
   - Writes keys 0–4 with initial values

2. Sanity Checks
   - Reads unique keys from each server
   - Reads the same key from all servers

3. Simple Overlap Test (Key 0)
   - A write with overlapping reads to different servers

4. Multiple Writes Test (Key 1)
   - Three writes in quick succession
   - A read that must wait until all versions resolve

5. Interleaved Test – Round 1 (Key 2)
   - Multiple writes and staggered reads
   - Demonstrates intermediate and final visible states

6. Interleaved Test – Round 2 (Key 3)
   - One write, two reads, then two closely spaced writes
   - Final read probes behavior under stacked pending versions


---

## What to Look For in the Logs

The logs illustrate several important behaviors:

- Stale reads
  Early reads may return older committed values

- Intermediate visibility
  Some servers may return intermediate versions (e.g., version 2 before version 4)

- Delayed reads
  Reads (especially at the head) may wait until all pending writes for a key are resolved

- Latest-version consistency
  Reads after multiple writes return the most recent committed version

- Eventual convergence
  After sufficient time, all servers return the same value and version

---

## Notes

- This is a deterministic, timing-based test using time.Sleep
- It is intended for behavioral validation, not performance benchmarking
- Results may vary slightly depending on system timing, but should remain logically consistent

---

## Summary

This stress test demonstrates that the CRAQ implementation:

- Correctly propagates writes through the chain
- Handles multiple pending versions of the same key
- Provides consistent read behavior under concurrency
- Converges to a consistent global state after propagation
