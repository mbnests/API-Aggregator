# Multi-API Asynchronous Aggregator

A high-performance, asynchronous Python data pipeline that concurrently ingests, strictly validates (Pydantic), and persistently stores data from multiple APIs into a local database.

This project demonstrates the core principles of robust backend engineering, utilizing a producer-consumer architecture to handle multiple data streams simultaneously without overwhelming external servers.

## Key Architecture & Features

*   **Asynchronous Concurrency:** Utilizes `asyncio` and `aiohttp` to manage a swarm of non-blocking workers, dramatically reducing network wait times.
*   **Strict Data Validation:** Implements `Pydantic` blueprints to guarantee that incoming, unpredictable internet data strictly adheres to expected schemas before entering the system.
*   **Network Rate Limiting:** Protects external APIs from being overwhelmed by using an `asyncio.Semaphore` to strictly throttle concurrent outgoing requests.
*   **Database Persistence:** Utilizes asynchronous SQLite (`aiosqlite`) to execute safe, non-blocking SQL transactions, ensuring validated data is permanently stored (`INSERT OR REPLACE`) into local tables.
*   **Dynamic Task Routing:** Workers dynamically inspect task queues to determine the correct API endpoint and validation schema on the fly.
*   **Structured Observability:** Replaces standard print statements with a permanent, timestamped `logging` system to track worker efficiency and pipeline health.

## Tech Stack
*   Python 3.x
*   `asyncio`
*   `aiohttp`
*   `pydantic`
*   `aiosqlite`
