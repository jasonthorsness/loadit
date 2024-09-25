## Instructions

Paste the following into three cells in a SingleStore Notebook at [portal.singlestore.com](https://portal.singlestore.com).

## Prerequisites

```python
%sql CREATE DATABASE IF NOT EXISTS test;
!pip install faker
```

**After running this cell, make sure to select the database!**

## Generate

Paste this into an LLM (such as ChatGPT o1-preview) and ask it to customize it for your needs.

```python
# version 2024-09-25
import singlestoredb as s2
import io
import random
import os
import json
from faker import Faker

# Globals should reflect the number of rows to load and the table name
NUM_ROWS = 1_000_000
TARGET_TABLE_NAME = "tbla"


# The setup function is immediately invoked below to create the target table
def setup(cur):
    cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE_NAME}")
    cur.execute(
        f"""
        CREATE TABLE {TARGET_TABLE_NAME} (
          `a` BIGINT,
          `b` LONGTEXT NOT NULL,
          `c` LONGBLOB NOT NULL,
          `d` BIGINT NOT NULL,
          `json_data` JSON
        )
    """
    )


# connection_url is already defined in global scope
with s2.connect(connection_url) as conn:
    with conn.cursor() as cur:
        setup(cur)

faker = Faker(use_weighting=False)


# The generate function will be called once for each row
# i is a unique 0-based integer row number
# it should return fields in the order required to INSERT in the table above
def generate(i):
    return [
        i,
        "hello",
        os.urandom(64),
        random.randint(1, 10),
        json.dumps({"name": faker.name(), "email": faker.email()}),
    ]

```

## Loader

This cell does not need to be modified; just copy it straight to the notebook.

```python
# version 2024-09-25
import datetime
import io
import multiprocessing
import threading
import time
import singlestoredb as s2
from queue import Queue
from concurrent.futures import ProcessPoolExecutor

NUM_LOADERS = multiprocessing.cpu_count()


def producer(queue, i, n):
    def fieldsToLine(fields):
        def toBytes(data):
            if isinstance(data, bytes):
                pass
            elif isinstance(data, str):
                data = data.encode("utf-8")
            else:
                data = str(data).encode("utf-8")
            return (
                data.replace(b"\x1b", b"\x1b\x1b")
                .replace(b"\t", b"\x1b\t")
                .replace(b"\n", b"\x1b\n")
            )

        fields = [toBytes(field) for field in fields]
        return b"\t".join(fields) + b"\n"

    buffer = io.BytesIO()
    start_time = time.perf_counter()
    flush_time = start_time
    rows_per_batch = 100
    rows_generated = 0
    rows_generated_since = 0
    while n > 0:
        if time.perf_counter() - flush_time > 1:
            flush_time = time.perf_counter()
            rows_per_batch = max(
                100, int((rows_generated / (time.perf_counter() - start_time)) / 10)
            )
            queue.put(None)
            queue.put(rows_generated_since)
            rows_generated_since = 0
        next = min(n, rows_per_batch)
        n -= next
        buffer.seek(0)
        buffer.truncate()
        for j in range(next):
            fields = generate(i + j)
            buffer.write(fieldsToLine(fields))
        batch = buffer.getvalue()
        queue.put(batch)
        rows_generated += next
        rows_generated_since += next
        i += next
    queue.put(None)
    queue.put(rows_generated_since)


def consumer(i, array, event, queue):
    config = {
        "user": connection_user,
        "password": connection_password,
        "host": connection_host,
        "port": connection_port,
        "database": connection_default_database,
        "local_infile": True,
    }
    with s2.connect(**config) as conn:
        with conn.cursor() as cur:
            while True:
                cur.execute(
                    f"LOAD DATA LOCAL INFILE ':stream:' INTO TABLE {TARGET_TABLE_NAME} FIELDS TERMINATED BY '\t' ESCAPED BY 0x1b LINES TERMINATED BY '\n'",
                    infile_stream=queue,
                )
                array[i] += queue.get()
                if queue.qsize() == 0 and event.is_set():
                    break


def monitor(event, loader_current, total_rows_to_add, initial_count, start_time):
    def print_stats(current_count, initial_count, elapsed_seconds):
        rows_added = current_count - initial_count
        rows_per_second = rows_added / elapsed_seconds if elapsed_seconds > 0 else 0

        if rows_per_second > 0 and total_rows_to_add > 0:
            estimated_total_time = total_rows_to_add / rows_per_second
            estimated_time_remaining = estimated_total_time - elapsed_seconds
            estimated_time_str = f"{estimated_time_remaining:.1f}s"
        else:
            estimated_time_str = "N/A"

        print(
            f"Total rows: {current_count} "
            f"Elapsed time: {elapsed_seconds:.0f}s "
            f"Rows added: {rows_added} "
            f"Rows/sec: {rows_per_second:.0f} "
            f"Estimated time remaining: {estimated_time_str}"
            "                                               ",
            end="\r",
            flush=True,
        )

    def update_and_print_stats():
        elapsed_time = datetime.datetime.now() - start_time
        elapsed_seconds = elapsed_time.total_seconds()
        print_stats(initial_count + sum(loader_current), initial_count, elapsed_seconds)

    print_stats(initial_count, initial_count, 0)

    while not event.is_set():
        update_and_print_stats()
        time.sleep(0.75)

    update_and_print_stats()
    print("\ndone")


shared_array = None


def childProcessMain(i, offset, n):
    queue = Queue(maxsize=10)

    producer_thread = threading.Thread(
        target=producer,
        args=(
            queue,
            offset,
            n,
        ),
    )
    producer_thread.start()

    event = threading.Event()

    consumer_thread = threading.Thread(
        target=consumer,
        args=(
            i,
            shared_array,
            event,
            queue,
        ),
    )
    consumer_thread.start()

    producer_thread.join()

    event.set()

    consumer_thread.join()


def main():
    initial_count = 0
    with s2.connect(connection_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE_NAME}")
            initial_count = cur.fetchone()[0]

    start_time = datetime.datetime.now()

    event = threading.Event()
    array = multiprocessing.Array("i", NUM_LOADERS)

    monitor_thread = threading.Thread(
        target=monitor, args=(event, array, NUM_ROWS, initial_count, start_time)
    )
    monitor_thread.start()

    batch_size = NUM_ROWS // NUM_LOADERS
    remainder = NUM_ROWS % NUM_LOADERS
    batches = [batch_size] * NUM_LOADERS
    for i in range(remainder):
        batches[i] += 1

    offset = 0
    offsets = []
    for batch in batches:
        offsets.append(offset)
        offset += batch

    def init_shared_array(array):
        global shared_array
        shared_array = array

    try:
        with ProcessPoolExecutor(
            max_workers=NUM_LOADERS, initializer=init_shared_array, initargs=(array,)
        ) as executor:
            futures = []
            for i in range(NUM_LOADERS):
                futures.append(
                    executor.submit(childProcessMain, i, offsets[i], batches[i])
                )
            for future in futures:
                future.result()
    finally:
        event.set()

    monitor_thread.join()


if __name__ == "__main__":
    main()
```
