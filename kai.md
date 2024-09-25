## Instructions

Paste the following into three cells in a SingleStore Notebook at [portal.singlestore.com](https://portal.singlestore.com).

## Prerequisites

```python
%sql CREATE DATABASE IF NOT EXISTS test;
!pip install faker
!pip install pymongo
```

**After running this cell, make sure to select the database!**

## Generate

Paste this into an LLM (such as ChatGPT o1-preview) and ask it to customize it for your needs.

```python
import singlestoredb as s2
import io
import random
from faker import Faker
from bson import ObjectId, encode
import binascii
from pymongo import MongoClient

faker = Faker(use_weighting=False)

NUM_DOCS = 1_000_000
TARGET_COLLECTION_NAME = "tblb"


def setup():
    client = MongoClient(connection_url_kai)
    db = client[connection_default_database]
    collection = db[TARGET_COLLECTION_NAME]
    collection.drop()


setup()

faker = Faker(use_weighting=False)


def generate(i):
    return {
        "_id": i,
        "a": "hello",
        "b": os.urandom(64),
        "c": random.randint(1, 10),
        "info": {"name": faker.name(), "email": faker.email()},
    }

```

## Loader

This cell does not need to be modified; just copy it straight to the notebook.

```python
import datetime
import threading
import time
import multiprocessing
from queue import Queue
from concurrent.futures import ProcessPoolExecutor
from pymongo import MongoClient

NUM_LOADERS = multiprocessing.cpu_count()


def producer(queue, i, n):
    while n > 0:
        next = min(n, 10000)
        n -= next
        batch = []
        for j in range(next):
            doc = generate(i + j)
            batch.append(doc)
        queue.put(batch)
        i += next
    queue.put(None)


def consumer(i, array, queue):
    client = MongoClient(connection_url_kai)
    db = client[connection_default_database]
    collection = db[TARGET_COLLECTION_NAME]
    while True:
        data = queue.get()
        if data is None:
            break
        collection.insert_many(data, ordered=False)
        array[i] += len(data)
    client.close()


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
    producer_thread = threading.Thread(target=producer, args=(queue, offset, n))
    producer_thread.start()
    consumer_thread = threading.Thread(
        target=consumer,
        args=(
            i,
            shared_array,
            queue,
        ),
    )
    consumer_thread.start()
    producer_thread.join()
    consumer_thread.join()


def main():
    client = MongoClient(connection_url_kai)
    db = client[connection_default_database]
    collection = db[TARGET_COLLECTION_NAME]
    initial_count = collection.count_documents({})
    start_time = datetime.datetime.now()

    event = threading.Event()
    array = multiprocessing.Array("i", NUM_LOADERS)

    monitor_thread = threading.Thread(
        target=monitor, args=(event, array, NUM_DOCS, initial_count, start_time)
    )
    monitor_thread.start()

    batch_size = NUM_DOCS // NUM_LOADERS
    remainder = NUM_DOCS % NUM_LOADERS
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
