import pandas as pd
import multiprocessing
from multiprocessing import Queue
from typing import Union
import time
import os

FILE_PATH = "../1brc/measurements.txt"
TOTAL_LINES = 1000000000
CHUNK_COUNT = 1000
CHUNK_SIZE_LINES = TOTAL_LINES // CHUNK_COUNT
POOL_SIZE = 1
SLEEP_SECONDS = 0.1


def queue_worker(chunk_queue):
    histo = {}
    my_pid = os.getpid()
    chunk_count = 0

    print(f"[{my_pid}] waiting for chunks")

    while True:
        if chunk_queue.empty():
            # print(f"[{my_pid}] queue is empty, sleeping")
            time.sleep(SLEEP_SECONDS)  # Sleep for 0.1 seconds if the queue is empty
            continue

        chunk = chunk_queue.get()
        if chunk is None:
            print(f"[{my_pid}] got None, exiting")
            return histo

        chunk_count += 1
        print(f"[{my_pid}] processing chunk")
        print(f"#{len(chunk)} chars: {chunk[:100]}")

        line_count = 0
        for line in chunk.splitlines():
            line_count += 1
            if line_count % 1000 == 0:
                print(f"[{my_pid}] processing line #{line_count}")
            idx = line.find(";")
            if idx == -1:
                continue

            city = line[:idx]
            temp_float = float(line[idx + 1 : idx + 11])

            line_count += 1
            if city in histo:
                item = histo[city]
                # item.qty += 1
                # item.sum_temp += temp_float
                # item.max_temp = max(temp_float, item.max_temp)
                # item.min_temp = min(temp_float, item.min_temp)
                item[0] += 1
                item[1] += temp_float
                item[2] = max(temp_float, item[2])
                item[3] = min(temp_float, item[3])
            else:
                # histo[city] = Measurement(1, temp_float, temp_float, temp_float)
                histo[city] = [1, temp_float, temp_float, temp_float]
        print(f"[{my_pid}] dibe processing chunk")


if __name__ == "__main__":
    with multiprocessing.Manager() as manager:
        chunk_queue: manager.Queue() = manager.Queue()

        # create a multiprocessing pool of 10 workers
        print(f"Creating pool of {POOL_SIZE} workers")
        pool = multiprocessing.Pool(POOL_SIZE)

        # create 10 pool workers running queue_worker
        print(f"Creating {POOL_SIZE} workers")
        results = []  # Store the results here
        for i in range(POOL_SIZE):
            result = pool.apply_async(queue_worker, args=(chunk_queue,))
            # result = pool.apply_async(queue_worker, args=("suck my dick",))
            results.append(result)

        chunk_number = 0
        for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE_LINES, delimiter=";"):
            chunk_number += 1
            chunk_queue.put(chunk)
            if (chunk_number % 10) == 0:
                progress = round(chunk_number / CHUNK_COUNT * 100, 1)
                print(f"Put chunk #{chunk_number} in the queue {progress}%")

        # place POOL_SIZE Nones in the queue to signal the workers to exit
        print(f"Telling workers to quit")
        for i in range(POOL_SIZE):
            chunk_queue.put(None)

        print(f"Waiting for workers to finish")
        for result in results:
            result.get()
        pool.close()
        pool.join()
