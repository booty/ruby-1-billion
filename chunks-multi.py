import os
import multiprocessing
import time

FILE_PATH = "../1brc/measurements.txt"
CPU_COUNT = os.cpu_count()
FILE_SIZE_BYTES = os.path.getsize(FILE_PATH)
CHUNK_SIZE_BYTES = (FILE_SIZE_BYTES // CPU_COUNT) + 1000
CHUNK_LIMIT = 999_999_999
OUTPUT_INTERVAL_READING = 1
OUTPUT_INTERVAL_WORKING_LINES = 5000


class Measurement:
    def __init__(self, qty, max_temp, min_temp, sum_temp):
        self.qty = qty
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.sum_temp = sum_temp


class ChunkResult:
    def __init__(self, line_count, histo):
        self.line_count = line_count
        self.histo = histo


def process_histo(worker_id, byte_offset):
    start_time = time.time()
    line_count = 0
    histogram = {}

    with open(FILE_PATH, "r") as file:
        print(
            f"[{worker_id}][{os.getpid()}] reading chunk at byte_offset {byte_offset}"
        )
        file.seek(byte_offset)
        chunk = file.read(CHUNK_SIZE_BYTES)
        print(f"[{worker_id}][{os.getpid()}] processing chunk")

        for line in chunk.splitlines():
            idx = line.find(";")
            if idx == -1:
                continue

            city = line[:idx]
            temp_float = float(line[idx + 1 : idx + 11])

            line_count += 1
            if city in histogram:
                item = histogram[city]
                # item.qty += 1
                # item.sum_temp += temp_float
                # item.max_temp = max(temp_float, item.max_temp)
                # item.min_temp = min(temp_float, item.min_temp)
                item[0] += 1
                item[1] += temp_float
                item[2] = max(temp_float, item[2])
                item[3] = min(temp_float, item[3])
            else:
                # histogram[city] = Measurement(1, temp_float, temp_float, temp_float)
                histogram[city] = [1, temp_float, temp_float, temp_float]

    elapsed_time = time.time() - start_time
    print(
        f"[{worker_id}][{os.getpid()}] done; processed {line_count} lines. elapsed: {elapsed_time:.1f}s"
    )
    return ChunkResult(line_count, histogram)


def read_chunk(file_path, chunk_size_bytes, byte_offset):
    with open(file_path, "r") as file:
        file.seek(byte_offset)
        return file.read(chunk_size_bytes)


def main():
    byte_offset = 0
    chunk_count = 0
    writer_line_count = 0
    start_time = time.time()
    worker_id = 0

    print(
        f"FILE_SIZE_BYTES: {FILE_SIZE_BYTES} CPU_COUNT: {CPU_COUNT} CHUNK_SIZE_BYTES: {CHUNK_SIZE_BYTES}"
    )

    pool = multiprocessing.Pool(CPU_COUNT)
    results = []

    with open(FILE_PATH, "r") as file:
        while byte_offset < FILE_SIZE_BYTES and chunk_count < CHUNK_LIMIT:
            # chunk = read_chunk(
            #     FILE_PATH, CHUNK_SIZE_BYTES, offset
            # )
            file.seek(byte_offset)
            chunk = file.read(CHUNK_SIZE_BYTES)
            last_newline_index = chunk.rfind("\n")
            byte_offset += last_newline_index + 1
            # processed_chunk = chunk[:last_newline_index]

            # result = pool.apply_async(process_chunk_histo, (processed_chunk,))
            result = pool.apply_async(
                process_histo,
                (
                    worker_id,
                    byte_offset,
                ),
            )
            worker_id += 1
            # results.append(result)

            chunk_count += 1
            if chunk_count % OUTPUT_INTERVAL_READING == 0:
                elapsed_time = time.time() - start_time
                print(
                    f"chunks:{chunk_count} byte_offset: {byte_offset}/{FILE_SIZE_BYTES} ({byte_offset / FILE_SIZE_BYTES * 100:.2f}%) elapsed: {elapsed_time:.1f}s"
                )

        pool.close()
        pool.join()

    for result in results:
        writer_line_count += result.get().line_count

    elapsed_time = time.time() - start_time
    print(
        f"chunks:{chunk_count} writer_line_count: {writer_line_count} elapsed: {elapsed_time:.1f}s"
    )


if __name__ == "__main__":
    main()
