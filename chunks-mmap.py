import mmap
import os
import multiprocessing
from line_profiler import LineProfiler

FILE_PATH = "../1brc/measurements.txt"
MILLION = 1000000
HUNDRED_MILLION = MILLION * 100
BILLION = MILLION * MILLION
LINE_LIMIT = BILLION
OUTPUT_EVERY_LINES = MILLION  # LINE_LIMIT // 1000
CHUNK_SIZE_BYTES = 1024 * 1024 * 100
CPU_COUNT = os.cpu_count()


# def process_chunk(worker_id, undecoded_chunk):
#     print(f"[worker {worker_id}] processing chunk")
#     decoded_chunk = undecoded_chunk.decode("utf-8")
#     histo = {}
#     for line in decoded_chunk.splitlines():
#         idx = line.find(";")
#         if idx == -1:
#             continue

#         city = line[:idx]
#         temp_float = float(line[idx + 1 : idx + 11])

#         # line_count += 1
#         if city in histo:
#             item = histo[city]
#             # item.qty += 1
#             # item.sum_temp += temp_float
#             # item.max_temp = max(temp_float, item.max_temp)
#             # item.min_temp = min(temp_float, item.min_temp)
#             item[0] += 1
#             item[1] += temp_float
#             item[2] = max(temp_float, item[2])
#             item[3] = min(temp_float, item[3])
#         else:
#             # histo[city] = Measurement(1, temp_float, temp_float, temp_float)
#             histo[city] = [1, temp_float, temp_float, temp_float]
#     print(f"[worker {worker_id}] done")
#     return histo


def do_some_processing(worker_id, start_byte, end_byte):
    with open(FILE_PATH, "r+b") as file:
        mmapped_file = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)
        # Process the chunk here using mmapped_file[start_byte:end_byte]
        # Ensure to decode and handle the byte string as needed
        print(f"[worker {worker_id}] Processing chunk from {start_byte} to {end_byte}")
        chunk = mmapped_file[start_byte:end_byte].decode("utf-8")
        print(f"[worker {worker_id}] Decoded, calculating histo")
        histo = {}
        # iterate over lines in chunk
        for line in chunk.splitlines():
            idx = line.find(";")
            if idx == -1:
                continue

            city = line[:idx]
            temp_float = float(line[idx + 1 : idx + 11])

            # line_count += 1
            if city in histo:
                item = histo[city]
                item[0] += 1
                item[1] += temp_float
                item[2] = max(temp_float, item[2])
                item[3] = min(temp_float, item[3])
            else:
                histo[city] = [1, temp_float, temp_float, temp_float]

        print(f"[worker {worker_id}] Calculated histo")
        mmapped_file.close()

    return histo


def read_file_in_chunks(file_path=FILE_PATH):
    file_size = os.path.getsize(file_path)
    base_chunk_size = file_size // CPU_COUNT
    pool = multiprocessing.Pool(processes=CPU_COUNT)
    results = []
    worker_id = 0

    with open(file_path, "r+b") as file:
        mmapped_file = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)

        start_byte = 0
        for _ in range(CPU_COUNT):
            end_byte = min(start_byte + base_chunk_size, file_size)

            # Adjust end_byte to end on a complete line
            while (
                end_byte < file_size and mmapped_file[end_byte : end_byte + 1] != b"\n"
            ):
                end_byte += 1

            if end_byte < file_size:
                end_byte += 1  # Include the newline character

            # Add processing task to the pool
            worker_id += 1
            results.append(
                pool.apply_async(do_some_processing, (worker_id, start_byte, end_byte))
            )

            start_byte = end_byte  # Next chunk starts where the previous one ended

        mmapped_file.close()

    pool.close()
    pool.join()
    for result in results:
        print(result.get())


# do_stuff()
# lp = LineProfiler()
# lp.add_function(read_file_in_chunks)
# lp.run("do_stuff()")
# lp.print_stats()

if __name__ == "__main__":
    read_file_in_chunks()
