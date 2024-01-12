# Takes about 1:02 to run on my machine
# Macbook Pro M1 Max (10 cores) 64GB RAM Sonoma 14.1.2
# Python 3.12.... runtime is 1:02m
# pypy3.10-7.3.14.... runtime is 19.8s (!!!!!!)

import mmap
import os
import multiprocessing

# from line_profiler import LineProfiler

FILE_PATH = "../1brc/measurements.txt"
MILLION = 1000000
HUNDRED_MILLION = MILLION * 100
BILLION = MILLION * MILLION
LINE_LIMIT = BILLION
OUTPUT_EVERY_LINES = MILLION  # LINE_LIMIT // 1000
CHUNK_SIZE_BYTES = 1024 * 1024 * 100
CPU_COUNT = os.cpu_count()
FILE_SIZE_BYTES = os.path.getsize(FILE_PATH)
MMAP_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
SHOULD_PROFILE = False
CITY_COUNT = 0
CITY_SUM = 1
CITY_MAX = 2
CITY_MIN = 3
CITY_AVG = 4


def process_line(line, histo):
    decoded_line = line.decode("utf-8")
    idx = decoded_line.find(";")
    if idx == -1:
        return

    city = decoded_line[:idx]
    temp_float = float(decoded_line[idx + 1 : idx + 11])

    if city in histo:
        item = histo[city]
        item[CITY_COUNT] += 1
        item[CITY_SUM] += temp_float
        if temp_float > item[2]:
            item[CITY_MAX] = temp_float
            next
        if temp_float < item[3]:
            item[CITY_MIN] = temp_float
    else:
        histo[city] = [1, temp_float, temp_float, temp_float, 0]


# Will get OS errors if mmap offset is not aligned to page size
def align_offset(offset, page_size):
    return (offset // page_size) * page_size


def do_some_processing(worker_id, start_byte, end_byte):
    aligned_offset = align_offset(start_byte, MMAP_PAGE_SIZE)
    line_count = 0
    histo = {}

    with open(FILE_PATH, "rb") as file:
        length = end_byte - start_byte

        print(
            f"[worker {worker_id}] opened file, will mmap {length} bytes from offset {start_byte} (length {length}) MMAP_PAGE_SIZE: {MMAP_PAGE_SIZE}"
        )

        mmapped_file = mmap.mmap(
            file.fileno(),
            length,
            access=mmap.ACCESS_READ,
            offset=aligned_offset,
        )

        mmapped_file.seek(start_byte - aligned_offset)

        for line in iter(mmapped_file.readline, b""):
            # line_count += 1
            # if line_count % 10000000 == 0:
            #     print(f"[worker {worker_id}] Processing line #{line_count}")
            # if line_count > 1000:
            #     return histo
            process_line(line, histo)
        mmapped_file.close()
    print(f"[worker {worker_id}] Done")
    return histo


def do_some_processing_profile(worker_id, start_byte, end_byte):
    if worker_id == 1 and SHOULD_PROFILE:
        lp = LineProfiler()
        lp.add_function(do_some_processing)
        lp.add_function(process_line)
        with lp:
            result = do_some_processing(worker_id, start_byte, end_byte)
        lp.print_stats()
        return result
    else:
        return do_some_processing(worker_id, start_byte, end_byte)


def reduce_histos(histos):
    final_histo = {}
    for histo in histos:
        for city, item in histo.items():
            if city in final_histo:
                final_histo[city][CITY_COUNT] += item[CITY_COUNT]
                final_histo[city][CITY_SUM] += item[CITY_SUM]
                final_histo[city][CITY_MAX] = max(
                    final_histo[city][CITY_MAX], item[CITY_MAX]
                )
                final_histo[city][CITY_MIN] = min(
                    final_histo[city][CITY_MIN], item[CITY_MIN]
                )
            else:
                final_histo[city] = item
    for city, item in final_histo.items():
        item[CITY_AVG] = item[CITY_SUM] / item[CITY_COUNT]
    return final_histo


def read_file_in_chunks(file_path=FILE_PATH):
    base_chunk_size = FILE_SIZE_BYTES // CPU_COUNT
    pool = multiprocessing.Pool(processes=CPU_COUNT)
    results = []
    worker_id = 0

    print(
        f"FILE_SIZE_BYTES: {FILE_SIZE_BYTES} CPU_COUNT: {CPU_COUNT} CHUNK_SIZE_BYTES: {base_chunk_size}"
    )

    with open(file_path, "r+b") as file:
        mmapped_file = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)

        start_byte = 0
        for _ in range(CPU_COUNT):
            end_byte = min(start_byte + base_chunk_size, FILE_SIZE_BYTES)

            # Adjust end_byte to end on a complete line
            while (
                end_byte < FILE_SIZE_BYTES
                and mmapped_file[end_byte : end_byte + 1] != b"\n"
            ):
                end_byte += 1

            if end_byte < FILE_SIZE_BYTES:
                end_byte += 1  # Include the newline character

            worker_id += 1
            results.append(
                pool.apply_async(
                    do_some_processing_profile, (worker_id, start_byte, end_byte)
                )
            )

            start_byte = end_byte  # Next chunk starts where the previous one ended

        mmapped_file.close()

    pool.close()
    pool.join()
    histos = []
    for result in results:
        histos.append(result.get())

    print(reduce_histos(histos))


if __name__ == "__main__":
    read_file_in_chunks()
