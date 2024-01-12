import mmap
import os
import multiprocessing
from collections import defaultdict
from collections import namedtuple
from line_profiler import LineProfiler

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

CityData = namedtuple("CityData", ["count", "total_temp", "max_temp", "min_temp"])


def process_line_citydata(line, histo):
    decoded_line = line.decode("utf-8")
    idx = decoded_line.find(";")
    if idx == -1:
        return

    city = decoded_line[:idx]
    temp_float = float(decoded_line[idx + 1 : idx + 11])

    item = histo[city]
    updated_item = CityData(
        count=item.count + 1,
        total_temp=item.total_temp + temp_float,
        max_temp=max(item.max_temp, temp_float),
        min_temp=min(item.min_temp, temp_float),
    )

    histo[city] = updated_item


def process_line(line, histo):
    decoded_line = line.decode("utf-8")
    idx = decoded_line.find(";")
    # if no semicolon is found, skip the line
    if idx == -1:
        return

    city = decoded_line[:idx]
    temp_float = float(decoded_line[idx + 1 : idx + 11])

    if city in histo:
        item = histo[city]
        item[0] += 1
        item[1] += temp_float
        if temp_float > item[2]:
            item[2] = temp_float
            next
        if temp_float < item[3]:
            item[3] = temp_float
    else:
        histo[city] = [1, temp_float, temp_float, temp_float]


def align_offset(offset, page_size):
    return (offset // page_size) * page_size


def do_some_processing(worker_id, start_byte, end_byte):
    print(f"[worker {worker_id}] Processing chunk from {start_byte} to {end_byte}")
    aligned_offset = align_offset(start_byte, MMAP_PAGE_SIZE)

    line_count = 0
    # histo = defaultdict(lambda: CityData(0, 0.0, float("-inf"), float("inf")))
    histo = {}

    with open(FILE_PATH, "rb") as file:
        length = end_byte - start_byte

        print(
            f"[worker {worker_id}] opened file, will mmap {length} bytes from offset {start_byte} (length {length}) MMAP_PAGE_SIZE: {MMAP_PAGE_SIZE}"
        )
        if end_byte > FILE_SIZE_BYTES:
            print(
                f"[worker {worker_id}] end_byte {end_byte} > FILE_SIZE_BYTES {FILE_SIZE_BYTES}"
            )
            raise Exception("end_byte > FILE_SIZE_BYTES")
        mmapped_file = mmap.mmap(
            file.fileno(),
            length,
            access=mmap.ACCESS_READ,
            offset=aligned_offset,
        )

        mmapped_file.seek(start_byte - aligned_offset)
        print(f"[worker {worker_id}] mapped file")

        for line in iter(mmapped_file.readline, b""):
            line_count += 1
            if line_count % 10000000 == 0:
                print(f"[worker {worker_id}] Processing line #{line_count}")
            process_line(line, histo)
        mmapped_file.close()
    print(f"[worker {worker_id}] Done")
    return histo


def do_some_processing_profile(worker_id, start_byte, end_byte):
    if worker_id == -1:
        lp = LineProfiler()
        lp.add_function(do_some_processing)
        lp.add_function(process_line)
        with lp:
            result = do_some_processing(worker_id, start_byte, end_byte)
        lp.print_stats()
        return result
    else:
        return do_some_processing(worker_id, start_byte, end_byte)


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

            # Add processing task to the pool
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
    for result in results:
        print(len(result.get()))


# do_stuff()
# lp = LineProfiler()
# lp.add_function(read_file_in_chunks)
# lp.run("read_file_in_chunks()")
# lp.print_stats()


if __name__ == "__main__":
    read_file_in_chunks()
