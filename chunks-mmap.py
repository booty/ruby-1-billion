import mmap
from line_profiler import LineProfiler

FILE_PATH = "../1brc/measurements.txt"
MILLION = 1000000
HUNDRED_MILLION = MILLION * 100
BILLION = MILLION * MILLION
LINE_LIMIT = BILLION
OUTPUT_EVERY_LINES = MILLION  # LINE_LIMIT // 1000
CHUNK_SIZE_BYTES = 1024 * 1024 * 100


def process_chunk(undecoded_chunk):
    # decoded_chunk = undecoded_chunk.decode("utf-8")
    return 42


def read_file_in_chunks(file_path, chunk_size=4096):
    line_count = 0

    with open(file_path, "r+b") as file:
        mmapped_file = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)
        buffer = b""  # Keep buffer as bytes

        while line_count < LINE_LIMIT:
            chunk = mmapped_file.read(chunk_size)
            if not chunk:
                break

            chunk = buffer + chunk

            # Find the index of the last newline character
            last_newline_index = chunk.rfind(b"\n")

            # If no newline character is found, continue reading more of the file
            if last_newline_index == -1:
                buffer = chunk
                continue

            # Put the last incomplete line in the buffer
            buffer = chunk[last_newline_index + 1 :]

            complete_lines_chunk = chunk[:last_newline_index]

            process_chunk(complete_lines_chunk)

        if buffer:
            # Process the remaining buffer
            process_chunk(buffer)

        mmapped_file.close()


def do_stuff():  # Example usage
    read_file_in_chunks(FILE_PATH)


do_stuff()
# lp = LineProfiler()
# lp.add_function(read_file_in_chunks)
# lp.run("do_stuff()")
# lp.print_stats()
