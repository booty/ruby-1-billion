from line_profiler import LineProfiler

FILE_PATH = "../1brc/measurements.txt"
MILLION = 1000000
HUNDRED_MILLION = MILLION * 100
BILLION = MILLION * MILLION
LINE_LIMIT = BILLION
OUTPUT_EVERY_LINES = MILLION  # LINE_LIMIT // 1000
CHUNK_SIZE_BYTES = 1024 * 1024 * 100


def process_chunk(lines):
    return 42


def read_file_in_chunks(file_path, chunk_size=4096):
    line_count = 0
    # next_line_count_milestone = OUTPUT_EVERY_LINES

    with open(file_path, "r") as file:
        buffer = ""
        while True and (line_count < LINE_LIMIT):
            chunk = file.read(chunk_size)
            if not chunk:
                # No more data to read
                break

            chunk = buffer + chunk  # Add the previous incomplete line

            # find the index of the last newline character
            last_newline_index = chunk.rfind("\n")

            # put the last incomplete line in the buffer
            buffer = chunk[last_newline_index + 1 :]

            lines = chunk[:last_newline_index]

            # line_count += lines.count("\n")
            # if line_count >= next_line_count_milestone:
            #     print(f"processing line #{line_count}")
            #     next_line_count_milestone += OUTPUT_EVERY_LINES

            process_chunk(lines)


def do_stuff():  # Example usage
    read_file_in_chunks(FILE_PATH)


lp = LineProfiler()
lp.add_function(read_file_in_chunks)
lp.run("do_stuff()")
lp.print_stats()
