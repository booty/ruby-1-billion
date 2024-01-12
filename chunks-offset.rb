# frozen_string_literal: true

# WIP, obviously! This does not actually merge the histograms yet (however, that should not take more than 1s)
# Execution time on my MBP M1 Max (10 cores):
#   Ruby 3.3 with YJIT:  ~45sec
#   Ruby 3.3 without YJIT:  ~60sec

require "etc"

if defined?(RubyVM::YJIT.enable)
  puts "👍 YJIT is enabled"
  RubyVM::YJIT.enable
else
  puts "⚠️  YJIT is not enabled. Enabling it will give a ~25-50% speedup for Ruby 3.3+ here."
end

FILE_PATH = "../1brc/measurements.txt"
CPU_COUNT = Etc.nprocessors
FILE_SIZE_BYTES = File.size(FILE_PATH)
# CHUNK_SIZE_BYTES = 1024 * 1024 * 50
CHUNK_SIZE_BYTES = (FILE_SIZE_BYTES / CPU_COUNT) + 1000
CHUNK_LIMIT = 999_999_999
OUTPUT_INTERVAL_READING = 1
OUTPUT_INTERVAL_WORKING_LINES = 5000

Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp)
ChunkResult = Struct.new(:line_count, :histo)

def process_chunk_histo(chunk)
  start_time = Time.now.utc
  line_count = 0
  histogram = {}
  string_time = 0
  hash_time = 0
  # chunk = chunk.delete('.')
  chunk.each_line do |line|
    # string_start_time = Time.now.utc
    city, temp = line.split(";")
    next if temp.nil?

    temp_float = temp.to_f
    city = city.to_sym
    line_count += 1
    # string_time += Time.now.utc - string_start_time

    # hash_start_time = Time.now.utc
    if (item = histogram[city])
      item.qty += 1
      item.sum_temp += temp_float
      item.max_temp = temp_float if temp_float > item.max_temp
      item.min_temp = temp_float if temp_float < item.min_temp
    else
      histogram[city] = Measurement.new(1, temp_float, temp_float, temp_float)
    end
    # hash_time += Time.now.utc - hash_start_time
  end
  puts "[#{Process.pid}] done; processed #{line_count} lines. elapsed: #{(Time.now.utc - start_time).round(1)}s, string_time: #{string_time.round(1)}s, hash_time: #{hash_time.round(1)}s"
  ChunkResult.new(line_count, histogram)
end

offset = 0
chunk_count = 0
reader_line_count = 0
writer_line_count = 0
start_time = Time.now.utc
line_time = 0

# Store child process ids
child_processes = {}

puts("FILE_SIZE_BYTES: #{FILE_SIZE_BYTES} CPU_COUNT: #{CPU_COUNT} CHUNK_SIZE_BYTES: #{CHUNK_SIZE_BYTES}")
while offset < FILE_SIZE_BYTES && (chunk_count < CHUNK_LIMIT)
  chunk = IO.read(FILE_PATH, CHUNK_SIZE_BYTES, offset)
  chunk_count += 1

  last_newline_index = chunk.rindex("\n")
  offset -= (chunk.length - last_newline_index) + 1
  processed_chunk = chunk[0..last_newline_index]

  reader, writer = IO.pipe
  pid = fork do
    reader.close
    writer.puts(process_chunk_histo(processed_chunk).line_count)
    writer.close
  end
  writer.close
  child_processes[pid] = reader

  offset += CHUNK_SIZE_BYTES
  reader_line_count += processed_chunk.count("\n") # todo: disable, probably slow
  if chunk_count % OUTPUT_INTERVAL_READING == 0
    puts "chunks:#{chunk_count} " \
         "offset: #{offset}/#{FILE_SIZE_BYTES} (#{(offset.to_f / FILE_SIZE_BYTES.to_f * 100).round(2)}%) " \
         "reader_line_count: #{reader_line_count} " \
         "writer_line_count: #{writer_line_count} " \
         "elapsed: #{(Time.now.utc - start_time).round(1)}s"
  end

  # Non-blocking check for finished child processes
  child_processes.each do |child_pid, child_reader|
    line_count = child_reader.read_nonblock(1024).to_i
    writer_line_count += line_count
    child_reader.close
    Process.wait(child_pid)
    child_processes.delete(child_pid)
  rescue IO::WaitReadable
    # Child process not finished yet
  end
end

# Wait for all child processes to finish
puts("waiting for child processes to finish")
child_processes.each do |pid, reader|
  writer_line_count += reader.read.to_i
  reader.close
  Process.wait(pid)
end

puts "chunks:#{chunk_count} " \
     "reader_line_count: #{reader_line_count} " \
     "writer_line_count: #{writer_line_count} " \
     "elapsed: #{(Time.now.utc - start_time).round(1)}s"
