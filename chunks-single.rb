# frozen_string_literal: true

require_relative "shared/shared"
require "pry-byebug"

CHUNK_SIZE_BYTES = 1024 * 1024 * 5
CHUNK_LIMIT = 999_999_999
FILE_SIZE = File.size(FILE_PATH)

OUTPUT_INTERVAL_READING = 50
OUTPUT_INTERVAL_WORKING_LINES = 5000

Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)

offset = 0
chunk_count = 0
current_worker_num = 0
reader_line_count = 0
start_time = Time.now.utc
line_time = 0

def process_chunk(chunk)
  chunk.count("\n")
end

while offset < FILE_SIZE && (chunk_count < CHUNK_LIMIT)
  chunk = IO.read(FILE_PATH, CHUNK_SIZE_BYTES, offset)
  chunk_count += 1

  # find the last newline in the chunk
  last_newline_index = chunk.rindex("\n")

  # rewind offset to the character after the last newline
  offset -= (chunk.length - last_newline_index) + 1

  line_start_time = Time.now.utc
  # chunk[0..last_newline_index].each_line do |_line|
  #   reader_line_count += 1
  # end
  reader_line_count += process_chunk(chunk[0..last_newline_index])
  line_time += (Time.now.utc - line_start_time)

  offset += CHUNK_SIZE_BYTES
  # if chunk_count % OUTPUT_INTERVAL_READING == 0
  #   puts "*** chunks:#{chunk_count} offset: #{offset}/#{FILE_SIZE} (#{(offset.to_f / FILE_SIZE.to_f * 100).round(2)}%) reader_line_count: #{reader_line_count} line_time: #{line_time.round(1)} elapsed: #{(Time.now.utc - start_time).round(1)}s"
  # end
end
puts "*** chunks:#{chunk_count} offset: #{offset}/#{FILE_SIZE} reader_line_count: #{reader_line_count} line_time: #{line_time.round(1)}s elapsed: #{(Time.now.utc - start_time).round(1)}s"
