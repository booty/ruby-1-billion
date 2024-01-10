require_relative "shared/shared"
require "pry-byebug"

READ_CHUNK_SIZE_BYTES = 1024 * 1024 * 50 # 1MB
bytes_read = 0
chunks_read = 0

CHUNK_SIZE_BYTES = 1024 * 1024 * 50
CHUNK_LIMIT = 999_999_999_999
file_size = File.size(FILE_PATH)

offset = 0
chunk_count = 0
while offset < file_size # && (chunk_count < CHUNK_LIMIT)
  chunk = IO.read(FILE_PATH, CHUNK_SIZE_BYTES, offset)

  # find the last newline in the chunk
  last_newline_index = chunk.rindex("\n")

  # rewind offset to the character after the last newline
  offset -= (chunk.length - last_newline_index)

  # puts "*** raw chunk: #{chunk}"
  # fixed_chunk = chunk[0..last_newline_index]
  # puts "*** fixed chunk: #{fixed_chunk}"

  chunk_count += 1
  offset += CHUNK_SIZE_BYTES
  if chunk_count % 100000 == 0
    puts "*** chunks:#{chunk_count} offset: #{offset}/#{file_size}"
  end
end
puts "*** chunks:#{chunk_count} offset: #{offset}/#{file_size}"
