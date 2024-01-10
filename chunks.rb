require_relative "shared/shared"
require "pry-byebug"

READ_CHUNK_SIZE_BYTES = 1024 * 1024 * 50 # 1MB
bytes_read = 0
chunks_read = 0

CHUNK_SIZE_BYTES = 1024 * 1024 * 50
CHUNK_LIMIT = 10 # 999_999_999
FILE_SIZE = File.size(FILE_PATH)

OUTPUT_INTERVAL_READING = 5
OUTPUT_INTERVAL_WORKING_LINES = 5000
NUM_WORKERS = 1
END_OF_CHUNK = :end_of_chunk
END_OF_WORK = :end_of_work
workers = []
worker_pipes = []

Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)

def non_blocking_write(pipe, data)
  pipe.write_nonblock(data)
rescue IO::WaitWritable
  pipe.wait_writable
  retry
end

# worker processes
NUM_WORKERS.times do
  parent_reader, child_writer = IO.pipe
  child_reader, parent_writer = IO.pipe
  pid = fork do
    parent_reader.close
    parent_writer.close
    buffer = ""
    local_line_count = 0
    pid = Process.pid
    loop do
      # if child_reader.wait_readable(1).nil?
      #   puts "[#{pid}] child_reader not ready, sleeping"
      #   sleep 1
      #   next
      # end

      begin
        # Non-blocking read from the pipe
        chunk = child_reader.read_nonblock(CHUNK_SIZE_BYTES)
        buffer << chunk
        puts "[#{pid}] read #{chunk.length} bytes"
      rescue IO::WaitReadable
        # No data to read right now, can optionally sleep or do other tasks
        puts "[#{pid}] IO::WaitReadable, sleeping"
        sleep(0.1)
      rescue EOFError
        puts("[#{pid}] EOFError, breaking (local_line_count: #{local_line_count})")
        # break
      end

      # Process complete lines in the buffer
      if buffer.include?(END_OF_WORK.to_s)
        puts("[#{pid}] END_OF_WORK (local_line_count: #{local_line_count})")
        break
      end
      while line = buffer.slice!(/.*\n/)

        local_line_count += 1
        if local_line_count % OUTPUT_INTERVAL_WORKING_LINES == 0
          puts "[#{pid}] local_line_count: #{local_line_count}"
        end
        # ...
      end
    end

    puts "[#{pid}] we're done? local_line_count: #{local_line_count}"
  end

  child_reader.close
  child_writer.close

  workers << pid
  worker_pipes << [parent_reader, parent_writer, child_reader, child_writer]
end

offset = 0
chunk_count = 0
current_worker_num = 0
reader_line_count = 0

while offset < FILE_SIZE && (chunk_count < CHUNK_LIMIT)
  chunk = IO.read(FILE_PATH, CHUNK_SIZE_BYTES, offset)

  # find the last newline in the chunk
  last_newline_index = chunk.rindex("\n")

  # rewind offset to the character after the last newline
  offset -= (chunk.length - last_newline_index)

  # puts "*** raw chunk: #{chunk}"
  # fixed_chunk = chunk[0..last_newline_index]
  # puts "*** fixed chunk: #{fixed_chunk}"
  pipe = worker_pipes[current_worker_num][1]
  fixed_chunk = chunk[0..last_newline_index]
  reader_line_count += fixed_chunk.count("\n")
  non_blocking_write(pipe, fixed_chunk)
  # non_blocking_write(pipe, END_OF_CHUNK)
  current_worker_num = (current_worker_num + 1) % NUM_WORKERS

  chunk_count += 1
  offset += CHUNK_SIZE_BYTES
  if chunk_count % OUTPUT_INTERVAL_READING == 0
    puts "*** chunks:#{chunk_count} offset: #{offset}/#{FILE_SIZE} (#{(offset.to_f / FILE_SIZE.to_f * 100).round(2)}%) reader_line_count: #{reader_line_count}"
  end
end
puts "*** chunks:#{chunk_count} offset: #{offset}/#{FILE_SIZE} reader_line_count: #{reader_line_count}"

# Signal end of work and close pipes
worker_pipes.each do |_, parent_writer, _, _|
  non_blocking_write(parent_writer, "\n" + END_OF_WORK.to_s + "\n")
end

# Wait for all worker processes to finish
puts "waiting for workers to finish"
workers.each { |pid| Process.wait(pid) }

# Close the remaining pipe ends in the parent process
worker_pipes.each do |parent_reader, parent_writer|
  parent_reader.close
  parent_writer.close
end
