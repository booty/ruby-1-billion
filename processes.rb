require "drb/drb"
require_relative "shared/shared"

NUM_WORKERS = 4
END_OF_WORK = :end_of_work

start_time = Time.now.utc
Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)
DRB_URI = "druby://localhost:9999"
queue = Queue.new
DRb.start_service(DRB_URI, queue)

# OUTPUT_INTERVAL_READING = 500_000
# OUTPUT_INTERVAL_AVERAGING = 250
# ROW_LIMIT = 10_000_000

OUTPUT_INTERVAL_READING = 500_000
OUTPUT_INTERVAL_WORKING = 500_000
OUTPUT_INTERVAL_AVERAGING = 100
ROW_LIMIT = 9999999999999
MAX_QUEUE_SIZE = 5000
BUF_SIZE = 50000

# Create worker processes
workers = NUM_WORKERS.times.map do
  fork do
    DRb.start_service
    pid = Process.pid
    start_time = Time.now.utc

    puts "[#{pid}] starting worker"
    remote_queue = DRbObject.new_with_uri(DRB_URI)
    histogram = {}
    local_count = 0

    loop do
      next unless (lines = remote_queue.pop)

      # puts "[#{pid}] popped #{lines.length} lines"
      # puts "[#{pid}] popped #{lines}"
      if lines == END_OF_WORK
        puts "[#{pid}] finished, breaking. local_count:#{local_count}"
        break
      end

      lines.each do |line|
        local_count += 1
        puts elapsed("worker #{pid}", local_count, start_time) if local_count % OUTPUT_INTERVAL_WORKING == 0
        city, temp = line.split(";")
        temp_float = temp.to_f
        if (item = histogram[city])
          item.qty += 1
          item.sum_temp += temp_float
          if temp_float > item.max_temp
            item.max_temp = temp_float
            next
          end
          item.min_temp = temp_float if temp_float < item.min_temp
        else
          histogram[city] = Measurement.new(1, temp_float, temp_float, temp_float)
        end
      end

      lines.clear
    end

    # Send histogram back to the parent process, e.g., via DRb, pipe, etc.
  end
end

read_count = 0
enqueue_count = 0
buff = []
File.foreach(FILE_PATH) do |line|
  next if queue.length == MAX_QUEUE_SIZE

  # queue << line
  # puts "reader: #{line}"
  read_count += 1
  buff << line
  break if read_count == ROW_LIMIT

  if buff.length == BUF_SIZE
    queue << buff
    enqueue_count += buff.length
    buff = []
  end

  puts elapsed("reading", read_count, start_time) if read_count % OUTPUT_INTERVAL_READING == 0
end
queue << buff
enqueue_count += buff.length
puts "Done reading, enqueued #{enqueue_count} lines"
NUM_WORKERS.times { queue << END_OF_WORK }

# Wait for all worker processes to finish
workers.each { |pid| Process.wait(pid) }

# Collect and merge histograms from all processes
# ...

# Clean up DRb service
DRb.stop_service

puts elapsed("complete", nil, start_time)
puts "read_count:#{read_count} enequeue_count:#{enqueue_count}"
