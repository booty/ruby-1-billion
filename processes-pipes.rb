require_relative "shared/shared"

NUM_WORKERS = 8

start_time = Time.now.utc
Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)

# OUTPUT_INTERVAL_READING = 500_000
# OUTPUT_INTERVAL_AVERAGING = 250
# ROW_LIMIT = 10_000_000

OUTPUT_INTERVAL_READING = 500_000
OUTPUT_INTERVAL_WORKING = 500_000
OUTPUT_INTERVAL_AVERAGING = 100
ROW_LIMIT = 9999999999999
BUF_SIZE = 100000

workers = []
worker_pipes = []

NUM_WORKERS.times do
  parent_reader, child_writer = IO.pipe
  child_reader, parent_writer = IO.pipe

  pid = fork do
    parent_reader.close
    parent_writer.close

    pid = Process.pid
    start_time = Time.now.utc
    histogram = {}
    local_count = 0

    while line = child_reader.gets
      break if line.strip == END_OF_WORK.to_s

      local_count += 1
      puts elapsed("worker #{pid}", local_count, start_time) if local_count % OUTPUT_INTERVAL_WORKING == 0

      city, temp = line.split(";")
      temp_float = temp.to_f
      # ... Process data ...
    end

    child_reader.close
    child_writer.close

    # Send histogram back to the parent process (implement as needed)
  end

  workers << pid
  worker_pipes << [parent_reader, parent_writer, child_reader, child_writer]
end

read_count = 0
enqueue_count = 0
buff = []
current_worker_num = 0
File.foreach(FILE_PATH) do |line|
  read_count += 1
  buff << line
  break if read_count == ROW_LIMIT

  if buff.length == BUF_SIZE
    worker_pipes[current_worker_num][1].puts(buff.join)
    enqueue_count += buff.length
    buff = []
    current_worker_num = (current_worker_num + 1) % NUM_WORKERS
  end

  puts elapsed("reading", read_count, start_time) if read_count % OUTPUT_INTERVAL_READING == 0
end

# Signal end of work and close pipes
worker_pipes.each do |_, parent_writer, _, _|
  parent_writer.puts(END_OF_WORK)
  parent_writer.close
end

# Wait for all worker processes to finish
workers.each { |pid| Process.wait(pid) }

# Collect and merge histograms from all processes
# ...

puts elapsed("complete", nil, start_time)
puts "read_count:#{read_count} enequeue_count:#{enqueue_count}"
