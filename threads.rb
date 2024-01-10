# frozen_string_literal: true

require_relative "shared/shared"
# require "thread"

queue = Queue.new
NUM_WORKER_THREADS = 1
END_OF_WORK = :end_of_work

OUTPUT_INTERVAL_READING = 10_000_000
OUTPUT_INTERVAL_WORKING = 10_000_000
OUTPUT_INTERVAL_AVERAGING = 100
ROW_LIMIT = 999_999_999_999

# OUTPUT_INTERVAL_READING = 1_000_000
# OUTPUT_INTERVAL_WORKING = 1_000_000
# OUTPUT_INTERVAL_AVERAGING = 100
# ROW_LIMIT = 10000

Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)
start_time = Time.now.utc

reader_thread = Thread.new do
  count = 0
  File.foreach(FILE_PATH) do |line|
    puts elapsed("reading", count, start_time) if count % OUTPUT_INTERVAL_READING == 0
    count += 1
    queue << line

    if count == ROW_LIMIT
      NUM_WORKER_THREADS.times { queue << END_OF_WORK }
      break
    end
  end
  NUM_WORKER_THREADS.times { queue << END_OF_WORK }
end

workers = Array.new(NUM_WORKER_THREADS) do |_n|
  Thread.new do
    count = 0
    histogram = {}
    # Thread.current.name = rand(0..9999).to_s
    puts("[#{Thread.current.object_id}] hello!")

    loop do
      line = queue.pop
      unless line
        puts("[#{Thread.current.object_id}] queue.pop returned nil, continuing")
        next
      end
      if line == END_OF_WORK
        puts("[#{Thread.current.object_id}] finished, breaking. histogram.length:#{histogram.length}")
        break
      end

      count += 1
      puts elapsed("working (#{Thread.current.object_id})", count, start_time) if count % OUTPUT_INTERVAL_WORKING == 0

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

    histogram
  end
end

reader_thread.join
puts elapsed("reading complete", nil, start_time)

histograms = workers.map(&:value)
puts elapsed("working complete", nil, start_time)
# puts histograms
