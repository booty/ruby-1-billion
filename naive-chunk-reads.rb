# frozen_string_literal: true

FILE_PATH = "../1brc/measurements.txt"

# OUTPUT_INTERVAL_READING = 500_000
# OUTPUT_INTERVAL_AVERAGING = 250
# ROW_LIMIT = 10_000_000

OUTPUT_INTERVAL_READING = 10_000_000
OUTPUT_INTERVAL_AVERAGING = 500
ROW_LIMIT = 999_999_999_999

def commas(number)
  number.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
end

def elapsed(label, count, start_time)
  elapsed = Time.now.utc - start_time

  rate_per_sec = commas((1.0 / (elapsed / count)).round(1))

  "#{label} | count:#{commas(count)} elapsed:#{commas(elapsed.round(1))}s (#{rate_per_sec}/s)"
end

# "Globals"
Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)
histogram = {}
count = 0
start_time = Time.now.utc

CHUNK_SIZE = 1024 * 1024 * 50 # 1MB

File.open(FILE_PATH, "r") do |file|
  leftover = ""
  while chunk = file.read(CHUNK_SIZE)
    chunk.prepend(leftover)
    lines = chunk.split("\n")

    # Save the last line if it's incomplete and prepend it to the next chunk
    leftover = lines.last[-1] == "\n" ? "" : lines.pop

    lines.each do |line|
      count += 1
      puts elapsed("reading", count, start_time) if count % OUTPUT_INTERVAL_READING == 0

      city, temp = line.split(";")
      temp_float = temp.to_f

      if (item = histogram[city])
        item.qty += 1
        item.sum_temp += temp_float
        item.max_temp = temp_float if temp_float > item.max_temp
        item.min_temp = temp_float if temp_float < item.min_temp
      else
        histogram[city] = Measurement.new(1, temp_float, temp_float, temp_float)
      end

      break if count == ROW_LIMIT
    end
    break if count == ROW_LIMIT
  end
end
puts elapsed("reading complete", count, start_time)

# iterate over histogram and calculate averages
count = 0
histogram.each_value do |measurement|
  count += 1
  puts elapsed("averaging", count, start_time) if count % OUTPUT_INTERVAL_AVERAGING == 0
  measurement.average_temp = measurement.sum_temp / measurement.qty
end
puts elapsed("averaging complete", count, start_time)

puts "--- results (#{(Time.now.utc - start_time).round(1)}s) ---"
puts "histogram[\"Tamale\"]: #{histogram['Tamale']}"
puts "length: #{histogram.length}"
