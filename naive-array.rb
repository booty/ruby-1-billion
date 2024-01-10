# frozen_string_literal: true

# Note: this is actually slightly slower than naive.rb (with structs)
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
# Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)
QTY = 0
MAX = 1
MIN = 2
SUM = 3
AVG = 4

histogram = {}
count = 0
start_time = Time.now.utc

# Note: takes 105s to read every line in the file even if we do no processing
File.open(FILE_PATH, "r").each_line do |_line|
  count += 1
  puts elapsed("reading", count, start_time) if count % OUTPUT_INTERVAL_READING == 0

  city, temp = _line.split(";")
  temp_float = temp.to_f
  if (item = histogram[city])
    item[QTY] += 1
    item[MAX] = temp_float if temp_float > item[MAX]
    item[MIN] = temp_float if temp_float < item[MIN]
    item[SUM] += temp_float
  else
    histogram[city] = [1, temp_float, temp_float, temp_float, nil]
  end

  break if count == ROW_LIMIT
end
puts elapsed("reading complete", count, start_time)

# iterate over histogram and calculate averages
count = 0
histogram.each_value do |measurement|
  count += 1
  puts elapsed("averaging", count, start_time) if count % OUTPUT_INTERVAL_AVERAGING == 0
  measurement[AVG] = measurement[SUM] / measurement[QTY]
end
puts elapsed("averaging complete", count, start_time)

puts "--- results (#{(Time.now.utc - start_time).round(1)}s) ---"
puts "histogram[\"Tamale\"]: #{histogram['Tamale']}"
puts "length: #{histogram.length}"

# # stream FILE_PATH one line at a time
# File.open(FILE_PATH, "r").each_line do |_line|
#   count += 1
#   if count % 100_000 == 0
#     puts "count: #{count}"
#     city, temp = _line.split(";")
#     temp_float = temp.to_f

#   end
# end

# puts "count: #{count}"
