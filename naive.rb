# frozen_string_literal: true

require_relative "shared/shared"

# OUTPUT_INTERVAL_READING = 500_000
# OUTPUT_INTERVAL_AVERAGING = 250
# ROW_LIMIT = 10_000_000

OUTPUT_INTERVAL_READING = 10_000_000
OUTPUT_INTERVAL_AVERAGING = 100
ROW_LIMIT = 999_999_999_999

# "Globals"
Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)
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

  break if count == ROW_LIMIT
end
puts elapsed("reading complete", count, start_time)

# iterate over histogram and calculate averages
count = 0
ave_start_time = Time.now.utc
histogram.each_value do |measurement|
  count += 1
  puts elapsed("averaging", count, avg_start_time) if count % OUTPUT_INTERVAL_AVERAGING == 0
  measurement.average_temp = measurement.sum_temp / measurement.qty
end
puts elapsed("averaging complete", count, start_time)

puts "--- results (#{(Time.now.utc - start_time).round(1)}s) ---"
puts "histogram[\"Tamale\"]: #{histogram['Tamale']}"
puts "length: #{histogram.length}"
