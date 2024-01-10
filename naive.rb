# frozen_string_literal: true

FILE_PATH = "../1brc/measurements.txt"
OUTPUT_INTERVAL_READING = 500_000
OUTPUT_INTERVAL_AVERAGING = 100
ROW_LIMIT = 1_000_000

# declare a struct
Measurement = Struct.new(:qty, :max_temp, :min_temp, :sum_temp, :average_temp)

histogram = {}
count = 0

# Get current time
start_time = Time.now.utc

def elapsed(label, count, start_time)
  elapsed = Time.now.utc - start_time
  "#{label} count:#{count} elapsed:#{elapsed.round(1)} (#{(1 / (elapsed / count)).round(1).to_s.reverse.gsub(
    /(\d{3})(?=\d)/, '\\1,'
  ).reverse} per second)"
end

# Note: takes 105s to read every line in the file even if we do no processing
File.open(FILE_PATH, "r").each_line do |_line|
  count += 1
  puts elapsed("reading", count, start_time) if count % OUTPUT_INTERVAL_READING == 0

  city, temp = _line.split(";")
  temp_float = temp.to_f
  if histogram[city]
    histogram[city].qty += 1
    histogram[city].max_temp = temp_float if temp_float > histogram[city].max_temp
    histogram[city].min_temp = temp_float if temp_float < histogram[city].min_temp
    histogram[city].sum_temp += temp_float
  else
    histogram[city] = Measurement.new(1, temp_float, temp_float, temp_float)
  end

  break if count == ROW_LIMIT
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

puts "--- results ---"
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
