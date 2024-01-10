# frozen_string_literal: true

FILE_PATH = "../1brc/measurements.txt"

if defined?(RubyVM::YJIT.enable)
  puts "👍 YJIT is enabled"
  RubyVM::YJIT.enable
else
  puts <<~PROTIP
    ⚠️  YJIT is not enabled. Enabling it will give a ~50% speedup for Ruby 3.3+ here.

    To compile Ruby with YJIT, you want something like the following. Substitute the latest Ruby version for 3.3.0.

       $ brew install rust
       $ RUBY_CONFIGURE_OPTS="--enable-yjit" rbenv install 3.3.0   # for rbenv
       $ RUBY_CONFIGURE_OPTS="--enable-yjit" asdf install ruby 3.3.0   # for asdf
  PROTIP
end

COMMA_REGEX = /(\d{3})(?=\d)/

def commas(number)
  number.to_s.reverse.gsub(COMMA_REGEX, '\\1,').reverse
end

def elapsed(label, count, start_time)
  elapsed = Time.now.utc - start_time

  if count
    rate_per_sec = commas((1.0 / (elapsed / count)).round(1))

    "#{label} | count:#{commas(count)} elapsed:#{commas(elapsed.round(1))}s (#{rate_per_sec}/s)"
  else
    "#{label} | elapsed:#{commas(elapsed.round(1))}s"
  end
end
