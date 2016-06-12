def stream(filename)
  File.open("#{filename}.out", "w") do |out|
    File.foreach(filename) do |line|
      out.write(line) if filter_line(line)
    end
  end
end

def read(filename)
  File.write "#{filename}.out",
    File.readlines(filename)
        .select { |line| filter_line(line) }
        .join
end

def filter_line(line)
  first_col = line.split(",").first
  num = first_col.match(/\d+$/)[0].to_i

  num % 2 == 0 || num % 5 == 0
end

case ARGV[1]
when "stream"
  stream(ARGV[0])
when "read"
  read(ARGV[0])
end
