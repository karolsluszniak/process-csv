def generate(filename, lines, cols)
  line_tail = cols.times.map { |t| "addition #{t}" }.join(',')

  File.open(filename, "w") do |out|
    it = 0

    while it < lines
      out.write("line #{it},#{line_tail}\n")

      it += 1
    end
  end
end

generate(ARGV[0], ARGV[1].to_i, ARGV[2] ? ARGV[2].to_i : 3)
