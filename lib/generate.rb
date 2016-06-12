def generate(filename, lines)
  File.open(filename, "w") do |out|
    it = 0

    while it < lines
      out.write("line #{it},some text,other text,and one more\n")

      it += 1
    end
  end
end

generate(ARGV[0], ARGV[1].to_i)
