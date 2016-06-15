defmodule ProcessCsv do
  def main([ filename, "read" ]) do
    File.write filename <> ".out",
      File.read!(filename)
      |> String.splitter("\n", trim: true)
      |> Enum.filter(&filter_line/1)
      |> Enum.join("\n")
  end

  def main([ filename, "stream-initial" ]) do
    File.open! filename, [:read], fn pid ->
      pid
      |> IO.stream(:line)
      |> Enum.filter(&filter_line/1)
      |> Stream.into(File.stream!(filename <> ".out"))
      |> Stream.run
    end
  end

  def main([ filename, "stream" ]) do
    File.stream!(filename, read_ahead: 10_000_000)
    |> Stream.filter(&filter_line/1)
    |> Stream.into(File.stream!(filename <> ".out", [:delayed_write]))
    |> Stream.run
  end

  def main([ filename, "stream-chunks" ]) do
    out_file = File.open!(filename <> ".out", [:write, :raw, :delayed_write])

    File.stream!(filename, [], 4000)
    |> Enum.reduce({"", out_file}, &handle_chunk/2)

    File.close(out_file)
  end

  def main([ filename, "stream-chunks-nosplit" ]) do
    out_file = File.open!(filename <> ".out", [:write, :raw, {:delayed_write, 5_000_000, 50}])
    newline_pattern = :binary.compile_pattern("\n")

    File.stream!(filename, [], 1_000_000)
    |> Enum.reduce({nil, "", out_file, newline_pattern}, &handle_chunk_without_split/2)

    File.close(out_file)
  end

  defp handle_chunk(chunk, {unfinished_line, file}) do
    (unfinished_line <> chunk)
    |> String.split("\n")
    |> process_lines(file)
  end

  defp process_lines([unfinished_line], file) do
    {unfinished_line, file}
  end
  defp process_lines([line | rest], file) do
    if filter_line(line) do
      IO.binwrite(file, line <> "\n")
    end
    process_lines(rest, file)
  end

  defp filter_line(<<c::utf8, ?,::utf8, _::binary>>)
    when c in [?0, ?2, ?4, ?5, ?6, ?8],
    do: true
  defp filter_line(<<_::utf8, ?,::utf8, _::binary>>),
    do: false
  defp filter_line(<<_other::utf8, rest::binary>>),
    do: filter_line(rest)

  ## Here's a much slower equivalent:

  # defp filter_line(line) do
  #   [ first_col | _ ] = String.split(line, ",")
  #
  #   num = Regex.run(~r/\d+$/, first_col)
  #         |> hd
  #         |> String.to_integer
  #
  #   rem(num, 2) == 0 || rem(num, 5) == 0
  # end

  defp handle_chunk_without_split(chunk, {state, line, file, newline_pattern}) do
    case state do
      :write -> process_chunk_until_newline(chunk, line, file, newline_pattern)
      :skip  -> process_chunk_until_newline(chunk, false, file, newline_pattern)
      _      -> filter_chunk(chunk, line, file, newline_pattern)
    end
  end

  defp filter_chunk(<<>>, line, file, newline_pattern) do
    {nil, line, file, newline_pattern}
  end
  defp filter_chunk(<<c::utf8, ?,::utf8, rest::binary>>, line, file, newline_pattern)
  when c in [?0, ?2, ?4, ?5, ?6, ?8] do
    process_chunk_until_newline(rest, line <> <<c>> <> <<?,>>, file, newline_pattern)
  end
  defp filter_chunk(<<_c::utf8, ?,::utf8, rest::binary>>, _line, file, newline_pattern) do
    process_chunk_until_newline(rest, false, file, newline_pattern)
  end
  defp filter_chunk(<<c::utf8, rest::binary>>, line, file, newline_pattern) do
    filter_chunk(rest, line <> <<c>>, file, newline_pattern)
  end

  defp process_chunk_until_newline(chunk, line, file, newline_pattern) do
    case :binary.match(chunk, newline_pattern) do
      {pos, _} ->
        offset = pos + 1
        if line, do: IO.binwrite(file, line <> :binary.part(chunk, 0, offset))
        filter_chunk(:binary.part(chunk, offset, byte_size(chunk) - offset), "", file, newline_pattern)
      :nomatch when line ->
        {:write, line <> chunk, file, newline_pattern}
      :nomatch ->
        {:skip, false, file, newline_pattern}
    end
  end
end
