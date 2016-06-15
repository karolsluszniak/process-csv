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
    out_file = File.open!(filename <> ".out", [:write, :raw, :delayed_write])

    File.stream!(filename, [], 4000)
    |> Enum.reduce({"", nil, out_file}, &handle_chunk_without_split/2)

    File.close(out_file)
  end

  defp handle_chunk(chunk, {unfinished_line, file}) do
    (unfinished_line <> chunk)
    |> String.split("\n")
    |> process_lines(file)
  end

  defp process_lines([unfinished_line], file), do: {unfinished_line, file}
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

  defp handle_chunk_without_split(chunk, {line, filter, file}) do
    process_and_filter_chunk(chunk, line, filter, file)
  end
  defp process_and_filter_chunk(<<>>, line, filter, file) do
    {line, filter, file}
  end
  defp process_and_filter_chunk(<<?\n::utf8, rest::binary>>, line, true, file) do
    IO.binwrite(file, line <> "\n")
    process_and_filter_chunk(rest, "", nil, file)
  end
  defp process_and_filter_chunk(<<?\n::utf8, rest::binary>>, _line, false, file) do
    process_and_filter_chunk(rest, "", nil, file)
  end
  defp process_and_filter_chunk(<<c::utf8, ?,::utf8, rest::binary>>, line, nil, file)
  when c in [?0, ?2, ?4, ?5, ?6, ?8] do
    process_and_filter_chunk(rest, line <> <<c>> <> <<?,>>, true, file)
  end
  defp process_and_filter_chunk(<<_c::utf8, ?,::utf8, rest::binary>>, _line, nil, file) do
    process_and_filter_chunk(rest, nil, false, file)
  end
  defp process_and_filter_chunk(<<_c::utf8, rest::binary>>, _line, false, file) do
    process_and_filter_chunk(rest, nil, false, file)
  end
  defp process_and_filter_chunk(<<c::utf8, rest::binary>>, line, filter, file) do
    process_and_filter_chunk(rest, line <> <<c>>, filter, file)
  end
end
