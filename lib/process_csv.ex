defmodule ProcessCsv do
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

  def main([ filename, "read" ]) do
    File.write filename <> ".out",
      File.read!(filename)
      |> String.splitter("\n", trim: true)
      |> Enum.filter(&filter_line/1)
      |> Enum.join("\n")
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
end
