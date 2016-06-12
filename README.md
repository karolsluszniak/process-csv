This is a source code for the article [Elixir vs Ruby: File I/O performance](http://cloudless.pl/articles/12-elixir-vs-ruby-file-i-o-performance) that you can find on the [Phoenix on Rails blog](http://cloudless.pl/articles?series=phoenix-on-rails). It's basically a sample text file processing script implemented in Elixir and Ruby that does the following:

1. Loads the input CSV, line by line.
2. Parses first column which is of format Some text N.
3. Leaves only those lines where N is dividable by 2 or 5.
4. Saves those filtered, but unchanged lines into another CSV.

It does so both in a streaming manner, which is slower but works with all file sizes, and as a faster but less secure and less universal one-shot read.

## Generating samples

You can generate sample CSV file of given size, compilant with the algorithm, like this:

```sh
ruby lib/generate.rb sample-500k.csv 500000
```

## Running benchmarks

Elixir version:

```sh
MIX_ENV=prod mix escript.build
time ./process_csv sample-500k.csv [read | stream]
```

Ruby version:

```sh
time ruby lib/process_csv.rb sample-500k.csv [read | stream]
```

## Improvements

Please look into the [article](http://cloudless.pl/articles/12-elixir-vs-ruby-file-i-o-performance) to see which optimizations I've tried. Open Pull Request if you've found a better way.
