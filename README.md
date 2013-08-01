# Overview

This project implements basic disk-backed multi-way merge sort, with configurable input and output formats (i.e. not just textual sort).
It should be useful for systems that process large amounts of data, as a simple building block for sort phases.

# Documentation 

Checkout [project wiki](https://github.com/cowtowncoder/java-merge-sort/wiki) for more documentation, including javadocs.

# Usage

## Programmatic access

Main class to interact with is `com.fasterxml.sort.Sorter`, which needs to be constructed with four things:

* Configuration settings (default `SortConfig` works fine)
* `DataReaderFactory` which is used for creating readers for intermediate sort files (and input, if stream passed)
* `DataWriterFactory` which is used for creating writers for intermediate sort files (and results, if stream passed)
* `Comparator` for data items

An example of how this can be done can be found from `com.fasterxml.sort.std.TextFileSorter`.
Basic implementations exist for line-based text input (in package `com.fasterxml.sort.std`), and additional implementations may be added: for example, a JSON data sorter could be implement as an extension module of `Jackson`.
Fortunately implementing your own readers and writers is trivial.

With a Sorter instance, you can call one of two main sort methods:

    public void sort(InputStream source, OutputStream destination)
    public boolean sort(DataReader<T>  inputReader, DataWriter<T> resultWriter)

where former takes input as streams and uses configured reader/writer factories to construct `DataReader` for input and `DataWriter` for output; and latter just uses pre-constructed instances.

In addition to core sorting functionality, `Sorter` instance also gives access to progress information (it implements `SortingState` interface with accessor methods).

A very simple example of sorting a text file using line-by-line comparison is:

    TextSorter sorter = new TextFileSorter(new SortConfig().withMaxMemoryUsage(20 * 1000 * 1000));
    sorter.sort(new FileInputStream("input.txt"),
        new FileOutputStream("output.txt"));

which would read text from file "input.txt", sort using about 20 megs of heap (note: estimates for memory usage are rough), use temporary files if necessary (i.e. for small files it's just in-memoryu sort, for bigger real merge sort), and write output as file "output.txt".

## Command-line utility

Project jar is packaged such that it can be used as a primitive 'sort' tool like so:

    java -jar java-merge-sort-0.8.0.jar [input-file]

where sorted output gets printed to `stdout`; and argument is optional (if missing, reads input from stdout).
(implementation note: this uses standard `TextFileSorter` mentioned above)

Format is assumed to be basic text lines, similar to unix `sort`, and sorting order basic byte sorting (which works for most common encodings).

## More documentation

Here are some external links:

* [Sorting large data sets](http://www.cowtowncoder.com/blog/archives/2011/12/entry_465.html) (includes example for sorting JSON files)

# Getting involved

To access source, just clone [project](https://github.com/cowtowncoder/java-merge-sort)

Benchmark code is licensed under Apache License 2.0.

Note that as usual, license only covers (re)distribution of code, and does not apply to your own use of code (i.e. running tests locally), which you can do regardless of licensing.
