# Overview

This project implements basic disk-backed multi-way merge sort, with configurable input and output formats (i.e. not just textual sort).
It should be useful for systems that process large amounts of data, as a simple building block for sort phases.

# Documentation 

Checkout [https://github.com/cowtowncoder/java-merge-sort/wiki](project wiki) for more documentation, including javadocs.

# Usage

## Programmatic access

Main class to interact with is `com.fasterxml.sort.Sorter`, which needs to be constructed with four things:

# Configuration settings (default `SortConfig` works fine)
# `DataReaderFactory` which is used for creating readers for intermediate sort files (and input, if stream passed)
# `DataWriterFactory` which is used for creating writers for intermediate sort files (and results, if stream passed)
# `Comparator` for data items

An example of how this can be done can be found from `com.fasterxml.sort.std.TextFileSorter`.
Basic implementations exist for line-based text input (in package `com.fasterxml.sort.std`), and additional implementations may be added: for example, a JSON data sorter could be implement as an extension module of `Jackson`.
Fortunately implementing your own readers and writers is trivial.

With a Sorter instance, you can call one of two main sort methods:

    public void sort(InputStream source, OutputStream destination)
    public boolean sort(DataReader<T>  inputReader, DataWriter<T> resultWriter)

where former takes input as streams and uses configured reader/writer factories to construct `DataReader` for input and `DataWriter` for output; and latter just uses pre-constructed instances.

In addition to core sorting functionality, `Sorter` instance also gives access to progress information (it implements `SortingState` interface with accessor methods).

## Command-line utility

Project jar is packaged such that it can be used as a primitive 'sort' tool like so:

    java -jar java-merge-sort-0.5.0.jar [input-file]

where sorted output gets printed to `stdout`; and argument is optional (if missing, reads input from stdout).

Format is assumed to be basic text lines, similar to unix `sort`, and sorting order basic byte sorting (which works for most common encodings).

# Getting involved

To access source, just clone [project](https://github.com/cowtowncoder/java-merge-sort)

Benchmark code is licensed under Apache License 2.0.

Note that as usual, license only covers (re)distribution of code, and does not apply to your own use of code (i.e. running tests locally), which you can do regardless of licensing.
