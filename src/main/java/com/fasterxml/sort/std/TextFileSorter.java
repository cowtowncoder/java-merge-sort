package com.fasterxml.sort.std;

import java.io.*;

import com.fasterxml.sort.*;

/**
 * Basic {@link Sorter} implementation that operates on text line input.
 */
public class TextFileSorter extends Sorter<byte[]>
{
    protected final static DataReaderFactory<byte[]> TEXT_INPUT_READER_FACTORY = null;
    protected final static DataWriterFactory<byte[]> TEXT_INPUT_WRITER_FACTORY = null;
    
    public TextFileSorter() {
        this(new SortConfig());
    }
    
    public TextFileSorter(SortConfig config)
    {
        super(config,
                RawTextLineReader.factory(), RawTextLineWriter.factory(),
                new ByteArrayComparator());
    }

    /*
    /********************************************************************** 
    /* Main method for simple command-line operation for line-based
    /* sorting using default ISO-8859-1 collation (i.e. byte-by-byte sorting)
    /********************************************************************** 
     */

    public static void main(String[] args) throws Exception
    {
        if (args.length > 1) {
            System.err.println("Usage: java "+TextFileSorter.class.getName()+" [input-file]");
            System.err.println("(where input-file is optional; if missing, read from STDIN)");
            System.exit(1);
        }
        final TextFileSorter sorter = new TextFileSorter();
        final InputStream in;
        
        if (args.length == 0) {
            in = System.in;
        } else {
            File input = new File(args[0]);
            if (!input.exists() || input.isDirectory()) {
                System.err.println("File '"+input.getAbsolutePath()+"' does not exist (or is not file)");
                System.exit(2);
            }
            in = new FileInputStream(input);
        }

        // To be able to print out progress, need to spin one additional thread...
        new Thread(new Runnable() {
            @Override
            public void run() {
                final long start = System.currentTimeMillis();
                try {
                    while (!sorter.isCompleted()) {
                        Thread.sleep(5000L);
                        if (sorter.isPreSorting()) {
                            System.err.printf(" pre-sorting: %d files written\n", sorter.getNumberOfPreSortFiles());
                        } else if (sorter.isSorting()) {
                            System.err.printf(" sorting, round: %d/%d\n",
                                    sorter.getSortRound(), sorter.getNumberOfSortRounds());
                        }
                    }
                    double secs = (System.currentTimeMillis() - start) / 1000.0;
                    System.err.printf("Completed: took %.1f seconds.\n", secs);
                } catch (InterruptedException e) {
                    double secs = (System.currentTimeMillis() - start) / 1000.0;
                    System.err.printf("[INTERRUPTED] -- took %.1f seconds.\n", secs);
                }
            } 
        }).start();
        sorter.sort(in, System.out);
    }
}
