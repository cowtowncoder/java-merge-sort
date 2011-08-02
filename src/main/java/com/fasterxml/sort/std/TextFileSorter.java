package com.fasterxml.sort.std;

import java.io.File;

import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;

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
        super(config, TEXT_INPUT_READER_FACTORY, TEXT_INPUT_WRITER_FACTORY,
                new ByteArrayComparator());
    }

    /*
    /********************************************************************** 
    /* Main method for simple command-line operation for line-based
    /* sorting using default ISO-8859-1 collation (i.e. byte-by-byte sorting)
    /********************************************************************** 
     */

    public void sort(String[] args) throws Exception
    {
        if (args.length > 1) {
            System.err.println("Usage: java "+getClass().getName()+" [input-file]");
            System.err.println("(where input-file is optional; if missing, read from STDIN)");
            System.exit(1);
        }
        TextFileSorter sorter = new TextFileSorter();
        if (args.length == 0) {
            
        } else {
            File input = new File(args[0]);
            if (!input.exists() || input.isDirectory()) {
                System.err.println("File '"+input.getAbsolutePath()+"' does not exist (or is not file)");
                System.exit(2);
            }
        }
    }
}
