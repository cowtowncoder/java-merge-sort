package com.fasterxml.sort;

import java.io.*;
import java.util.Comparator;

/**
 * Main entry point for sorting functionality; object that drives
 * the sorting process from pre-sort to final output.
 * Instances are not thread-safe, although they are reusable.
 * Since the cost of creating new instances is trivial, there is usally
 * no benefit from reusing instances, other than possible convenience.
 */
public class Sorter<T>
    implements SortingState
{
    /*
    /********************************************************************** 
    /* Configuration
    /********************************************************************** 
     */
    
    protected final SortConfig _config;
    
    protected final DataReaderFactory<T> _readerFactory;
    
    protected final DataWriterFactory<T> _writerFactory;

    protected final Comparator<T> _comparator;
    
    /*
    /********************************************************************** 
    /* State
    /********************************************************************** 
     */
    
    protected SortingState.Phase _phase;

    protected volatile boolean _cancelRequest;
    
    protected Exception _cancelForException;
    
    /*
    /********************************************************************** 
    /* Construction
    /********************************************************************** 
     */
    
    /**
     * @param config Configuration for the sorter
     * @param readerFactory Factory used for creating readers for pre-sorted data;
     *   as well as for input if an {@link InputStream} is passed as source
     * @param writerFactory Factory used for creating writers for storing pre-sorted data;
     *   as well as for results if an {@link OutputStream} is passed as destination.
     */
    public Sorter(SortConfig config,
            DataReaderFactory<T> readerFactory, DataWriterFactory<T> writerFactory,
            Comparator<T> comparator)
    {
        _config = config;

        _readerFactory = readerFactory;
        _writerFactory = writerFactory;
        _comparator = comparator;
        
        _phase = null;
    }

    /*
    /********************************************************************** 
    /* SortingState implementation
    /********************************************************************** 
     */
    
    @Override
    public void cancel(RuntimeException e) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getNumberOfSortRounds() {
        return -1;
    }

    @Override
    public int getSortRound() {
        return -1;
    }

    @Override
    public boolean isCompleted() {
        return (_phase == SortingState.Phase.COMPLETE);
    }

    @Override
    public boolean isPreSorting() {
        return (_phase == SortingState.Phase.PRE_SORTING);
    }

    @Override
    public boolean isSorting() {
        return (_phase == SortingState.Phase.SORTING);
    }

    /*
    /********************************************************************** 
    /* Main sorting API
    /********************************************************************** 
     */

    /**
     * Method that will perform full sort on specified input, writing results
     * into specified destination. Data conversions needed are done
     * using {@link DataReaderFactory} and {@link DataWriterFactory} configured
     * for this sorter.
     */
    public void sort(InputStream source, OutputStream destination)
        throws IOException
    {
        sort(_readerFactory.constructReader(source),
                _writerFactory.constructWriter(destination));
    }

    /**
     * Method that will perform full sort on input data read using given
     * {@link DataReader}, and written out using specified {@link DataWriter}.
     * Conversions to and from intermediate sort files is done
     * using {@link DataReaderFactory} and {@link DataWriterFactory} configured
     * for this sorter.
     */
    public void sort(DataReader<T>  inputReader, DataWriter<T> resultWriter)
        throws IOException
    {
        // !!! TBI
    }
    
    /*
    /********************************************************************** 
    /* Internal methods
    /********************************************************************** 
     */

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
