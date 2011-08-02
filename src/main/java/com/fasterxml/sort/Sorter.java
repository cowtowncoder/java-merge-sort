package com.fasterxml.sort;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

    protected final AtomicBoolean _cancelRequest = new AtomicBoolean(false);
    
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
    public void cancel() {
        _cancelForException = null;
        _cancelRequest.set(true);
    }

    @Override
    public void cancel(RuntimeException e) {
        _cancelForException = e;
        _cancelRequest.set(true);
    }
    
    @Override
    public void cancel(IOException e) {
        _cancelForException = e;
        _cancelRequest.set(true);
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
        // First, pre-sort:
        _phase = SortingState.Phase.PRE_SORTING;
        Collection<File> presorted = presort(inputReader);
        if (_checkForCancel()) {
            return;
        }
        _phase = SortingState.Phase.SORTING;
        merge(presorted, resultWriter);
        if (_checkForCancel()) {
            return;
        }
        _phase = SortingState.Phase.COMPLETE;
    }
    
    /*
    /********************************************************************** 
    /* Internal methods, pre-sorting
    /********************************************************************** 
     */

    protected Collection<File> presort(DataReader<T> inputReader) throws IOException
    {
        ArrayList<File> result = new ArrayList<File>();
        // !!! TBI
        return result;
    }

    /*
    /********************************************************************** 
    /* Internal methods, sorting
    /********************************************************************** 
     */

    protected void merge(Collection<File> presorted, DataWriter<T> resultWriter)
        throws IOException
    {
        // !!! TBI
    }
    
    /*
    /********************************************************************** 
    /* Internal methods, other
    /********************************************************************** 
     */
    
    protected boolean _checkForCancel() throws IOException
    {
        if (!_cancelRequest.get()) {
            return false;
        }
        if (_cancelForException != null) {
            // can only be an IOException or RuntimeException, so
            if (_cancelForException instanceof RuntimeException) {
                throw (RuntimeException) _cancelForException;
            }
            throw (IOException) _cancelForException;
        }
        return true;
    }
}
