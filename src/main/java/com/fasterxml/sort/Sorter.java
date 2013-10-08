package com.fasterxml.sort;

import com.fasterxml.sort.util.SegmentedBuffer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Main entry point for sorting functionality; object that drives
 * the sorting process from pre-sort to final output.
 * Instances are not thread-safe, although they are reusable.
 * Since the cost of creating new instances is trivial, there is usally
 * no benefit from reusing instances, other than possible convenience.
 */
public class Sorter<T> extends SorterBase<T>
{
    /**
     * @param config Configuration for the sorter
     * @param readerFactory Factory used for creating readers for pre-sorted data;
     *   as well as for input if an {@link InputStream} is passed as source
     * @param writerFactory Factory used for creating writers for storing pre-sorted data;
     *   as well as for results if an {@link OutputStream} is passed as destination.
     */
    protected Sorter(SortConfig config,
                     DataReaderFactory<T> readerFactory,
                     DataWriterFactory<T> writerFactory,
                     Comparator<T> comparator)
    {
        super(config, readerFactory, writerFactory, comparator);
    }

    protected Sorter() {
        super();
    }

    protected Sorter(SortConfig config) {
        super(config);
    }

    protected Sorter<T> withReaderFactory(DataReaderFactory<T> f) {
        return new Sorter<T>(_config, f, _writerFactory, _comparator);
    }

    protected Sorter<T> withWriterFactory(DataWriterFactory<T> f) {
        return new Sorter<T>(_config, _readerFactory, f, _comparator);
    }

    protected Sorter<T> withComparator(Comparator<T> cmp) {
        return new Sorter<T>(_config, _readerFactory, _writerFactory, cmp);
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
     * 
     * @return true if sorting completed succesfully; false if it was cancelled
     */
    public boolean sort(DataReader<T> inputReader, DataWriter<T> resultWriter)
        throws IOException
    {
        // First, pre-sort:
        _phase = SortingState.Phase.PRE_SORTING;

        SegmentedBuffer buffer = new SegmentedBuffer();
        boolean inputClosed = false;
        boolean resultClosed = false;

        _presortFileCount = 0;
        _sortRoundCount = -1;
        _currentSortRound = -1;
        
        try {
            Object[] items = _readMax(inputReader, buffer, _config.getMaxMemoryUsage(), null);
            if (_checkForCancel()) {
                return false;
            }
            Arrays.sort(items, _rawComparator());
            T next = inputReader.readNext();
            /* Minor optimization: in case all entries might fit in
             * in-memory sort buffer, avoid writing intermediate file
             * and just write results directly.
             */
            if (next == null) {
                inputClosed = true;
                inputReader.close();
                _phase = SortingState.Phase.SORTING;
                _writeAll(resultWriter, items);
            } else { // but if more data than memory-buffer-full, do it right:
                List<File> presorted = new ArrayList<File>();
                presorted.add(_writePresorted(items));
                items = null; // it's a big array, clear refs as early as possible
                _presort(inputReader, buffer, next, presorted);
                inputClosed = true;
                inputReader.close();
                _phase = SortingState.Phase.SORTING;
                if (_checkForCancel(presorted)) {
                    return false;
                }
                merge(presorted, resultWriter);
            }
            resultClosed = true;
            resultWriter.close();
            if (_checkForCancel()) {
                return false;
            }
            _phase = SortingState.Phase.COMPLETE;
        } finally {
            if (!inputClosed) {
                try {
                    inputReader.close();
                } catch (IOException e) { }
            }
            if (!resultClosed) {
                try {
                    resultWriter.close();
                } catch (IOException e) { }
            }
        }
        return true;
    }
}
