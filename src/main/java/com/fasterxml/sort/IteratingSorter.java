package com.fasterxml.sort;

import com.fasterxml.sort.util.SegmentedBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IteratingSorter<T> extends SorterBase<T> implements Closeable
{
    // Set iff sort spilled to disk
    private List<File> _mergerInputs;
    private DataReader<T> _merger;


    protected IteratingSorter(SortConfig config,
                              DataReaderFactory<T> readerFactory,
                              DataWriterFactory<T> writerFactory,
                              Comparator<T> comparator)
    {
        super(config, readerFactory, writerFactory, comparator);
    }

    protected IteratingSorter() {
        super();
    }

    protected IteratingSorter(SortConfig config) {
        super(config);
    }

    /**
     * Method that will perform full sort on input data read using given
     * {@link DataReader}.
     *
     * Conversions to and from intermediate sort files is done
     * using {@link DataReaderFactory} and {@link DataWriterFactory} configured
     * for this sorter.
     *
     * @return Iterator if sorting complete and output is ready to be written; null if it was cancelled
     */
    public Iterator<T> sort(DataReader<T> inputReader)
        throws IOException
    {
        // Clean up any previous sort
        close();

        // First, pre-sort:
        _phase = Phase.PRE_SORTING;
        boolean inputClosed = false;

        SegmentedBuffer buffer = new SegmentedBuffer();
        _presortFileCount = 0;
        _sortRoundCount = -1;
        _currentSortRound = -1;

        Iterator<T> iterator = null;
        try {
            Object[] items = _readMax(inputReader, buffer, _config.getMaxMemoryUsage(), null);
            if (_checkForCancel()) {
                close();
                return null;
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
                _phase = Phase.SORTING;
                iterator = new CastingIterator<T>(Arrays.asList(items).iterator());
            } else { // but if more data than memory-buffer-full, do it right:
                List<File> presorted = new ArrayList<File>();
                presorted.add(_writePresorted(items));
                items = null; // it's a big array, clear refs as early as possible
                _presort(inputReader, buffer, next, presorted);
                inputClosed = true;
                inputReader.close();
                _phase = Phase.SORTING;
                if (_checkForCancel(presorted)) {
                    close();
                    return null;
                }
                _mergerInputs = presorted;
                _merger = _createMergeReader(merge(presorted));
                iterator = new MergerIterator<T>(_merger);
            }
        } finally {
            if (!inputClosed) {
                try {
                    inputReader.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
        if (_checkForCancel()) {
            close();
            return null;
        }
        _phase = Phase.COMPLETE;
        return iterator;
    }


    /*
    /**********************************************************************
    /* Closeable API
    /**********************************************************************
    */

    @Override
    public void close() {
        if (_merger != null) {
            try {
                _merger.close();
            }
            catch (IOException e) {
                // Ignore
            }
        }
        if (_mergerInputs != null) {
            for (File input : _mergerInputs) {
                input.delete();
            }
        }
        _mergerInputs = null;
        _merger = null;
    }


    /*
    /**********************************************************************
    /* Exception API
    /**********************************************************************
    */

    public static class IterableSorterException extends RuntimeException {
        public IterableSorterException(IOException cause) {
            super(cause);
        }
    }


    /*
    /**********************************************************************
    /* Iterator API
    /**********************************************************************
    */

    private static class CastingIterator<T> implements Iterator<T> {
        private final Iterator<Object> _it;

        public CastingIterator(Iterator<Object> it) {
            _it = it;
        }

        @Override
        public boolean hasNext() {
            return _it.hasNext();
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            return (T)_it.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MergerIterator<T> implements Iterator<T> {
        private final DataReader<T> _merger;
        private T _next;

        private MergerIterator(DataReader<T> merger) {
            _merger = merger;
        }

        private void prepNext() {
            if (_next != null) {
                try {
                    _next = _merger.readNext();
                } catch (IOException e) {
                    throw new IterableSorterException(e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            prepNext();
            return (_next != null);
        }

        @Override
        public T next() {
            prepNext();
            if (_next == null) {
                throw new NoSuchElementException();
            }
            T t = _next;
            _next = null;
            return t;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
