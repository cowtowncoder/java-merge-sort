package com.fasterxml.sort;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.sort.util.SegmentedBuffer;

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
    /* each entry (in buffer) takes about 4 bytes on 32-bit machine; but let's be
     * conservative and use 8 as base, plus size of object itself.
     */
    private final static long ENTRY_SLOT_SIZE = 8L;
    
    /*
    /********************************************************************** 
    /* Configuration
    /********************************************************************** 
     */
    
    protected final SortConfig _config;
    
    /**
     * Factory used for reading intermediate sorted files.
     */
    protected DataReaderFactory<T> _readerFactory;
    
    /**
     * Factory used for writing intermediate sorted files.
     */
    protected DataWriterFactory<T> _writerFactory;

    /**
     * Comparator to use for sorting entries; defaults to 'C
     */
    protected Comparator<T> _comparator;
    
    /*
    /********************************************************************** 
    /* State
    /********************************************************************** 
     */
    
    protected SortingState.Phase _phase;

    protected int _presortFileCount;
    
    protected int _sortRoundCount;

    protected int _currentSortRound;
    
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

    protected Sorter() {
        this(new SortConfig());
    }
    
    protected Sorter(SortConfig config) {
        this(config, null, null, null);
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
    public Phase getPhase() {
        return _phase;
    }
    
    @Override
    public int getNumberOfSortRounds() {
        return _sortRoundCount;
    }

    @Override
    public int getNumberOfPreSortFiles() {
        return _presortFileCount;
    }
    
    @Override
    public int getSortRound() {
        return _currentSortRound;
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
            } else {
                // but if more data than memory-buffer-full, do it right:
                List<File> presorted = presort(inputReader, buffer, items, next);
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
    
    /*
    /********************************************************************** 
    /* Internal methods, pre-sorting
    /********************************************************************** 
     */

    /**
     * Helper method that will fill given buffer with data read using
     * given reader, obeying given memory usage constraints.
     */
    private Object[] _readMax(DataReader<T> inputReader, SegmentedBuffer buffer,
            long memoryToUse, T firstItem)
        throws IOException
    {
        // how much memory do we expect largest remaining entry to take?
        int ptr = 0;
        Object[] segment = buffer.resetAndStart();
        int segmentLength = segment.length;
        long minMemoryNeeded;

        if (firstItem != null) {
            segment[ptr++] = firstItem;
            long firstSize = ENTRY_SLOT_SIZE + inputReader.estimateSizeInBytes(firstItem);
            minMemoryNeeded = Math.max(firstSize, 256L);
        } else  {
            minMemoryNeeded = 256L;
        }

        // reduce mem amount by buffer cost too:
        memoryToUse -= (ENTRY_SLOT_SIZE * segmentLength);
        
        while (true) {
            T value = inputReader.readNext();
            if (value == null) {
                break;
            }
            long size = inputReader.estimateSizeInBytes(value);
            if (size > minMemoryNeeded) {
                minMemoryNeeded = size;
            }
            if (ptr >= segmentLength) {
                segment = buffer.appendCompletedChunk(segment);
                segmentLength = segment.length;
                memoryToUse -= (ENTRY_SLOT_SIZE * segmentLength);
                ptr = 0;
            }
            segment[ptr++] = value;
            memoryToUse -= size;
            if (memoryToUse < minMemoryNeeded) {
                break;
            }
        }
        return buffer.completeAndClearBuffer(segment, ptr);
    }
    
    protected List<File> presort(DataReader<T> inputReader,
            SegmentedBuffer buffer,
            Object[] firstSortedBatch, T nextValue) throws IOException
    {
        ArrayList<File> presorted = new ArrayList<File>();
        presorted.add(_writePresorted(firstSortedBatch));
        // important: clear out the ref to let possibly sizable array to be GCed
        firstSortedBatch = null;
        do {
            Object[] items = _readMax(inputReader, buffer, _config.getMaxMemoryUsage(), nextValue);
            Arrays.sort(items, _rawComparator());
            presorted.add(_writePresorted(items));
            nextValue = inputReader.readNext();
        } while (nextValue != null);
        return presorted;
    }

    protected File _writePresorted(Object[] items) throws IOException
    {
        File tmp = _config.getTempFileProvider().provide();
        @SuppressWarnings("unchecked")
        DataWriter<Object> writer = (DataWriter<Object>) _writerFactory.constructWriter(new FileOutputStream(tmp));
        ++_presortFileCount;
        for (int i = 0, end = items.length; i < end; ++i) {
            writer.writeEntry(items[i]);
            // to further reduce transient mem usage, clear out the ref
            items[i] = null;
        }
        writer.close();
        return tmp;
    }
    
    /*
    /********************************************************************** 
    /* Internal methods, sorting, output
    /********************************************************************** 
     */

    /**
     * Main-level merge method called during once during sorting.
     */
    protected void merge(List<File> presorted, DataWriter<T> resultWriter)
        throws IOException
    {
        // Ok, let's see how many rounds we should have...
        final int mergeFactor = _config.getMergeFactor();
        _sortRoundCount = _calculateRoundCount(presorted.size(), mergeFactor);
        _currentSortRound = 0;

        // first intermediate rounds
        List<File> inputs = presorted;
        while (inputs.size() > mergeFactor) {
            ArrayList<File> outputs = new ArrayList<File>(1 + ((inputs.size() + mergeFactor - 1) / mergeFactor));
            for (int offset = 0, end = inputs.size(); offset < end; offset += mergeFactor) {
                int localEnd = Math.min(offset + mergeFactor, end);
                outputs.add(_merge(inputs.subList(offset, localEnd)));
            }
            ++_currentSortRound;
            // and then switch result files to be input files
            inputs = outputs;
        }
        // and then last around to produce the result file
        _merge(inputs, resultWriter);
    }

    protected void _writeAll(DataWriter<T> resultWriter, Object[] items)
        throws IOException
    {
        // need to go through acrobatics, due to type erasure... works, if ugly:
        @SuppressWarnings("unchecked")
        DataWriter<Object> writer = (DataWriter<Object>) resultWriter;
        for (Object item : items) {
            writer.writeEntry(item);
        }
    }

    protected File _merge(List<File> inputs)
        throws IOException
    {
        File resultFile = _config.getTempFileProvider().provide();
        _merge(inputs, _writerFactory.constructWriter(new FileOutputStream(resultFile)));
        return resultFile;
    }

    protected void _merge(List<File> inputs, DataWriter<T> writer)
        throws IOException
    {
        ArrayList<DataReader<T>> readers = new ArrayList<DataReader<T>>(inputs.size());
        try {
            for (File mergedInput : inputs) {
                readers.add(_readerFactory.constructReader(new FileInputStream(mergedInput)));
            }
            DataReader<T> merger = Merger.mergedReader(_comparator, readers);
            T value;
            while ((value = merger.readNext()) != null) {
                writer.writeEntry(value);
            }
            writer.close();
        } finally {
            for (File input : inputs) {
                input.delete();
            }
        }
    }
    
    /*
    /********************************************************************** 
    /* Internal methods, other
    /********************************************************************** 
     */

    protected static int _calculateRoundCount(int files, int mergeFactor)
    {
        int count = 1;
        while (files > mergeFactor) {
            ++count;
            files = (files + mergeFactor - 1) / mergeFactor;
        }
        return count;
    }
    
    protected boolean _checkForCancel() throws IOException
    {
        return _checkForCancel(null);
    }

    protected boolean _checkForCancel(Collection<File> tmpFilesToDelete) throws IOException
    {
        if (!_cancelRequest.get()) {
            return false;
        }
        if (tmpFilesToDelete != null) {
            for (File f : tmpFilesToDelete) {
                f.delete();
            }
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

    @SuppressWarnings("unchecked")
    protected Comparator<Object> _rawComparator() {
        return (Comparator<Object>) _comparator;
    }
}
