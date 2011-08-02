package com.fasterxml.sort;

import java.io.IOException;
import java.util.Comparator;

/**
 * Object used to merge items from multiple input sources into one.
 */
public abstract class Merger<T>
    extends DataReader<T>
{
    protected final Comparator<T> _comparator;

    /*
    /********************************************************************** 
    /* Construction
    /********************************************************************** 
     */
    
    public Merger(Comparator<T> cmp) {
        _comparator = cmp;
    }
    
    public static <T> DataReader<T> mergedReader(Comparator<T> cmp, DataReader<T>[] inputs)
        throws IOException
    {
        switch (inputs.length) {
        case 0:
            throw new IllegalArgumentException("Can not pass empty DataReader array");
        case 1:
            return inputs[0];
        case 2:
            return new PairwiseMerger<T>(cmp, inputs[0], inputs[1]);
        default:
            // !! TBI
            throw new IllegalArgumentException("Multi-way merges not yet supported");
        }
        
    }

    /*
    /********************************************************************** 
    /* Concrete implementations
    /********************************************************************** 
     */

    protected static class PairwiseMerger<T>
        extends Merger<T>
    {
        protected final DataReader<T> _reader1;
        protected final DataReader<T> _reader2;

        protected T _data1;
        protected T _data2;
        
        public PairwiseMerger(Comparator<T> comparator,
                DataReader<T> reader1, DataReader<T> reader2)
            throws IOException
        {
            super(comparator);
            _reader1 = reader1;
            _reader2 = reader2;
        }

        @Override
        public T readNext() throws IOException
        {
            if (_data1 == null) {
                if (_data2 == null) {
                    return null;
                }
                T result = _data2;
                _data2 = _reader2.readNext();
                return result;
            }
            if (_data2 == null) {
                T result = _data1;
                _data1 = _reader1.readNext();
                return result;
            }
            // neither is null, compare
            T result;
            if (_comparator.compare(_data1, _data2) <= 0) {
                result = _data1;
                _data1 = _reader1.readNext();
            } else {
                result = _data2;
                _data2 = _reader2.readNext();
            }
            return result;
        }

        @Override
        public int estimateSizeInBytes(T item) {
            // should not matter so
            return _reader1.estimateSizeInBytes(item);
        }

        @Override
        public void close() throws IOException {
            _reader1.close();
            _reader2.close();
        }
    }
}
