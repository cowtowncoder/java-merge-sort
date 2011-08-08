package com.fasterxml.sort.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.sort.DataReader;

/**
 * Base implementation for {@link DataReader} that uses a
 * {@link BlockingQueue} for getting input.
 * The only missing part is implementation for
 * {@link #estimateSizeInBytes(Object)}, since there is no way
 * to provide a meaningful estimate without knowing object type.
 */
public abstract class BlockingQueueReader<E>
    extends DataReader<E>
{
    protected final BlockingQueue<E> _queue;
    
    public BlockingQueueReader(BlockingQueue<E> q) {
        _queue = q;
    }
    
    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public abstract int estimateSizeInBytes(E item);

    @Override
    public E readNext() throws IOException {
        try {
            return _queue.take();
        } catch (InterruptedException e) {
            InterruptedIOException ie = new InterruptedIOException();
            ie.initCause(e);
            throw ie;
        }
    }

}
