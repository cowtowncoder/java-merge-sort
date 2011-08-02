package com.fasterxml.sort.util;

import java.io.IOException;
import java.util.*;

import com.fasterxml.sort.DataReader;

/**
 * Simple {@link DataReader} implementation that can be used to
 * serve items from a {@link Collection} (or {@link Iterator})
 */
public class CollectionReader<T> extends DataReader<T>
{
    protected Iterator<T> _items;

    public CollectionReader(Collection<T> items) {
        this(items.iterator());
    }

    public CollectionReader(Iterator<T> items) {
        _items = items;
    }
    
    @Override
    public T readNext()
    {
        if (_items == null) {
            return null;
        }
        if (!_items.hasNext()) {
            _items = null;
            return null;
        }
        return _items.next();
    }
    
    @Override
    public void close() throws IOException {
        // no-op
    }

}
