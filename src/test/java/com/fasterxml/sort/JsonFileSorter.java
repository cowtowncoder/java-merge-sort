package com.fasterxml.sort;

import java.io.*;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.type.JavaType;

import com.fasterxml.sort.std.StdComparator;

public class JsonFileSorter<T extends Comparable<T>> extends Sorter<T>
{
    public JsonFileSorter(Class<T> entryType) throws IOException {
        this(entryType, new SortConfig(), new ObjectMapper());
    }

    public JsonFileSorter(Class<T> entryType, SortConfig config, ObjectMapper mapper) throws IOException {
        this(mapper.constructType(entryType), config, mapper);
    }

    public JsonFileSorter(JavaType entryType, SortConfig config, ObjectMapper mapper)
        throws IOException
    {
        super(config, new ReaderFactory<T>(mapper.reader(entryType)),
                new WriterFactory<T>(mapper),
                new StdComparator<T>());
    }

    static class ReaderFactory<R> extends DataReaderFactory<R>
    {
        private final ObjectReader _reader;

        public ReaderFactory(ObjectReader r) {
            _reader = r;
        }
        
        @Override
        public DataReader<R> constructReader(InputStream in) throws IOException {
            MappingIterator<R> it = _reader.readValues(in);
            return new Reader<R>(it);
        }
    }

    static class Reader<E> extends DataReader<E>
    {
        protected final MappingIterator<E> _iterator;
 
        public Reader(MappingIterator<E> it) {
            _iterator = it;
        }

        @Override
        public E readNext() throws IOException {
            if (_iterator.hasNext()) {
                return _iterator.nextValue();
            }
            return null;
        }

        @Override
        public int estimateSizeInBytes(E item) {
            // 2 int fields, object, rough approximation
            return 24;
        }

        @Override
        public void close() throws IOException {
            // auto-closes when we reach end
        }
    }
    
    static class WriterFactory<W> extends DataWriterFactory<W>
    {
        protected final ObjectMapper _mapper;
        
        public WriterFactory(ObjectMapper m) {
            _mapper = m;
        }
        
        @Override
        public DataWriter<W> constructWriter(OutputStream out) throws IOException {
            return new Writer<W>(_mapper, out);
        }
    }

    static class Writer<E> extends DataWriter<E>
    {
        protected final ObjectMapper _mapper;
        protected final JsonGenerator _generator;

        public Writer(ObjectMapper mapper, OutputStream out) throws IOException {
            _mapper = mapper;
            _generator = _mapper.getJsonFactory().createJsonGenerator(out);
        }

        @Override
        public void writeEntry(E item) throws IOException {
            _mapper.writeValue(_generator, item);
            // not 100% necesary, but for readability, add linefeeds
            _generator.writeRaw('\n');
        }

        @Override
        public void close() throws IOException {
            _generator.close();
        }
    }
}
