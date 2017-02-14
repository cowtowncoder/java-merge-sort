package com.fasterxml.sort;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.sort.std.ByteArrayComparator;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;

// for issue [#14], problem with lines longer than 32k
public class TestLongLineSort extends SortTestBase
{
    protected static class CollectingWriter<T> extends DataWriter<T> {
        private final List<T> _contents = new ArrayList<T>();

        public List<T> contents() {
            return _contents;
        }

        @Override
        public void writeEntry(T item) {
            _contents.add(item);
        }

        @Override
        public void close() {
            // None
        }
    }

    public void testLongLine() throws Exception
    {
        String line1 = _generate("cxxx", 33000);
        String line2 = _generate("abab", 33003);
        String line3 = _generate("byyy", 32900);
        byte[] input = String.format("%s\n%s\n%s\n", line1, line2, line3)
                .getBytes(CHARSET);

        Sorter<byte[]> sorter = new Sorter<byte[]>(
                new SortConfig(),
                RawTextLineReader.factory(),
                RawTextLineWriter.factory(),
                new ByteArrayComparator()
            );
        CollectingWriter<byte[]> collator = new CollectingWriter<byte[]>();
        
        sorter.sort(new RawTextLineReader(new ByteArrayInputStream(input)),
                collator);
        sorter.close();
        List<byte[]> results = collator.contents();
        assertEquals(3, results.size());
        _verify(line2, results, 0);
        _verify(line3, results, 1);
        _verify(line1, results, 2);
    }

    private void _verify(String input, List<byte[]> results, int index)
    {
        byte[] output = results.get(index);
        String outputStr = new String(output, CHARSET);
        // first assert lengths are equal
        assertEquals(input.length(), outputStr.length());
        // and then content
        assertEquals(outputStr, input);
    }

    private String _generate(String part, int len) {
        StringBuilder sb = new StringBuilder(len + part.length());
        do {
            sb.append(part);
        } while (sb.length() < len);
        return sb.toString();
    }

}
