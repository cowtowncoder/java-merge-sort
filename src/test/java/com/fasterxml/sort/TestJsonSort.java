package com.fasterxml.sort;

import java.io.*;

public class TestJsonSort extends SortTestBase
{
    static class Point implements Comparable<Point>
    {
        public int x, y;
        
        @Override
        public int compareTo(Point o) {
            int diff = y - o.y;
            if (diff == 0) {
                diff = x - o.x;
            }
            return diff;
        }
    }
    
    /*
    /********************************************************************** 
    /* Unit tests
    /********************************************************************** 
     */
    
    public void testSimple() throws IOException
    {
        final String input =
                 "{\"x\":1, \"y\":1}\n"
                +"{\"x\":2, \"y\":8}\n"
                +"{\"x\":3, \"y\":2}\n"
                +"{\"x\":4, \"y\":4}\n"
                +"{\"x\":5, \"y\":5}\n"
                +"{\"x\":6, \"y\":0}\n"
                +"{\"x\":7, \"y\":10}\n"
                +"{\"x\":8, \"y\":-4}\n"
                ;
        JsonFileSorter<Point> sorter = new JsonFileSorter<Point>(Point.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream(1000);
        sorter.sort(new ByteArrayInputStream(input.getBytes("UTF-8")), out);
        final String output = out.toString("UTF-8");
        assertEquals(""
                +"{\"x\":8,\"y\":-4}\n"
                +" {\"x\":6,\"y\":0}\n"
                +" {\"x\":1,\"y\":1}\n"
                +" {\"x\":3,\"y\":2}\n"
                +" {\"x\":4,\"y\":4}\n"
                +" {\"x\":5,\"y\":5}\n"
                +" {\"x\":2,\"y\":8}\n"
                +" {\"x\":7,\"y\":10}\n"
                ,output);
    }
}
