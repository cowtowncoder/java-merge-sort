package com.fasterxml.sort;

import java.io.IOException;

public abstract class DataReader<T>
{
    public abstract T readNext() throws IOException;

    public abstract void close() throws IOException;
}
