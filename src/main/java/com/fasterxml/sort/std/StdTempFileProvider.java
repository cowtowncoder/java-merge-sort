package com.fasterxml.sort.std;

import java.io.*;

import com.fasterxml.sort.TempFileProvider;

/**
 * Default {@link TempFileProvider} implementation which uses JDK default
 * temporary file generation mechanism.
 * 
 * @author tatu
 */
public class StdTempFileProvider
    implements TempFileProvider
{
    public StdTempFileProvider() { }
    
    @Override
    public File provide(String prefix, String suffix) throws IOException
    {
        File f = File.createTempFile(prefix, suffix);
        f.deleteOnExit();
        return f;
    }
}
