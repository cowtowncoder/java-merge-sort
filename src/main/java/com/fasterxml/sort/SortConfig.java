package com.fasterxml.sort;

import com.fasterxml.sort.impl.StdTempFileProvider;

/**
 * Configuration object used for changing details of sorting
 * process. Default settings are usable, so often
 * instance is created without arguments and used as is.
 */
public class SortConfig
{
    /**
     * By default we will use 40 megs for pre-sorting.
     */
    public final static long DEFAULT_MEMORY_USAGE = 40 * 1024 * 1024;

    protected long _maxMemoryUsage;
    
    protected TempFileProvider _tempFileProvider;

    /*
    /************************************************************************
    /* Construction
    /************************************************************************
     */

    public SortConfig() {
        _maxMemoryUsage = DEFAULT_MEMORY_USAGE;
        _tempFileProvider = new StdTempFileProvider();
    }

    protected SortConfig(SortConfig base, long maxMem) {
        _maxMemoryUsage = maxMem;
        _tempFileProvider = base._tempFileProvider;
    }

    protected SortConfig(SortConfig base, TempFileProvider prov) {
        _maxMemoryUsage = base._maxMemoryUsage;
        _tempFileProvider = prov;
    }
    
    /*
    /************************************************************************
    /* Accessors
    /************************************************************************
     */

    public long getMaxMemoryUsage() { return _maxMemoryUsage; }

    public TempFileProvider getTempFileProvider() { return _tempFileProvider; }
    
    /*
    /************************************************************************
    /* Fluent construction methods
    /************************************************************************
     */
    
    /**
     * Method for constructing configuration instance that defines that maximum amount
     * of memory to use for pre-sorting. This is generally a crude approximation and
     * implementations make best effort to honor it.
     * 
     * @param maxMem Maximum memory that pre-sorted should use for in-memory sorting
     * @return New 
     */
    public SortConfig withMaxMemoryUsage(long maxMem)
    {
        if (maxMem == _maxMemoryUsage) {
            return this;
        }
        return new SortConfig(this, maxMem);
    }

    public SortConfig withTempFileProvider(TempFileProvider provider)
    {
        if (provider == _tempFileProvider) {
            return this;
        }
        return new SortConfig(this, provider);
    }

}
