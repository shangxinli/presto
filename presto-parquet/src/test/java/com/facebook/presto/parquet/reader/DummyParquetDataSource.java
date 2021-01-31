package com.facebook.presto.parquet.reader;

import com.facebook.presto.parquet.AbstractParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DumpParquetDataSource
        extends AbstractParquetDataSource
{
    public DumpParquetDataSource()
    {
        super(new ParquetDataSourceId("test"));
    }

    @Override
    public void close()
            throws IOException
    {
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
    }

    @Override
    public ColumnIndex readColumnIndex(ColumnChunkMetaData column) throws IOException
    {

    }

    @Override
    public OffsetIndex readOffsetIndex(ColumnChunkMetaData column) throws IOException
    {
    }

}
