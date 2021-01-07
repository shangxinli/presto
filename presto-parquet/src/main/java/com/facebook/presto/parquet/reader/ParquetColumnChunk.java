/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.reader;

import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.cache.MetadataReader;
import io.airlift.slice.Slice;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.airlift.slice.Slices.wrappedBuffer;

public class ParquetColumnChunk
{
    private final ColumnChunkDescriptor descriptor;
    private final ByteBufferInputStream stream;
    private final OffsetIndex offsetIndex;

    public ParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            List<ByteBuffer> data,
            OffsetIndex offsetIndex)
    {
        this.stream = ByteBufferInputStream.wrap(data);
        this.descriptor = descriptor;
        this.offsetIndex = offsetIndex;
    }

    public ColumnChunkDescriptor getDescriptor()
    {
        return descriptor;
    }

    protected PageHeader readPageHeader()
            throws IOException
    {
        return Util.readPageHeader(stream);
    }

    public PageReader readAllPages()
            throws IOException
    {
        return readAllPagesInternal();
    }

    private PageReader readAllPagesInternal()
            throws IOException
    {
        List<DataPage> pages = new ArrayList<>();
        DictionaryPage dictionaryPage = null;
        long valueCount = 0;
        int dataPageCount = 0;
        while (hasMorePages(valueCount, dataPageCount)) {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    valueCount += readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    ++dataPageCount;
                    break;
                case DATA_PAGE_V2:
                    valueCount += readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    ++dataPageCount;
                    break;
                default:
                    //TODO: uncomment it later
                    // skip(compressedPageSize);
                    break;
            }
        }
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar)
    {
        return offsetIndex == null ? valuesCountReadSoFar < descriptor.getColumnChunkMetaData().getValueCount()
                : dataPageCountReadSoFar < offsetIndex.getPageCount();
    }

    /*private PageReader readAllPagesInternal(List<OffsetRange> offsetRanges)
            throws IOException
    {
        // TODO: asssert offsetRanges has > 0 elements
        List<DataPage> pages = new ArrayList<>();
        DictionaryPage dictionaryPage = null;

        int i = 0;
        OffsetRange offsetRange = offsetRanges.get(i);
        // TODO: for long to int check
        this.pos = (int) offsetRange.getOffset();
        while (i < offsetRanges.size()) {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    offsetRange = offsetRanges.get(i);
                    // TODO: for long to int check
                    this.pos = (int) offsetRange.getOffset();
                    i++;
                    break;
                case DATA_PAGE_V2:
                    readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    offsetRange = offsetRanges.get(i);
                    // TODO: for long to int check
                    this.pos = (int) offsetRange.getOffset();
                    i++;
                    break;
                default:
                    skip(compressedPageSize);
                    offsetRange = offsetRanges.get(i);
                    // TODO: for long to int check
                    this.pos = (int) offsetRange.getOffset();
                    i++;
                    break;
            }
        }
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage);
    } */

    /*public int getPosition()
    {
        return pos;
    }*/

    private Slice getSlice(int size) throws IOException
    {
        BytesInput bytesInput = BytesInput.from(stream.sliceBuffers(size));
        // Todo: Check if using array() and arrayOffset() are correct,
        // Todo: 0 is correct?
        Slice slice = wrappedBuffer(bytesInput.toByteArray(), 0, size);
        return slice;
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    private long readDataPageV1(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<DataPage> pages)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new DataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                MetadataReader.readStats(
                        dataHeaderV1.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name()))));
        return dataHeaderV1.getNum_values();
    }

    private long readDataPageV2(PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<DataPage> pages)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        pages.add(new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                getSlice(dataSize),
                uncompressedPageSize,
                MetadataReader.readStats(
                        dataHeaderV2.getStatistics(),
                        descriptor.getColumnDescriptor().getType()),
                dataHeaderV2.isIs_compressed()));
        return dataHeaderV2.getNum_values();
    }
}
