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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.parquet.reader.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

public final class ParquetMetaDataUtils
{
    static ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.NO_FILTER;

    private ParquetMetaDataUtils()
    {
    }

    public static ParquetMetadata getParquetMetadata(FileDecryptionProperties fileDecryptionProperties,
                                                     InternalFileDecryptor fileDecryptor,
                                                     Configuration configuration,
                                                     Path path,
                                                     long fileSize,
                                                     FSDataInputStream fsDataInputStream) throws IOException
    {
        ParquetReadOptions options = createReadOptions(configuration, filter);
        ParquetMetadataConverter converter = new ParquetMetadataConverter(options);

        SeekableInputStream in = createStream(fsDataInputStream);
        return MetadataReader.readFooter(path, fileSize, options, in, converter, fileDecryptionProperties, fileDecryptor);
    }

    private static ParquetReadOptions createReadOptions(Configuration configuration, ParquetMetadataConverter.MetadataFilter filter)
    {
        if (configuration != null) {
            return HadoopReadOptions.builder(configuration).withMetadataFilter(filter).build();
        }
        else {
            return ParquetReadOptions.builder().withMetadataFilter(filter).build();
        }
    }

    private static SeekableInputStream createStream(FSDataInputStream fsDataInputStream)
    {
        return HadoopStreams.wrap(fsDataInputStream);
    }
}
