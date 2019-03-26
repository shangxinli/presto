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
package com.facebook.presto.parquet.cache;

import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.crypto.AesCipher;
import com.facebook.presto.parquet.crypto.AesGcmEncryptor;
import com.facebook.presto.parquet.crypto.FileDecryptionProperties;
import com.facebook.presto.parquet.crypto.HiddenColumnChunkMetaData;
import com.facebook.presto.parquet.crypto.InternalColumnDecryptionSetup;
import com.facebook.presto.parquet.crypto.InternalFileDecryptor;
import com.facebook.presto.parquet.crypto.KeyAccessDeniedException;
import com.facebook.presto.parquet.crypto.ModuleCipherFactory.ModuleType;
import com.facebook.presto.parquet.crypto.ParquetCryptoRuntimeException;
import com.facebook.presto.parquet.crypto.TagVerificationException;
import com.facebook.presto.parquet.format.BlockCipher.Decryptor;
import com.facebook.presto.parquet.format.Util;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.parquet.format.Util.readFileCryptoMetaData;
import static com.facebook.presto.parquet.format.Util.readFileMetaData;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class MetadataReader
        implements ParquetMetadataSource
{
    private static final Slice MAGIC = wrappedBuffer("PAR1".getBytes(US_ASCII));
    private static final Slice EMAGIC = wrappedBuffer("PARE".getBytes(US_ASCII));
    private static final int POST_SCRIPT_SIZE = Integer.BYTES + MAGIC.length();
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;
    private static final ParquetMetadataConverter PARQUET_METADATA_CONVERTER = new ParquetMetadataConverter();

    public static ParquetFileMetadata readFooter(FileSystem fileSystem, Path file, long fileSize, FileDecryptionProperties fileDecryptionProperties, InternalFileDecryptor fileDecryptor)
            throws IOException
    {
        try (FSDataInputStream inputStream = fileSystem.open(file)) {
            return readFooter(inputStream, file, fileSize, fileDecryptionProperties, fileDecryptor);
        }
    }

    public static ParquetFileMetadata readFooter(FSDataInputStream inputStream, Path file, long fileSize, FileDecryptionProperties fileDecryptionProperties, InternalFileDecryptor fileDecryptor)
            throws IOException

    {
        // Parquet File Layout: https://github.com/apache/parquet-format/blob/master/Encryption.md

        validateParquet(fileSize >= MAGIC.length() + POST_SCRIPT_SIZE, "%s is not a valid Parquet File", file);

        //  EXPECTED_FOOTER_SIZE is an int, so this will never fail
        byte[] buffer = new byte[toIntExact(min(fileSize, EXPECTED_FOOTER_SIZE))];
        inputStream.readFully(fileSize - buffer.length, buffer);
        Slice tailSlice = wrappedBuffer(buffer);

        Slice magic = tailSlice.slice(tailSlice.length() - MAGIC.length(), MAGIC.length());

        boolean encryptedFooterMode;
        if (MAGIC.equals(magic)) {
            encryptedFooterMode = false;
        }
        else if (EMAGIC.equals(magic)) {
            encryptedFooterMode = true;
        }
        else {
            throw new ParquetCorruptionException(format("Not valid Parquet file: %s expected magic number: %s or %s got: %s", file, Arrays.toString(MAGIC.getBytes()), Arrays.toString(EMAGIC.getBytes()), Arrays.toString(magic.getBytes())));
        }

        int metadataLength = tailSlice.getInt(tailSlice.length() - POST_SCRIPT_SIZE);
        int completeFooterSize = metadataLength + POST_SCRIPT_SIZE;

        long metadataFileOffset = fileSize - completeFooterSize;
        validateParquet(metadataFileOffset >= MAGIC.length() && metadataFileOffset + POST_SCRIPT_SIZE < fileSize, "Corrupted Parquet file: %s metadata index: %s out of range", file, metadataFileOffset);
        //  Ensure the slice covers the entire metadata range
        if (tailSlice.length() < completeFooterSize) {
            byte[] footerBuffer = new byte[completeFooterSize];
            inputStream.readFully(metadataFileOffset, footerBuffer, 0, footerBuffer.length - tailSlice.length());
            // Copy the previous slice contents into the new buffer
            tailSlice.getBytes(0, footerBuffer, footerBuffer.length - tailSlice.length(), tailSlice.length());
            tailSlice = wrappedBuffer(footerBuffer, 0, footerBuffer.length);
        }

        return readParquetMetadata(tailSlice.slice(tailSlice.length() - completeFooterSize, metadataLength).getInput(), file, metadataLength, fileDecryptionProperties, fileDecryptor, encryptedFooterMode);
    }

    private static ParquetFileMetadata readParquetMetadata(BasicSliceInput input, Path file, int metadataLength, FileDecryptionProperties fileDecryptionProperties, InternalFileDecryptor fileDecryptor, boolean encryptedFooterMode)
            throws IOException
    {
        if (encryptedFooterMode && fileDecryptionProperties == null) {
            new IllegalArgumentException("fileDecryptionProperties cannot be null when encryptedFooterMode is true");
        }
        //InternalFileDecryptor fileDecryptor = (fileDecryptionProperties == null) ? null : new InternalFileDecryptor(fileDecryptionProperties);
        Decryptor footerDecryptor = null;
        byte[] aad = null;

        if (encryptedFooterMode) {
            FileCryptoMetaData fileCryptoMetaData = readFileCryptoMetaData(input);
            fileDecryptor.setFileCryptoMetaData(fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
            footerDecryptor = fileDecryptor.fetchFooterDecryptor();
            aad = AesCipher.createFooterAad(fileDecryptor.getFileAAD());
        }

        FileMetaData fileMetaData = readFileMetaData(input, footerDecryptor, aad);
        return convertToParquetMetadata(input, fileMetaData, file, metadataLength, fileDecryptor, encryptedFooterMode);
    }

    private static ParquetFileMetadata convertToParquetMetadata(BasicSliceInput input, FileMetaData fileMetaData, Path file, int metadataLength, InternalFileDecryptor fileDecryptor, boolean encryptedFooter)
            throws IOException
    {
        List<SchemaElement> schema = fileMetaData.getSchema();
        if (!encryptedFooter && null != fileDecryptor) {
            if (!fileMetaData.isSetEncryption_algorithm()) { // Plaintext file
                fileDecryptor.setPlaintextFile();
                // Done to detect files that were not encrypted by mistake
                if (!fileDecryptor.plaintextFilesAllowed()) {
                    throw new ParquetCryptoRuntimeException("Applying decryptor on plaintext file");
                }
            }
            else {  // Encrypted file with plaintext footer
                // if no fileDecryptor, can still read plaintext columns
                fileDecryptor.setFileCryptoMetaData(fileMetaData.getEncryption_algorithm(), false,
                        fileMetaData.getFooter_signing_key_metadata());
                if (fileDecryptor.checkFooterIntegrity()) {
                    verifyFooterIntegrity(input, fileDecryptor, metadataLength);
                }
            }
        }

        validateParquet(!schema.isEmpty(), "Empty Parquet schema in file: %s", file);

        MessageType messageType = readParquetSchema(schema);
        List<BlockMetaData> blocks = new ArrayList<>();
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups != null) {
            for (RowGroup rowGroup : rowGroups) {
                BlockMetaData blockMetaData = new BlockMetaData();
                blockMetaData.setRowCount(rowGroup.getNum_rows());
                blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), "No columns in row group: %s", rowGroup);
                String filePath = columns.get(0).getFile_path();
                int columnOrdinal = -1;
                for (ColumnChunk columnChunk : columns) {
                    columnOrdinal++;
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            "all column chunks of the same row group must be in the same file");

                    ColumnMetaData metaData = columnChunk.meta_data;
                    ColumnCryptoMetaData cryptoMetaData = columnChunk.getCrypto_metadata();
                    ColumnPath columnPath = null;
                    boolean encryptedMetadata = false;

                    if (null == cryptoMetaData) { // Plaintext column
                        columnPath = getPath(metaData);
                        if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
                            // mark this column as plaintext in encrypted file decryptor
                            fileDecryptor.setColumnCryptoMetadata(columnPath, false, false, (byte[]) null, columnOrdinal);
                        }
                    }
                    else {  // Encrypted column
                        if (cryptoMetaData.isSetENCRYPTION_WITH_FOOTER_KEY()) { // Column encrypted with footer key
                            if (!encryptedFooter) {
                                throw new ParquetCryptoRuntimeException("Column encrypted with footer key in file with plaintext footer");
                            }
                            if (null == metaData) {
                                throw new ParquetCryptoRuntimeException("ColumnMetaData not set in Encryption with Footer key");
                            }
                            if (null == fileDecryptor) {
                                throw new ParquetCryptoRuntimeException("Column encrypted with footer key: No keys available");
                            }
                            columnPath = getPath(metaData);
                            fileDecryptor.setColumnCryptoMetadata(columnPath, true, true, (byte[]) null, columnOrdinal);
                        }
                        else { // Column encrypted with column key
                            encryptedMetadata = true;
                        }
                    }

                    try {
                        if (encryptedMetadata) {
                            // TODO: We decrypted data before filter projection. This could send unnecessary traffic to KMS.
                            //  In parquet-mr, it uses lazy decyrption but that required to change ColumnChunkMetadata. We will improve it alter.
                            metaData = decryptMetadata(rowGroup, cryptoMetaData, columnChunk, fileDecryptor, columnOrdinal);
                            columnPath = getPath(metaData);
                        }
                        ColumnChunkMetaData column = buildColumnChunkMetaData(metaData, columnPath, messageType.getType(columnPath.toArray()).asPrimitiveType());
                        column.setColumnIndexReference(toColumnIndexReference(columnChunk));
                        column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
                        blockMetaData.addColumn(column);
                    }
                    catch (KeyAccessDeniedException e) {
                        ColumnChunkMetaData column = new HiddenColumnChunkMetaData(columnPath, filePath);
                        blockMetaData.addColumn(column);
                    }
                }
                blockMetaData.setPath(filePath);
                blocks.add(blockMetaData);
            }
        }

        Map<String, String> keyValueMetaData = new HashMap<>();
        List<KeyValue> keyValueList = fileMetaData.getKey_value_metadata();
        if (keyValueList != null) {
            for (KeyValue keyValue : keyValueList) {
                keyValueMetaData.put(keyValue.key, keyValue.value);
            }
        }
        ParquetMetadata parquetMetadata = new ParquetMetadata(new org.apache.parquet.hadoop.metadata.FileMetaData(messageType, keyValueMetaData, fileMetaData.getCreated_by()), blocks);
        return new ParquetFileMetadata(parquetMetadata, toIntExact(metadataLength));
    }

    private static ColumnMetaData decryptMetadata(RowGroup rowGroup, ColumnCryptoMetaData cryptoMetaData, ColumnChunk columnChunk, InternalFileDecryptor fileDecryptor, int columnOrdinal)
    {
        EncryptionWithColumnKey columnKeyStruct = cryptoMetaData.getENCRYPTION_WITH_COLUMN_KEY();
        List<String> pathList = columnKeyStruct.getPath_in_schema();
        byte[] columnKeyMetadata = columnKeyStruct.getKey_metadata();
        ColumnPath columnPath = ColumnPath.get(pathList.toArray(new String[pathList.size()]));
        byte[] encryptedMetadataBuffer = columnChunk.getEncrypted_column_metadata();

        // Decrypt the ColumnMetaData
        InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.setColumnCryptoMetadata(columnPath, true, false, columnKeyMetadata, columnOrdinal);
        ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadataBuffer);
        byte[] columnMetaDataAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleType.ColumnMetaData, rowGroup.ordinal, columnOrdinal, -1);
        try {
            return Util.readColumnMetaData(tempInputStream, columnDecryptionSetup.getMetaDataDecryptor(), columnMetaDataAAD);
        }
        catch (IOException e) {
            throw new ParquetCryptoRuntimeException(columnPath + ". Failed to decrypt column metadata", e);
        }
    }

    public static ColumnChunkMetaData buildColumnChunkMetaData(ColumnMetaData metaData, ColumnPath columnPath, PrimitiveType type)
    {
        return ColumnChunkMetaData.get(
                columnPath,
                type,
                CompressionCodecName.fromParquet(metaData.codec),
                PARQUET_METADATA_CONVERTER.convertEncodingStats(metaData.encoding_stats),
                readEncodings(metaData.encodings),
                readStats(metaData.statistics, type.getPrimitiveTypeName()),
                metaData.data_page_offset,
                metaData.dictionary_page_offset,
                metaData.num_values,
                metaData.total_compressed_size,
                metaData.total_uncompressed_size);
    }

    private static ColumnPath getPath(ColumnMetaData metaData)
    {
        String[] path = metaData.path_in_schema.toArray(new String[0]);
        return ColumnPath.get(path);
    }

    private static void verifyFooterIntegrity(BasicSliceInput from, InternalFileDecryptor fileDecryptor, int combinedFooterLength)
    {
        byte[] nonce = new byte[AesCipher.NONCE_LENGTH];
        from.read(nonce);
        byte[] gcmTag = new byte[AesCipher.GCM_TAG_LENGTH];
        from.read(gcmTag);

        AesGcmEncryptor footerSigner = fileDecryptor.createSignedFooterEncryptor();
        int footerSignatureLength = AesCipher.NONCE_LENGTH + AesCipher.GCM_TAG_LENGTH;
        byte[] serializedFooter = new byte[combinedFooterLength - footerSignatureLength];
        from.setPosition(0);
        from.read(serializedFooter, 0, serializedFooter.length);

        byte[] signedFooterAAD = AesCipher.createFooterAad(fileDecryptor.getFileAAD());
        byte[] encryptedFooterBytes = footerSigner.encrypt(false, serializedFooter, nonce, signedFooterAAD);
        byte[] calculatedTag = new byte[AesCipher.GCM_TAG_LENGTH];
        System.arraycopy(encryptedFooterBytes, encryptedFooterBytes.length - AesCipher.GCM_TAG_LENGTH, calculatedTag, 0, AesCipher.GCM_TAG_LENGTH);
        if (!Arrays.equals(gcmTag, calculatedTag)) {
            throw new TagVerificationException("Signature mismatch in plaintext footer");
        }
    }

    private static MessageType readParquetSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getTypeName(element.type), Repetition.valueOf(element.repetition_type.name()));
                if (element.isSetType_length()) {
                    primitiveBuilder.length(element.type_length);
                }
                if (element.isSetPrecision()) {
                    primitiveBuilder.precision(element.precision);
                }
                if (element.isSetScale()) {
                    primitiveBuilder.scale(element.scale);
                }
                typeBuilder = primitiveBuilder;
            }

            if (element.isSetConverted_type()) {
                typeBuilder.as(getOriginalType(element.converted_type));
            }
            if (element.isSetField_id()) {
                typeBuilder.id(element.field_id);
            }
            typeBuilder.named(element.name.toLowerCase(Locale.ENGLISH));
        }
    }

    public static org.apache.parquet.column.statistics.Statistics<?> readStats(Statistics statistics, PrimitiveTypeName type)
    {
        org.apache.parquet.column.statistics.Statistics<?> stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type);
        if (statistics != null) {
            if (statistics.isSetMax() && statistics.isSetMin()) {
                stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
            }
            stats.setNumNulls(statistics.null_count);
        }
        return stats;
    }

    private static Set<org.apache.parquet.column.Encoding> readEncodings(List<Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (Encoding encoding : encodings) {
            columnEncodings.add(org.apache.parquet.column.Encoding.valueOf(encoding.name()));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static PrimitiveTypeName getTypeName(Type type)
    {
        switch (type) {
            case BYTE_ARRAY:
                return PrimitiveTypeName.BINARY;
            case INT64:
                return PrimitiveTypeName.INT64;
            case INT32:
                return PrimitiveTypeName.INT32;
            case BOOLEAN:
                return PrimitiveTypeName.BOOLEAN;
            case FLOAT:
                return PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveTypeName.DOUBLE;
            case INT96:
                return PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private static OriginalType getOriginalType(ConvertedType type)
    {
        switch (type) {
            case UTF8:
                return OriginalType.UTF8;
            case MAP:
                return OriginalType.MAP;
            case MAP_KEY_VALUE:
                return OriginalType.MAP_KEY_VALUE;
            case LIST:
                return OriginalType.LIST;
            case ENUM:
                return OriginalType.ENUM;
            case DECIMAL:
                return OriginalType.DECIMAL;
            case DATE:
                return OriginalType.DATE;
            case TIME_MILLIS:
                return OriginalType.TIME_MILLIS;
            case TIMESTAMP_MICROS:
                return OriginalType.TIMESTAMP_MICROS;
            case TIMESTAMP_MILLIS:
                return OriginalType.TIMESTAMP_MILLIS;
            case INTERVAL:
                return OriginalType.INTERVAL;
            case INT_8:
                return OriginalType.INT_8;
            case INT_16:
                return OriginalType.INT_16;
            case INT_32:
                return OriginalType.INT_32;
            case INT_64:
                return OriginalType.INT_64;
            case UINT_8:
                return OriginalType.UINT_8;
            case UINT_16:
                return OriginalType.UINT_16;
            case UINT_32:
                return OriginalType.UINT_32;
            case UINT_64:
                return OriginalType.UINT_64;
            case JSON:
                return OriginalType.JSON;
            case BSON:
                return OriginalType.BSON;
            default:
                throw new IllegalArgumentException("Unknown converted type " + type);
        }
    }

    @Override
    public ParquetFileMetadata getParquetMetadata(FSDataInputStream inputStream, ParquetDataSourceId parquetDataSourceId, long fileSize, boolean cacheable, FileDecryptionProperties fileDecryptionProperties, InternalFileDecryptor fileDecryptor)
            throws IOException
    {
        return readFooter(inputStream, new Path(parquetDataSourceId.toString()), fileSize, fileDecryptionProperties, fileDecryptor);
    }

    private static IndexReference toColumnIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetColumn_index_offset() && columnChunk.isSetColumn_index_length()) {
            return new IndexReference(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
        }
        return null;
    }

    private static IndexReference toOffsetIndexReference(ColumnChunk columnChunk)
    {
        if (columnChunk.isSetOffset_index_offset() && columnChunk.isSetOffset_index_length()) {
            return new IndexReference(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
        }
        return null;
    }
}
