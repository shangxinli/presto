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

import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.ColumnReaderFactory;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetResultVerifierUtils;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.OffsetRange;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private final List<BlockMetaData> blocks;
    private final List<PrimitiveColumnIO> columns;
    private final ParquetDataSource dataSource;
    private final AggregatedMemoryContext systemMemoryContext;
    private final boolean batchReadEnabled;
    private final boolean enableVerification;

    private int currentBlock = -1;
    private BlockMetaData currentBlockMetadata;
    private long currentPosition;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private int batchSize;

    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private final ColumnReader[] columnReaders;
    protected final ColumnReader[] verificationColumnReaders;
    private long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;
    private final long maxReadBlockBytes;
    private int maxBatchSize = MAX_VECTOR_LENGTH;

    private AggregatedMemoryContext currentRowGroupMemoryContext;

    private final List<ColumnIndexStore> blockIndexStores;
    private final List<RowRanges> blockRowRanges;
    private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();

    public ParquetReader(MessageColumnIO
            messageColumnIO,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource,
            AggregatedMemoryContext systemMemoryContext,
            DataSize maxReadBlockSize,
            boolean batchReadEnabled,
            boolean enableVerification)
    {
        this.blocks = blocks;
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();
        this.maxReadBlockBytes = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null").toBytes();
        this.batchReadEnabled = batchReadEnabled;
        columns = messageColumnIO.getLeaves();
        columnReaders = new ColumnReader[columns.size()];
        this.enableVerification = enableVerification;
        verificationColumnReaders = enableVerification ? new ColumnReader[columns.size()] : null;
        maxBytesPerCell = new long[columns.size()];
        this.blockIndexStores = listWithNulls(this.blocks.size());
        this.blockRowRanges = listWithNulls(this.blocks.size());
        for (PrimitiveColumnIO column : columns) {
            ColumnDescriptor columnDescriptor = column.getColumnDescriptor();
            this.paths.put(ColumnPath.get(columnDescriptor.getPath()), columnDescriptor);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        currentRowGroupMemoryContext.close();
        dataSource.close();
    }

    public long getPosition()
    {
        return currentPosition;
    }

    public int nextBatch()
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_VECTOR_LENGTH);
        batchSize = toIntExact(min(batchSize, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        Arrays.stream(columnReaders)
                .forEach(reader -> reader.prepareNextRead(batchSize));

        if (enableVerification) {
            Arrays.stream(verificationColumnReaders)
                    .forEach(reader -> reader.prepareNextRead(batchSize));
        }

        return batchSize;
    }

    private boolean advanceToNextRowGroup()
    {
        currentBlock = currentBlock + 1;
        currentRowGroupMemoryContext.close();
        currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();

        if (currentBlock == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentBlock);

        getColumnIndexStore(currentBlock);
        RowRanges rowRanges = getRowRanges(currentBlock);
        long rowCount = rowRanges.rowCount();
        if (rowCount == 0) {
            return false;
        }
        if (rowCount == blocks.get(currentBlock).getRowCount()) {
            // TODO: All rows are matching -> fall back to the non-filtering path
        }

        nextRowInGroup = 0L;
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        initializeColumnReaders();
        return true;
    }

    private ColumnChunk readArray(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %d", parameters.size());
        Field elementField = field.getChildren().get(0).get();
        ColumnChunk columnChunk = readColumnChunk(elementField);
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();

        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block arrayBlock = ArrayBlock.fromElementBlock(valueIsNull.size(), Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), columnChunk.getBlock());
        return new ColumnChunk(arrayBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readMap(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %d", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        ColumnChunk columnChunk = readColumnChunk(field.getChildren().get(0).get());
        blocks[0] = columnChunk.getBlock();
        blocks[1] = readColumnChunk(field.getChildren().get(1).get()).getBlock();
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();
        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block mapBlock = ((MapType) field.getType()).createBlockFromKeyValue(offsets.size() - 1, Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), blocks[0], blocks[1]);
        return new ColumnChunk(mapBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readStruct(GroupField field)
            throws IOException
    {
        List<TypeSignatureParameter> fields = field.getType().getTypeSignature().getParameters();
        Block[] blocks = new Block[fields.size()];
        ColumnChunk columnChunk = null;
        List<Optional<Field>> parameters = field.getChildren();
        for (int i = 0; i < fields.size(); i++) {
            Optional<Field> parameter = parameters.get(i);
            if (parameter.isPresent()) {
                columnChunk = readColumnChunk(parameter.get());
                blocks[i] = columnChunk.getBlock();
            }
        }
        for (int i = 0; i < fields.size(); i++) {
            if (blocks[i] == null) {
                blocks[i] = RunLengthEncodedBlock.create(field.getType(), null, columnChunk.getBlock().getPositionCount());
            }
        }
        BooleanList structIsNull = StructColumnReader.calculateStructOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        boolean[] structIsNullVector = structIsNull.toBooleanArray();
        Block rowBlock = RowBlock.fromFieldBlocks(structIsNullVector.length, Optional.of(structIsNullVector), blocks);
        return new ColumnChunk(rowBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readPrimitive(PrimitiveField field)
            throws IOException
    {
        ColumnDescriptor columnDescriptor = field.getDescriptor();

        int fieldId = field.getId();
        ColumnReader columnReader = columnReaders[fieldId];
        if (!columnReader.isInitialized()) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = metadata.getStartingPos();
            int totalSize = toIntExact(metadata.getTotalSize());
            byte[] buffer = allocateBlock(totalSize);
            dataSource.readFully(startingPosition, buffer);
            ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
            ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor, buffer, 0);

            OffsetIndex offsetIndex = getColumnIndexStore(currentBlock).getOffsetIndex(metadata.getPath());
            OffsetIndex filteredOffsetIndex = ColumnIndexFilterUtils.filterOffsetIndex(offsetIndex, getRowRanges(currentBlock),
                    blocks.get(currentBlock).getRowCount());

            List<OffsetRange> offsetRanges = ColumnIndexFilterUtils.calculateOffsetRanges(filteredOffsetIndex, metadata,
                    offsetIndex.getOffset(0), startingPosition);
            // List<OffsetRange> offsetRanges = calculateOffsetRanges(columnChunk, startingPosition);
            columnReader.init(columnChunk.readAllPages(null), field);

            if (enableVerification) {
                ColumnReader verificationColumnReader = verificationColumnReaders[field.getId()];
                ParquetColumnChunk columnChunkVerfication = new ParquetColumnChunk(descriptor, buffer, 0);
                verificationColumnReader.init(columnChunkVerfication.readAllPages(null), field);
            }
        }

        ColumnChunk columnChunk = columnReader.readNext();
        columnChunk = typeCoercion(columnChunk, field.getDescriptor().getPrimitiveType().getPrimitiveTypeName(), field.getType());

        if (enableVerification) {
            ColumnReader verificationColumnReader = verificationColumnReaders[field.getId()];
            ColumnChunk expected = verificationColumnReader.readNext();
            ParquetResultVerifierUtils.verifyColumnChunks(columnChunk, expected, columnDescriptor.getPath().length > 1, field, dataSource.getId());
        }

        // update max size per primitive column chunk
        long bytesPerCell = columnChunk.getBlock().getSizeInBytes() / batchSize;
        if (maxBytesPerCell[fieldId] < bytesPerCell) {
            // update batch size
            maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[fieldId] + bytesPerCell;
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxReadBlockBytes / maxCombinedBytesPerRow)));
            maxBytesPerCell[fieldId] = bytesPerCell;
        }
        return columnChunk;
    }

    private List<OffsetRange> calculateOffsetRanges(ParquetColumnChunk columnChunk, long startingPosition)
    {
        List<OffsetRange> offsetRanges = new ArrayList<>();
        if (columnChunk.getDescriptor().getColumnDescriptor().getPath()[0].equals("intcol")) {
            offsetRanges.add(new OffsetRange(4 - startingPosition, 104));
        }
        else {
            offsetRanges.add(new OffsetRange(53436 - startingPosition, 153));
        }
        return offsetRanges;
    }

    private byte[] allocateBlock(int length)
    {
        byte[] buffer = new byte[length];
        LocalMemoryContext blockMemoryContext = currentRowGroupMemoryContext.newLocalMemoryContext(ParquetReader.class.getSimpleName());
        blockMemoryContext.setBytes(buffer.length);
        return buffer;
    }

    private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : currentBlockMetadata.getColumns()) {
            if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Metadata is missing for column: %s", columnDescriptor);
    }

    private void initializeColumnReaders()
    {
        for (PrimitiveColumnIO columnIO : columns) {
            RichColumnDescriptor column = new RichColumnDescriptor(columnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            columnReaders[columnIO.getId()] = ColumnReaderFactory.createReader(column, batchReadEnabled);
            if (enableVerification) {
                verificationColumnReaders[columnIO.getId()] = ColumnReaderFactory.createReader(column, false);
            }
        }
    }

    public Block readBlock(Field field)
            throws IOException
    {
        return readColumnChunk(field).getBlock();
    }

    private ColumnChunk readColumnChunk(Field field)
            throws IOException
    {
        ColumnChunk columnChunk;
        if (ROW.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readStruct((GroupField) field);
        }
        else if (MAP.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readMap((GroupField) field);
        }
        else if (ARRAY.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readArray((GroupField) field);
        }
        else {
            columnChunk = readPrimitive((PrimitiveField) field);
        }
        return columnChunk;
    }

    public ParquetDataSource getDataSource()
    {
        return dataSource;
    }

    public AggregatedMemoryContext getSystemMemoryContext()
    {
        return systemMemoryContext;
    }

    /**
     * The reader returns the Block type (LongArrayBlock or IntArrayBlock) based on the physical type of the column in Parquet file, but
     * operators after scan expect output block type based on the metadata defined in Hive table. This causes issues as the operators call
     * methods that are based on the expected output block type which the returned block type may not have implemented.
     * To overcome this issue rewrite the block.
     */
    private static ColumnChunk typeCoercion(ColumnChunk columnChunk, PrimitiveTypeName physicalDataType, Type outputType)
    {
        Block newBlock = null;
        if (SMALLINT.equals(outputType) || TINYINT.equals(outputType)) {
            if (columnChunk.getBlock() instanceof IntArrayBlock) {
                newBlock = rewriteIntegerArrayBlock((IntArrayBlock) columnChunk.getBlock(), outputType);
            }
            else if (columnChunk.getBlock() instanceof LongArrayBlock) {
                newBlock = rewriteLongArrayBlock((LongArrayBlock) columnChunk.getBlock(), outputType);
            }
        }
        else if (INTEGER.equals(outputType) && physicalDataType == PrimitiveTypeName.INT64) {
            if (columnChunk.getBlock() instanceof LongArrayBlock) {
                newBlock = rewriteLongArrayBlock((LongArrayBlock) columnChunk.getBlock(), outputType);
            }
        }
        else if (BIGINT.equals(outputType) && physicalDataType == PrimitiveTypeName.INT32) {
            if (columnChunk.getBlock() instanceof IntArrayBlock) {
                newBlock = rewriteIntegerArrayBlock((IntArrayBlock) columnChunk.getBlock(), outputType);
            }
        }

        if (newBlock != null) {
            return new ColumnChunk(newBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        }

        return columnChunk;
    }

    private static Block rewriteIntegerArrayBlock(IntArrayBlock intArrayBlock, Type targetType)
    {
        int positionCount = intArrayBlock.getPositionCount();
        BlockBuilder newBlockBuilder = targetType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (intArrayBlock.isNull(position)) {
                newBlockBuilder.appendNull();
            }
            else {
                targetType.writeLong(newBlockBuilder, intArrayBlock.getInt(position));
            }
        }

        return newBlockBuilder.build();
    }

    private static Block rewriteLongArrayBlock(LongArrayBlock longArrayBlock, Type targetType)
    {
        int positionCount = longArrayBlock.getPositionCount();
        BlockBuilder newBlockBuilder = targetType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (longArrayBlock.isNull(position)) {
                newBlockBuilder.appendNull();
            }
            else {
                targetType.writeLong(newBlockBuilder, longArrayBlock.getLong(position, 0));
            }
        }

        return newBlockBuilder.build();
    }

    private static <T> List<T> listWithNulls(int size)
    {
        return Stream.generate(() -> (T) null).limit(size).collect(Collectors.toCollection(ArrayList<T>::new));
    }

    private long getFilteredRecordCount()
    {
        // TODO: Add switch for useColumnIndexFilter
        long total = 0;
        for (int i = 0, n = blocks.size(); i < n; ++i) {
            total += getRowRanges(i).rowCount();
        }
        return total;
    }

    private ColumnIndexStore getColumnIndexStore(int blockIndex)
    {
        ColumnIndexStore ciStore = blockIndexStores.get(blockIndex);
        if (ciStore == null) {
            ciStore = ColumnIndexStoreImpl.create(dataSource, blocks.get(blockIndex), paths.keySet());
            blockIndexStores.set(blockIndex, ciStore);
        }
        return ciStore;
    }

    private RowRanges getRowRanges(int blockIndex)
    {
        RowRanges rowRanges = blockRowRanges.get(blockIndex);
        if (rowRanges == null) {
            // TODO: Hardcoded parquet filters
            rowRanges = ColumnIndexFilter.calculateRowRanges(FilterCompat.NOOP, getColumnIndexStore(blockIndex),
                    paths.keySet(), blocks.get(blockIndex).getRowCount());
            blockRowRanges.set(blockIndex, rowRanges);
        }
        return rowRanges;
    }
}
