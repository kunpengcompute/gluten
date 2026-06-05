/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution;

import com.huawei.boostkit.spark.jni.OrcColumnarBatchWriter;
import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.connector.write.DataFileJson;
import org.apache.gluten.connector.write.MetricsWrapper;
import org.apache.gluten.connector.write.PartitionDataJson;
import org.apache.gluten.metrics.BatchWriteMetrics;
import org.apache.gluten.runtime.OmniRuntime;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.orc.OrcFile;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * JNI wrapper for Iceberg columnar write (Omni backend). Initialized with StructType and omniTypes.
 * When format=1 uses ParquetColumnarBatchWriter for Parquet; when format=0 uses
 * OrcColumnarBatchWriter for ORC. On commit, builds DataFileJson, serializes to JSON array, and
 * returns to the caller for IcebergWriteUtil.commitDataFiles.
 *
 * @since 2026
 */
public class IcebergWriteJniWrapper implements RuntimeAware {
    /** Must match IcebergWriteExec.getFileFormat: PARQUET=1, ORC=0 */
    private static final int FORMAT_PARQUET = 1;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OmniRuntime runtime;
    private WriterState state;

    public IcebergWriteJniWrapper(OmniRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Returns the runtime handle.
     *
     * @return the runtime handle
     */
    @Override
    public long rtHandle() {
        return runtime.getHandle();
    }

    /**
     * Initializes writer state with StructType and omniTypes. Must be called once before write/commit/metrics.
     *
     * @param schema   write schema
     * @param omniTypes omni type ids per column
     * @param params   directory, format, codec, partition/task ids, partition spec, sort order, rebase flag
     */
    public void init(StructType schema, int[] omniTypes, IcebergWriterInitParams params) {
        if (state != null) {
            throw new IllegalStateException("Already initialized");
        }
        state = new WriterState(schema, omniTypes, params);
    }

    /**
     * Writes one columnar batch: restores ColumnarBatch from batchHandle and passes to current writer.
     *
     * @param batchHandle native batch handle to restore
     * @deprecated Use {@link #write(ColumnarBatch)}; caller supplies ColumnarBatch.
     */
    @Deprecated
    public void write(long batchHandle) {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        ColumnarBatch batch = ColumnarBatches.create(batchHandle);
        try {
            state.write(batch);
        } finally {
            // Caller owns batch lifecycle; do not close here. No-op to avoid empty block (G.FMT.07).
            batch.hashCode();
        }
    }

    /**
     * Writes one columnar batch. Uses Parquet/ORC writer.write internally.
     *
     * @param batch columnar batch to write
     */
    public void write(ColumnarBatch batch) {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        state.write(batch);
    }

    /**
     * Commits the current writer: closes files and returns serialized DataFileJson JSON array.
     *
     * @return array of DataFileJson JSON strings
     */
    public String[] commit() {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        WriterState s = state;
        state = null;
        try {
            List<String> list = s.commit();
            return list.toArray(new String[0]);
        } finally {
            s.close();
        }
    }

    /**
     * Returns write metrics (bytes written, file count, IO time, etc.).
     *
     * @return metrics instance
     */
    public BatchWriteMetrics metrics() {
        if (state == null) {
            return new BatchWriteMetrics(0L, 0, 0L, 0L);
        }
        return state.metrics();
    }

    /** Holder for init parameters to keep method parameter count within limit. */
    public static final class IcebergWriterInitParams {
        /** File format (1=Parquet, 0=ORC). */
        public final int format;

        /** Write directory path. */
        public final String directory;

        /** Compression codec. */
        public final String codec;

        /** Spark partition id. */
        public final int partitionId;

        /** Spark task id. */
        public final long taskId;

        /** Operation id (e.g. queryId-epochId). */
        public final String operationId;

        /** Partition spec. */
        public final PartitionSpec partitionSpec;

        /** Sort order. */
        public final SortOrder sortOrder;

        /** Whether to use LEGACY date rebase for Parquet. */
        public final boolean isLegacyDatetimeRebase;

        public IcebergWriterInitParams(int format, String directory, String codec, int partitionId, long taskId,
                String operationId, PartitionSpec partitionSpec, SortOrder sortOrder, boolean isLegacyDatetimeRebase) {
            this.format = format;
            this.directory = directory;
            this.codec = codec;
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.operationId = operationId;
            this.partitionSpec = partitionSpec;
            this.sortOrder = sortOrder;
            this.isLegacyDatetimeRebase = isLegacyDatetimeRebase;
        }
    }

    /**
     * Per-writer state: single writer when unpartitioned; multiple writers per partition key when
     * partitioned.
     */
    private static final class WriterState {
        private final StructType schema;
        private final int[] omniTypes;
        private final int format;
        private final String directory;
        private final String codec;
        private final int partitionId;
        private final long taskId;
        private final String operationId;
        private final PartitionSpec partitionSpec;
        private final SortOrder sortOrder;
        private final long startTimeNs = System.nanoTime();
        private final boolean isLegacyDatetimeRebase;

        /** Used when unpartitioned (Parquet or ORC depending on format). */
        private Object currentWriter;

        private String currentPath;
        private long currentRecordCount;
        private int fileIndex;
        private final List<String> dataFilesJson = new ArrayList<>();
        private long totalBytesWritten;
        private int numFiles;

        /** For unpartitioned: from first row; for partitioned: per-writer. */
        private String partitionDataJson;

        /** When partitioned: partitionKey (partitionDataJson) -> writer info for that partition. */
        private final Map<String, FileWriterInfo> partitionWriters = new LinkedHashMap<>();

        WriterState(StructType schema, int[] omniTypes, IcebergWriterInitParams params) {
            this.schema = schema;
            this.omniTypes = omniTypes;
            this.format = params.format;
            this.directory = params.directory;
            this.codec = params.codec;
            this.partitionId = params.partitionId;
            this.taskId = params.taskId;
            this.operationId = params.operationId;
            this.partitionSpec = params.partitionSpec;
            this.sortOrder = params.sortOrder;
            this.isLegacyDatetimeRebase = params.isLegacyDatetimeRebase;
        }

        /** Writer, path, row count, and partition JSON for one partition (writer is Parquet or ORC). */
        private static final class FileWriterInfo {
            final Object writer;
            final String path;
            long recordCount;
            final String partitionDataJson;

            FileWriterInfo(Object writer, String path, String partitionDataJson) {
                this.writer = writer;
                this.path = path;
                this.recordCount = 0L;
                this.partitionDataJson = partitionDataJson;
            }
        }

        void write(ColumnarBatch batch) {
            if (partitionSpec.fields().isEmpty()) {
                writeUnpartitioned(batch);
                return;
            }
            writePartitioned(batch);
        }

        private void writeUnpartitioned(ColumnarBatch batch) {
            if (currentWriter == null) {
                openNewFile(null);
            }
            boolean[] dataColumnsIds = new boolean[schema.fields().length];
            Arrays.fill(dataColumnsIds, true);
            if (format == FORMAT_PARQUET) {
                if (currentWriter instanceof ParquetColumnarBatchWriter) {
                    ((ParquetColumnarBatchWriter) currentWriter).write(omniTypes, dataColumnsIds, batch);
                } else if (currentWriter instanceof OrcColumnarBatchWriter) {
                    ((OrcColumnarBatchWriter) currentWriter).write(omniTypes, dataColumnsIds, batch);
                } else {
                    throw new IllegalStateException("Unexpected writer type: " + currentWriter.getClass().getName());
                }
            } else if (currentWriter instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) currentWriter).write(omniTypes, dataColumnsIds, batch);
            } else {
                throw new IllegalStateException("Unexpected writer type: " + currentWriter.getClass().getName());
            }
            currentRecordCount += batch.numRows();
        }

        /**
         * Partitioned write: compute partition key per row, group by key, use multiple writers per
         * partition and splitWrite for contiguous ranges.
         *
         * @param batch columnar batch to write
         */
        private void writePartitioned(ColumnarBatch batch) {
            int numRows = batch.numRows();
            if (numRows == 0) {
                return;
            }
            Map<String, List<Integer>> partitionToRows = new LinkedHashMap<>();
            for (int row = 0; row < numRows; row++) {
                String key = extractPartitionDataJson(batch, row);
                partitionToRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
            boolean[] dataColumnsIds = new boolean[schema.fields().length];
            Arrays.fill(dataColumnsIds, true);
            for (Map.Entry<String, List<Integer>> e : partitionToRows.entrySet()) {
                String partitionKey = e.getKey();
                List<Integer> indices = e.getValue();
                indices.sort(Integer::compareTo);
                FileWriterInfo info = getOrCreateWriter(partitionKey);
                List<int[]> ranges = contiguousRanges(indices);
                writeRangesToPartition(info, ranges, dataColumnsIds, batch);
            }
        }

        private void writeRangesToPartition(FileWriterInfo info, List<int[]> ranges,
                boolean[] dataColumnsIds, ColumnarBatch batch) {
            for (int[] r : ranges) {
                int start = r[0];
                int end = r[1];
                if (format == FORMAT_PARQUET) {
                    if (info.writer instanceof ParquetColumnarBatchWriter) {
                        ((ParquetColumnarBatchWriter) info.writer).splitWrite(omniTypes, omniTypes,
                                dataColumnsIds, batch, start, end);
                    } else if (info.writer instanceof OrcColumnarBatchWriter) {
                        ((OrcColumnarBatchWriter) info.writer).splitWrite(omniTypes, omniTypes,
                                dataColumnsIds, batch, start, end);
                    } else {
                        throw new IllegalStateException("Unexpected writer type: " + info.writer.getClass().getName());
                    }
                } else if (info.writer instanceof OrcColumnarBatchWriter) {
                    ((OrcColumnarBatchWriter) info.writer).splitWrite(omniTypes, omniTypes,
                            dataColumnsIds, batch, start, end);
                } else {
                    throw new IllegalStateException("Unexpected writer type: " + info.writer.getClass().getName());
                }
                info.recordCount += (end - start);
            }
        }

        /**
         * Converts sorted row indices into contiguous [start, end) ranges.
         *
         * @param sortedIndices sorted row indices
         * @return list of [start, end) ranges
         */
        private static List<int[]> contiguousRanges(List<Integer> sortedIndices) {
            List<int[]> out = new ArrayList<>();
            if (sortedIndices.isEmpty()) {
                return out;
            }
            int start = sortedIndices.get(0);
            int prev = start;
            for (int i = 1; i < sortedIndices.size(); i++) {
                int cur = sortedIndices.get(i);
                if (cur != prev + 1) {
                    out.add(new int[] {start, prev + 1});
                    start = cur;
                }
                prev = cur;
            }
            out.add(new int[] {start, prev + 1});
            return out;
        }

        private FileWriterInfo getOrCreateWriter(String partitionKey) {
            FileWriterInfo info = partitionWriters.get(partitionKey);
            if (info != null) {
                return info;
            }
            fileIndex++;
            String ext = (format == FORMAT_PARQUET) ? "parquet" : "orc";
            String pathSegment = PartitionDataJson.toPathSegment(partitionKey, partitionSpec);
            String fileName = String.format(Locale.ROOT, "00000-%d-%s-%d-%05d.%s",
                    partitionId, operationId, taskId, fileIndex, ext);
            String path;
            if (pathSegment.isEmpty()) {
                path = String.format(Locale.ROOT, "%s/%s", directory, fileName);
            } else {
                path = String.format(Locale.ROOT, "%s/%s/%s", directory, pathSegment, fileName);
            }
            try {
                Path p = new Path(path);
                FileSystem fs = p.getFileSystem(new Configuration());
                fs.mkdirs(p.getParent());
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to create partition directory for " + path, ex);
            }
            Object writer;
            if (format == FORMAT_PARQUET) {
                ParquetColumnarBatchWriter pw = new ParquetColumnarBatchWriter(isLegacyDatetimeRebase);
                pw.initializeSchemaJava(schema);
                try {
                    pw.initializeWriterJava(new Path(path));
                } catch (IOException ex) {
                    throw new IllegalStateException("Failed to open " + path, ex);
                }
                writer = pw;
            } else {
                // Iceberg ORC: use corrected timestamp write mode; plain Omni ORC uses default writer.
                OrcColumnarBatchWriter ow = new OrcColumnarBatchWriter(true);
                Path pathObj = new Path(path);
                Configuration conf = new Configuration();
                ow.initializeOutputStreamJava(pathObj.toUri());
                ow.initializeSchemaTypeJava(schema);
                try {
                    OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).fileSystem(pathObj.getFileSystem(conf));
                    ow.initializeWriterJava(pathObj.toUri(), schema, opts);
                } catch (IOException ex) {
                    throw new IllegalStateException("Failed to open " + path, ex);
                }
                writer = ow;
            }
            info = new FileWriterInfo(writer, path, partitionKey);
            partitionWriters.put(partitionKey, info);
            return info;
        }

        private void openNewFile(String partitionKey) {
            String ext = (format == FORMAT_PARQUET) ? "parquet" : "orc";
            fileIndex++;
            currentPath = String.format(Locale.ROOT, "%s/00000-%d-%s-%d-%05d.%s",
                    directory, partitionId, operationId, taskId, fileIndex, ext);
            partitionDataJson = partitionKey;
            if (format == FORMAT_PARQUET) {
                ParquetColumnarBatchWriter pw = new ParquetColumnarBatchWriter(isLegacyDatetimeRebase);
                pw.initializeSchemaJava(schema);
                try {
                    pw.initializeWriterJava(new Path(currentPath));
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to open " + currentPath, e);
                }
                currentWriter = pw;
            } else {
                // Iceberg ORC: use corrected timestamp write mode; plain Omni ORC uses default writer.
                OrcColumnarBatchWriter ow = new OrcColumnarBatchWriter(true);
                Path pathObj = new Path(currentPath);
                Configuration conf = new Configuration();
                ow.initializeOutputStreamJava(pathObj.toUri());
                ow.initializeSchemaTypeJava(schema);
                try {
                    OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).fileSystem(pathObj.getFileSystem(conf));
                    ow.initializeWriterJava(pathObj.toUri(), schema, opts);
                } catch (IOException ex) {
                    throw new IllegalStateException("Failed to open " + currentPath, ex);
                }
                currentWriter = ow;
            }
            currentRecordCount = 0L;
        }

        /**
         * Extracts partition column values for the given row and serializes to partitionDataJson.
         *
         * @param batch columnar batch
         * @param row  row index
         * @return partition data JSON string
         */
        private String extractPartitionDataJson(ColumnarBatch batch, int row) {
            List<Object> values = new ArrayList<>(partitionSpec.fields().size());
            for (int i = 0; i < partitionSpec.fields().size(); i++) {
                String name = partitionSpec.fields().get(i).name();
                int colIdx = schema.fieldIndex(name);
                ColumnVector col = batch.column(colIdx);
                values.add(getPartitionValue(col, schema.fields()[colIdx].dataType(), row).orElse(null));
            }
            return PartitionDataJson.toJson(values).orElse("");
        }

        private static Optional<Object> getPartitionValue(ColumnVector col, DataType dataType, int row) {
            if (col.isNullAt(row)) {
                return Optional.empty();
            }
            if (dataType instanceof BooleanType) {
                return Optional.of(col.getBoolean(row));
            }
            if (dataType instanceof IntegerType) {
                return Optional.of(col.getInt(row));
            }
            if (dataType instanceof LongType) {
                return Optional.of(col.getLong(row));
            }
            if (dataType instanceof ShortType) {
                return Optional.of(col.getShort(row));
            }
            if (dataType instanceof ByteType) {
                return Optional.of(col.getByte(row));
            }
            if (dataType instanceof FloatType) {
                return Optional.of(col.getFloat(row));
            }
            if (dataType instanceof DoubleType) {
                return Optional.of(col.getDouble(row));
            }
            if (dataType instanceof StringType || dataType instanceof CharType || dataType instanceof VarcharType) {
                return Optional.of(col.getUTF8String(row).toString());
            }
            if (dataType instanceof DateType) {
                return Optional.of(col.getInt(row));
            }
            if (dataType instanceof TimestampType) {
                return Optional.of(col.getLong(row));
            }
            if (dataType instanceof BinaryType) {
                return Optional.of(col.getBinary(row));
            }
            if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                return Optional.of(col.getDecimal(row, decimalType.precision(), decimalType.scale())
                        .toJavaBigDecimal());
            }
            throw new UnsupportedOperationException("Unsupported partition column type: " + dataType);
        }

        private void closeCurrentFile() {
            if (currentWriter == null) {
                return;
            }
            if (currentWriter instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) currentWriter).close();
            } else if (currentWriter instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) currentWriter).close();
            } else {
                throw new IllegalStateException("Unexpected writer type: " + currentWriter.getClass().getName());
            }
            long fileSize = 0L;
            try {
                Path path = new Path(currentPath);
                FileSystem fs = path.getFileSystem(new Configuration());
                if (fs.exists(path)) {
                    fileSize = fs.getFileStatus(path).getLen();
                }
            } catch (IOException ignored) {
                // Best-effort file size; proceed without it.
            }
            totalBytesWritten += fileSize;
            numFiles++;
            DataFileJson df = new DataFileJson();
            df.setPath(currentPath);
            df.setFileSizeInBytes(fileSize);
            df.setMetrics(new MetricsWrapper(new Metrics(currentRecordCount, null, null, null, null, null, null)));
            df.setSplitOffsets(Collections.emptyList());
            df.setPartitionDataJson(partitionDataJson);
            try {
                dataFilesJson.add(MAPPER.writeValueAsString(df));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalStateException("Serialize DataFileJson failed", e);
            }
            currentWriter = null;
        }

        private void closePartitionFiles() {
            for (FileWriterInfo info : partitionWriters.values()) {
                if (info.writer instanceof ParquetColumnarBatchWriter) {
                    ((ParquetColumnarBatchWriter) info.writer).close();
                } else if (info.writer instanceof OrcColumnarBatchWriter) {
                    ((OrcColumnarBatchWriter) info.writer).close();
                } else {
                    throw new IllegalStateException("Unexpected writer type: " + info.writer.getClass().getName());
                }
                long fileSize = 0L;
                try {
                    Path path = new Path(info.path);
                    FileSystem fs = path.getFileSystem(new Configuration());
                    if (fs.exists(path)) {
                        fileSize = fs.getFileStatus(path).getLen();
                    }
                } catch (IOException ignored) {
                    // Best-effort file size; proceed without it.
                }
                totalBytesWritten += fileSize;
                numFiles++;
                DataFileJson df = new DataFileJson();
                df.setPath(info.path);
                df.setFileSizeInBytes(fileSize);
                df.setMetrics(new MetricsWrapper(new Metrics(info.recordCount, null, null, null, null, null, null)));
                df.setSplitOffsets(Collections.emptyList());
                df.setPartitionDataJson(info.partitionDataJson);
                try {
                    dataFilesJson.add(MAPPER.writeValueAsString(df));
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    throw new IllegalStateException("Serialize DataFileJson failed", e);
                }
            }
            partitionWriters.clear();
        }

        /**
         * Commits all written files and returns their DataFileJson JSON strings.
         *
         * @return list of DataFileJson JSON strings
         */
        List<String> commit() {
            if (partitionSpec.fields().isEmpty()) {
                closeCurrentFile();
            } else {
                closePartitionFiles();
            }
            return new ArrayList<>(dataFilesJson);
        }

        BatchWriteMetrics metrics() {
            long wallNs = System.nanoTime() - startTimeNs;
            return new BatchWriteMetrics(totalBytesWritten, numFiles, 0L, wallNs);
        }

        void close() {
            if (currentWriter != null) {
                if (format == FORMAT_PARQUET && currentWriter instanceof ParquetColumnarBatchWriter) {
                    ((ParquetColumnarBatchWriter) currentWriter).close();
                } else if (currentWriter instanceof OrcColumnarBatchWriter) {
                    ((OrcColumnarBatchWriter) currentWriter).close();
                } else {
                    throw new IllegalStateException(
                            "Unexpected writer type: " + currentWriter.getClass().getName());
                }
                currentWriter = null;
            }
            for (FileWriterInfo info : partitionWriters.values()) {
                if (format == FORMAT_PARQUET && info.writer instanceof ParquetColumnarBatchWriter) {
                    ((ParquetColumnarBatchWriter) info.writer).close();
                } else if (info.writer instanceof OrcColumnarBatchWriter) {
                    ((OrcColumnarBatchWriter) info.writer).close();
                } else {
                    throw new IllegalStateException(
                            "Unexpected writer type: " + info.writer.getClass().getName());
                }
            }
            partitionWriters.clear();
        }
    }
}
