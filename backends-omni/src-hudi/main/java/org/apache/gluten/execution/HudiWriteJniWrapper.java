/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution;

import com.huawei.boostkit.spark.jni.OrcColumnarBatchWriter;
import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gluten.connector.write.HudiFileInfoJson;
import org.apache.gluten.expression.OmniExpressionAdaptor;
import org.apache.gluten.metrics.BatchWriteMetrics;
import org.apache.gluten.runtime.OmniRuntime;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.vectorized.OmniColumnVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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
import org.apache.spark.sql.types.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * Task-local bridge from Spark columnar batches to Hudi base files using Omni native writers
 * ({@link ParquetColumnarBatchWriter}, {@link OrcColumnarBatchWriter}). Used by DSv2-style
 * columnar INSERT into Hudi (see {@code OmniHudiColumnarBatchDataWriter}).
 *
 * <p><b>Lifecycle</b> (one instance per Spark {@code DataWriter} task): {@link #init} once →
 * {@link #write} zero or more times → {@link #commit} (flushes files, returns JSON stats, clears
 * state for possible reuse) or {@link #close} on abort without commit.
 *
 * <p><b>Layout</b>: Non-partitioned tables append to a single rolling file under
 * {@link HudiWriterInitParams#directory}. If {@link HudiWriterInitParams#partitionColumns} is
 * non-empty, each distinct partition value tuple gets a Hive-style subdirectory
 * ({@code col1=v1/col2=v2}), a dedicated writer, and {@code .hoodie_partition_metadata} on first
 * create.
 *
 * <p><b>Hoodie metadata columns</b>: When the input schema does not already contain
 * {@code _hoodie_commit_time}, this class prepends the five standard Hudi metadata string columns
 * before writing so file layout matches Hoodie expectations; see {@link #HOODIE_META_COLUMNS}.
 *
 * <p><b>File names</b>: {@code {fileId}_{writeToken}_{operationId}.parquet|orc} where {@code fileId}
 * embeds partition id and UUID (Hudi-style uniqueness).
 *
 * <p><b>Commit payload</b>: {@link #commit} returns one JSON string per closed file
 * ({@link org.apache.gluten.connector.write.HudiFileInfoJson}) for Scala
 * {@code HudiCommitMessageBuilder} to build {@code WriterCommitMessage}.
 *
 * @since 2026
 */
public class HudiWriteJniWrapper implements RuntimeAware {
    private static final Logger LOG = LoggerFactory.getLogger(HudiWriteJniWrapper.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time";
    private static final String COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno";
    private static final String RECORD_KEY_METADATA_FIELD = "_hoodie_record_key";
    private static final String PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path";
    private static final String FILE_NAME_METADATA_FIELD = "_hoodie_file_name";

    /**
     * Order of prepended Hoodie metadata columns when {@link WriterState#shouldWriteHudiMetaColumns} is true.
     * Must stay in sync with {@link WriterState#withHudiMetaColumns} column construction.
     */
    private static final String[] HOODIE_META_COLUMNS = new String[] {
            COMMIT_TIME_METADATA_FIELD,
            COMMIT_SEQNO_METADATA_FIELD,
            RECORD_KEY_METADATA_FIELD,
            PARTITION_PATH_METADATA_FIELD,
            FILE_NAME_METADATA_FIELD
    };

    private final OmniRuntime runtime;
    private WriterState state;

    /**
     * Creates a wrapper bound to the given Omni native runtime handle.
     *
     * @param runtime Omni runtime used for JNI context (see {@link #rtHandle()}).
     */
    public HudiWriteJniWrapper(OmniRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Returns the native runtime handle for this writer (Gluten runtime integration).
     *
     * @return handle
     */
    @Override
    public long rtHandle() {
        return runtime.getHandle();
    }

    /**
     * Initializes the columnar writer for one Hudi write task.
     * Must be called once before {@link #write(ColumnarBatch)}.
     *
     * @param schema    Spark struct schema of the data to write.
     * @param omniTypes Omni type ids aligned with schema fields (from expression adaptor).
     * @param params    task output directory, format, Spark writer ids, partition/record-key columns,
     *                  and Parquet datetime rebase flag (see {@link HudiWriterInitParams}).
     */
    public void init(StructType schema, int[] omniTypes, HudiWriterInitParams params) {
        if (state != null) {
            throw new IllegalStateException("Already initialized");
        }
        state = new WriterState(schema, omniTypes, params);
    }

    /**
     * Appends one columnar batch to the current Hudi base file (opens a new file on first write).
     *
     * @param batch rows to write in columnar form.
     */
    public void write(ColumnarBatch batch) {
        if (state == null) {
            throw new IllegalStateException("Not initialized");
        }
        state.write(batch);
    }

    /**
     * Finalizes all open files and returns a JSON string per written file ({@link HudiFileInfoJson})
     * for building Hudi's {@code WriterCommitMessage}. After this call the wrapper must be
     * {@link #init(StructType, int[], HudiWriterInitParams) initialized} again for reuse.
     *
     * @return non-null array of JSON objects (possibly empty if nothing was written).
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
     * Returns write-side metrics for the current open writer state (bytes, file count, wall time).
     * After {@link #commit()}, returns zeroed metrics until {@link #init} is called again.
     *
     * @return BatchWriteMetrics
     */
    public BatchWriteMetrics metrics() {
        if (state == null) {
            return new BatchWriteMetrics(0L, 0, 0L, 0L);
        }
        return state.metrics();
    }

    /**
     * Immutable parameters passed from Spark's columnar {@code DataWriter} factory (typically
     * {@code OmniHudiDataWriteFactory}) into {@link #init}. Includes output directory, base file
     * format, Spark writer ids for file naming, partition and record-key column names for layout
     * and Hoodie metadata synthesis, and Parquet datetime rebase policy.
     */
    public static final class HudiWriterInitParams {
        /**
         * Hudi table base path subdirectory where this task writes base files.
         */
        public final String directory;

        /**
         * Compression codec name (informational; native writer may use global config).
         */
        public final String codec;

        /**
         * Hudi base file format, either parquet or orc.
         */
        public final String fileFormat;

        /**
         * Spark DataWriter partition index.
         */
        public final int partitionId;

        /**
         * Spark DataWriter task attempt id.
         */
        public final long taskId;

        /**
         * Unique id for generated file names (often query id + epoch for streaming).
         */
        public final String operationId;

        /**
         * When true, use legacy datetime rebasing for Parquet write (matches {@code SQLConf}).
         */
        public final boolean isLegacyDatetimeRebase;

        /**
         * Hudi partition columns. Empty for non-partitioned tables.
         */
        public final String[] partitionColumns;

        /**
         * Hudi record key column names (from {@code hoodie.datasource.write.recordkey.field(s)}).
         * When empty, {@link WriterState#recordKey} falls back to the first column of {@code schema}.
         */
        public final String[] recordKeyColumns;

        /**
         * Delegates to the 9-argument constructor with empty partition and record-key arrays
         * (non-partitioned table; record key = first data column).
         *
         * @param directory task output directory
         * @param codec compression codec label
         * @param fileFormat Hudi base file format, {@code parquet} or {@code orc}
         * @param partitionId Spark writer partition index
         * @param taskId Spark task attempt id
         * @param operationId Hudi instant or query id
         * @param isLegacyDatetimeRebase whether Parquet datetime values use legacy rebasing
         */
        public HudiWriterInitParams(String directory, String codec, String fileFormat, int partitionId, long taskId,
                                    String operationId, boolean isLegacyDatetimeRebase) {
            this(directory, codec, fileFormat, partitionId, taskId, operationId, isLegacyDatetimeRebase,
                    new String[0], new String[0]);
        }

        /**
         * Delegates with empty {@code recordKeyColumns} (first schema column used as record key).
         *
         * @param directory task output directory
         * @param codec compression codec label
         * @param fileFormat Hudi base file format, {@code parquet} or {@code orc}
         * @param partitionId Spark writer partition index
         * @param taskId Spark task attempt id
         * @param operationId Hudi instant or query id
         * @param isLegacyDatetimeRebase whether Parquet datetime values use legacy rebasing
         * @param partitionColumns partition field names in order
         */
        public HudiWriterInitParams(String directory, String codec, String fileFormat, int partitionId, long taskId,
                                    String operationId, boolean isLegacyDatetimeRebase, String[] partitionColumns) {
            this(directory, codec, fileFormat, partitionId, taskId, operationId, isLegacyDatetimeRebase,
                    partitionColumns, new String[0]);
        }

        /**
         * Immutable task parameters for {@link HudiWriteJniWrapper#init}.
         *
         * @param directory              task output directory (Hudi base path segment for this write)
         * @param codec                  compression codec label (informational for callers)
         * @param fileFormat             {@code parquet} or {@code orc} (case-insensitive)
         * @param partitionId            Spark {@code DataWriter} partition index
         * @param taskId                 Spark task attempt id (part of write token in file names)
         * @param operationId            Hudi instant / query id embedded in file names
         * @param isLegacyDatetimeRebase passed to Parquet writer when format is parquet
         * @param partitionColumns       partition field names in order (empty = non-partitioned write)
         * @param recordKeyColumns       record key field names (empty = use first schema column)
         */
        public HudiWriterInitParams(String directory, String codec, String fileFormat, int partitionId, long taskId,
                                    String operationId, boolean isLegacyDatetimeRebase, String[] partitionColumns,
                                    String[] recordKeyColumns) {
            this.directory = directory;
            this.codec = codec;
            this.fileFormat = fileFormat;
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.operationId = operationId;
            this.isLegacyDatetimeRebase = isLegacyDatetimeRebase;
            this.partitionColumns = partitionColumns == null ? new String[0] : partitionColumns.clone();
            this.recordKeyColumns = recordKeyColumns == null ? new String[0] : recordKeyColumns.clone();
        }
    }

    /**
     * Mutable per-task state: optional Hoodie metadata column prepending, fan-out writers for
     * partitions, file naming, and JSON lines accumulated for {@link HudiWriteJniWrapper#commit}.
     */
    private static final class WriterState {
        private final StructType schema;
        private final StructType fileSchema;
        private final int[] omniTypes;
        private final int[] fileOmniTypes;
        private final String directory;
        private final String operationId;
        private final String fileFormat;
        private final int partitionId;
        private final long taskId;
        private final boolean isLegacyDatetimeRebase;
        private final String[] partitionColumns;
        private final String[] recordKeyColumns;

        /**
         * True when input schema lacks Hoodie metadata columns and we must prepend them for file layout.
         */
        private final boolean shouldWriteHudiMetaColumns;

        private final long startTimeNs = System.nanoTime();

        private Object currentWriter;
        private String currentPath;
        private String currentFileId;
        private long currentRecordCount;
        private int fileIndex;
        private final List<String> fileInfoJsonList = new ArrayList<>();
        private final Map<String, FileWriterInfo> partitionWriters = new LinkedHashMap<>();
        private long totalBytesWritten;
        private int numFiles;

        /**
         * Captures immutable write configuration and derives file schemas when Hoodie metadata columns
         * must be synthesized.
         *
         * @param schema Spark input schema for the task
         * @param omniTypes Omni type ids aligned with {@code schema}
         * @param params immutable writer initialization parameters
         */
        WriterState(StructType schema, int[] omniTypes, HudiWriterInitParams params) {
            this.schema = schema;
            this.omniTypes = omniTypes;
            this.shouldWriteHudiMetaColumns = !hasHudiMetaColumns(schema);
            this.fileSchema = shouldWriteHudiMetaColumns ? prependHudiMetaColumns(schema) : schema;
            this.fileOmniTypes = shouldWriteHudiMetaColumns ? prependStringOmniTypes(omniTypes) : omniTypes;
            this.directory = params.directory;
            this.operationId = params.operationId;
            this.fileFormat = normalizeFileFormat(params.fileFormat);
            this.partitionId = params.partitionId;
            this.taskId = params.taskId;
            this.isLegacyDatetimeRebase = params.isLegacyDatetimeRebase;
            this.partitionColumns = params.partitionColumns.clone();
            this.recordKeyColumns = params.recordKeyColumns.clone();
        }

        private static final class FileWriterInfo {
            /**
             * Native Parquet or Orc writer handle.
             */
            final Object writer;

            /**
             * Absolute path of the open base file.
             */
            final String path;

            /**
             * Rows written through this writer for commit statistics.
             */
            long recordCount;

            /**
             * Creates writer information for one open Hudi base file.
             *
             * @param writer native Parquet or Orc writer handle
             * @param path absolute output file path
             */
            FileWriterInfo(Object writer, String path) {
                this.writer = writer;
                this.path = path;
                this.recordCount = 0L;
            }
        }

        /**
         * Checks whether the input schema already contains Hudi metadata fields.
         *
         * @param schema Spark input schema
         * @return {@code true} when metadata fields are already present
         */
        private static boolean hasHudiMetaColumns(StructType schema) {
            return Arrays.asList(schema.fieldNames()).contains(COMMIT_TIME_METADATA_FIELD);
        }

        /**
         * Builds file schema as Hoodie metadata string columns followed by original data columns.
         *
         * @param dataSchema Spark data schema before metadata synthesis
         * @return file schema written to Hudi base files
         */
        private static StructType prependHudiMetaColumns(StructType dataSchema) {
            org.apache.spark.sql.types.StructField[] dataFields = dataSchema.fields();
            org.apache.spark.sql.types.StructField[] fields =
                    new org.apache.spark.sql.types.StructField[HOODIE_META_COLUMNS.length + dataFields.length];
            for (int i = 0; i < HOODIE_META_COLUMNS.length; i++) {
                fields[i] = DataTypes.createStructField(HOODIE_META_COLUMNS[i], DataTypes.StringType, true);
            }
            System.arraycopy(dataFields, 0, fields, HOODIE_META_COLUMNS.length, dataFields.length);
            return new StructType(fields);
        }

        /**
         * Prepends Omni string type ids for synthesized Hoodie metadata columns.
         *
         * @param dataOmniTypes Omni type ids aligned with the input schema
         * @return Omni type ids aligned with {@link #prependHudiMetaColumns}
         */
        private static int[] prependStringOmniTypes(int[] dataOmniTypes) {
            int[] types = new int[HOODIE_META_COLUMNS.length + dataOmniTypes.length];
            int stringType = OmniExpressionAdaptor.sparkTypeToOmniType(
                    DataTypes.StringType, Metadata.empty()).getId().toValue();
            Arrays.fill(types, 0, HOODIE_META_COLUMNS.length, stringType);
            System.arraycopy(dataOmniTypes, 0, types, HOODIE_META_COLUMNS.length, dataOmniTypes.length);
            return types;
        }

        /**
         * Appends a batch: partitioned tables split rows by {@link #partitionPath}; non-partitioned
         * append to the current file (opening one on first use).
         *
         * @param batch input columnar batch to write
         */
        void write(ColumnarBatch batch) {
            if (partitionColumns.length > 0) {
                writePartitioned(batch);
                return;
            }
            writeUnpartitioned(batch);
        }

        /**
         * Writes a full batch to the single rolling writer under {@link #directory}.
         *
         * @param batch input columnar batch to write
         */
        private void writeUnpartitioned(ColumnarBatch batch) {
            if (currentWriter == null) {
                openNewFile();
            }
            boolean[] dataColumnsIds = new boolean[schema.fields().length];
            Arrays.fill(dataColumnsIds, true);
            writeBatch(currentWriter, dataColumnsIds, batch, currentPath);
            currentRecordCount += batch.numRows();
        }

        /**
         * Groups rows by Hive-style partition path segment, then writes contiguous row ranges per
         * partition writer (avoids rewriting the whole batch per partition).
         *
         * @param batch input columnar batch to partition and write
         */
        private void writePartitioned(ColumnarBatch batch) {
            int numRows = batch.numRows();
            if (numRows == 0) {
                return;
            }
            Map<String, List<Integer>> partitionToRows = new LinkedHashMap<>();
            for (int row = 0; row < numRows; row++) {
                String pathSegment = partitionPath(batch, row);
                partitionToRows.computeIfAbsent(pathSegment, k -> new ArrayList<>()).add(row);
            }
            boolean[] dataColumnsIds = new boolean[schema.fields().length];
            Arrays.fill(dataColumnsIds, true);
            for (Map.Entry<String, List<Integer>> entry : partitionToRows.entrySet()) {
                FileWriterInfo info = getOrCreatePartitionWriter(entry.getKey());
                List<int[]> ranges = contiguousRanges(entry.getValue());
                for (int[] range : ranges) {
                    splitWriteBatch(info, dataColumnsIds, batch, range);
                    info.recordCount += range[1] - range[0];
                }
            }
        }

        /**
         * Writes an entire batch to an already-open native writer.
         *
         * @param writer native Parquet or Orc writer handle
         * @param dataColumnsIds column mask for the original input batch
         * @param batch input columnar batch to write
         * @param filePath target base file path used for Hoodie metadata columns
         */
        private void writeBatch(
                Object writer,
                boolean[] dataColumnsIds,
                ColumnarBatch batch,
                String filePath) {
            ColumnarBatch writeBatch = withHudiMetaColumns(batch, filePath);
            boolean[] writeColumnIds =
                    shouldWriteHudiMetaColumns ? allDataColumns(writeBatch.numCols()) : dataColumnsIds;
            if (writer instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) writer).write(fileOmniTypes, writeColumnIds, writeBatch);
            } else if (writer instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) writer).write(fileOmniTypes, writeColumnIds, writeBatch);
            } else {
                throw new IllegalStateException("Unexpected writer type: " + writer.getClass().getName());
            }
            closeHudiMetaColumns(writeBatch, shouldWriteHudiMetaColumns);
        }

        /**
         * Sub-batch write for partitioned layout: writes rows {@code [start, end)} without copying
         * the full batch on the native side.
         *
         * @param info open writer and file path for one partition
         * @param dataColumnsIds column mask for the original input batch
         * @param batch input columnar batch to write
         * @param range half-open row range {@code [start, end)}
         */
        private void splitWriteBatch(
                FileWriterInfo info,
                boolean[] dataColumnsIds,
                ColumnarBatch batch,
                int[] range) {
            int start = range[0];
            int end = range[1];
            ColumnarBatch writeBatch = withHudiMetaColumns(batch, info.path);
            boolean[] writeColumnIds =
                    shouldWriteHudiMetaColumns ? allDataColumns(writeBatch.numCols()) : dataColumnsIds;
            if (info.writer instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) info.writer).splitWrite(
                        fileOmniTypes, fileOmniTypes, writeColumnIds, writeBatch, start, end);
            } else if (info.writer instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) info.writer).splitWrite(
                        fileOmniTypes, fileOmniTypes, writeColumnIds, writeBatch, start, end);
            } else {
                throw new IllegalStateException("Unexpected writer type: " + info.writer.getClass().getName());
            }
            closeHudiMetaColumns(writeBatch, shouldWriteHudiMetaColumns);
        }

        /**
         * Optionally prepends the five Hoodie metadata string columns ahead of user columns for
         * this write batch. {@code filePath} is used to populate {@code _hoodie_file_name}.
         *
         * @param batch original input batch
         * @param filePath target base file path used for {@code _hoodie_file_name}
         * @return original batch or a wrapped batch with synthesized Hoodie metadata columns
         */
        private ColumnarBatch withHudiMetaColumns(ColumnarBatch batch, String filePath) {
            if (!shouldWriteHudiMetaColumns) {
                return batch;
            }
            int rows = batch.numRows();
            ColumnVector[] columns = new ColumnVector[HOODIE_META_COLUMNS.length + batch.numCols()];
            columns[0] = stringVector(rows, row -> operationId);
            columns[1] = stringVector(rows, row -> operationId + "_" + partitionId + "_" + row);
            columns[2] = stringVector(rows, row -> recordKey(batch, row));
            columns[3] = stringVector(rows, row -> partitionPath(batch, row));
            columns[4] = stringVector(rows, row -> fileName(filePath));
            for (int i = 0; i < batch.numCols(); i++) {
                columns[HOODIE_META_COLUMNS.length + i] = batch.column(i);
            }
            return new ColumnarBatch(columns, rows);
        }

        /**
         * Builds a column mask that includes every column in the batch passed to native write.
         *
         * @param numCols number of columns in the write batch
         * @return column mask containing {@code true} for every column
         */
        private static boolean[] allDataColumns(int numCols) {
            boolean[] columns = new boolean[numCols];
            Arrays.fill(columns, true);
            return columns;
        }

        /**
         * Releases synthetic metadata vectors created in {@link #withHudiMetaColumns}.
         *
         * @param batch write batch that may contain synthesized metadata columns
         * @param hasPrependedMetaColumns whether the first columns were allocated by this writer
         */
        private static void closeHudiMetaColumns(ColumnarBatch batch, boolean hasPrependedMetaColumns) {
            if (!hasPrependedMetaColumns || batch.numCols() < HOODIE_META_COLUMNS.length) {
                return;
            }
            for (int i = 0; i < HOODIE_META_COLUMNS.length; i++) {
                batch.column(i).close();
            }
        }

        /**
         * Supplies one UTF-8 string per row for synthetic {@link OmniColumnVector} columns.
         */
        private interface StringValueProvider {
            /**
             * Returns the string value for one row.
             *
             * @param row row id in the current batch
             * @return value to write, or {@code null}
             */
            String value(int row);
        }

        /**
         * Builds a dense string {@link OmniColumnVector} for Hoodie metadata columns.
         *
         * @param rows number of rows to populate
         * @param provider callback that supplies one value per row
         * @return populated string column vector
         */
        private static ColumnVector stringVector(int rows, StringValueProvider provider) {
            OmniColumnVector vector = new OmniColumnVector(rows, DataTypes.StringType, true);
            for (int row = 0; row < rows; row++) {
                String value = provider.value(row);
                if (value == null) {
                    vector.putNull(row);
                } else {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    vector.putByteArray(row, bytes, 0, bytes.length);
                }
            }
            return vector;
        }

        /**
         * Comma-separated record key for {@code _hoodie_record_key}. Uses
         * {@link HudiWriterInitParams#recordKeyColumns} or the first data column when unset.
         *
         * @param batch input columnar batch
         * @param row row id in the input batch
         * @return Hudi record key string
         */
        private String recordKey(ColumnarBatch batch, int row) {
            String[] keys = recordKeyColumns.length == 0 ? new String[] {schema.fieldNames()[0]} : recordKeyColumns;
            List<String> values = new ArrayList<>(keys.length);
            for (String key : keys) {
                int columnIndex = schema.fieldIndex(key);
                values.add(String.valueOf(partitionValue(
                        batch.column(columnIndex), schema.fields()[columnIndex].dataType(), row).orElse(null)));
            }
            return String.join(",", values);
        }

        /**
         * Extracts the base file name segment from an absolute path.
         *
         * @param path absolute or relative file path
         * @return final path segment used for {@code _hoodie_file_name}
         */
        private static String fileName(String path) {
            int slash = path == null ? -1 : path.lastIndexOf('/');
            return slash >= 0 ? path.substring(slash + 1) : path;
        }

        /**
         * Merges sorted row indexes into half-open ranges {@code [start, end)} so native
         * {@code splitWrite} can append contiguous row blocks efficiently.
         *
         * @param rowIndexes row indexes belonging to one partition
         * @return ordered half-open row ranges
         */
        private static List<int[]> contiguousRanges(List<Integer> rowIndexes) {
            List<int[]> ranges = new ArrayList<>();
            if (rowIndexes.isEmpty()) {
                return ranges;
            }
            rowIndexes.sort(Integer::compareTo);
            int start = rowIndexes.get(0);
            int previous = start;
            for (int i = 1; i < rowIndexes.size(); i++) {
                int current = rowIndexes.get(i);
                if (current != previous + 1) {
                    ranges.add(new int[] {start, previous + 1});
                    start = current;
                }
                previous = current;
            }
            ranges.add(new int[] {start, previous + 1});
            return ranges;
        }

        /**
         * Hive-style partition directory segment {@code col1=v1/col2=v2} for the given row
         * (also used for {@code _hoodie_partition_path} metadata).
         *
         * @param batch input columnar batch
         * @param row row id in the input batch
         * @return Hive-style partition path segment
         */
        private String partitionPath(ColumnarBatch batch, int row) {
            List<String> segments = new ArrayList<>(partitionColumns.length);
            for (String column : partitionColumns) {
                int columnIndex = schema.fieldIndex(column);
                Object value = partitionValue(batch.column(columnIndex), schema.fields()[columnIndex].dataType(), row)
                        .orElse("default");
                segments.add(column + "=" + String.valueOf(value));
            }
            return String.join("/", segments);
        }

        /**
         * Reads a single partition column cell as a Java object for path encoding.
         *
         * @param column partition column vector
         * @param dataType Spark data type of the partition column
         * @param row row id to read
         * @return optional Java value, empty when the cell is null
         */
        private static Optional<Object> partitionValue(ColumnVector column, DataType dataType, int row) {
            if (column.isNullAt(row)) {
                return Optional.empty();
            }
            if (dataType instanceof BooleanType) {
                return Optional.of(column.getBoolean(row));
            }
            if (dataType instanceof ByteType) {
                return Optional.of(column.getByte(row));
            }
            if (dataType instanceof ShortType) {
                return Optional.of(column.getShort(row));
            }
            if (dataType instanceof IntegerType || dataType instanceof DateType) {
                return Optional.of(column.getInt(row));
            }
            if (dataType instanceof LongType || dataType instanceof TimestampType) {
                return Optional.of(column.getLong(row));
            }
            if (dataType instanceof FloatType) {
                return Optional.of(column.getFloat(row));
            }
            if (dataType instanceof DoubleType) {
                return Optional.of(column.getDouble(row));
            }
            if (dataType instanceof StringType || dataType instanceof CharType || dataType instanceof VarcharType) {
                return Optional.of(column.getUTF8String(row).toString());
            }
            if (dataType instanceof BinaryType) {
                return Optional.of(new String(column.getBinary(row)));
            }
            if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                return Optional.of(column.getDecimal(row, decimalType.precision(), decimalType.scale())
                        .toJavaBigDecimal());
            }
            throw new UnsupportedOperationException("Unsupported Hudi partition column type: " + dataType);
        }

        /**
         * Returns an open writer for {@code pathSegment} (partition key path under {@link #directory}),
         * creating the file, Hudi-style name, and {@code .hoodie_partition_metadata} on first use.
         *
         * @param pathSegment Hive-style partition path segment
         * @return open writer information for the partition
         */
        private FileWriterInfo getOrCreatePartitionWriter(String pathSegment) {
            FileWriterInfo info = partitionWriters.get(pathSegment);
            if (info != null) {
                return info;
            }
            fileIndex++;
            String fileId = String.format(Locale.ROOT, "%05d-%s-%05d", partitionId, UUID.randomUUID(), fileIndex);
            String writeToken = String.format(Locale.ROOT, "%d-%d-%d", partitionId, 0, taskId);
            String fileName = String.format(Locale.ROOT, "%s_%s_%s.%s", fileId, writeToken, operationId, fileFormat);
            String path = pathSegment.isEmpty()
                    ? String.format(Locale.ROOT, "%s/%s", directory, fileName)
                    : String.format(Locale.ROOT, "%s/%s/%s", directory, pathSegment, fileName);
            Object writer = openWriter(path);
            if (!pathSegment.isEmpty()) {
                writePartitionMetadata(pathSegment);
            }
            info = new FileWriterInfo(writer, path);
            partitionWriters.put(pathSegment, info);
            return info;
        }

        /**
         * Writes {@code .hoodie_partition_metadata} once per partition directory (commit time + depth).
         *
         * @param pathSegment Hive-style partition path segment
         */
        private void writePartitionMetadata(String pathSegment) {
            String partitionDir = String.format(Locale.ROOT, "%s/%s", directory, pathSegment);
            Path metaPath = new Path(partitionDir, ".hoodie_partition_metadata");
            try {
                FileSystem fs = metaPath.getFileSystem(new Configuration());
                if (fs.exists(metaPath)) {
                    return;
                }
                Properties props = new Properties();
                props.setProperty("commitTime", operationId);
                props.setProperty("partitionDepth", String.valueOf(partitionDepth(pathSegment)));
                try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(metaPath, false)) {
                    props.store(out, "partition metadata");
                }
            } catch (IOException e) {
                try {
                    FileSystem fs = metaPath.getFileSystem(new Configuration());
                    if (fs.exists(metaPath)) {
                        return;
                    }
                } catch (IOException ignored) {
                    // Surface the original failure below.
                }
                throw new IllegalStateException("Failed to write Hudi partition metadata for " + partitionDir, e);
            }
        }

        /**
         * Computes the number of partition directory levels in a slash-separated path segment.
         *
         * @param pathSegment Hive-style partition path segment
         * @return partition depth, or zero for non-partitioned paths
         */
        private static int partitionDepth(String pathSegment) {
            if (pathSegment == null || pathSegment.isEmpty()) {
                return 0;
            }
            return pathSegment.split("/").length;
        }

        /**
         * Starts a new non-partitioned base file with a fresh Hudi-style file id and write token.
         */
        private void openNewFile() {
            fileIndex++;
            currentFileId = String.format(Locale.ROOT, "%05d-%s-%05d", partitionId, UUID.randomUUID(), fileIndex);
            String writeToken = String.format(Locale.ROOT, "%d-%d-%d", partitionId, 0, taskId);
            String fileName = String.format(Locale.ROOT, "%s_%s_%s.%s", currentFileId, writeToken,
                    operationId, fileFormat);
            currentPath = String.format(Locale.ROOT, "%s/%s", directory, fileName);
            currentWriter = openWriter(currentPath);
            currentRecordCount = 0L;
        }

        /**
         * Ensures parent directories exist and opens a Parquet or Orc writer for the path.
         *
         * @param path output base file path
         * @return native writer handle
         */
        private Object openWriter(String path) {
            try {
                Path p = new Path(path);
                FileSystem fs = p.getFileSystem(new Configuration());
                fs.mkdirs(p.getParent());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create directory for " + path, e);
            }
            if ("orc".equals(fileFormat)) {
                return openOrcWriter(path);
            }
            return openParquetWriter(path);
        }

        /**
         * Opens a native Parquet writer configured with {@link #fileSchema} and datetime rebase policy.
         *
         * @param path output base file path
         * @return initialized Parquet writer
         */
        private ParquetColumnarBatchWriter openParquetWriter(String path) {
            ParquetColumnarBatchWriter writer = new ParquetColumnarBatchWriter(isLegacyDatetimeRebase);
            writer.initializeSchemaJava(fileSchema);
            try {
                writer.initializeWriterJava(new Path(path));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open " + path, e);
            }
            return writer;
        }

        /**
         * Native Orc writer: passes {@code false} for Spark-Orc local timezone mode to avoid
         * TIMESTAMP LTZ issues on read-back for common Hudi deployments.
         *
         * @param path output base file path
         * @return initialized Orc writer
         */
        private OrcColumnarBatchWriter openOrcWriter(String path) {
            OrcColumnarBatchWriter writer = new OrcColumnarBatchWriter(false);
            Path pathObj = new Path(path);
            Configuration conf = new Configuration();
            writer.initializeOutputStreamJava(pathObj.toUri());
            writer.initializeSchemaTypeJava(fileSchema);
            try {
                OrcFile.WriterOptions options = OrcFile.writerOptions(conf).fileSystem(pathObj.getFileSystem(conf));
                writer.initializeWriterJava(pathObj.toUri(), fileSchema, options);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open " + path, e);
            }
            return writer;
        }

        /**
         * Closes the non-partitioned rolling file and records {@link HudiFileInfoJson} for commit.
         */
        private void closeCurrentFile() {
            if (currentWriter == null) {
                return;
            }
            closeWriter(currentWriter);
            long fileSize = 0L;
            try {
                Path path = new Path(currentPath);
                FileSystem fs = path.getFileSystem(new Configuration());
                if (fs.exists(path)) {
                    fileSize = fs.getFileStatus(path).getLen();
                }
            } catch (IOException e) {
                LOG.warn("Failed to stat Hudi output file for size; fileSizeInBytes will be 0. path="
                        + currentPath, e);
            }
            totalBytesWritten += fileSize;
            numFiles++;
            addFileInfo(currentPath, fileSize, currentRecordCount);
            currentWriter = null;
        }

        /**
         * Closes every partition writer and appends one {@link HudiFileInfoJson} line per file.
         */
        private void closePartitionFiles() {
            for (FileWriterInfo info : partitionWriters.values()) {
                closeWriter(info.writer);
                long fileSize = 0L;
                try {
                    Path path = new Path(info.path);
                    FileSystem fs = path.getFileSystem(new Configuration());
                    if (fs.exists(path)) {
                        fileSize = fs.getFileStatus(path).getLen();
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to stat Hudi output file for size; fileSizeInBytes will be 0. path="
                            + info.path, e);
                }
                totalBytesWritten += fileSize;
                numFiles++;
                addFileInfo(info.path, fileSize, info.recordCount);
            }
            partitionWriters.clear();
        }

        /**
         * Serializes one output file summary into {@link #fileInfoJsonList}.
         *
         * @param path output base file path
         * @param fileSize size of the base file in bytes
         * @param recordCount number of records written into the base file
         */
        private void addFileInfo(String path, long fileSize, long recordCount) {
            HudiFileInfoJson info = new HudiFileInfoJson();
            info.setPath(path);
            info.setFileSizeInBytes(fileSize);
            info.setRecordCount(recordCount);
            try {
                fileInfoJsonList.add(MAPPER.writeValueAsString(info));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Serialize HudiFileInfoJson failed", e);
            }
        }

        /**
         * Flushes open writers and returns accumulated JSON lines (then caller clears this state).
         * Partitioned mode closes all {@link #partitionWriters}; non-partitioned closes {@link #currentWriter}.
         *
         * @return JSON lines describing written files
         */
        List<String> commit() {
            if (partitionColumns.length > 0) {
                closePartitionFiles();
            } else {
                closeCurrentFile();
            }
            return new ArrayList<>(fileInfoJsonList);
        }

        /**
         * Builds best-effort bytes, file count, and wall time metrics since construction.
         *
         * @return batch write metrics snapshot
         */
        BatchWriteMetrics metrics() {
            long wallNs = System.nanoTime() - startTimeNs;
            return new BatchWriteMetrics(totalBytesWritten, numFiles, 0L, wallNs);
        }

        /**
         * Idempotent teardown: closes rolling and partition writers without adding commit lines
         * (used when {@link HudiWriteJniWrapper#commit} already ran or on task abort).
         */
        void close() {
            if (currentWriter != null) {
                closeWriter(currentWriter);
                currentWriter = null;
            }
            for (FileWriterInfo info : partitionWriters.values()) {
                closeWriter(info.writer);
            }
            partitionWriters.clear();
        }

        /**
         * Normalizes user-facing format string to {@code orc} or {@code parquet}.
         *
         * @param fileFormat requested Hudi base file format
         * @return normalized format string
         */
        private static String normalizeFileFormat(String fileFormat) {
            if (fileFormat != null && "orc".equalsIgnoreCase(fileFormat)) {
                return "orc";
            }
            return "parquet";
        }

        /**
         * Dispatches to native writer {@code close()} implementations.
         *
         * @param writer native writer handle, or {@code null}
         */
        private static void closeWriter(Object writer) {
            if (writer instanceof ParquetColumnarBatchWriter) {
                ((ParquetColumnarBatchWriter) writer).close();
            } else if (writer instanceof OrcColumnarBatchWriter) {
                ((OrcColumnarBatchWriter) writer).close();
            } else if (writer != null) {
                throw new IllegalStateException("Unexpected writer type: " + writer.getClass().getName());
            } else {
                return;
            }
        }
    }
}
