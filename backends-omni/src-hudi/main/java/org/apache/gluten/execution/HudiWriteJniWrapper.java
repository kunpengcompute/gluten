/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.execution;

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gluten.connector.write.HudiFileInfoJson;
import org.apache.gluten.metrics.BatchWriteMetrics;
import org.apache.gluten.runtime.OmniRuntime;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * JNI wrapper for Hudi columnar write (Omni backend), aligned with Iceberg write.
 * Parquet only, unpartitioned. On commit, returns HudiFileInfoJson JSON array for
 * building Hudi WriterCommitMessage.
 *
 * @since 2026
 */

public class HudiWriteJniWrapper implements RuntimeAware {
    private static final Logger LOG = LoggerFactory.getLogger(HudiWriteJniWrapper.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
     * Initializes the Parquet columnar writer for one Hudi write task.
     * Must be called once before {@link #write(ColumnarBatch)}.
     *
     * @param schema    Spark struct schema of the data to write.
     * @param omniTypes Omni type ids aligned with schema fields (from expression adaptor).
     * @param params    directory, codec hint, task ids, and datetime rebase mode.
     */
    public void init(StructType schema, int[] omniTypes, HudiWriterInitParams params) {
        if (state != null) {
            throw new IllegalStateException("Already initialized");
        }
        state = new WriterState(schema, omniTypes, params);
    }

    /**
     * Appends one columnar batch to the current Parquet file (opens a new file on first write).
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
     * Immutable parameters passed from Spark's DataWriter task to {@link #init}.
     */
    public static final class HudiWriterInitParams {
        /**
         * Hudi table base path subdirectory where this task writes Parquet files.
         */
        public final String directory;

        /**
         * Parquet compression codec name (informational; native writer may use global config).
         */
        public final String codec;

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
         * HudiWriterInitParams
         *
         * @param directory              output directory for Parquet files
         * @param codec                  compression codec label
         * @param partitionId            writer partition id
         * @param taskId                 writer task id
         * @param operationId            string embedded in generated file names
         * @param isLegacyDatetimeRebase parquet datetime rebase mode flag
         */
        public HudiWriterInitParams(String directory, String codec, int partitionId, long taskId, String operationId,
                                    boolean isLegacyDatetimeRebase) {
            this.directory = directory;
            this.codec = codec;
            this.partitionId = partitionId;
            this.taskId = taskId;
            this.operationId = operationId;
            this.isLegacyDatetimeRebase = isLegacyDatetimeRebase;
        }
    }

    private static final class WriterState {
        private final StructType schema;
        private final int[] omniTypes;
        private final String directory;
        private final String operationId;
        private final int partitionId;
        private final long taskId;
        private final boolean isLegacyDatetimeRebase;
        private final long startTimeNs = System.nanoTime();

        private ParquetColumnarBatchWriter currentWriter;
        private String currentPath;
        private long currentRecordCount;
        private int fileIndex;
        private final List<String> fileInfoJsonList = new ArrayList<>();
        private long totalBytesWritten;
        private int numFiles;

        WriterState(StructType schema, int[] omniTypes, HudiWriterInitParams params) {
            this.schema = schema;
            this.omniTypes = omniTypes;
            this.directory = params.directory;
            this.operationId = params.operationId;
            this.partitionId = params.partitionId;
            this.taskId = params.taskId;
            this.isLegacyDatetimeRebase = params.isLegacyDatetimeRebase;
        }

        void write(ColumnarBatch batch) {
            if (currentWriter == null) {
                openNewFile();
            }
            boolean[] dataColumnsIds = new boolean[schema.fields().length];
            for (int i = 0; i < dataColumnsIds.length; i++) {
                dataColumnsIds[i] = true;
            }
            currentWriter.write(omniTypes, dataColumnsIds, batch);
            currentRecordCount += batch.numRows();
        }

        private void openNewFile() {
            fileIndex++;
            String fileName = String.format(Locale.ROOT, "00000-%d-%s-%d-%05d.parquet", partitionId, operationId,
                    taskId, fileIndex);
            currentPath = String.format(Locale.ROOT, "%s/%s", directory, fileName);
            try {
                Path p = new Path(currentPath);
                FileSystem fs = p.getFileSystem(new Configuration());
                fs.mkdirs(p.getParent());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create directory for " + currentPath, e);
            }
            ParquetColumnarBatchWriter pw = new ParquetColumnarBatchWriter(isLegacyDatetimeRebase);
            pw.initializeSchemaJava(schema);
            try {
                pw.initializeWriterJava(new Path(currentPath));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to open " + currentPath, e);
            }
            currentWriter = pw;
            currentRecordCount = 0L;
        }

        private void closeCurrentFile() {
            if (currentWriter == null) {
                return;
            }
            currentWriter.close();
            long fileSize = 0L;
            try {
                Path path = new Path(currentPath);
                FileSystem fs = path.getFileSystem(new Configuration());
                if (fs.exists(path)) {
                    fileSize = fs.getFileStatus(path).getLen();
                }
            } catch (IOException e) {
                LOG.warn("Failed to stat Hudi Parquet output file for size; fileSizeInBytes will be 0. path="
                        + currentPath, e);
            }
            totalBytesWritten += fileSize;
            numFiles++;
            HudiFileInfoJson info = new HudiFileInfoJson();
            info.setPath(currentPath);
            info.setFileSizeInBytes(fileSize);
            info.setRecordCount(currentRecordCount);
            try {
                fileInfoJsonList.add(MAPPER.writeValueAsString(info));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Serialize HudiFileInfoJson failed", e);
            }
            currentWriter = null;
        }

        List<String> commit() {
            closeCurrentFile();
            return new ArrayList<>(fileInfoJsonList);
        }

        BatchWriteMetrics metrics() {
            long wallNs = System.nanoTime() - startTimeNs;
            return new BatchWriteMetrics(totalBytesWritten, numFiles, 0L, wallNs);
        }

        void close() {
            if (currentWriter != null) {
                currentWriter.close();
                currentWriter = null;
            }
        }
    }
}
