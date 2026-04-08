/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.execution;

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchWriter;

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.util.Arrays;

/**
 * Helpers for Delta Lake native FileFormat Parquet write when the columnar batch includes partition
 * columns but {@link ParquetColumnarBatchWriter} is initialized with the data schema only: project to
 * data columns, then invoke JNI like Iceberg columnar Parquet write ({@code omniTypes.length ==
 * batch.numCols()}, all {@code dataColumnsIds} true).
 *
 * @since 2026
 */
public final class DeltaNativeParquetWrite {
    /**
     * Job configuration key set by {@code OmniFileFormatWriter} for Delta transactional Parquet
     * projection mode only.
     */
    public static final String JOB_CONF_PROJECT_DATA_COLUMNS =
            "spark.gluten.omni.delta.parquet.projectDataColumnsOnly";

    private DeltaNativeParquetWrite() {
    }

    /**
     * Builds a batch whose columns are the file data columns (references vectors from the full batch).
     * Do not {@link ColumnarBatch#close()} the result when the parent batch is still owned by the caller.
     *
     * @param full                        columnar batch including partition and data columns
     * @param dataColumnIndicesInFullBatch indices of data columns in {@code full}, in file column order
     * @return a new batch with one column per file field and the same row count as {@code full}
     */
    public static ColumnarBatch projectDataColumns(ColumnarBatch full, int[] dataColumnIndicesInFullBatch) {
        ColumnVector[] cols = new ColumnVector[dataColumnIndicesInFullBatch.length];
        for (int j = 0; j < dataColumnIndicesInFullBatch.length; j++) {
            cols[j] = full.column(dataColumnIndicesInFullBatch[j]);
        }
        return new ColumnarBatch(cols, full.numRows());
    }

    /**
     * Writes the full batch using the same JNI contract as Iceberg unpartitioned Parquet write.
     *
     * @param writer   native Parquet batch writer
     * @param omniTypes omni type id per batch column (length equals {@code batch.numCols()})
     * @param batch    projected data-column batch
     */
    public static void writeLikeIceberg(ParquetColumnarBatchWriter writer, int[] omniTypes, ColumnarBatch batch) {
        boolean[] dataColumnsIds = new boolean[omniTypes.length];
        Arrays.fill(dataColumnsIds, true);
        writer.write(omniTypes, dataColumnsIds, batch);
    }

    /**
     * Writes a row range using the same JNI contract as Iceberg partitioned {@code splitWrite} for Parquet.
     *
     * @param writer   native Parquet batch writer
     * @param omniTypes omni type id per batch column (length equals {@code batch.numCols()})
     * @param batch    projected data-column batch
     * @param startPos inclusive start row index
     * @param endPos   exclusive end row index
     */
    public static void splitWriteLikeIceberg(
            ParquetColumnarBatchWriter writer,
            int[] omniTypes,
            ColumnarBatch batch,
            long startPos,
            long endPos) {
        boolean[] dataColumnsIds = new boolean[omniTypes.length];
        Arrays.fill(dataColumnsIds, true);
        writer.splitWrite(omniTypes, omniTypes, dataColumnsIds, batch, startPos, endPos);
    }
}
