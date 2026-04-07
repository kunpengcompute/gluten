/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.jni;

import com.huawei.boostkit.write.jni.OrcColumnarBatchJniWriter;

import nova.hetu.omniruntime.vector.*;

import org.apache.gluten.vectorized.OmniColumnVector;
import org.apache.orc.OrcFile;
import org.apache.spark.sql.types.BooleanType;
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
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.json.JSONObject;

import java.net.URI;

/**
 * OrcColumnarBatchWriter
 */
public class OrcColumnarBatchWriter {
    /**
     * ORC has two timestamp flavors: TIMESTAMP (no timezone) and TIMESTAMP_INSTANT (timestamp with
     * local timezone semantics). Some writers (e.g. Iceberg) require TIMESTAMP_INSTANT for Spark
     * TIMESTAMP columns to avoid schema projection/promotion issues when reading.
     */
    private final boolean shouldWriteTimestampAsInstant;

    public OrcColumnarBatchWriter() {
        this(false);
    }

    public OrcColumnarBatchWriter(boolean shouldWriteTimestampAsInstant) {
        jniWriter = new OrcColumnarBatchJniWriter();
        this.shouldWriteTimestampAsInstant = shouldWriteTimestampAsInstant;
    }

    /**
     * OrcLibTypeKind
     */
    public enum OrcLibTypeKind {
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BINARY,
        TIMESTAMP,
        LIST,
        MAP,
        STRUCT,
        UNION,
        DECIMAL,
        DATE,
        VARCHAR,
        CHAR,
        TIMESTAMP_INSTANT
    }

    /**
     * initializeOutputStreamJava
     * @param uri URI
     */
    public void initializeOutputStreamJava(URI uri) {
        JSONObject uriJson = new JSONObject();

        uriJson.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        uriJson.put("host", uri.getHost() == null ? "" : uri.getHost());
        uriJson.put("port", uri.getPort());
        uriJson.put("path", uri.getPath() == null ? "" : uri.getPath());

        outputStream = jniWriter.initializeOutputStream(uriJson);
    }

    public void initializeSchemaTypeJava(StructType dataSchema) {
        List<Integer> flatTypes = new ArrayList<>();
        List<Integer> flatChildCounts = new ArrayList<>();
        List<String> flatFieldNames = new ArrayList<>();
        List<int[]> flatDecimalParams = new ArrayList<>();
        for (StructField field : dataSchema.fields()) {
            addTypeRecursive(field.name(), field.dataType(), flatTypes, flatChildCounts, flatFieldNames, flatDecimalParams);
        }

        //convert list to array so it can be transfered by jni
        int[] orcTypeIds = flatTypes.stream().mapToInt(i -> i).toArray();
        int[] childCounts = flatChildCounts.stream().mapToInt(i -> i).toArray();
        String[] fieldNames = flatFieldNames.toArray(new String[0]);
        int[][] decimalParams = flatDecimalParams.toArray(new int[0][0]);

        schemaType = jniWriter.initializeSchemaType(
                orcTypeIds,
                childCounts,
                fieldNames,
                decimalParams,
                dataSchema.length()
        );
    }

    private void addTypeRecursive(String fieldName,
                                  DataType dataType,
                                  List<Integer> result,
                                  List<Integer> childCounts,
                                  List<String> fieldNames,
                                  List<int[]> decimalParams) {
        if (dataType instanceof ArrayType) {
            result.add(OrcLibTypeKind.LIST.ordinal());
            childCounts.add(1);
            fieldNames.add(fieldName);
            decimalParams.add(new int[] {0, 0});
            ArrayType arrayType = (ArrayType) dataType;
            addTypeRecursive("element", arrayType.elementType(), result, childCounts, fieldNames, decimalParams);
        } else if (dataType instanceof MapType) {
            result.add(OrcLibTypeKind.MAP.ordinal());
            childCounts.add(2);
            fieldNames.add(fieldName);
            decimalParams.add(new int[] {0, 0});
            MapType mapType = (MapType) dataType;
            addTypeRecursive("key", mapType.keyType(), result, childCounts, fieldNames, decimalParams);
            addTypeRecursive("value", mapType.valueType(), result, childCounts, fieldNames, decimalParams);
        } else if (dataType instanceof StructType) {
            result.add(OrcLibTypeKind.STRUCT.ordinal());
            StructType structType = (StructType) dataType;
            childCounts.add(structType.fields().length);
            fieldNames.add(fieldName);
            decimalParams.add(new int[] {0, 0});
            for (StructField field : structType.fields()) {
                addTypeRecursive(field.name(), field.dataType(), result, childCounts, fieldNames, decimalParams);
            }
        } else {
            result.add(sparkTypeToOrcLibType(dataType));
            childCounts.add(0);
            fieldNames.add(fieldName);
            if (dataType instanceof DecimalType) {
                DecimalType decimal = (DecimalType) dataType;
                decimalParams.add(new int[] {decimal.precision(), decimal.scale()});
            } else {
                decimalParams.add(new int[] {0, 0});
            }
        }
    }

    /**
     * initializeWriterJava
     * @param uri URI
     * @param dataSchema StructType
     * @param options OrcFile WriterOptions
     */
    public void initializeWriterJava(URI uri, StructType dataSchema, OrcFile.WriterOptions options) {
        JSONObject writerOptionsJson = new JSONObject();

        JSONObject versionJob = new JSONObject();
        versionJob.put("major", options.getVersion().getMajor());
        versionJob.put("minor", options.getVersion().getMinor());
        writerOptionsJson.put("file version", versionJob);

        writerOptionsJson.put("compression", options.getCompress().ordinal());
        writerOptionsJson.put("strip size", options.getStripeSize());
        writerOptionsJson.put("compression block size", options.getBlockSize());
        writerOptionsJson.put("row index stride", options.getRowIndexStride());
        writerOptionsJson.put("compression strategy", options.getCompressionStrategy().ordinal());
        writerOptionsJson.put("padding tolerance", options.getPaddingTolerance());
        writerOptionsJson.put("columns use bloom filter", options.getBloomFilterColumns());
        writerOptionsJson.put("bloom filter fpp", options.getBloomFilterFpp());

        //for jniWriter to initialize writerOptions and OmniTimestampColumnWriter
        String tzId = java.util.TimeZone.getDefault().getID();
        if ("GMT+08:00".equals(tzId)) {
            tzId = "Asia/Shanghai";
        } else if (tzId != null && tzId.startsWith("GMT") && java.util.TimeZone.getDefault().getRawOffset() == 28800000) {
            tzId = "Asia/Shanghai";
        }
        if (tzId == null || tzId.isEmpty()) {
            tzId = "Asia/Shanghai";
        }
        writerOptionsJson.put("timezone", tzId);

        writer = jniWriter.initializeWriter(outputStream, schemaType, writerOptionsJson);
    }

    /**
     * sparkTypeToOrcLibType
     * @param dataSchema StructType
     * @return int array
     */
    public int[] sparkTypeToOrcLibType(StructType dataSchema) {
        int[] orcLibType = new int[dataSchema.length()];
        for (int i = 0; i < dataSchema.length(); i++) {
            orcLibType[i] = sparkTypeToOrcLibType(dataSchema.fields()[i].dataType());
        }
        return orcLibType;
    }

    public int sparkTypeToOrcLibType(DataType dataType) {
        if (dataType instanceof BooleanType) {
            return OrcLibTypeKind.BOOLEAN.ordinal();
        } else if (dataType instanceof ShortType) {
            return OrcLibTypeKind.SHORT.ordinal();
        } else if (dataType instanceof IntegerType) {
            return OrcLibTypeKind.INT.ordinal();
        } else if (dataType instanceof LongType) {
            return OrcLibTypeKind.LONG.ordinal();
        } else if (dataType instanceof DateType) {
            return OrcLibTypeKind.DATE.ordinal();
        } else if (dataType instanceof DoubleType) {
            return OrcLibTypeKind.DOUBLE.ordinal();
        } else if (dataType instanceof VarcharType) {
            return OrcLibTypeKind.VARCHAR.ordinal();
        } else if (dataType instanceof StringType) {
            return OrcLibTypeKind.STRING.ordinal();
        } else if (dataType instanceof CharType) {
            return OrcLibTypeKind.CHAR.ordinal();
        } else if (dataType instanceof DecimalType) {
            return OrcLibTypeKind.DECIMAL.ordinal();
        } else if (dataType instanceof FloatType) {
            return OrcLibTypeKind.FLOAT.ordinal();
        } else if (dataType instanceof BinaryType) {
            return OrcLibTypeKind.BINARY.ordinal();
        } else if (dataType instanceof ByteType) {
            return OrcLibTypeKind.BYTE.ordinal();
        } else if (dataType instanceof TimestampType) {
            return shouldWriteTimestampAsInstant
                    ? OrcLibTypeKind.TIMESTAMP_INSTANT.ordinal()
                    : OrcLibTypeKind.TIMESTAMP.ordinal();
        } else if (dataType instanceof StructType) {
            return OrcLibTypeKind.STRUCT.ordinal();
        } else {
            throw new RuntimeException(
                    "UnSupport type convert  spark type " + dataType.simpleString() + " to orc lib type");
        }
    }

    /**
     * extractSchemaName
     * @param dataSchema StructType
     * @return String array
     */
    public String[] extractSchemaName(StructType dataSchema) {
        String[] schemaNames = new String[dataSchema.length()];
        for (int i = 0; i < dataSchema.length(); i++) {
            schemaNames[i] = dataSchema.fields()[i].name();
        }
        return schemaNames;
    }

    /**
     * extractDecimalParam
     * @param dataSchema StructType
     * @return int[][]
     */
    public int[][] extractDecimalParam(StructType dataSchema) {
        int paramNum = 2;
        int precisionIndex = 0;
        int scaleIndex = 1;
        int[][] decimalParams = new int[dataSchema.length()][paramNum];
        for (int i = 0; i < dataSchema.length(); i++) {
            DataType dataType = dataSchema.fields()[i].dataType();
            if (dataType instanceof DecimalType) {
                DecimalType decimal = (DecimalType) dataType;
                decimalParams[i][precisionIndex] = decimal.precision();
                decimalParams[i][scaleIndex] = decimal.scale();
            }
        }
        return decimalParams;
    }

    /**
     * write
     * @param omniTypes int array
     * @param dataColumnsIds boolean array
     * @param columBatch ColumnarBatch
     */
    public void write(int[] omniTypes, boolean[] dataColumnsIds, ColumnarBatch columBatch) {
        long[] vecNativeIds = new long[columBatch.numCols()];
        for (int i = 0; i < columBatch.numCols(); i++) {
            OmniColumnVector omniVec = (OmniColumnVector) columBatch.column(i);
            Vec vec = omniVec.getVec();
            vecNativeIds[i] = vec.getNativeVector();
        }

        jniWriter.write(writer, vecNativeIds, omniTypes, dataColumnsIds, columBatch.numRows());
    }

    /**
     * splitWrite
     * @param omniTypes omniTypes
     * @param allOmniTypes allOmniTypes
     * @param dataColumnsIds dataColumnsIds
     * @param inputBatch inputBatch
     * @param startPos startPos
     * @param endPos endPos
     */
    public void splitWrite(int[] omniTypes, int[] allOmniTypes, boolean[] dataColumnsIds, ColumnarBatch inputBatch, long startPos, long endPos) {
        long[] vecNativeIds = new long[inputBatch.numCols()];
        for (int i = 0; i < inputBatch.numCols(); i++) {
            OmniColumnVector omniVec = (OmniColumnVector) inputBatch.column(i);
            Vec vec = omniVec.getVec();
            vecNativeIds[i] = vec.getNativeVector();
        }

        jniWriter.splitWrite(writer, vecNativeIds, omniTypes, dataColumnsIds, startPos, endPos);
    }

    /**
     * close
     */
    public void close() {
        jniWriter.close(outputStream, schemaType, writer);
    }

    public long outputStream;

    public long schemaType;

    public long writer;

    public OrcColumnarBatchJniWriter jniWriter;
}
