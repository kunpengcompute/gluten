/*
 * Copyright (C) 2026-2026. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.vectorized;

import com.huawei.boostkit.spark.serialize.SerializerMeta;
import com.huawei.boostkit.spark.serialize.ShuffleDataSerializer;

import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * shuffle ColumnarBatch Iterator
 *
 * @since 2026/03/04
 */
public class ShuffleColumnarBatchOutIterator extends ClosableIterator implements RuntimeAware {
    private final long iterHandle;

    private List<TypeNode> outputTypes;

    public ShuffleColumnarBatchOutIterator(long iterHandle) {
        super();
        this.iterHandle = iterHandle;
    }

    public void setOutputTypes(List<TypeNode> outputTypes) {
        this.outputTypes = outputTypes;
    }

    @Override
    public long rtHandle() {
        return -1;
    }

    public long itrHandle() {
        return iterHandle;
    }

    private native boolean nativeHasNext(long iterHandle);

    private native long nativeNext(long iterHandle);

    private native void nativeClose(long iterHandle);

    private native SerializerMeta nativeMetaInfo(long iterHandle);

    @Override
    public boolean hasNext0() throws IOException {
        return true;
    }

    @Override
    public ColumnarBatch next0() throws IOException {
        long batchHandle = nativeNext(iterHandle);
        if (batchHandle == -1L) {
            return null;
        }

        SerializerMeta meta = nativeMetaInfo(iterHandle);

        int vecCount = meta.getVecCount();
        int rowCount = meta.getRowCount();
        int[] typeIdArray = meta.getTypeIdArray();
        int[] precisionArray = meta.getPrecisionArray();
        int[] scaleArray = meta.getScaleArray();
        long[] vecNativeIdArray = meta.getVecNativeIdArray();
        ColumnVector[] vecs = new ColumnVector[vecCount];
        for (int i = 0; i < vecCount; i++) {
            vecs[i] = ShuffleDataSerializer
                    .buildVector(typeIdArray[i], vecNativeIdArray[i], rowCount, precisionArray[i], scaleArray[i]);
        }
        return new ColumnarBatch(vecs, rowCount);
    }

    @Override
    protected void close0() {
        nativeClose(iterHandle);
    }
}
