/*
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
package org.apache.gluten.vectorized;

import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * Bridges the native Omni execution pipeline to Spark's vectorized batch API for the Gluten Omni
 * backend.
 *
 * @since 2026
 */
public class OmniColumnarBatchOutIterator extends ClosableIterator implements RuntimeAware {
    private final long iterHandle;

    private List<TypeNode> outputTypes;

    /**
     * Wraps a native iterator handle returned from the Omni runtime for JNI-driven iteration.
     *
     * @param iterHandle opaque native handle to the Omni output iterator
     */
    public OmniColumnarBatchOutIterator(long iterHandle) {
        super();
        this.iterHandle = iterHandle;
    }

    /**
     * Sets the Substrait output schema used to allocate {@link OmniColumnVector} wrappers for each
     * native column. Must be set before consuming batches if column types are required for
     * allocation.
     *
     * @param outputTypes logical output type nodes, one per column
     */
    public void setOutputTypes(List<TypeNode> outputTypes) {
        this.outputTypes = outputTypes;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This iterator does not expose a separate native runtime handle; returns {@code -1}.
     */
    @Override
    public long rtHandle() {
        return -1;
    }

    /**
     * Returns the native Omni output iterator handle used by JNI calls.
     *
     * @return opaque iterator handle passed to {@code native*} methods
     */
    public long itrHandle() {
        return iterHandle;
    }

    private native boolean nativeHasNext(long iterHandle);

    private native long nativeNext(long iterHandle);

    private native long nativeSpill(long iterHandle, long size);

    private native void nativeClose(long iterHandle);

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasNext0() throws IOException {
        return nativeHasNext(iterHandle);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GlutenException if a native vector type does not match the declared Substrait output
     *     type for a column (see {@link OmniColumnVector#setVec})
     */
    @Override
    protected ColumnarBatch next0() throws IOException {
        long batchHandle = nativeNext(iterHandle);
        if (batchHandle == -1L) {
            return null;
        }
        VecBatch vecBatch = transform(batchHandle);
        OmniColumnVector[] omniColumnVectors = OmniColumnVector.allocateColumns(vecBatch.getRowCount(),
                outputTypes.toArray(new TypeNode[0]), false);
        for (int i = 0; i < omniColumnVectors.length; i++) {
            try {
                omniColumnVectors[i].setVec(vecBatch.getVector(i));
            } catch (ClassCastException cce) {
                throw new GlutenException(
                        buildVectorTypeMismatchMessage(i, outputTypes, vecBatch, omniColumnVectors),
                        cce);
            }
        }
        return new ColumnarBatch(omniColumnVectors, vecBatch.getRowCount());
    }

    private VecBatch transform(long nativeBatch) {
        return nativeTransform(nativeBatch);
    }

    /**
     * Converts a native batch handle to an Omni {@link VecBatch} (JNI).
     *
     * @param nativeBatch opaque native batch handle from {@link #nativeNext(long)}
     * @return decoded columnar batch in Omni runtime representation
     */
    public native VecBatch nativeTransform(long nativeBatch);

    /**
     * Requests the native iterator to spill up to the given number of bytes, if supported.
     *
     * @param size hint in bytes for how much memory to try to release
     * @return implementation-specific spill result (e.g. bytes released), or {@code 0} if closed
     */
    public long spill(long size) {
        if (!closed.get()) {
            return nativeSpill(iterHandle, size);
        } else {
            return 0L;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Releases native resources associated with the iterator handle.
     */
    @Override
    protected void close0() {
        nativeClose(iterHandle);
    }

    private static String buildVectorTypeMismatchMessage(
            int columnIndex,
            List<TypeNode> outputTypes,
            VecBatch vecBatch,
            OmniColumnVector[] omniColumnVectors) {
        StringBuilder detail = new StringBuilder();
        detail.append("Omni vector type does not match Substrait output type at column index ")
                .append(columnIndex)
                .append(", outputType=")
                .append(outputTypes.get(columnIndex).getClass().getSimpleName())
                .append(", vecClass=")
                .append(vecBatch.getVector(columnIndex).getClass().getName())
                .append(", rowCount=")
                .append(vecBatch.getRowCount())
                .append(", vectorCount=")
                .append(omniColumnVectors.length)
                .append(", mapping=[");
        for (int j = 0; j < omniColumnVectors.length; j++) {
            if (j > 0) {
                detail.append("; ");
            }
            detail.append(j)
                    .append(":")
                    .append(outputTypes.get(j).getClass().getSimpleName())
                    .append("<-")
                    .append(vecBatch.getVector(j).getClass().getSimpleName());
        }
        detail.append("]");
        return detail.toString();
    }
}
