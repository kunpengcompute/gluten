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

package org.apache.gluten.metrics;

import org.apache.gluten.runtime.RuntimeAware;
import org.apache.gluten.vectorized.OmniColumnarBatchOutIterator;

/**
 * OmniIteratorMetricsJniWrapper
 *
 * This class is mainly used to obtain the measurement information of the iterator.
 *
 * @since 2025-06-03
 */
public class OmniIteratorMetricsJniWrapper implements RuntimeAware {
    private OmniIteratorMetricsJniWrapper() {
    }

    /**
     * create OmniIteratorMetricsJniWrapper object
     *
     * @return OmniIteratorMetricsJniWrapper
     */
    public static OmniIteratorMetricsJniWrapper create() {
        return new OmniIteratorMetricsJniWrapper();
    }

    /**
     * fetch Metrics from native
     *
     * @param out out
     * @return Metrics
     */
    public Metrics fetch(OmniColumnarBatchOutIterator out) {
        return nativeFetchMetrics(out.itrHandle());
    }

    private native Metrics nativeFetchMetrics(long itrHandle);

    /**
     * get rtHandle
     *
     * @return -1
     */
    @Override
    public long rtHandle() {
        return -1;
    }
}
