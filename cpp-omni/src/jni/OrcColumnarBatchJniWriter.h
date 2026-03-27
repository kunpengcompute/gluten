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

/* Header for class OMNI_RUNTIME_ORCCOLUMNARBATCHJNIWRITER_H */

#ifndef OMNI_RUNTIME_ORCCOLUMNARBATCHJNIWRITER_H
#define OMNI_RUNTIME_ORCCOLUMNARBATCHJNIWRITER_H

#include <iostream>
#include <jni.h>
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      initializeOutputStream
 * Signature:
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeOutputStream(
    JNIEnv *env, jobject jObj, jobject uriJson);

/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      initializeSchemaType
 * Signature:
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeSchemaType(
    JNIEnv *env,
    jobject jObj,
    jintArray orcTypeIds,
    jintArray childCounts,
    jobjectArray fieldNames,
    jobjectArray decimalParam,
    jint topLevelFieldCount);

/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      initializeWriter
 * Signature:
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeWriter(
    JNIEnv *env, jobject jObj, jlong outputStream, jlong schemaType, jobject writeOptionsJson);


/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      write
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jint numRows);

/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      write
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jlong startPos, jlong endPos);

/*
 * Class:       com_huawei_boostkit_writer_jni_OrcColumnarBatchJniWriter
 * Method:      close
 * Signature:
 */
JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
                                                                                          jlong outputStream,
                                                                                          jlong schemaType,
                                                                                          jlong writer);

#ifdef __cplusplus
}
#endif
#endif