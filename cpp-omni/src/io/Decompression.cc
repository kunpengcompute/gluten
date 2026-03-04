/**
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

#include "Decompression.hh"
#include <memory>
#include <stdexcept>
#include <vector/vector_common.h>
#include "shuffle/splitter.h"

namespace spark {

std::pair<char*, int32_t> LZ4DecompressionStream::doDecompression(char* input, int32_t inputLength) {
    int actualLength = LZ4_decompress_safe(input, output, inputLength, shuffleCompressBlockSize);
    if (actualLength < 0) {
        throw std::runtime_error("LZ4 decompression failed");
    }
    return std::make_pair(output, actualLength);
}

std::pair<char*, int32_t> SnappyDecompressionStream::doDecompression(char* input, int32_t inputLength) {
    size_t unCompressedSize;
    if (!snappy::GetUncompressedLength(input, inputLength, &unCompressedSize)) {
        throw std::runtime_error("Failed to get uncompressed length.");
    }

    if (!snappy::RawUncompress(input, inputLength, output)) {
        throw std::runtime_error("Failed to decompress data.");
    }
    return std::make_pair(output, unCompressedSize);
}

std::pair<char*, int32_t> ZlibDecompressionStream::doDecompression(char* input, int32_t inputLength) {

    z_stream stream;
    memset(&stream, 0, sizeof(stream));
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int err = inflateInit2(&stream, -15);
    if (err != Z_OK) {
        throw std::runtime_error("Failed to initialize zlib decompression stream: " + std::string(zError(err)));
    }

    stream.next_in = (Bytef*)input;
    stream.avail_in = inputLength;
    stream.next_out = (Bytef*)output;
    stream.avail_out = shuffleCompressBlockSize;

    err = inflate(&stream, Z_NO_FLUSH);
    if (err != Z_STREAM_END && err != Z_OK) {
        inflateEnd(&stream);
        throw std::runtime_error("Failed to decompress data: " + std::string(stream.msg));
    }

    // Clean up the decompression stream
    err = inflateEnd(&stream);
    if (err != Z_OK) {
        throw std::runtime_error("Failed to clean up zlib decompression stream: " + std::string(zError(err)));
    }
    return std::make_pair(output, stream.total_out);
}

std::pair<char*, int32_t> ZstdDecompressionStream::doDecompression(char* input, int32_t inputLength) {
    auto actualLength = ZSTD_getDecompressedSize(input, inputLength);
    if (actualLength == 0) {
        throw std::runtime_error("ZSTD decompression size failed");
    }

    auto retCode = ZSTD_decompress(output, actualLength, input, inputLength);
    if (ZSTD_isError(retCode)) {
        throw std::runtime_error("ZSTD decompression failed:" + std::string(ZSTD_getErrorName(retCode)));
    }
    return std::make_pair(output, actualLength);
}

std::pair<char*, int32_t> UncompressionStream::decompress(JNIEnv *env, int32_t dataSize) {
    if (uncompress.size() < dataSize) {
        uncompress.resize(dataSize);
    }

    int32_t actualLength = 0;
    auto data = uncompress.data();
    while (actualLength < dataSize) {
        std::pair<char*, int32_t> res = readData(env, dIn, dataSize);
        if (res.second == -1) {
            break;
        }
        memcpy_s(data + actualLength, dataSize - actualLength, res.first, res.second);
        actualLength += res.second;
    }

    if (actualLength == 0) {
        return std::make_pair(nullptr, -1);
    }
    return std::make_pair(data, actualLength);
}

std::pair<char*, int32_t> DecompressionStream::decompress(JNIEnv *env, int32_t dataSize) {
    if (uncompress.size() < dataSize) {
        uncompress.resize(dataSize);
    }

    int32_t actualLength = 0;
    auto data = uncompress.data();
    while (actualLength < dataSize) {
         std::pair<char*, int32_t> res = readHeader(env);
        if (res.second == -1) {
            break;
        }
        memcpy_s(data + actualLength, dataSize - actualLength, res.first, res.second);
        actualLength += res.second;
    }

    if (actualLength == 0) {
        return std::make_pair(nullptr, -1);
    }
    return std::make_pair(data, actualLength);
}

ShuffleReaderDeserializer::ShuffleReaderDeserializer(JNIEnv* env, jobject jniIn,
    CompressionKind codec, int64_t shuffleCompressBlockSize, jboolean isRowShuffle)
: env(env), shuffleCompressBlockSize(shuffleCompressBlockSize), isRowShuffle(JNI_TRUE == isRowShuffle)
{
    if (env->GetJavaVM(&vm_) != JNI_OK) {
        throw std::runtime_error("GetJavaVM failed");
    }
    this->jniIn = env->NewGlobalRef(jniIn);
    switch (static_cast<int64_t>(codec)) {
        case CompressionKind_LZ4: {
            this->decompressionStream = std::make_unique<LZ4DecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_SNAPPY: {
            this->decompressionStream = std::make_unique<SnappyDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_ZLIB: {
            this->decompressionStream = std::make_unique<ZlibDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_ZSTD: {
            this->decompressionStream = std::make_unique<ZstdDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_NONE: {
            this->decompressionStream = std::make_unique<UncompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        default:
            throw std::logic_error("decompression codec not supported");
    }
}

int32_t DecompressionStream::columnarShuffleParseBatch(JNIEnv *env, spark::VecBatch* vecBatch)
{
    int32_t vecCount = vecBatch->veccnt();
    int32_t rowCount = vecBatch->rowcnt();
    // convert vecBatch into an omni array of vec values as the final result
    omniruntime::vec::BaseVector* vecs[vecCount]{};

    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    for (auto i = 0; i < vecCount; ++i) {
        const spark::Vec& protoVec = vecBatch->vecs(i);
        const spark::VecType& protoType = protoVec.vectype();
        scaleArrayElements[i] = protoType.scale();
        precisionArrayElements[i] = protoType.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoType.typeid_());

        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoType.typeid_());

        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = Splitter::ProtoTypeToOmniType(protoType);
            vecs[i] = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);

        Splitter::DeserializeProtoVecToOmniVector(protoVec, vecs[i]);
    }

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

int32_t DecompressionStream::rowShuffleParseBatch(JNIEnv *env, spark::ProtoRowBatch* protoRowBatch)
{
    int32_t vecCount = protoRowBatch->veccnt();
    int32_t rowCount = protoRowBatch->rowcnt();
    omniruntime::vec::BaseVector* vecs[vecCount];
    std::vector<omniruntime::type::DataTypeId> omniDataTypeIds(vecCount);

    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    for (auto i = 0; i < vecCount; ++i) {
        const spark::VecType& protoTypeId = protoRowBatch->vectypes(i);
        scaleArrayElements[i] = protoTypeId.scale();
        precisionArrayElements[i] = protoTypeId.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoTypeId.typeid_());
        omniDataTypeIds[i] = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());

        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
        if (vectorDataTypeId == OMNI_ARRAY) {
            if (protoTypeId.children_size() <= 0) {
                throw std::runtime_error("columnarShuffleParseBatch: Array type must have child type information");
            }
            const spark::VecType& childProtoType = protoTypeId.children(0);
            auto elementDataTypeId = static_cast<omniruntime::type::DataTypeId>(childProtoType.typeid_());
            std::shared_ptr<DataType> elementDataType = std::make_shared<DataType>(elementDataTypeId);

            auto arrayType = std::make_shared<type::ArrayType>(elementDataType);
            vecs[i] = VectorHelper::CreateEmptyComplexVector(arrayType.get(), rowCount);
            BaseVector* elementVector = VectorHelper::CreateVector(OMNI_FLAT, elementDataTypeId, 0);
            auto arrayVec =  reinterpret_cast<ArrayVector *>(vecs[i]);
            arrayVec->SetElementVector(std::shared_ptr<BaseVector>(elementVector));
        } else {
            vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);
    }

    std::unique_ptr<RowParser> parser = std::make_unique<RowParser>(omniDataTypeIds);
    char *rows = const_cast<char*>(protoRowBatch->rows().data());
    const int32_t *offsets = reinterpret_cast<const int32_t*>(protoRowBatch->offsets().data());
    for (auto i = 0; i < rowCount; ++i) {
        char *rowPtr = rows + offsets[i];
        parser->ParseOneRow(reinterpret_cast<uint8_t*>(rowPtr), vecs, i);
    }

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

int32_t DecompressionStream::createResult(JNIEnv *env, int rowCount, int vecCount,
    jint* typeIdArrayElements, jint* precisionArrayElements,
    jint* scaleArrayElements, jlong* vecNativeIdArrayElements)
{
    this->result = env->NewObject(metaInfoClass, ctor);
    if (result == nullptr) return -1;

    jintArray typeIdsArr = env->NewIntArray(vecCount);
    jintArray precArr    = env->NewIntArray(vecCount);
    jintArray scaleArr   = env->NewIntArray(vecCount);
    jlongArray vecIdArr  = env->NewLongArray(vecCount);

    env->SetIntArrayRegion(typeIdsArr, 0, vecCount, typeIdArrayElements);
    env->SetIntArrayRegion(precArr,    0, vecCount, precisionArrayElements);
    env->SetIntArrayRegion(scaleArr,   0, vecCount, scaleArrayElements);
    env->SetLongArrayRegion(vecIdArr,  0, vecCount, vecNativeIdArrayElements);

    // === 5. 设置字段值 ===
    env->SetObjectField(result, fidTypeIds, typeIdsArr);
    env->SetObjectField(result, fidPrec,    precArr);
    env->SetObjectField(result, fidScales,  scaleArr);
    env->SetObjectField(result, fidVecIds,  vecIdArr);
    env->SetIntField(result, fidRowCount, rowCount);
    env->SetIntField(result, fidVecCount, vecCount);

    return rowCount;
}

jobject ShuffleReaderDeserializer::getMetaInfo(JNIEnv *pEnv)
{
    return this->decompressionStream->result;
}

omniruntime::vec::VectorBatch* ShuffleReaderDeserializer::Next()
{
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);

    int32_t dataSize = this->decompressionStream->readSize(env);
    if (dataSize == -1 || dataSize == 0) {
        return nullptr;
    }

    auto uncompress = this->decompressionStream->decompress(env, dataSize);

    int32_t rowCnt = 0;
    if (this->isRowShuffle) {
        auto *protoRowBatch = new spark::ProtoRowBatch();
        protoRowBatch->ParseFromArray(uncompress.first, uncompress.second);
        rowCnt = this->decompressionStream->rowShuffleParseBatch(env, protoRowBatch);
        delete protoRowBatch;
    } else {
        auto *vecBatch = new spark::VecBatch();
        vecBatch->ParseFromArray(uncompress.first, uncompress.second);
        rowCnt = this->decompressionStream->columnarShuffleParseBatch(env, vecBatch);
        delete vecBatch;
    }

    if (rowCnt == 0) {
        return nullptr;
    }

    auto vectorBatch = new omniruntime::vec::VectorBatch(1);
    return vectorBatch;
}

}
