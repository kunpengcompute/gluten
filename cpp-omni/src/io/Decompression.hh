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

#ifndef SPARK_DECOMPRESSION_HH
#define SPARK_DECOMPRESSION_HH

#include "SparkFile.hh"
#include "jni.h"
#include "Common.hh"
#include "MemoryPool.hh"
#include "jni/jni_common.h"
#include "lz4.h"
#include "zlib.h"
#include "zstd.h"
#include "wrap/snappy_wrapper.h"

#include "vec_data.pb.h"

namespace spark {
class DecompressionStream
{
public:
    int64_t shuffleCompressBlockSize;
    jobject dIn;
    jobject result;
    std::vector<char> uncompress;
    char* output = nullptr;

    DecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : dIn(dIn), shuffleCompressBlockSize(shuffleCompressBlockSize) {}

    virtual ~DecompressionStream()
    {
        delete[] output;
    }

    virtual std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) = 0;

    int32_t columnarShuffleParseBatch(JNIEnv *env, spark::VecBatch* vecBatch);

    int32_t rowShuffleParseBatch(JNIEnv *env, spark::ProtoRowBatch* protoRowBatch);

    int32_t createResult(JNIEnv *env, int rowCount, int vecCount, jint* typeIdArrayElements, jint* precisionArrayElements,
        jint* scaleArrayElements, jlong* vecNativeIdArrayElements);

    virtual std::pair<char*, int32_t> decompress(JNIEnv *pEnv, int32_t dataSize);

    virtual std::pair<char *, int32_t> readHeader(JNIEnv *env)
    {
        // Read the header (3 bytes)
        char buf[3];
        int header[3] = {};
        jlong ret = env->CallLongMethod(dIn, readByteMethod, reinterpret_cast<jlong>(buf), 3);
        if (ret < 3) {
            return std::make_pair(nullptr, -1);
        }
        for (int i = 0; i < 3; i++) {
            header[i] = buf[i] & 0xff;
        }

        bool isOriginal = (header[0] & 0x01) == 1;
        int chunkLength = (header[2] << 15) | (header[1] << 7) | (header[0] >> 1);

        char *compressed = new char [chunkLength];
        jlong readBytes = 0;
        while (readBytes < chunkLength) {
            jlong ret = env->CallLongMethod(dIn, readByteMethod, reinterpret_cast<jlong>(compressed), chunkLength - readBytes);
            if (ret == -1 || ret == 0) {
                break;
            }
            readBytes += ret;
        }
        if (readBytes < chunkLength) {
            delete[] compressed;
            throw std::runtime_error("failed to read chunk!");
        }
        if (!isOriginal) {
            if (output == nullptr) {
                output = new char [shuffleCompressBlockSize];
            }
            auto data = doDecompression(compressed, chunkLength);
            delete[] compressed;
            return data;
        }
        return std::make_pair(compressed, chunkLength);
    }

    int32_t readSize(JNIEnv *env)
    {
        auto pair = readHeader(env);
        if (pair.second == -1) {
            return -1;
        }

        auto data = pair.first;
        int header[4] = {};
        for (int i = 0; i < 4; i++) {
            header[i] = data[i] & 0xff;
        }
        if ((header[0] | header[1] | header[2] | header[3]) < 0)
            return -1;
        return ((header[0] << 24) + (header[1] << 16) + (header[2] << 8) + (header[3] << 0));
    }

};

class UncompressionStream final: public DecompressionStream {
public:
    UncompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

    std::pair<char*, int32_t> decompress(JNIEnv *pEnv, int32_t dataSize) override;

    std::pair<char *, int32_t> readHeader(JNIEnv *env) override {
        char buf[4];
        jlong ret = env->CallLongMethod(dIn, readByteMethod, reinterpret_cast<jlong>(buf), 4);
        if (ret < 3) {
            return std::make_pair(nullptr, -1);
        }
        return std::make_pair(buf, 4);
    }

    std::pair<char *, int32_t> readData(JNIEnv *env, jobject dIn, int32_t chunkLength) {
        char *uncompressed = new char [chunkLength];
        jlong readBytes = 0;
        while (readBytes < chunkLength) {
            jlong ret = env->CallLongMethod(dIn, readByteMethod, reinterpret_cast<jlong>(uncompressed), chunkLength - readBytes);
            if (ret == -1 || ret == 0) {
                break;
            }
            readBytes += ret;
        }
        if (readBytes < chunkLength) {
            delete[] uncompressed;
            throw std::runtime_error("failed to read chunk!");
        }
        return std::make_pair(uncompressed, chunkLength);
    }

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override {
        return {nullptr, -1};
    }
};

class LZ4DecompressionStream final: public DecompressionStream {
public:
    LZ4DecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class SnappyDecompressionStream final: public DecompressionStream {
public:
    SnappyDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ZlibDecompressionStream final: public DecompressionStream {
public:
    ZlibDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ZstdDecompressionStream final: public DecompressionStream {
public:
    ZstdDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ShuffleReaderDeserializer final: public omniruntime::ColumnarBatchIterator {
public:
    ShuffleReaderDeserializer(JNIEnv* env, jobject jniIn, CompressionKind codec, int64_t shuffleCompressBlockSize, jboolean isRowShuffle);

    ~ShuffleReaderDeserializer()
    {
        AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
        env->DeleteGlobalRef(jniIn);
        vm_->DetachCurrentThread();
    };

    omniruntime::vec::VectorBatch* Next() override;
    jobject getMetaInfo(JNIEnv *env);
private:
    JavaVM* vm_;
    JNIEnv* env;
    jobject jniIn;
    int64_t shuffleCompressBlockSize;
    bool isRowShuffle;
    std::unique_ptr<DecompressionStream> decompressionStream;
};

}
#endif