/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "ParquetColumnarBatchJniWriter.h"
#include "jni_common.h"
#include "reader/parquet/ParquetWriter.h"
#include "reader/common/UriInfo.h"
#include "arrow/status.h"
#include <arrow/type.h>
#include <arrow/api.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>

#ifdef Type
#undef Type
#endif

using namespace omniruntime::writer;

static constexpr int32_t DECIMAL_PRECISION_INDEX = 0;
static constexpr int32_t DECIMAL_SCALE_INDEX = 1;

static std::string ToLower(std::string input)
{
    std::transform(input.begin(), input.end(), input.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return input;
}

static std::shared_ptr<::arrow::DataType> ParseSparkTypeString(const std::string &typeStr)
{
    auto lower = ToLower(typeStr);
    if (lower == "boolean") return ::arrow::boolean();
    if (lower == "byte") return ::arrow::int8();
    if (lower == "short") return ::arrow::int16();
    if (lower == "integer" || lower == "int") return ::arrow::int32();
    if (lower == "long") return ::arrow::int64();
    if (lower == "float") return ::arrow::float32();
    if (lower == "double") return ::arrow::float64();
    if (lower == "string") return ::arrow::utf8();
    if (lower == "binary") return ::arrow::binary();
    if (lower == "date") return ::arrow::date32();
    if (lower == "timestamp" || lower == "timestamp_ntz" || lower == "timestamp_ltz") {
        return ::arrow::timestamp(::arrow::TimeUnit::MICRO);
    }
    if (lower.rfind("decimal(", 0) == 0) {
        auto left = lower.find('(');
        auto comma = lower.find(',', left + 1);
        auto right = lower.find(')', comma + 1);
        if (left != std::string::npos && comma != std::string::npos && right != std::string::npos) {
            auto precision = std::stoi(lower.substr(left + 1, comma - left - 1));
            auto scale = std::stoi(lower.substr(comma + 1, right - comma - 1));
            return ::arrow::decimal128(precision, scale);
        }
    }
    if (lower.rfind("char(", 0) == 0 || lower.rfind("varchar(", 0) == 0) {
        return ::arrow::utf8();
    }
    if (lower == "null") return ::arrow::null();
    throw std::invalid_argument("Unsupported spark type string: " + typeStr);
}

static std::shared_ptr<::arrow::DataType> ParseSparkType(const nlohmann::json &node)
{
    if (node.is_string()) {
        return ParseSparkTypeString(node.get<std::string>());
    }
    if (!node.is_object()) {
        throw std::invalid_argument("Invalid spark type json node");
    }

    auto typeNodeIt = node.find("type");
    if (typeNodeIt != node.end() && typeNodeIt->is_string()) {
        auto typeStr = typeNodeIt->get<std::string>();
        auto lower = ToLower(typeStr);
        if (lower == "struct") {
            std::vector<std::shared_ptr<::arrow::Field>> fields;
            const auto &jsonFields = node.at("fields");
            for (const auto &fieldNode : jsonFields) {
                auto name = fieldNode.at("name").get<std::string>();
                auto nullable = fieldNode.value("nullable", true);
                auto childType = ParseSparkType(fieldNode.at("type"));
                fields.emplace_back(::arrow::field(name, childType, nullable));
            }
            return ::arrow::struct_(fields);
        }
        if (lower == "array") {
            auto elementType = ParseSparkType(node.at("elementType"));
            auto containsNull = node.value("containsNull", true);
            return ::arrow::list(::arrow::field("element", elementType, containsNull));
        }
        if (lower == "map") {
            auto keyType = ParseSparkType(node.at("keyType"));
            auto valueType = ParseSparkType(node.at("valueType"));
            auto valueContainsNull = node.value("valueContainsNull", true);
            return ::arrow::map(keyType, valueType, valueContainsNull);
        }
        if (lower == "decimal") {
            auto precision = node.value("precision", 0);
            auto scale = node.value("scale", 0);
            return ::arrow::decimal128(precision, scale);
        }
        if (lower == "char" || lower == "varchar") {
            return ::arrow::utf8();
        }
        return ParseSparkTypeString(typeStr);
    }

    throw std::invalid_argument("Unsupported spark type json");
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeWriter(
    JNIEnv *env, jobject jObj, jobject jsonObj, jlong writer)
{
    JNI_FUNC_START
    jstring uri = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("uri"));
    const char *uriStr = env->GetStringUTFChars(uri, JNI_FALSE);
    std::string uriString(uriStr);
    env->ReleaseStringUTFChars(uri, uriStr);

    jstring ugiTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("ugi"));
    const char *ugi = env->GetStringUTFChars(ugiTemp, JNI_FALSE);
    std::string ugiString(ugi);
    env->ReleaseStringUTFChars(ugiTemp, ugi);

    jstring schemaTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("scheme"));
    const char *schema = env->GetStringUTFChars(schemaTemp, JNI_FALSE);
    std::string schemaString(schema);
    env->ReleaseStringUTFChars(schemaTemp, schema);

    jstring hostTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("host"));
    const char *host = env->GetStringUTFChars(hostTemp, JNI_FALSE);
    std::string hostString(host);
    env->ReleaseStringUTFChars(hostTemp, host);

    jstring pathTemp = (jstring)env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("path"));
    const char *path = env->GetStringUTFChars(pathTemp, JNI_FALSE);
    std::string pathString(path);
    env->ReleaseStringUTFChars(pathTemp, path);

    jint port = (jint)env->CallIntMethod(jsonObj, jsonMethodInt, env->NewStringUTF("port"));

    UriInfo uriInfo(uriString, schemaString, pathString, hostString, std::to_string(port));
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->InitRecordWriter(uriInfo, ugiString);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_initializeSchema(
    JNIEnv *env, jobject JObj, jlong writer, jobjectArray fieldNames, jintArray fieldTypes,
    jbooleanArray nullables, jobjectArray decimalParam, jstring schemaJson)
{
    JNI_FUNC_START
    auto pWriter = std::make_unique<ParquetWriter>();
    const char *schemaJsonPtr = (schemaJson == nullptr) ? nullptr : env->GetStringUTFChars(schemaJson, JNI_FALSE);
    std::string schemaJsonStr = schemaJsonPtr ? std::string(schemaJsonPtr) : std::string();
    if (schemaJsonPtr != nullptr) {
        env->ReleaseStringUTFChars(schemaJson, schemaJsonPtr);
    }
    auto fieldTypesPtr = env->GetIntArrayElements(fieldTypes, JNI_FALSE);
    auto nullablesPtr = env->GetBooleanArrayElements(nullables, JNI_FALSE);
    if (nullablesPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "the nullablesPtr is null");
        return writer;
    }
    if (fieldTypesPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "Parquet type ids should not be null");
        return writer;
    }
    auto schemaLength = (int32_t)env->GetArrayLength(fieldTypes);
    ::arrow::FieldVector fieldVector;
    if (!schemaJsonStr.empty()) {
        nlohmann::json root;
        try {
            root = nlohmann::json::parse(schemaJsonStr);
        } catch (const std::exception &e) {
            env->ThrowNew(runtimeExceptionClass, e.what());
            return writer;
        }
        std::shared_ptr<::arrow::DataType> schemaType;
        try {
            schemaType = ParseSparkType(root);
        } catch (const std::exception &e) {
            env->ThrowNew(runtimeExceptionClass, e.what());
            return writer;
        }
        if (schemaType->id() != ::arrow::Type::STRUCT) {
            env->ThrowNew(runtimeExceptionClass, "Schema json root type must be struct");
            return writer;
        }
        auto structType = std::static_pointer_cast<::arrow::StructType>(schemaType);
        fieldVector = structType->fields();
        for (const auto &field : fieldVector) {
            if (field->type()->id() == ::arrow::Type::DECIMAL) {
                auto decimalType = std::static_pointer_cast<::arrow::Decimal128Type>(field->type());
                pWriter->precisions.push_back(decimalType->precision());
                pWriter->scales.push_back(decimalType->scale());
            }
        }
    } else {
        for (int i = 0; i < schemaLength; i++) {
            jint parquetType = fieldTypesPtr[i];
            jboolean nullable = nullablesPtr[i];
            jstring fieldName = (jstring)env->GetObjectArrayElement(fieldNames, i);
            const char *cFieldName = env->GetStringUTFChars(fieldName, nullptr);
            std::shared_ptr<::arrow::DataType> writeParquetType;

            auto decimalParamArray = (jintArray)env->GetObjectArrayElement(decimalParam, i);
            auto decimalParamArrayPtr = env->GetIntArrayElements(decimalParamArray, JNI_FALSE);
            if (decimalParamArrayPtr == NULL) {
                env->ThrowNew(runtimeExceptionClass, "the decimalParamArrayPtr is null");
                return writer;
            }
            auto precision = decimalParamArrayPtr[DECIMAL_PRECISION_INDEX];
            auto scale = decimalParamArrayPtr[DECIMAL_SCALE_INDEX];
            switch (static_cast<::arrow::Type::type>(parquetType)) {
            case ::arrow::Type::type::DECIMAL:
                 pWriter->precisions.push_back(precision);
                 pWriter->scales.push_back(scale);
                 writeParquetType = ::arrow::decimal128(precision, scale);
                 break;
            case ::arrow::Type::type::BOOL:
                writeParquetType = ::arrow::boolean();
                break;
            case ::arrow::Type::type::INT8:
                writeParquetType = ::arrow::int8();
                break;
            case ::arrow::Type::type::INT16:
                writeParquetType = ::arrow::int16();
                break;
            case ::arrow::Type::type::INT32:
                writeParquetType = ::arrow::int32();
                break;
            case ::arrow::Type::type::INT64:
                writeParquetType = ::arrow::int64();
                break;
            case ::arrow::Type::type::DATE32:
                writeParquetType = ::arrow::date32();
                break;
            case ::arrow::Type::type::DATE64:
                writeParquetType = ::arrow::date64();
                break;
            case ::arrow::Type::type::FLOAT:
                writeParquetType = ::arrow::float32();
                break;
            case ::arrow::Type::type::DOUBLE:
                writeParquetType = ::arrow::float64();
                break;
            case ::arrow::Type::type::STRING:
                writeParquetType = ::arrow::utf8();
                break;
            case ::arrow::Type::type::LARGE_STRING:
                writeParquetType = ::arrow::large_utf8();
                break;
            case ::arrow::Type::type::BINARY:
                writeParquetType = ::arrow::binary();
                break;
            case ::arrow::Type::type::LARGE_BINARY:
                writeParquetType = ::arrow::large_binary();
                break;
            case ::arrow::Type::type::TIMESTAMP:
                writeParquetType = ::arrow::timestamp(::arrow::TimeUnit::MICRO);
                break;
            case ::arrow::Type::type::LIST:
            case ::arrow::Type::type::LARGE_LIST:
            case ::arrow::Type::type::MAP:
            case ::arrow::Type::type::STRUCT:
                env->ThrowNew(runtimeExceptionClass, "Nested type requires child schema info");
                env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
                env->ReleaseStringUTFChars(fieldName, cFieldName);
                return writer;
            default: {
                std::string error = "Unsupported parquet type: " + std::to_string(parquetType);
                env->ThrowNew(runtimeExceptionClass, error.c_str());
                env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
                env->ReleaseStringUTFChars(fieldName, cFieldName);
                return writer;
            }
            }
            fieldVector.emplace_back(::arrow::field(cFieldName, writeParquetType, nullable));
            env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
            env->ReleaseStringUTFChars(fieldName, cFieldName);
        }
    }
    if (pWriter == nullptr) {
            env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
            return writer;
    }
    pWriter->schema_ = std::make_shared<::arrow::Schema>(fieldVector);
    ParquetWriter *pWriterNew= pWriter.release();
    env->ReleaseIntArrayElements(fieldTypes, fieldTypesPtr, JNI_ABORT);
    env->ReleaseBooleanArrayElements(nullables, nullablesPtr, JNI_ABORT);
    return (jlong)(pWriterNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId,
    jintArray omniTypes, jbooleanArray dataColumnsIds, jint numRows)
{
    JNI_FUNC_START
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->write(vecNativeIdPtr, colNums, omniTypesPtr, dataColumnsIdsPtr);
    env->ReleaseLongArrayElements(vecNativeId, vecNativeIdPtr, 0);
    env->ReleaseIntArrayElements(omniTypes, omniTypesPtr, 0);
    env->ReleaseBooleanArrayElements(dataColumnsIds, dataColumnsIdsPtr, 0);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    auto writeRows = endPos - startPos;
    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "the pWriter is null");
        return;
    }
    pWriter->write(vecNativeIdPtr, colNums, omniTypesPtr, dataColumnsIdsPtr, true, startPos, endPos);

    env->ReleaseLongArrayElements(vecNativeId, vecNativeIdPtr, 0);
    env->ReleaseIntArrayElements(omniTypes, omniTypesPtr, 0);
    env->ReleaseBooleanArrayElements(dataColumnsIds, dataColumnsIdsPtr, 0);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_ParquetColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
   jlong writer)
{
    JNI_FUNC_START

    ParquetWriter *pWriter = (ParquetWriter *)writer;
    if (pWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for writer");
        return;
    }
    pWriter->arrow_writer->Close();
    delete pWriter;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
