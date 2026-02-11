/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef CPP_COMMON_H
#define CPP_COMMON_H

#include <vector/vector_common.h>
#include <cstring>
#include <chrono>
#include <memory>
#include <list>
#include <set>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "../io/Common.hh"
#include "../utils/macros.h"
#include "BinaryLocation.h"
#include "debug.h"
#include "Buffer.h"
#include "ArrayLocation.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

template<bool hasNull>
int32_t BytesGen(uint64_t offsetsAddr, std::string &nullStr, uint64_t valuesAddr, VCBatchInfo& vcb)
{
    int32_t* offsets = reinterpret_cast<int32_t *>(offsetsAddr);  // 字符串的偏移量
    char *nulls = nullptr;
    char* values = reinterpret_cast<char *>(valuesAddr); // 字符串裸指针
    std::vector<VCLocation> &lst = vcb.getVcList();
    int itemsTotalLen = lst.size(); // 有多少个字符串

    int valueTotalLen = 0; // 字符串的长度总量
    if constexpr (hasNull) {
        nullStr.resize(itemsTotalLen, 0);  // 有空值的话就给nulls赋值，不然就直接空指针
        nulls = nullStr.data();
    }

    for (int i = 0; i < itemsTotalLen; i++) { // 遍历每一个字符串
        char* addr = reinterpret_cast<char *>(lst[i].get_vc_addr()); // 这个字符串的地址
        int len = lst[i].get_vc_len(); // 字符串字节长度
        if (i == 0) {
            offsets[0] = 0;
        } else {
            offsets[i] = offsets[i -1] + lst[i - 1].get_vc_len();
        }
        if constexpr(hasNull) {
            if (lst[i].get_is_null()) {
                nulls[i] = 1;
            }
        }

        if (len != 0) {
            memcpy((char *) (values + offsets[i]), addr, len);
            valueTotalLen += len;
        }
    }
    offsets[itemsTotalLen] = offsets[itemsTotalLen -1] + lst[itemsTotalLen - 1].get_vc_len();
    return valueTotalLen;
}

template<bool hasNull, DataTypeId dataTypeId>
int32_t BytesGenStringArray(int32_t* nums, char *nulls, char* values, int32_t* offsets, uint64_t valuesAddr,
    std::vector<ArrayLocation> &lst)
{
    // 使用 NativeType 获取具体的元素类型
    using ElementType = typename NativeType<dataTypeId>::type;

    int elementTotalNum = lst.size(); // ArrayVector 的 array 的总数量
    int elementTotalLen = 0; // ArrayVector 元素字节长度总数

    offsets[0] = 0;
    int index = 1;
    // 遍历每一个字符串数组
    for (int i = 0; i < elementTotalNum; i++) {
        ElementType* src = reinterpret_cast<ElementType *>(lst[i].get_element_addr()); // 获取字符串
        int len = lst[i].get_element_len(); // 字符串总长度
        int num = lst[i].get_element_num(); // 子字符串数量
        nums[i] = num;

        if constexpr(hasNull) {
            if (lst[i].get_is_null()) {
                nulls[i] = 1;
            }
        }

        if (len != 0) {
            memcpy((char *) (values + elementTotalLen), src, len); // TODO: value 赋值
            elementTotalLen += len;
        }

        int32_t* varcharOffsets = reinterpret_cast<int32_t *>(lst[i].get_offset_addr());
        for (int j = 0; j < num; j++) {
            offsets[index] = offsets[index - 1] + (varcharOffsets[j + 1] - varcharOffsets[j]);
            index++;
        }
    }

    return elementTotalLen;
}

template<bool hasNull, DataTypeId dataTypeId>
int32_t BytesGenFixedArray(int32_t* nums, char *nulls, char* values, uint64_t valuesAddr, std::vector<ArrayLocation> &lst)
{
    // 使用 NativeType 获取具体的元素类型
    using ElementType = typename NativeType<dataTypeId>::type;

    int elementTotalNum = lst.size(); // ArrayVector 的 array 的总数量
    int elementTotalLen = 0; // ArrayVector 元素字节长度总数

    ElementType* actualValues = reinterpret_cast<ElementType*>(valuesAddr);
    // 处理定长元素数组
    for (int i = 0; i < elementTotalNum; i++) {
        ElementType* src = reinterpret_cast<ElementType *>(lst[i].get_element_addr());
        int len = lst[i].get_element_len();
        int num = lst[i].get_element_num();
        nums[i] = num;

        if constexpr(hasNull) {
            if (lst[i].get_is_null()) {
                nulls[i] = 1;
                continue;
            }
        }

        if (len != 0) {
            memcpy(values + elementTotalLen, src, len);
            elementTotalLen += len;
        }
    }

    return elementTotalLen;
}

template<bool hasNull>
int32_t BytesGenArray(uint64_t numsAddr, std::string &nullStr, uint64_t valuesAddr,
    uint64_t offsetAddr, DataTypeId dataTypeId, ArrayBatchInfo& arrayBatchInfo)
{
    int32_t* nums = reinterpret_cast<int32_t *>(numsAddr); // arrayVector 每个数组多少元素的数组指针
    char *nulls = nullptr; // arrayVector 的空值列表
    char* values = reinterpret_cast<char *>(valuesAddr);
    int32_t* offsets = reinterpret_cast<int32_t *>(offsetAddr);

    std::vector<ArrayLocation> &lst = arrayBatchInfo.getArrayList();

    if constexpr (hasNull) {
        nullStr.resize(lst.size(), 0);
        nulls = nullStr.data();
    }

    switch (dataTypeId) {
        case OMNI_INT:
            return BytesGenFixedArray<hasNull, OMNI_INT>(nums, nulls, values, valuesAddr, lst);
        case OMNI_DATE32:
            return BytesGenFixedArray<hasNull, OMNI_DATE32>(nums, nulls, values, valuesAddr, lst);
        case OMNI_SHORT:
            return BytesGenFixedArray<hasNull, OMNI_SHORT>(nums, nulls, values, valuesAddr, lst);
        case OMNI_BYTE:
            return BytesGenFixedArray<hasNull, OMNI_BYTE>(nums, nulls, values, valuesAddr, lst);
        case OMNI_LONG:
            return BytesGenFixedArray<hasNull, OMNI_LONG>(nums, nulls, values, valuesAddr, lst);
        case OMNI_TIMESTAMP:
            return BytesGenFixedArray<hasNull, OMNI_TIMESTAMP>(nums, nulls, values, valuesAddr, lst);
        case OMNI_DATE64:
            return BytesGenFixedArray<hasNull, OMNI_DATE64>(nums, nulls, values, valuesAddr, lst);
        case OMNI_DECIMAL64:
            return BytesGenFixedArray<hasNull, OMNI_DECIMAL64>(nums, nulls, values, valuesAddr, lst);
        case OMNI_DECIMAL128:
            return BytesGenFixedArray<hasNull, OMNI_DECIMAL128>(nums, nulls, values, valuesAddr, lst);
        case OMNI_DOUBLE:
            return BytesGenFixedArray<hasNull, OMNI_DOUBLE>(nums, nulls, values, valuesAddr, lst);
        case OMNI_FLOAT:
            return BytesGenFixedArray<hasNull, OMNI_FLOAT>(nums, nulls, values, valuesAddr, lst);
        case OMNI_BOOLEAN:
            return BytesGenFixedArray<hasNull, OMNI_BOOLEAN>(nums, nulls, values, valuesAddr, lst);
        case OMNI_VARCHAR:
            return BytesGenStringArray<hasNull, OMNI_VARCHAR>(nums, nulls, values, offsets, valuesAddr, lst);
        case OMNI_CHAR:
            return BytesGenStringArray<hasNull, OMNI_CHAR>(nums, nulls, values, offsets, valuesAddr, lst);
        default: {
            std::string omniExceptionInfo =
                "In function BytesGenArray, no such data type " + std::to_string(dataTypeId);
            throw omniruntime::exception::OmniException("BYTES_GEN_ARRAY_ERROR", omniExceptionInfo);
        }
    }
}



uint32_t reversebytes_uint32t(uint32_t value);

spark::CompressionKind GetCompressionType(const std::string& name);

int IsFileExist(const std::string path);

#endif //CPP_COMMON_H