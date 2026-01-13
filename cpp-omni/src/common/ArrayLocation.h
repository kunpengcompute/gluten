/*
* Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef CPP_OMNI_ARRAYLOCATION_H
#define CPP_OMNI_ARRAYLOCATION_H

class ArrayLocation {
public:
    ArrayLocation(uint64_t element_addr, uint64_t nulls_addr, uint32_t element_num,
        uint32_t element_len, uint64_t offset_addr, bool is_null):
            element_addr(element_addr), nulls_addr(nulls_addr), element_num(element_num),
                element_len(element_len), offset_addr(offset_addr), is_null(is_null) {
    }

    ~ArrayLocation() {
    }

    uint64_t get_element_addr() {
        return element_addr;
    }

    uint64_t get_nulls_addr() {
        return nulls_addr;
    }

    uint32_t get_element_num() {
        return element_num;
    }

    uint32_t get_element_len() {
        return element_len;
    }

    uint64_t get_offset_addr() {
        return offset_addr;
    }

    bool get_is_null() {
        return is_null;
    }

public:
    uint64_t element_addr;
    uint64_t nulls_addr; // array 的每个元素的空值情况
    uint32_t element_num;
    uint32_t element_len;
    uint64_t offset_addr;
    bool is_null; // 这个 array 本身是否为空
};

class ArrayBatchInfo {
public:
    ArrayBatchInfo(uint32_t element_capacity, uint32_t element_type) {
        this->array_list.reserve(element_capacity);
        this->element_capacity = element_capacity;
        this->element_type = element_type;
        this->element_total_len = 0;
    }

    ~ArrayBatchInfo() {
        array_list.clear();
    }

    uint32_t getElementCapacity() {
        return element_capacity;
    }

    uint32_t getElementTotalLen() {
        return element_total_len;
    }

    uint32_t getElementType() {
        return element_type;
    }

    std::vector<ArrayLocation> &getArrayList() {
        return array_list;
    }

    bool hasNull() const {
        return hasNullFlag;
    }

    void SetNullFlag(bool hasNull) {
        hasNullFlag = hasNull;
    }

public:
    uint32_t element_capacity;
    uint32_t element_total_len;
    uint32_t element_type;
    std::vector<ArrayLocation> array_list;
    bool hasNullFlag = false;
};

#endif // CPP_OMNI_ARRAYLOCATION_H