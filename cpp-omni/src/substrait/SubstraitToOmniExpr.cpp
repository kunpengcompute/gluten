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

#include "SubstraitToOmniExpr.h"
#include "expression/parserhelper.h"
#include "codegen/func_registry.h"
#include "codegen/bloom_filter.h"
#include "type/data_type.h"
#include "codegen/bloom_filter.h"

constexpr const char *SUBSTRAIT_PARSE_ERROR = "SUBSTRAIT_PARSE_ERROR";
namespace omniruntime {

DataTypePtr GetScalarType(const ::substrait::Expression::Literal &literal)
{
    auto typeCase = literal.literal_type_case();
    switch (typeCase) {
        case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
            return BooleanType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
            return ByteType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
            return ShortType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
            return IntType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
            return LongType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
            return FloatType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
            return DoubleType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
            auto precision = literal.decimal().precision();
            auto scale = literal.decimal().scale();
            if (precision <= DECIMAL64_DEFAULT_PRECISION) {
                auto type = Decimal64Type(precision, scale);
                return type;
            } else {
                auto type = Decimal128Type(precision, scale);
                return type;
            }
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
            return Date32Type();
        case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
            return TimestampType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kString:
            return VarcharType();
        case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
            return VarcharType();
        default:
            OMNI_THROW(
                "GET_SCALAR_TYPE_ERROR:", "the given typeCase is not supported: '{}' ", std::to_string(typeCase));
    }
}

/// Whether null will be returned on cast failure.
bool IsNullOnFailure(::substrait::Expression::Cast::FailureBehavior failureBehavior)
{
    switch (failureBehavior) {
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_UNSPECIFIED:
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION:
            return false;
        case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL:
            return true;
        default:
            OMNI_THROW("SUBSTRAIT_ERROR:", "The given failure behavior is NOT supported: '{}'",
                std::to_string(failureBehavior));
    }
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::FieldReference &substraitField, const DataTypesPtr &inputType)
{
    auto typeCase = substraitField.reference_type_case();
    switch (typeCase) {
        case ::substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
            const auto &directRef = substraitField.direct_reference();
            FieldExpr *fieldAccess = nullptr;
            const auto *tmp = &directRef.struct_field();

            auto inputColumnType = inputType;
            for (;;) {
                auto idx = tmp->field();
                fieldAccess = new FieldExpr(idx, inputColumnType->GetType(idx), idx, fieldAccess);

                if (!tmp->has_child()) {
                    break;
                }

                if (auto e = std::dynamic_pointer_cast<RowType>(inputType->GetType(idx))) {
                    inputColumnType = std::make_shared<DataTypes>(e->Children());
                } else {
                    OMNI_THROW("SUBSTRAIT_ERROR:", "Substrait conversion not supported for Reference '{}'",
                        std::to_string(typeCase));
                }

                tmp = &tmp->child().struct_field();
            }
            return fieldAccess;
        }
        default:
            OMNI_THROW(
                "SUBSTRAIT_ERROR:", "Substrait conversion not supported for Reference '{}'", std::to_string(typeCase));
    }
}

void writeIntBigEndian(int32_t val, int8_t *buf) {
    *buf = (val>>24) & 0xff;
    *(buf+1) = (val>>16) & 0xff;
    *(buf+2) = (val>>8) & 0xff;
    *(buf+3) = val & 0xff;
}

void writeLongBigEndian(int64_t val, int8_t *buf) {
    *buf = (val>>56) & 0xff;
    *(buf+1) = (val>>48) & 0xff;
    *(buf+2) = (val>>40) & 0xff;
    *(buf+3) = (val>>32) & 0xff;
    *(buf+4) = (val>>24) & 0xff;
    *(buf+5) = (val>>16) & 0xff;
    *(buf+6) = (val>>8) & 0xff;
    *(buf+7) = val & 0xff;
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::ScalarFunction &substraitFunc, const DataTypesPtr &inputType)
{
    const auto &omniFunction = SubstraitParser::FindOmniFunction(functionMap_, substraitFunc.function_reference());
    const auto &outputType = SubstraitParser::ParseType(substraitFunc.output_type());
    auto type = omniFunction.first;
    auto funcName = omniFunction.second;
    expressions::Operator op = StringToOperator(funcName);
    std::vector<Expr *> args;
    args.reserve(substraitFunc.arguments().size());
    for (const auto &sArg : substraitFunc.arguments()) {
        args.emplace_back(ToOmniExpr(sArg.value(), inputType));
    }
    if (funcName == "get_array_item") {
        //
    }
    if (type == IS_NOT_NULL_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        auto isNullExpr = new IsNullExpr(args[0]);
        return new UnaryExpr(expressions::Operator::NOT, isNullExpr, std::make_shared<BooleanDataType>());
    } else if (type == IS_NULL_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        return new IsNullExpr(args[0]);
    } else if (type == UNARY_OMNI_EXPR_TYPE) {
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        OMNI_CHECK(op != expressions::Operator::INVALIDOP, "the operator is INVALIDOP");
        return new UnaryExpr(op, args[0], std::make_shared<BooleanDataType>());
    } else if (type == BINARY_OMNI_EXPR_TYPE) {
        OMNI_CHECK(outputType != nullptr, "outputType is null");
        OMNI_CHECK(args[0] != nullptr, "args[0] is null");
        OMNI_CHECK(op != expressions::Operator::INVALIDOP, "the operator is INVALIDOP");
        if (args[1] == nullptr) {
            delete args[0];
            OMNI_THROW("SUBSTRAIT_ERROR:", "The args[1] in ScalarFunction is nullptr");
        }
        return new BinaryExpr(op, args[0], args[1], std::move(outputType));
    } else if (type == FUNCTION_OMNI_EXPR_TYPE) {
        if (funcName == "concat") {
            return UnfoldConcatStringFunc(args, outputType);
        }
        if (funcName == "MakeDecimal" && args.size() == 2) {
            // only use first arg in func MakeDecimal
            return new FuncExpr(funcName, {args[0]}, std::move(outputType));
        }
        if ((funcName == "RLike") && args.size() == RLIKE_INPUT) {
            auto secondArg = args[1];
            if (secondArg->GetType() != ExprType::LITERAL_E) {
                Expr::DeleteExprs(args);
                OMNI_THROW("SUBSTRAIT_ERROR:", "The type of args[1] is not equal to LITERAL_E");
            }
            auto literalExpr = static_cast<LiteralExpr *>(secondArg);
        }
        if (funcName == "mm3hash" || funcName == "xxhash64") {
            auto *func = new FuncExpr(funcName, {args[0], args[args.size() - 1]}, outputType);
            for (int32_t i = 1; i < args.size() - 1; i++) {
                func = new FuncExpr(funcName, {args[i], func}, outputType);
            }
            return func;
        }
        if (funcName == "might_contain") {
           LiteralExpr *childExpr = dynamic_cast<LiteralExpr *>(args[0]);
           std::string *sp = childExpr->stringVal;
           if (*sp == "NULL") {
               LiteralExpr *nChildExpr = new LiteralExpr(0L, LongType(), true);
               BloomFilterFuncExpr *bfFuncExpr = new BloomFilterFuncExpr(funcName, {nChildExpr, args[1]}, outputType, nullptr, nullptr);
               delete childExpr;
               return bfFuncExpr;
           }
           int64_t len = sp->length();
           const char *dataPtr = sp->c_str();
           std::unique_ptr<mem::AlignedBuffer<int8_t>>buf=std::make_unique<mem::AlignedBuffer<int8_t>>(len);
           int8_t *bfBuf = buf->GetBuffer();

           //decode
           int32_t version = *reinterpret_cast<const int32_t *>(dataPtr);
           dataPtr += 4;
           int32_t numHashFunctions = *reinterpret_cast<const int32_t *>(dataPtr);
           dataPtr += 4;
           int32_t numWords = *reinterpret_cast<const int32_t *>(dataPtr);
           dataPtr += 4;
           writeIntBigEndian(version, bfBuf);
           version = *reinterpret_cast<int32_t *>(bfBuf);
           bfBuf += 4;
           writeIntBigEndian(numHashFunctions, bfBuf);
           bfBuf += 4;
           writeIntBigEndian(numWords, bfBuf);
           numWords = *reinterpret_cast<int32_t *>(bfBuf);
           bfBuf += 4;

           int64_t word;
           for(int32_t i = 0; i < numWords; i++) {
               word = *reinterpret_cast<const int64_t *>(dataPtr);
               writeLongBigEndian(word, bfBuf);
               dataPtr += 8;
               bfBuf += 8;
           }
           std::unique_ptr<op::BloomFilter>bloomFilter = std::make_unique<op::BloomFilter>(buf->GetBuffer(), version);
           LiteralExpr *nChildExpr = new LiteralExpr(reinterpret_cast<intptr_t>(bloomFilter.get()), LongType());
           BloomFilterFuncExpr *bfFuncExpr = new BloomFilterFuncExpr(funcName, {nChildExpr, args[1]}, outputType, std::move(buf), std::move(bloomFilter));
           delete childExpr;
           return bfFuncExpr;
         }
        // check the signature matches
        std::vector<DataTypeId> argTypes(args.size());
        std::transform(args.begin(), args.end(), argTypes.begin(),
            [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
        return new FuncExpr(funcName, args, std::move(outputType));
    } else if (type == COALESCE_OMNI_EXPR_TYPE) {
        if (args.size() < 2) {
            OMNI_THROW("SUBSTRAIT_ERROR:", "coalesce expression requires at least two input parameters");
        }
        for (size_t i = 0; i < args.size(); ++i) {
            if (args[i] == nullptr) {
                OMNI_THROW("SUBSTRAIT_ERROR:", "The args[{}] in COALESCE_OMNI_EXPR_TYPE is nullptr", i);
            }
        }
        CoalesceExpr* coalesceExpr = nullptr;
        if (args.size() == 2) {
            coalesceExpr = new CoalesceExpr(args[0], args[1]);
        } else {
            coalesceExpr = BuildNestedCoalesceExpr(args);
        }
        return coalesceExpr;
    } else if (type == HIVE_UDF_FUNCTION_OMNI_EXPR_TYPE) {
        DataTypePtr retType;
        auto &hiveUdfClass = omniruntime::codegen::FunctionRegistry::LookupHiveUdf(funcName);
        if (!hiveUdfClass.empty()) {
            return new FuncExpr(hiveUdfClass, args, std::move(retType), HIVE_UDF);
        }
        throw omniruntime::exception::OmniException(SUBSTRAIT_PARSE_ERROR, "The UDF function Unsupported yet");
    } else {
        OMNI_THROW(
            "SUBSTRAIT_ERROR:", "function type {} and function {} is unsupported yet", std::to_string(type), funcName);
    }
}

TypedExprPtr SubstraitOmniExprConverter::UnfoldConcatStringFunc(std::vector<Expr *> args,
    DataTypePtr outputType)
{
    int concatParams = 2;
    int argSize = args.size();
    if (argSize == concatParams) {
        return new FuncExpr("concat", {args[0], args[1]}, std::move(outputType));
    }
    std::vector<Expr*> newArgs(args.begin() + 1, args.end());
    TypedExprPtr ret = UnfoldConcatStringFunc(newArgs, outputType);
    return new FuncExpr("concat", {args[0], ret}, std::move(outputType));
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::SingularOrList &singularOrList, const DataTypesPtr &inputType)
{
    std::vector<Expr *> args;
    // first element of arguments is the value to be compared to every other
    // argument
    args.push_back(ToOmniExpr(singularOrList.value(), inputType));
    for (const auto &option : singularOrList.options()) {
        Expr *arg = ToOmniExpr(option.literal());
        if (arg != nullptr) {
            args.push_back(arg);
        } else {
            Expr::DeleteExprs(args);
            OMNI_THROW("SUBSTRAIT_ERROR:", "The OmniExpression of the singularOrList.literal here is null");
        }
    }
    return new InExpr(args);
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::Cast &castExpr, const DataTypesPtr &inputType)
{
    auto retType = SubstraitParser::ParseType(castExpr.type());
    auto expr = ToOmniExpr(castExpr.input(), inputType, retType);
    auto retTypeId = retType->GetId();
    auto argReturnType = expr->GetReturnType();
    if (retTypeId == argReturnType->GetId()) {
        if (TypeUtil::IsStringType(argReturnType->GetId())) {
            auto argWidth = static_cast<VarcharDataType *>(argReturnType.get())->GetWidth();
            auto retWidth = static_cast<VarcharDataType *>(retType.get())->GetWidth();
            if (argWidth <= retWidth) {
                return expr;
            }
        } else if (TypeUtil::IsDecimalType(retTypeId)) {
            auto argScale = static_cast<DecimalDataType *>(argReturnType.get())->GetScale();
            auto argPrecision = static_cast<DecimalDataType *>(argReturnType.get())->GetPrecision();
            auto retScale = static_cast<DecimalDataType *>(retType.get())->GetScale();
            auto retPrecision = static_cast<DecimalDataType *>(retType.get())->GetPrecision();
            if (argScale == retScale && argPrecision <= retPrecision) {
                return expr;
            }
        } else {
            return expr;
        }
    }
    std::vector<Expr *> args;
    args.push_back(expr);
    std::vector<DataTypeId> argTypes(args.size());
    std::transform(
        args.begin(), args.end(), argTypes.begin(), [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    return new FuncExpr("CAST", args, std::move(retType));
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(const ::substrait::Expression::Literal &substraitLit, const DataTypePtr defaultType)
{
    auto typeCase = substraitLit.literal_type_case();
    switch (typeCase) {
        case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
            return new LiteralExpr(substraitLit.boolean(), BooleanType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
            return new LiteralExpr(static_cast<int8_t>(substraitLit.i8()), ByteType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
            return new LiteralExpr(static_cast<int16_t>(substraitLit.i16()), ShortType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
            return new LiteralExpr(substraitLit.i32(), IntType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
            return new LiteralExpr(substraitLit.i64(), LongType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
            return new LiteralExpr(substraitLit.fp32(), FloatType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
            return new LiteralExpr(substraitLit.fp64(), DoubleType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
            return new LiteralExpr(substraitLit.date(), Date32Type());
        case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
            return new LiteralExpr(substraitLit.timestamp(), TimestampType());
        case ::substrait::Expression_Literal::LiteralTypeCase::kString: {
            auto *stringVal = new std::string(substraitLit.string());
            return new LiteralExpr(stringVal, VarcharType(stringVal->length()));
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kBinary: {
            auto *stringVal = new std::string(substraitLit.binary());
            return new LiteralExpr(stringVal, VarBinaryType(stringVal->length()));
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
            auto decimal = substraitLit.decimal().value();
            auto precision = substraitLit.decimal().precision();
            auto scale = substraitLit.decimal().scale();
            int128_t decimalValue;
            memcpy_s(&decimalValue, sizeof(int128_t), decimal.c_str(), sizeof(int128_t));
            if (precision <= DECIMAL64_DEFAULT_PRECISION) {
                return new LiteralExpr(static_cast<int64_t>(decimalValue), Decimal64Type(precision, scale));
            } else {
                auto *dec128String = new std::string(Uint128ToStr(decimalValue));
                return new LiteralExpr(dec128String, Decimal128Type(precision, scale));
            }
        }
        case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
            DataTypePtr dataType;
            if (defaultType != nullptr) {
                dataType = defaultType;
            } else {
                dataType = SubstraitParser::ParseType(substraitLit.null());
            }
            LiteralExpr *expr;
            if (TypeUtil::IsDecimalType(dataType->GetId())) {
                auto precision = std::dynamic_pointer_cast<DecimalDataType>(dataType)->GetPrecision();
                auto scale = std::dynamic_pointer_cast<DecimalDataType>(dataType)->GetScale();
                expr = ParserHelper::GetDefaultValueForType(dataType->GetId(), precision, scale);
            } else {
                expr = ParserHelper::GetDefaultValueForType(dataType->GetId());
            }
            if (expr == nullptr) {
                OMNI_THROW("SUBSTRAIT_ERROR:", "The LiteralExpr in kNull case here is null");
            }
            expr->isNull = true;
            return expr;
        }
        default:
            throw omniruntime::exception::OmniException(SUBSTRAIT_PARSE_ERROR,
                "Substrait conversion not supported for type case '{}' " + std::to_string(typeCase));
    }
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const ::substrait::Expression::IfThen &substraitIfThen, const DataTypesPtr &inputType, const int32_t index)
{
    const auto& ifs = substraitIfThen.ifs();
    Expr *cond = ToOmniExpr(ifs.Get(index).if_(), inputType);
    if (cond == nullptr) {
        return nullptr;
    }
    Expr *trueExpr = ToOmniExpr(ifs.Get(index).then(), inputType);
    if (trueExpr == nullptr) {
        delete cond;
        return nullptr;
    }
    Expr *falseExpr = nullptr;
    if (index == ifs.size() - 1) {
        falseExpr = ToOmniExpr(substraitIfThen.else_(), inputType);
    } else {
        falseExpr = ToOmniExpr(substraitIfThen, inputType, index + 1);
    }
    if (falseExpr == nullptr) {
        delete cond;
        delete trueExpr;
        return nullptr;
    }

    if (TypeUtil::IsStringType(falseExpr->GetReturnTypeId()) && falseExpr->GetType() == ExprType::LITERAL_E &&
        static_cast<LiteralExpr *>(falseExpr)->stringVal->compare("null") == 0) {
        delete falseExpr;
        auto literalExpr = ParserHelper::GetDefaultValueForType(trueExpr->GetReturnTypeId());
        if (literalExpr == nullptr) {
            delete cond;
            delete trueExpr;
            literalExpr->isNull = true;
            OMNI_THROW("substrait_error", "the literal expression in substraitIfThen case is null here");
        }
        return new IfExpr(cond, trueExpr, literalExpr);
    }
    return new IfExpr(cond, trueExpr, falseExpr);
}

TypedExprPtr SubstraitOmniExprConverter::ToOmniExpr(
    const substrait::Expression &substraitExpr, const DataTypesPtr &inputType, DataTypePtr defaultType)
{
    auto typeCase = substraitExpr.rex_type_case();
    switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kLiteral:
            return ToOmniExpr(substraitExpr.literal(), defaultType);
        case ::substrait::Expression::RexTypeCase::kScalarFunction:
            return ToOmniExpr(substraitExpr.scalar_function(), inputType);
        case ::substrait::Expression::RexTypeCase::kSelection:
            return ToOmniExpr(substraitExpr.selection(), inputType);
        case ::substrait::Expression::RexTypeCase::kCast:
            return ToOmniExpr(substraitExpr.cast(), inputType);
        case ::substrait::Expression::RexTypeCase::kIfThen:
            return ToOmniExpr(substraitExpr.if_then(), inputType);
        case ::substrait::Expression::RexTypeCase::kSingularOrList:
            return ToOmniExpr(substraitExpr.singular_or_list(), inputType);
        default:
            OMNI_THROW(
                "Substrait_Error:", "Substrait conversion not supported for Expression '{}'", std::to_string(typeCase));
    }
}

CoalesceExpr* SubstraitOmniExprConverter::BuildNestedCoalesceExpr(const std::vector<Expr*>& args) {
    Expr* current = new CoalesceExpr(args[args.size()-2], args[args.size()-1]);

    for (int i = args.size() - 3; i >= 0; --i) {
        current = new CoalesceExpr(args[i], current);
    }

    return static_cast<CoalesceExpr*>(current);
}
} // namespace omniruntime
