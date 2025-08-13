#include "translators/scalar/regulartype.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "Translator.hpp"

namespace bosstocexpression {


int32_t string_hash_i32(const std::string &str) {
    std::hash<std::string> hasher;
    size_t h = hasher(str);                  // platform-dependent size_t hash
    return static_cast<int32_t>(h & 0xFFFFFFFF); // take lower 32 bits
}


std::pair<bool, Expression> RegularTypeTranslator::Match(Expression &&bossExpr) {
  return std::visit(
      boss::utilities::overload(
          [&](int val) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(val));
          },
          [&](bool val) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(val));
          },
          [&](double val) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(val));
          },
          [&](float val) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(val));
          },
          [&](std::string val) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(val));
          },
          [&](auto &&val) -> std::pair<bool, Expression> { return std::make_pair(false, std::move(val)); }),
      std::move(bossExpr));
}

RetType<EmptyStruct> RegularTypeTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](int val) -> RetType<EmptyStruct> {
            return {CUtils::PexprScalarConstInt4(mp, val), true};
          },
          [&](bool val) -> RetType<EmptyStruct> {
            return {CUtils::PexprScalarConstBool(mp, val), true};
          },
          [&](double val) -> RetType<EmptyStruct> {
            // Create a double constant using CDatumGenericGPDB
            return {utils::CreateGenericType(mp, GPDB_FLOAT8_OID, &val, sizeof(double),
                                     static_cast<LINT>(val), CDouble(val)), true};
          },
          [&](float val) -> RetType<EmptyStruct> {
            std::cout << "YOYOYO" << std::endl;
            // Create a float constant using CDatumGenericGPDB
            return {utils::CreateGenericType(mp, GPDB_FLOAT4_OID, &val, sizeof(float),
                                     static_cast<LINT>(val), CDouble(val)), true};
          },
          [&](std::string val) -> RetType<EmptyStruct> {
            int i = string_hash_i32(val);
            orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
            if (dynamicRegistry->auxContains("intToStringMapping")) {
              auto ptr = std::any_cast<std::shared_ptr<std::unordered_map<int, std::string>>>(dynamicRegistry->GetAuxiliaryState("intToStringMapping"));
              (*ptr)[i] = val;
              return {CUtils::PexprScalarConstInt4(mp, i), true};
            }

            return {utils::CreateGenericType(mp, GPDB_TEXT_OID, const_cast<char*>(val.c_str()), val.length(), i, CDouble(i)), true};
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int RegularTypeTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
