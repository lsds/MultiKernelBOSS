#include "translators/scalar/list.hpp"
#include "Translator.hpp"

namespace bosstocexpression {

std::pair<bool, Expression> ListTranslator::Match(Expression &&bossExpr) {
  return utils::ScalarComplexExpressionMatches(std::move(bossExpr), {"List"});
}

RetType<EmptyStruct> ListTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetType<EmptyStruct> {
            auto [head, __, args, ___] = std::move(cexpr).decompose();
            const std::string& opType = head.getName();
            // Create an array of expressions to hold the list elements
            CExpressionArray* pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
            // Convert each list element
            for (auto& arg : args) {
              RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(arg), colMap);
              if (!ret.success) {
                utils::safeRelease(pdrgpexpr);
                return {nullptr, false};
              }
              CExpression* pexprChild = ret.expr;
              if (!pexprChild) {
                std::cerr << "Failed to convert list element" << std::endl;
                utils::safeRelease(pdrgpexpr);
                throw std::runtime_error("Failed to convert list element");
              }
              pdrgpexpr->Append(pexprChild);
            }

            // For the array, we need element and array type IDs
            // Using INT4 type ID as default for now, should be improved later
            // to use appropriate types based on the actual elements
            CMDAccessor* md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
            IMDId* elem_type_mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);

            // Create the array type ID - it can be the same as the element
            // type for simplicity since we don't have a specific array type
            // OID
            IMDId* array_type_mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);

            // Create a scalar array operator
            CScalarArray* popScalarArray =
                GPOS_NEW(mp) CScalarArray(mp, elem_type_mdid, array_type_mdid,
                                          false  // not multidimensional
                );

            // Create the array expression with all the converted elements
            return {GPOS_NEW(mp) CExpression(mp, popScalarArray, pdrgpexpr), true};
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int ListTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
