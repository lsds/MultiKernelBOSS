#include "c2btranslators/project/CPhysicalComputeScalarToVeloxProject.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
RetTypeC2B<EmptyStruct> CPhysicalComputeScalarToVeloxProject::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {

  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> projectListExpr = CPhysicalComputeScalarToBOSSProject::GetProjectListExpr(expr, converter);
  success &= projectListExpr.success;
  newRequiredColumns.insert(projectListExpr.aux.begin(), projectListExpr.aux.end());

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.success;

  ExpressionArguments exprArgs;

  return std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> RetTypeC2B<EmptyStruct> {
            auto [head, args1, args2, args3] = std::move(cexpr).decompose();
              ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
              ColSet interimProjectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
              ColSet alreadyProjectedColumns;
              std::vector<std::string> vectorAlrProjectColumns;
              for (int i = 0; i < args2.size(); i+=2) {
                std::string colName = std::visit(
                  boss::utilities::overload(
                      [&](Symbol&& sym) -> std::string {
                        return sym.getName();
                      },
                      [&](auto&&) -> std::string {
                        __builtin_unreachable();
                      }),
                  std::move(args2[i]));
                alreadyProjectedColumns.insert(colName);
                vectorAlrProjectColumns.push_back(colName);
              }

              ColSet projectColumns = utils::GetSetDifference(interimProjectColumns, alreadyProjectedColumns);

              ExpressionArguments finalArguments;

              for (int i = 0; i < vectorAlrProjectColumns.size(); ++i) {
                finalArguments.push_back(Symbol{vectorAlrProjectColumns[i]});
                finalArguments.push_back(std::move(args2[i * 2 + 1])); // every odd element
              }

              for (const auto& col : projectColumns) {
                finalArguments.push_back(Symbol{col});
                finalArguments.push_back(Symbol{col});
              }

              auto expr = "Project"_(
                std::move(childExpr.expr),
                ComplexExpression{"As"_, std::move(args1), std::move(finalArguments), std::move(args3)});

              return {std::move(expr), success};

          },
          [&](auto &&val) -> RetTypeC2B<EmptyStruct> { return {Expression {}, false};  }),
      std::move(projectListExpr.expr)
  );
}

int CPhysicalComputeScalarToVeloxProject::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
