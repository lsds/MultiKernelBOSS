#include "c2btranslators/join/CGPUJoin2AFJoin.hpp"

#include "CExpressionToBOSS.hpp"
// #include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

namespace cexpressiontoboss::translation {
  using namespace orcaextender;
bool CGPUJoin2AFJoin::Match(const CExpression* expr) {
  DynamicRegistry* dynamicRegistry = DynamicRegistry::GetInstance();
  auto engineType = dynamicRegistry->GetEngineType("ArrayFire");
  return expr->Pop()->Eopid() == dynamicRegistry->GetOperatorId(engineType, "CPhysicalGPUJoin");
}

RetTypeC2B<EmptyStruct> CGPUJoin2AFJoin::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> joinCondExpr =
      CHashJoinToBOSSHashJoin::GetJoinCondExpr(expr, converter);
  success &= joinCondExpr.success;
  newRequiredColumns.insert(joinCondExpr.aux.begin(), joinCondExpr.aux.end());

  // Create the BOSS expression based on the hash join type
  ProjectInfo newAux = aux;
  newAux.requiredColumns = newRequiredColumns;
  newAux.parentOp = expr->Pop()->Eopid();
  std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> childExpr =
      utils::GetChildExprBinary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.first.success;
  success &= childExpr.second.success;
  Expression outerExpr = std::move(childExpr.first.expr);
  Expression innerExpr = std::move(childExpr.second.expr);

  Expression joinExpr = std::visit(
      boss::utilities::overload(
          [&](ComplexExpression&& cexpr) -> Expression {
            auto [head, args1, args2, args3] = std::move(cexpr).decompose();

            CExpression* outerCExpr = (*expr)[0];
            CExpression* innerCExpr = (*expr)[1];
            CExpression* pexprPred = (*expr)[2];

            if (pexprPred->Pop()->Eopid() == COperator::EopScalarCmp) {
              CScalarCmp *cmp = CScalarCmp::PopConvert(pexprPred->Pop());
              if (cmp->ParseCmpType() == IMDType::EcmptEq) {
                CExpression* pred1 = (*pexprPred)[0];
                CExpression* pred2 = (*pexprPred)[1];
                if (pred1->Pop()->Eopid() == COperator::EopScalarIdent && pred2->Pop()->Eopid() == COperator::EopScalarIdent) {
                  CColRefSet *cols = outerCExpr->DeriveOutputColumns();
                  CScalarIdent *i1 = CScalarIdent::PopConvert(pred1->Pop());
                  if (cols->FMember(i1->Pcr())) {
                    ExpressionArguments newDyns;
                    newDyns.push_back(std::move(args2[1]));
                    newDyns.push_back(std::move(args2[0]));
                    return "Join"_(std::move(innerExpr), std::move(outerExpr),
                    "Where"_(std::move(ComplexExpression{std::move(head), std::move(args1), std::move(newDyns), std::move(args3)})));
                  }
                }
              }

            }

            return "Join"_(std::move(innerExpr), std::move(outerExpr),
                   "Where"_(std::move(ComplexExpression{std::move(head), std::move(args1), std::move(args2), std::move(args3)})));
          },
          [&](auto &&val) -> Expression { return val;  }),
      std::move(joinCondExpr.expr));

  orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  if (!(aux.parentOp == COperator::EopPhysicalComputeScalar || aux.parentOp == dynamicRegistry->GetOperatorId(dynamicRegistry->GetEngineType("ArrayFire"), "CPhysicalGPUProject"))) {
    ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
    ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
    if (projectColumns != outputColumns) {
      Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
      return {"Project"_(std::move(joinExpr), std::move(projectList)), success};
    }
  }
  return {std::move(joinExpr), success};
}

int CGPUJoin2AFJoin::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
