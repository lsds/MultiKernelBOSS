#include "c2btranslators/join/CHashJoinToVeloxHashJoin.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CHashJoinToVeloxHashJoin::Match(const CExpression* expr) {
  if (!expr) return false;

  COperator::EOperatorId opid = expr->Pop()->Eopid();

  // Match all hash join types
  return (opid == COperator::EopPhysicalInnerHashJoin ||
          opid == COperator::EopPhysicalLeftOuterHashJoin ||
          opid == COperator::EopPhysicalLeftSemiHashJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiHashJoin ||
          opid == COperator::EopPhysicalLeftAntiSemiHashJoinNotIn);
}



RetTypeC2B<EmptyStruct> CHashJoinToVeloxHashJoin::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  // Get the operator ID to determine the hash join type
  COperator::EOperatorId opid = expr->Pop()->Eopid();
  bool success = true;
  ColSet newRequiredColumns;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  Expression joinExpr;

  if (opid == COperator::EopPhysicalInnerHashJoin) {
    std::vector<std::string> joinColsLeft;
    std::vector<std::string> joinColsRight;
    CExpression *outerCExpr = (*expr)[0];
    CExpression *innerCExpr = (*expr)[1];
    CColRefSet *outerCExprSet = outerCExpr->DeriveOutputColumns();
    CColRefSet *innerCExprSet = innerCExpr->DeriveOutputColumns();
    CExpression *pred = (*expr)[2];
  
    bool parsedSpecialJoinCond = true;
    if (pred->Pop()->Eopid() == COperator::EopScalarBoolOp) {
      for (int i = 0; i < pred->Arity(); i++) {
        CExpression *child = (*pred)[i];
        if (child->Pop()->Eopid() == COperator::EopScalarCmp) {
          CExpression *leftChild = (*child)[0];
          CExpression *rightChild = (*child)[1];
          if (leftChild->Pop()->Eopid() == COperator::EopScalarIdent && rightChild->Pop()->Eopid() == COperator::EopScalarIdent) {
            std::string innerCol;
            std::string outerCol;

            const CColRef *leftColRef = CScalarIdent::PopConvert(leftChild->Pop())->Pcr();
            const CColRef *rightColRef = CScalarIdent::PopConvert(rightChild->Pop())->Pcr();

            if (innerCExprSet->FMember(leftColRef)) {
              innerCol = utils::WStringToString(leftColRef->Name().Pstr()->GetBuffer());
              outerCol = utils::WStringToString(rightColRef->Name().Pstr()->GetBuffer());
            } else {
              innerCol = utils::WStringToString(rightColRef->Name().Pstr()->GetBuffer());
              outerCol = utils::WStringToString(leftColRef->Name().Pstr()->GetBuffer());
            }
            
            if (std::find(joinColsLeft.begin(), joinColsLeft.end(), innerCol) != joinColsLeft.end() || std::find(joinColsRight.begin(), joinColsRight.end(), outerCol) != joinColsRight.end()) {
              continue;
            }
            joinColsLeft.push_back(innerCol);
            joinColsRight.push_back(outerCol);
          } else {
            parsedSpecialJoinCond = false;
            break;
          }
        } else {
          parsedSpecialJoinCond = false;
          break;
        }
      }
    } else {
      parsedSpecialJoinCond = false;
    }

    if (parsedSpecialJoinCond) {
      for (auto col : joinColsLeft) {
        newRequiredColumns.insert(col);
      }
      for (auto col : joinColsRight) {
        newRequiredColumns.insert(col);
      }

      Expression leftList;
      Expression rightList;

      if (joinColsLeft.size() == 1) {
        leftList = Symbol{joinColsLeft[0]};
      } else {
        ExpressionArguments argsLeft;
        for (auto col : joinColsLeft) {
          argsLeft.push_back(Symbol{col});
        }
        leftList = ComplexExpression{"List"_, {}, std::move(argsLeft), {}};
      }
      
      if (joinColsRight.size() == 1) {
        rightList = Symbol{joinColsRight[0]};
      } else {
        ExpressionArguments argsRight;
        for (auto col : joinColsRight) {
          argsRight.push_back(Symbol{col});
        }
        rightList = ComplexExpression{"List"_, {}, std::move(argsRight), {}};
      }


      // Create the BOSS expression based on the hash join type
      ProjectInfo newAux;
      newAux.parentOp = expr->Pop()->Eopid();
      newAux.requiredColumns = newRequiredColumns;
      std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> children =
          utils::GetChildExprBinary(expr, converter, newAux);
      RetTypeC2B<EmptyStruct> outerChild = std::move(children.first);
      RetTypeC2B<EmptyStruct> innerChild = std::move(children.second);
      success &= outerChild.success;
      success &= innerChild.success;

      joinExpr = "Join"_(
          std::move(innerChild.expr), std::move(outerChild.expr),
          "Where"_("Equal"_(std::move(leftList), std::move(rightList))));
    } else {
      RetTypeC2B<ColSet> joinCondExpr = GetJoinCondExpr(expr, converter);
      success &= joinCondExpr.success;
      newRequiredColumns.insert(joinCondExpr.aux.begin(), joinCondExpr.aux.end());

      // Create the BOSS expression based on the hash join type
      ProjectInfo newAux;
      newAux.parentOp = expr->Pop()->Eopid();
      newAux.requiredColumns = newRequiredColumns;
      std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> children =
          utils::GetChildExprBinary(expr, converter, newAux);
      RetTypeC2B<EmptyStruct> outerChild = std::move(children.first);
      RetTypeC2B<EmptyStruct> innerChild = std::move(children.second);
      success &= outerChild.success;
      success &= innerChild.success;

      joinExpr = std::visit(
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
                    return "Join"_(std::move(innerChild.expr), std::move(outerChild.expr),
                    "Where"_(std::move(ComplexExpression{std::move(head), std::move(args1), std::move(newDyns), std::move(args3)})));
                  }
                }
              }

            }

            return "Join"_(std::move(innerChild.expr), std::move(outerChild.expr),
                   "Where"_(std::move(ComplexExpression{std::move(head), std::move(args1), std::move(args2), std::move(args3)})));
          },
          [&](auto &&val) -> Expression { return val;  }),
      std::move(joinCondExpr.expr));
    }
  } else {
    std::pair<std::pair<Expression, Expression>, std::pair<bool, ColSet>> keyListExprs = GetInnerAndOuterKeyLists(expr, converter);
    newRequiredColumns.insert(keyListExprs.second.second.begin(), keyListExprs.second.second.end());
    success &= keyListExprs.second.first;
    Expression outerKeyList = std::move(keyListExprs.first.first);
    Expression innerKeyList = std::move(keyListExprs.first.second);

    RetTypeC2B<ColSet> joinCondExpr = GetJoinCondExpr(expr, converter);
    success &= joinCondExpr.success;
    newRequiredColumns.insert(joinCondExpr.aux.begin(), joinCondExpr.aux.end());


    // Create the BOSS expression based on the hash join type
    ProjectInfo newAux;
    newAux.parentOp = expr->Pop()->Eopid();
    newAux.requiredColumns = newRequiredColumns;
    std::pair<RetTypeC2B<EmptyStruct>, RetTypeC2B<EmptyStruct>> children =
        utils::GetChildExprBinary(expr, converter, newAux);
    RetTypeC2B<EmptyStruct> outerChild = std::move(children.first);
    RetTypeC2B<EmptyStruct> innerChild = std::move(children.second);
    success &= outerChild.success;
    success &= innerChild.success;
    
    switch (opid) {
      case COperator::EopPhysicalLeftOuterHashJoin:
        joinExpr = "LeftOuterHashJoin"_(
            std::move(innerChild.expr), std::move(outerChild.expr),
            std::move(joinCondExpr.expr),
            "OuterKeys"_(std::move(outerKeyList)),
            "InnerKeys"_(std::move(innerKeyList)));
        break;

      case COperator::EopPhysicalLeftSemiHashJoin:
        joinExpr = "LeftSemiHashJoin"_(
            std::move(innerChild.expr), std::move(outerChild.expr),
            std::move(joinCondExpr.expr),
            "OuterKeys"_(std::move(outerKeyList)),
            "InnerKeys"_(std::move(innerKeyList)));
        break;

      case COperator::EopPhysicalLeftAntiSemiHashJoin:
        joinExpr = "LeftAntiSemiHashJoin"_(
            std::move(innerChild.expr), std::move(outerChild.expr),
            std::move(joinCondExpr.expr),
            "OuterKeys"_(std::move(outerKeyList)),
            "InnerKeys"_(std::move(innerKeyList)));
        break;

      case COperator::EopPhysicalLeftAntiSemiHashJoinNotIn:
        joinExpr = "LeftAntiSemiHashJoinNotIn"_(
            std::move(innerChild.expr), std::move(outerChild.expr),
            std::move(joinCondExpr.expr),
            "OuterKeys"_(std::move(outerKeyList)),
            "InnerKeys"_(std::move(innerKeyList)));
        break;

      default:
        throw std::runtime_error("Unsupported hash join type"); 
    }
  }

  if (!(aux.parentOp == COperator::EopPhysicalComputeScalar)) {
    ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
    ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
    if (projectColumns != outputColumns) {
      Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
      return {"Project"_(std::move(joinExpr), std::move(projectList)), success};
    }
  }
  return {std::move(joinExpr), success};
}

int CHashJoinToVeloxHashJoin::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
