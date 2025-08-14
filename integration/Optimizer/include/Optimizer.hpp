#pragma once

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpression.h"  // ORCA's CExpression definition
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
// #include "gpopt/engine/CEngine.h"
// #include "gpopt/metadata/CTableDescriptor.h"
// #include "gpopt/optimizer/COptimizer.h"

#include <BOSS.hpp>  // BOSS's expression definition
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>

#include "gpdbcost/CCostModelGPDBLegacy.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/exception.h"
#include "gpopt/init.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpos/_api.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/init.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "BOSSToCExpression.hpp"
#include "CExpressionToBOSS.hpp"
#include "BOSSToCExpressionDefault.hpp"
#include "CExpressionToBOSSDefault.hpp"
#include "gpoptextender/DynamicRegistry/DynamicOperatorArgs.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/EngineInterface/Engine.hpp"
#include "translation/scalar/CScalarAggFuncToBOSSScalarAggFunc.hpp"
#include "translation/scalar/CScalarArrayToBOSSScalarList.hpp"
#include "translation/scalar/CScalarBoolOpToBOSSScalarBoolOp.hpp"
#include "translation/scalar/CScalarCmpToBOSSScalarCmp.hpp"
#include "translation/scalar/CScalarConstToBOSSScalarConst.hpp"
#include "translation/scalar/CScalarIdentToBOSSScalarIdent.hpp"
#include "translation/scalar/CScalarNullTestToBOSSScalarNullTest.hpp"
#include "translation/scalar/CScalarOpToBOSSScalarOp.hpp"
#include "translation/scalar/CScalarProjectListToBOSSProjectList.hpp"
#include "translators/getcolumns.hpp"
#include "translators/groupby.hpp"
#include "translators/join.hpp"
#include "translators/orderby.hpp"
#include "translators/project.hpp"
#include "translators/scalar/arithmeticop.hpp"
#include "translators/scalar/booleanop.hpp"
#include "translators/scalar/columnref.hpp"
#include "translators/scalar/comparisonop.hpp"
#include "translators/scalar/date.hpp"
#include "translators/scalar/list.hpp"
#include "translators/scalar/regulartype.hpp"
#include "translators/scalar/stringcontainsq.hpp"
#include "translators/scan.hpp"
#include "translators/select.hpp"
#include "translators/top.hpp"
// #include <iostream>

// Add necessary includes for GPDB types
#include <cstdlib>
#include <string>
#include <vector>

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalFullOuterJoin.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/types.h"  // For ULong
#include "naucrates/base/CDatumBoolGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "unittest/gpopt/CTestUtils.h"
#include "gpos/memory/CMemoryPoolManager.h"
// Add dynamic library loading support
#include <dlfcn.h>
#include <unordered_map>


using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;
using boss::ExpressionArguments;
using boss::get;
using boss::Span;
using boss::Symbol;
using boss::expressions::CloneReason;
using boss::expressions::ExpressionSpanArgument;
using boss::expressions::ExpressionSpanArguments;

typedef CCacheAccessor<IMDCacheObject*, CMDKey*> CacheAccessorMD;

struct InpMainTask {
  const CHAR *mdFile; 
  CMemoryPool *mp;

  InpMainTask(const CHAR *mdFile, CMemoryPool *mp) : mdFile(mdFile), mp(mp) {}
};


class Optimizer {
  public:
    Optimizer() = default;
    ~Optimizer() {
      CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
    };

    void Init();
    void Cleanup();

    void AddLoadLibraryTask(const std::string &libPath) {
      LibArgs* args = new LibArgs(libPath);
      tasks.push_back(LoadLib);
      taskArgs.push_back(args);
    }

    void AddGetSupportedOperatorsTask() {
      EmptyArgs* args = new EmptyArgs();
      tasks.push_back(GetOperators);
      taskArgs.push_back(args);
    }

    void AddUnloadLibraryTask(const std::string &libPath) {
      LibArgs* args = new LibArgs(libPath);
      tasks.push_back(UnloadLib);
      taskArgs.push_back(args);
    }

    void AddOptimizeQueryTask(ComplexExpression&& expr) {
      InpArgs *inpArgs = new InpArgs(std::move(expr));
      tasks.push_back(Optimize);
      taskArgs.push_back(inpArgs);
    }


    void AddSetMdConfigTask(const std::string &engineName, ULONG id, CDouble dVal, CDouble dLowerBound, CDouble dUpperBound) {
      MdConfigArgs *mdConfigArgs = new MdConfigArgs(engineName, id, dVal, dLowerBound, dUpperBound);
      tasks.push_back(ConfigMd);
      taskArgs.push_back(mdConfigArgs);
    }


    std::vector<Expression> ExecuteTasks(std::string mdFile);

  private:
    static std::vector<std::function<Expression(size_t)>> tasks;
    static std::vector<void*> taskArgs;
    static std::vector<Expression> resultExprs;

    struct InpArgs {
      InpArgs(ComplexExpression&& expr) : expr(std::move(expr)) {}
      ComplexExpression expr;
    };

    struct LibArgs {
      LibArgs(std::string libPath) : libPath(libPath) {}
      std::string libPath;
    };

    struct MdConfigArgs {
      MdConfigArgs(std::string engineName, ULONG id, CDouble dVal, CDouble dLowerBound, CDouble dUpperBound) : engineName(engineName), id(id), dVal(dVal), dLowerBound(dLowerBound), dUpperBound(dUpperBound) {}
      std::string engineName;
      ULONG id;
      CDouble dVal;
      CDouble dLowerBound;
      CDouble dUpperBound;
    };

    struct EmptyArgs {};

    // struct MainArgs {
    //   MainArgs(std::vector<Expression> resultExprs, CMemoryPool *mp) : resultExprs(resultExprs), mp(mp) {}
    //   std::vector<Expression> resultExprs;
    //   CMemoryPool *mp;
    // };


    CMemoryPool *mp;
    static std::unordered_map<std::string, void*> g_loadedLibraries;
    static std::unordered_map<std::string, std::string> g_loadedEngines;


    CMDProviderMemory* InitProviderFile(CMemoryPool *mp, const CHAR *szMDFileName)
    {
      GPOS_ASSERT(NULL != mp);
      return GPOS_NEW(mp) CMDProviderMemory(mp, szMDFileName);
    }


    static Expression LoadLib(size_t idx);

    static Expression UnloadLib(size_t idx);

    static Expression GetOperators(size_t idx);

    static Expression ConfigMd(size_t idx);

    static Expression Optimize(size_t idx) {
      InpArgs *inpArgs = static_cast<InpArgs *>(taskArgs[idx]);
      boss::ComplexExpression&& expr = std::move(inpArgs->expr);

      CMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
      orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();

      Expression ResultExpr;
      // std::cout << "original expr: " << expr << "\n" << std::endl;

      // Convert BOSS expression to CExpression
      orcaextender::B2CConverter* b2cconverter = dynamicRegistry->GetDefaultErasedBOSS2CExpressionConverter();
      // NOTE: a segfault here is likely due to an invalid expressions & not
      // proper freeing in the converter. Plz report if found.
      CExpression *convertedExpr = nullptr;
      try {
        auto ret = b2cconverter->ConvertExpr(std::move(expr));
        if (!ret.second) {
          std::cerr << "Error: Failed to convert BOSS expression to CExpression"
                    << std::endl;
          delete inpArgs;
          return Symbol{"failed"};
        }
        convertedExpr = ret.first;
      } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        delete inpArgs;
        return Symbol{"failed"};
      }
      // CExpression *convertedExpr = CTestUtils::PexprLogicalGbAggWithSum(mp);
      if (convertedExpr) {
        // CWStringDynamic str(mp);
        // COstreamString oss(&str);
        // convertedExpr->OsPrint(oss);
        // wprintf(L"Converted Expr:\n%ls\n", str.GetBuffer());
      }
      // Generate query context

      CQueryContext *pqc = CTestUtils::PqcGenerate(mp, convertedExpr);
      CExpression *pexprOptimized = COptimizer::PexprOptimize(mp, pqc, NULL);

      if (pexprOptimized) {
        // Print the converted expression
        // CWStringDynamic str(mp);
        // COstreamString oss(&str);
        // pexprOptimized->OsPrint(oss);
        // wprintf(L"Optimized Expr:\n%ls\n", str.GetBuffer());
        // wprintf(L"hi");
        try {
          orcaextender::C2BConverter* c2bconverter = dynamicRegistry->GetDefaultErasedCExpression2BOSSConverter();
          auto ret = c2bconverter->ConvertExpr(pexprOptimized);
          ResultExpr = std::move(ret.first);
        } catch (const std::exception &e) {
          std::cerr << "Error: " << e.what() << std::endl;
          ResultExpr = Symbol{"failed"};
          delete inpArgs;
          return std::move(ResultExpr);
        }
        convertedExpr->Release();
        pexprOptimized->Release();
      }

      GPOS_DELETE(pqc);
      delete inpArgs;
      return std::move(ResultExpr);
    }

    static void *MainTask(void *arg);

    static void FreeAllEngines();
    ComplexExpression getColumnsAsList(std::vector<std::string> const &columnNames);
    static boss::Expression getOperatorList();
    static void ConfigureEnvironment(const CHAR *mdFile);

};




