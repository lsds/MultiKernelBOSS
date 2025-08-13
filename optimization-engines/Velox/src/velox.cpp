#include "velox.hpp"
#include "c2btranslators/agg/CPhysicalHashAggToVeloxHashAgg.hpp"
#include "c2btranslators/agg/CPhysicalScalarAggToVeloxScalarAgg.hpp"
#include "c2btranslators/agg/CPhysicalStreamAggToVeloxStreamAgg.hpp"
#include "c2btranslators/filter/CFilterToVeloxFilter.hpp"
#include "c2btranslators/join/CHashJoinToVeloxHashJoin.hpp"
#include "c2btranslators/join/CMergeJoinToVeloxMergeJoin.hpp"
#include "c2btranslators/join/CNLJoinToVeloxNLJoin.hpp"
#include "c2btranslators/limit/CPhysicalLimitToVeloxPhysicalLimit.hpp"
#include "c2btranslators/limit/CPhysicalSortLimitToVeloxOrderBy.hpp"
#include "c2btranslators/project/CPhysicalComputeScalarToVeloxProject.hpp"
#include "c2btranslators/scan/CScanToVeloxGetColumns.hpp"
#include "c2btranslators/sort/CSortToVeloxSort.hpp"
#include "c2btranslators/passthrough/CPassthrough.hpp"

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
#include "translators/scan.hpp"
#include "translators/select.hpp"
#include "translators/top.hpp"
#include "translators/scalar/arithmeticop.hpp"
#include "translators/scalar/booleanop.hpp"
#include "translators/scalar/columnref.hpp"
#include "translators/scalar/comparisonop.hpp"
#include "translators/scalar/date.hpp"
#include "translators/scalar/list.hpp"
#include "translators/scalar/regulartype.hpp"
#include "translators/scalar/stringcontainsq.hpp"

#include "gpdbcost/CCostModelParamsGPDB.h"

#include <iostream>

CCost CostVeloxHashJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const orcaextender::BOSSCostModel *pcmgpdb,
							 const ICostModel::SCostingInfo  *pci)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
// const CDouble CCostModelParamsGPDB::DHJHashingTupWidthCostUnitVal = 1.97e-05;
// const CDouble CCostModelParamsGPDB::DJoinFeedingTupColumnCostUnitVal = 8.69e-05;

// // feeding cost per tuple per width in join operator
// const CDouble CCostModelParamsGPDB::DJoinFeedingTupWidthCostUnitVal = 6.09e-07;

	EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);

	const DOUBLE num_rows_outer = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->GetWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->GetWidth()[1];

  CCost costChild =
		orcaextender::BOSSCostModel::CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));
  // std::cout << dRowsInner << " " << num_rows_outer << " " << dWidthOuter << " " << costChild.Get() << std::endl;

  return CCost((dRowsInner / 100) + (num_rows_outer / 10000) + (dWidthOuter / 50), engine) + costChild;
}

CCost ZeroCost(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const orcaextender::BOSSCostModel *pcmgpdb,
							 const ICostModel::SCostingInfo  *pci) {
	EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);            
  CCost costChild =
		orcaextender::BOSSCostModel::CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));    
  return costChild;
}



CCost costGather(CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) {
    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);

    const DOUBLE N_out = pci->Rows();   
    const DOUBLE W_out = pci->Width();  

    const CDouble dMaterializeCostUnit =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CCostModelParamsGPDB::EcpMaterializeCostUnit)->Get();

    GPOS_ASSERT(0 < dMaterializeCostUnit);

    const DOUBLE GATHER_FACTOR = 0.1;

    const CCost costLocal = CCost(
        pci->NumRebinds() *
        (N_out * W_out * dMaterializeCostUnit) * GATHER_FACTOR, engine);


    CCost costChild = orcaextender::BOSSCostModel::CostChildren(
        mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));

    return costChild + costLocal;
}




// Custom engine implementation
Velox::Velox(std::string engineName) : orcaextender::Engine(engineName) {
}

Velox::~Velox() {
}

void Velox::ConfigureCostModel() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  DynamicRegistry->UseMaxCostModel(true);
}

void Velox::RegisterCostModelParams() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  DynamicRegistry->RegisterCostModelParams(m_engineType, GPOS_NEW(m_mp) gpdbcost::CCostModelParamsGPDB(m_mp));
}

void Velox::RegisterOperators() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();

  DynamicRegistry->UseDefaultCPUOps(DynamicRegistry->GetEngineType(VeloxEngineKeys::Velox));
  DynamicRegistry->AssignEngineToDefaultOp(COperator::EOperatorId::EopPhysicalTableScan, EetGP);

  DynamicRegistry->RegisterCostFunctionForOperator(COperator::EOperatorId::EopPhysicalTableScan, [](CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) -> CCost {
    EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);
    return CCost(0, engine);
  });

  DynamicRegistry->RegisterCostFunctionForOperator(COperator::EOperatorId::EopPhysicalInnerHashJoin, CostVeloxHashJoin);
  // DynamicRegistry->RegisterCostFunctionForOperator(COperator::EOperatorId::EopPhysicalComputeScalar, ZeroCost);

  DynamicRegistry->RegisterPhysicalOperator(VeloxOpKeys::CPhysicalVeloxGather, m_engineType, costGather);
  DynamicRegistry->RegisterLogicalOperator(VeloxOpKeys::CLogicalVeloxGather, m_engineType);
}

void Velox::RegisterTransforms() {
  orcaextender::DynamicRegistry* dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();

  dynamicRegistry->UseDefaultCPUTransforms();
  dynamicRegistry->RegisterTransform(VeloxTransformKeys::CXformGbVeloxLogicalGather2VeloxPhysicalGather, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbLogicalGather2VeloxPhysicalGather(m_mp));
}

void Velox::RegisterTranslators() {
    orcaextender::DynamicRegistry* dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
    auto stMap = std::make_shared<std::unordered_map<int, std::string>>();
    dynamicRegistry->DangerousAddAuxiliaryState("intToStringMapping", stMap);

    dynamicRegistry->RegisterOID("LINEITEM", 11111);
    dynamicRegistry->RegisterOID("CUSTOMER", 22222);
    dynamicRegistry->RegisterOID("ORDERS", 33333);
    dynamicRegistry->RegisterOID("REGION", 44444);
    dynamicRegistry->RegisterOID("NATION", 55555);
    dynamicRegistry->RegisterOID("PART", 66666);
    dynamicRegistry->RegisterOID("SUPPLIER", 77777);
    dynamicRegistry->RegisterOID("PARTSUPP", 88888);




    gpdxl::CMDAccessor *m_mda = COptCtxt::PoctxtFromTLS()->Pmda();
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::GetColumnsTranslator>(m_mp, m_mda));

    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::GroupByTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::JoinTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::OrderByTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::ProjectTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::ScanTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::SelectTranslator>(m_mp, m_mda));
    RegisterDefaultB2CTranslator(std::make_unique<bosstocexpression::TopTranslator>(m_mp, m_mda));

    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::ArithmeticOpTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::BooleanOpTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::ColumnRefTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::ComparisonOpTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::DateTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::ListTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::RegularTypeTranslator>(m_mp, m_mda));
    RegisterDefaultB2CScalarTranslator(std::make_unique<bosstocexpression::StringContainsQTranslator>(m_mp, m_mda));


    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CScanToVeloxGetColumns>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CFilterToVeloxFilter>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CHashJoinToVeloxHashJoin>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CNLJoinToVeloxNLJoin>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CMergeJoinToVeloxMergeJoin>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CSortToVeloxSort>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalComputeScalarToVeloxProject>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalLimitToVeloxPhysicalLimit>());
    // Register our new Sort+Limit operator
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalSortLimitToVeloxOrderBy>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalHashAggToVeloxHashAgg>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalStreamAggToVeloxStreamAgg>());
    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPhysicalScalarAggToVeloxScalarAgg>());
    // SCALAR TRANSLATORS
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarCmpToBOSSScalarCmp>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarBoolOpToBOSSScalarBoolOp>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarIdentToBOSSScalarIdent>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarConstToBOSSScalarConst>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarOpToBOSSScalarOp>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarAggFuncToBOSSScalarAggFunc>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarNullTestToBOSSScalarNullTest>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarProjectListToBOSSProjectList>());
    RegisterDefaultC2BScalarTranslator(std::make_unique<cexpressiontoboss::translation::CScalarArrayToBOSSScalarList>());

    RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CPassthrough>());
}


void Velox::RemoveTransforms() {

    // disable this for Q6.
    GPOPT_DISABLE_XFORM(CXform::ExfSplitGbAgg);
    GPOPT_DISABLE_XFORM(CXform::ExfGbAgg2StreamAgg);
    // GPOPT_DISABLE_XFORM(CXform::ExfInnerJoin2HashJoin);
    GPOPT_DISABLE_XFORM(CXform::ExfInnerJoin2NLJoin);
    // GPOS_SET_TRACE(EopttraceDisablePlanMasterOnlyQuery);
    // GPOS_SET_TRACE(EopttraceDisableMotionHashDistribute);
    // can remove operators / transformations as needed for BOSS purpose. (look at
    // CCostModelGPDB)
}

void Velox::RegisterEngineTransforms() {

}

void Velox::RegisterMetadataFilePath() {

}

// Export the factory function that will be called by the dynamic loader
extern "C" {
    orcaextender::Engine *CreateEngine() {
        return new Velox(VeloxEngineKeys::Velox);
    }
} 