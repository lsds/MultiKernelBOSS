#include "arrayfire.hpp"
#include "c2btranslators/project/CGPUProjectToAFProject.hpp"
#include "c2btranslators/filter/CGPUFilterToAFFilter.hpp"
#include "c2btranslators/join/CGPUJoin2AFJoin.hpp"

#include "translation/scalar/CScalarAggFuncToBOSSScalarAggFunc.hpp"
#include "translation/scalar/CScalarArrayToBOSSScalarList.hpp"
#include "translation/scalar/CScalarBoolOpToBOSSScalarBoolOp.hpp"
#include "translation/scalar/CScalarCmpToBOSSScalarCmp.hpp"
#include "translation/scalar/CScalarConstToBOSSScalarConst.hpp"
#include "translation/scalar/CScalarIdentToBOSSScalarIdent.hpp"
#include "translation/scalar/CScalarNullTestToBOSSScalarNullTest.hpp"
#include "translation/scalar/CScalarOpToBOSSScalarOp.hpp"
#include "translation/scalar/CScalarProjectListToBOSSProjectList.hpp"
#include "gpopt/base/CDrvdPropCtxtRelational.h"



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

#include <BOSS.hpp>


#include <iostream>



bool isGPUPartialSelect(COperator *op) {
    using orcaextender::DynamicRegistry;
    DynamicRegistry *dr = DynamicRegistry::GetInstance();
    EEngineType afEngine = dr->GetEngineType(AFEngineKeys::ArrayFire);
    COperator::EOperatorId gpuPartialSelect = dr->GetOperatorId(afEngine, AFOpKeys::CPhysicalGPUPartialSelect);

    return op->Eopid() == gpuPartialSelect;
}

bool isGPUFullSelect(COperator *op) {
    using orcaextender::DynamicRegistry;
    DynamicRegistry *dr = DynamicRegistry::GetInstance();
    EEngineType afEngine = dr->GetEngineType(AFEngineKeys::ArrayFire);
    COperator::EOperatorId gpuFullSelect = dr->GetOperatorId(afEngine, AFOpKeys::CPhysicalGPUFullSelect);

    return op->Eopid() == gpuFullSelect;  
}

bool isGPUJoin(COperator *op) {
    using orcaextender::DynamicRegistry;
    DynamicRegistry *dr = DynamicRegistry::GetInstance();
    EEngineType afEngine = dr->GetEngineType(AFEngineKeys::ArrayFire);
    COperator::EOperatorId gpuJoin = dr->GetOperatorId(afEngine, AFOpKeys::CPhysicalGPUJoin);

    return op->Eopid() == gpuJoin;  
}


// costs
CCost costGPUJoin(CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) {
    DOUBLE BUILD_SF = 1;
    DOUBLE PROBE_SF = 12.5;
    const DOUBLE MEM_THRESHOLD_SF = 2.9;

    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);

    const DOUBLE num_rows_outer = pci->PdRows()[0];
    const DOUBLE dWidthOuter = pci->GetWidth()[0];
    const DOUBLE dRowsInner = pci->PdRows()[1];
    const DOUBLE dWidthInner = pci->GetWidth()[1];

    // use GPU enum or doesn't matter since they are the same?
    const CDouble dJoinFeedingTupColumnCostUnit =
      pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)
        ->Get();
    const CDouble dJoinFeedingTupWidthCostUnit =
      pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)
        ->Get();
    const CDouble dHJHashingTupWidthCostUnit =
      pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpHJHashingTupWidthCostUnit)
        ->Get();
    const CDouble dJoinOutputTupCostUnit =
      pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)
        ->Get();
    const CDouble dHJSpillingMemThreshold =
      pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpHJSpillingMemThreshold)
        ->Get();

    GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
    GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
    GPOS_ASSERT(0 < dHJHashingTupWidthCostUnit);
    GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
    GPOS_ASSERT(0 < dHJSpillingMemThreshold);


    // If memory is tight, block a GPU join around a GPU select to avoid potentially disastrous plans.
    if (isGPUFullSelect(exprhdl.Pop(0)) || isGPUPartialSelect(exprhdl.Pop(0))) {
      PROBE_SF = PROBE_SF * 20;
      BUILD_SF = BUILD_SF * 20;
    }

    if (isGPUFullSelect(exprhdl.Pop(1)) || isGPUPartialSelect(exprhdl.Pop(1))) {
      PROBE_SF = PROBE_SF * 20;
      BUILD_SF = BUILD_SF * 20;
    }

    if (dRowsInner * dWidthInner * PROBE_SF + num_rows_outer * dWidthOuter * BUILD_SF > dHJSpillingMemThreshold * MEM_THRESHOLD_SF) {
      return CCost(1000000000000, engine);
    }

    // get the number of columns used in join condition
    CExpression *pexprJoinCond = exprhdl.PexprScalarRepChild(2);
    CColRefSet *pcrsUsed = pexprJoinCond->DeriveUsedColumns();
    const ULONG ulColsUsed = pcrsUsed->Size();

    CCost costLocal = CCost(
        pci->NumRebinds() *
        (
          // cost of feeding outer tuples
          ulColsUsed * num_rows_outer * dJoinFeedingTupColumnCostUnit +
          dWidthOuter * num_rows_outer * dJoinFeedingTupWidthCostUnit +
          // cost of matching inner tuples
          dWidthInner * dRowsInner * dHJHashingTupWidthCostUnit +
          // cost of output tuples
          pci->Rows() * pci->Width() * dJoinOutputTupCostUnit), engine);

    CCost costChild = orcaextender::BOSSCostModel::CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));

    return costChild + costLocal; 
}


CCost ZeroCost(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const orcaextender::BOSSCostModel *pcmgpdb,
							 const ICostModel::SCostingInfo  *pci) {
	EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);            
  CCost costChild =
		orcaextender::BOSSCostModel::CostChildren(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));    
  return costChild;
}

CCost costGPUPartialSelect(CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) {
    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    const DOUBLE WIDTH_SF = 8;
    const DOUBLE MEM_THRESHOLD_SF = 1;


    using orcaextender::CGPUCostModelParams;

    EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);


    const DOUBLE N_in   = pci->PdRows()[0];   
    const DOUBLE W_in   = pci->GetWidth()[0]; 
    const DOUBLE N_out  = pci->Rows();       
    const DOUBLE W_out  = pci->Width();   

    // Predicate columns used (scalar child for Select is at index 1)
    CExpression* pexprPred = exprhdl.PexprScalarRepChild(1);
    CColRefSet* pcrsUsed   = pexprPred->DeriveUsedColumns();
    const ULONG ulColsUsed = pcrsUsed->Size();

    // Pull constants
    const CDouble dFilterColCostUnit =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CGPUCostModelParams::EcpFilterColCostUnit)->Get();

    const CDouble dOutputTupCostUnit =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CGPUCostModelParams::EcpOutputTupCostUnit)->Get();

    // Use sort-per-tuple-width as a linear merge/intersect proxy
    const CDouble dSortTupWidthCostUnit =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CGPUCostModelParams::EcpSortTupWidthCostUnit)->Get();   

    GPOS_ASSERT(0 < dFilterColCostUnit);
    GPOS_ASSERT(0 < dOutputTupCostUnit);
    GPOS_ASSERT(0 < dSortTupWidthCostUnit);

    COperator *childOp = exprhdl.Pop(0);
    if (childOp->Eopid() == COperator::EopPhysicalEngineTransform) {
      childOp = exprhdl.PopGrandchild(0, 0, NULL);
    }


    BOOL isChildSelect = isGPUPartialSelect(childOp) && isGPUPartialSelect(exprhdl.Pop());

    const CDouble dHJSpillingMemThreshold =
    pcmgpdb->GetCostModelParams(engine)
        ->PcpLookup(CCostModelParamsGPDB::EcpHJSpillingMemThreshold)
        ->Get();
    // intersections only exist for nested partial selects
    int CHILD_FACTOR = isChildSelect ? 2 : 1; // meant to model the cols that will be loaded.
    // can we use this to determine output cols for the child select?
    // CCostContext *pccC = NULL;
    // exprhdl.PopDescendant({0}, &pccC);
    // CExpressionHandle newExpressionHandle(mp);
    // newExpressionHandle.Attach(pccC);
    // newExpressionHandle.DeriveProps(GPOS_NEW(mp) CDrvdPropCtxtRelational(mp)); 


    if (N_in * ulColsUsed * WIDTH_SF * CHILD_FACTOR > dHJSpillingMemThreshold * MEM_THRESHOLD_SF) {
        return CCost(1000000000000, engine);
    }

    const INT INTERSECT_MULTIPLIER = 60;

    CDouble costLocalDouble = pci->NumRebinds() * (
      // predicate evaluation to bit-array
      ulColsUsed * N_in * dFilterColCostUnit
      // producing selected positions
      + N_out * dOutputTupCostUnit
      // intersect if child is also a partial select.
      + (isChildSelect ? (N_in + N_out) * INTERSECT_MULTIPLIER * dSortTupWidthCostUnit :  0.0)
    );


    CCost costChild = orcaextender::BOSSCostModel::CostChildren(
        mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));

    return costChild + CCost(costLocalDouble, engine);
}

CCost costGPUFullSelect(CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) {
    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    using orcaextender::CGPUCostModelParams;

    EEngineType engine = orcaextender::BOSSCostModel::GetEngineType(mp, exprhdl);


    CCost costPartial =
        costGPUPartialSelect(mp, exprhdl, pcmgpdb, pci);

    const DOUBLE N_out = pci->Rows();
    const DOUBLE W_out = pci->Width();
    const CDouble dMaterializeCostUnit =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CGPUCostModelParams::EcpMaterializeCostUnit)->Get();

    GPOS_ASSERT(0 < dMaterializeCostUnit);

    CDouble dLookupCost =
        pci->NumRebinds() * (N_out * W_out * dMaterializeCostUnit);

    return costPartial + CCost(dLookupCost , engine);
}


CCost costGPU2CPUTransfer(CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) {
    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    GPOS_ASSERT(NULL != pcmgpdb);
    GPOS_ASSERT(NULL != pci);

    using orcaextender::CGPUCostModelParams;

    orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
    EEngineType engine = DynamicRegistry->GetEngineType(AFEngineKeys::ArrayFire);

    const DOUBLE N_out = pci->Rows();   
    const DOUBLE W_out = pci->Width(); 
    const DOUBLE N_in   = pci->PdRows()[0];   
    const DOUBLE W_in   = pci->GetWidth()[0]; 

    const DOUBLE BYTES_PER_POS = 4.0; 
    const DOUBLE PACKING_OVERHEAD = 1; // ignore marshaling overhead
    const CDouble dBaselineBW =
        pcmgpdb->GetCostModelParams(engine)
            ->PcpLookup(CGPUCostModelParams::EcpOutputBandwidth)->Get();
    const DOUBLE K_D2H_BW_MULT = 40;
    const DOUBLE D2H_BANDWIDTH = dBaselineBW.Get() * K_D2H_BW_MULT; 

    COperator* popChild0 = exprhdl.Pop(0);
    BOOL child_is_partial_select =
        (popChild0 != nullptr) && isGPUPartialSelect(popChild0);

    DOUBLE bytes_to_transfer =
        child_is_partial_select
            ? (N_out * BYTES_PER_POS)                 // positions only
            : (N_out * W_out * PACKING_OVERHEAD);     // full rows

    std::vector<ULONG> indices = {0};

    if (isGPUPartialSelect(popChild0) || popChild0->Eopid() == COperator::EopPhysicalEngineTransform) {
      while (popChild0 != NULL && (isGPUPartialSelect(popChild0) || popChild0->Eopid() == COperator::EopPhysicalEngineTransform)) {
        CCostContext *pccC = NULL;
        indices.push_back(0);
        popChild0 = exprhdl.PopDescendant(indices, &pccC);
      }

      if (popChild0 != NULL && (isGPUJoin(popChild0) || isGPUFullSelect(popChild0))) {
        // cuz we would have to transfer these too.
        bytes_to_transfer = bytes_to_transfer + N_out * W_out;
      }
    }


    const CDouble dLocal =
        pci->NumRebinds() * (bytes_to_transfer / D2H_BANDWIDTH);

    CCost costChild = orcaextender::BOSSCostModel::CostChildren(
        mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine));


    return costChild + CCost(dLocal, engine);
}


// REGISTRATION FUNCTIONS


// Custom engine implementation
ArrayFire::ArrayFire(std::string engineName) : orcaextender::Engine(engineName) {
}

ArrayFire::~ArrayFire() {
}

void ArrayFire::RegisterCostModelParams() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  DynamicRegistry->RegisterCostModelParams(m_engineType, GPOS_NEW(m_mp) orcaextender::CGPUCostModelParams(m_mp));
}

void ArrayFire::RegisterOperators() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  DynamicRegistry->RegisterPhysicalOperator(AFOpKeys::CPhysicalGPUJoin, m_engineType, costGPUJoin);

  DynamicRegistry->RegisterLogicalOperator(AFOpKeys::CLogicalPartialSelect, m_engineType);
  // DynamicRegistry->RegisterPhysicalOperator(AFOpKeys::CPhysicalGPUSelect, m_engineType, orcaextender::BOSSCostModel::CostFilter);
  DynamicRegistry->RegisterPhysicalOperator(AFOpKeys::CPhysicalGPUPartialSelect, m_engineType, costGPUPartialSelect);

  DynamicRegistry->RegisterPhysicalOperator(AFOpKeys::CPhysicalGPUFullSelect, m_engineType, costGPUFullSelect);

  DynamicRegistry->RegisterPhysicalOperator(AFOpKeys::CPhysicalGPUProject, m_engineType, orcaextender::BOSSCostModel::CostUnary);

  DynamicRegistry->HookOpToTransform(CXform::ExfProject2ComputeScalar, DynamicRegistry->GetOperatorId(m_engineType, AFOpKeys::CPhysicalGPUProject), [](orcaextender::DynamicOperatorArgs& args) -> gpopt::COperator* {
    orcaextender::CProjectArgs& projectArgs = dynamic_cast<orcaextender::CProjectArgs&>(args);
    CMemoryPool *mp = projectArgs.mp;
    return GPOS_NEW(mp) orcaextender::CPhysicalGPUProject(mp);
  });
}

void ArrayFire::RegisterTransforms() {
  orcaextender::DynamicRegistry* dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbSelect2SelectGather, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbSelect2SelectGather(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalSelect, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbSelect2SelectGather));

  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect(m_mp));
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbLogicalSelect2PhysicalGPUSelect, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbLogicalSelect2PhysicalGPUSelect(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalSelect, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbLogicalSelect2PhysicalGPUSelect));
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbLogicalSelectGather2Select, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbLogicalSelectGather2Select(m_mp));
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbFSelectConjunct2NestedFSelect, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbFSelectConjunct2NestedFSelect(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalSelect, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbFSelectConjunct2NestedFSelect));
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbSwapGPUSelect, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbSwapGPUSelect(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalSelect, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbSwapGPUSelect));


  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformLogicalJoin2PhysicalGPUJoin, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformLogicalJoin2PhysicalGPUJoin(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalInnerJoin, dynamicRegistry->GetTransformId(AFTransformKeys::CXformLogicalJoin2PhysicalGPUJoin));

  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbPostFilterLeft, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbPostFilterLeft(m_mp));
  dynamicRegistry->RegisterTransform(AFTransformKeys::CXformGbPostFilterRight, m_engineType, GPOS_NEW(m_mp) orcaextender::CXformGbPostFilterRight(m_mp));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalInnerJoin, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbPostFilterLeft));
  dynamicRegistry->HookTransformToOp(COperator::EopLogicalInnerJoin, dynamicRegistry->GetTransformId(AFTransformKeys::CXformGbPostFilterRight));
}

void ArrayFire::RegisterTranslators() {  
  RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CGPUProjectToAFProject>());
  RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CGPUFilterToAFFilter>());
  RegisterDefaultC2BTranslator(std::make_unique<cexpressiontoboss::translation::CGPUJoin2AFJoin>());
}

void ArrayFire::RemoveTransforms() {
}

void ArrayFire::RegisterEngineTransforms() {
  orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  DynamicRegistry->RegisterEngineTransform(m_engineType, EetGP, [this](CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) -> CCost {
    orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
    EEngineType engine = DynamicRegistry->GetEngineType(AFEngineKeys::ArrayFire);

    CCost costChild = orcaextender::BOSSCostModel::CostChildrenWithEngine(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine), engine);
    return costChild + CCost(100000000000, EetGP);
  });

  DynamicRegistry->RegisterEngineTransform(EetGP, m_engineType, [](CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) -> CCost {
    CCost costChild = orcaextender::BOSSCostModel::CostChildrenWithEngine(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(EetGP), EetGP);
    return costChild;
  });
  try {
    DynamicRegistry->RegisterEngineTransform(m_engineType, DynamicRegistry->GetEngineType(AFEngineKeys::Velox), costGPU2CPUTransfer);

    DynamicRegistry->RegisterEngineTransform(DynamicRegistry->GetEngineType(AFEngineKeys::Velox), m_engineType, [](CMemoryPool* mp, CExpressionHandle& exprhdl, const orcaextender::BOSSCostModel* pcmgpdb, const ICostModel::SCostingInfo* pci) -> CCost {
      orcaextender::DynamicRegistry* DynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
      EEngineType engine = DynamicRegistry->GetEngineType(AFEngineKeys::Velox);

      CCost costChild = orcaextender::BOSSCostModel::CostChildrenWithEngine(mp, exprhdl, pci, pcmgpdb->GetCostModelParams(engine), engine);

      return costChild + CCost(1000000000000, engine);
    });
  } catch (const std::exception& e) {
    std::cerr << "NEED TO LOAD VELOX BEFORE ARRAYFIRE" << std::endl;
    throw;
  }
}

void ArrayFire::RegisterMetadataFilePath() {}

// Export the factory function that will be called by the dynamic loader
extern "C" {
    orcaextender::Engine *CreateEngine() {
        return new ArrayFire(AFEngineKeys::ArrayFire);
    }
} 
