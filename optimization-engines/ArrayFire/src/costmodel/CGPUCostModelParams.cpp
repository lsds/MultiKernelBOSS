// these are extremely rough constants
// research will have to go into GPU internals to accurately set each constant. 

#include "costmodel/CGPUCostModelParams.hpp"
#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"
using namespace orcaextender;

// sequential i/o bandwidth
const CDouble CGPUCostModelParams::DSeqIOBandwidthVal = 900000.0; 
const CDouble CGPUCostModelParams::DRandomIOBandwidthVal = 100000.0;  
// tuple processing bandwidth
const CDouble CGPUCostModelParams::DTupProcBandwidthVal = 51200.0; 
// output bandwidth
const CDouble CGPUCostModelParams::DOutputBandwidthVal = 25600.0;  
// scan initialization cost factor
const CDouble CGPUCostModelParams::DInitScanFacorVal = 800.0;  
// table scan cost unit
const CDouble CGPUCostModelParams::DTableScanCostUnitVal = 5.50e-09; 
// index scan initialization cost factor
const CDouble CGPUCostModelParams::DInitIndexScanFactorVal = 200.0; 
// index block cost unit
const CDouble CGPUCostModelParams::DIndexBlockCostUnitVal = 1.27e-08;  
// index filtering cost unit
const CDouble CGPUCostModelParams::DIndexFilterCostUnitVal = 1.65e-06;  
// index scan cost unit per tuple per width
const CDouble CGPUCostModelParams::DIndexScanTupCostUnitVal = 3.66e-08;  
// index scan random IO factor
const CDouble CGPUCostModelParams::DIndexScanTupRandomFactorVal = 3.0;  
// filter column cost unit
const CDouble CGPUCostModelParams::DFilterColCostUnitVal = 3.29e-07; 
// output tuple cost unit
const CDouble CGPUCostModelParams::DOutputTupCostUnitVal = 1.86e-08; 
// sending tuple cost unit in gather motion
const CDouble CGPUCostModelParams::DGatherSendCostUnitVal = 4.58e-08;  
// receiving tuple cost unit in gather motion
const CDouble CGPUCostModelParams::DGatherRecvCostUnitVal = 2.20e-08; 
// sending tuple cost unit in redistribute motion
const CDouble CGPUCostModelParams::DRedistributeSendCostUnitVal = 2.33e-08; 

// receiving tuple cost unit in redistribute motion
const CDouble CGPUCostModelParams::DRedistributeRecvCostUnitVal = 8.0e-09;  

// sending tuple cost unit in broadcast motion
const CDouble CGPUCostModelParams::DBroadcastSendCostUnitVal = 4.965e-07;  

// receiving tuple cost unit in broadcast motion
const CDouble CGPUCostModelParams::DBroadcastRecvCostUnitVal = 1.35e-08;  

// tuple cost unit in No-Op motion
const CDouble CGPUCostModelParams::DNoOpCostUnitVal = 0;

// feeding cost per tuple per column in join operator
const CDouble CGPUCostModelParams::DJoinFeedingTupColumnCostUnitVal = 8.69e-07;  

// feeding cost per tuple per width in join operator
const CDouble CGPUCostModelParams::DJoinFeedingTupWidthCostUnitVal = 6.09e-09; 

// output cost per tuple in join operator
const CDouble CGPUCostModelParams::DJoinOutputTupCostUnitVal = 3.50e-08;  

// memory threshold for hash join spilling (in bytes)
const CDouble CGPUCostModelParams::DHJSpillingMemThresholdVal = 12.0 * 1024.0 * 1024.0 * 1024.0;  // 16GB for GPU memory

// initial cost for building hash table for hash join
const CDouble CGPUCostModelParams::DHJHashTableInitCostFactorVal = 800.0;  

// building hash table cost per tuple per column
const CDouble CGPUCostModelParams::DHJHashTableColumnCostUnitVal = 5.0e-07;  

// the unit cost to process each tuple with unit width when building a hash table
const CDouble CGPUCostModelParams::DHJHashTableWidthCostUnitVal = 3.0e-08;  

// hashing cost per tuple with unit width in hash join
const CDouble CGPUCostModelParams::DHJHashingTupWidthCostUnitVal = 1.97e-07; 

// feeding cost per tuple per column in hash join if spilling
const CDouble CGPUCostModelParams::DHJFeedingTupColumnSpillingCostUnitVal = 1.97e-06; 

// feeding cost per tuple with unit width in hash join if spilling
const CDouble CGPUCostModelParams::DHJFeedingTupWidthSpillingCostUnitVal = 3.0e-08;  

// hashing cost per tuple with unit width in hash join if spilling
const CDouble CGPUCostModelParams::DHJHashingTupWidthSpillingCostUnitVal = 2.30e-07; 

// cost for building hash table for per tuple per grouping column in hash aggregate
const CDouble CGPUCostModelParams::DHashAggInputTupColumnCostUnitVal = 1.20e-06;  

// cost for building hash table for per tuple with unit width in hash aggregate
const CDouble CGPUCostModelParams::DHashAggInputTupWidthCostUnitVal = 1.12e-09;  

// cost for outputting for per tuple with unit width in hash aggregate
const CDouble CGPUCostModelParams::DHashAggOutputTupWidthCostUnitVal = 5.61e-09; 

// sorting cost per tuple with unit width
const CDouble CGPUCostModelParams::DSortTupWidthCostUnitVal = 5.67e-08;  

// cost for processing per tuple with unit width
const CDouble CGPUCostModelParams::DTupDefaultProcCostUnitVal = 8.5e-07;

// cost for materializing per tuple with unit width
const CDouble CGPUCostModelParams::DMaterializeCostUnitVal = 4.68e-08;  

// tuple update bandwidth
const CDouble CGPUCostModelParams::DTupUpdateBandwidthVal = 25600.0;  

// network bandwidth
const CDouble CGPUCostModelParams::DNetBandwidthVal = 102400.0;  

// number of segments
const CDouble CGPUCostModelParams::DSegmentsVal = 8.0; 

// nlj factor
const CDouble CGPUCostModelParams::DNLJFactorVal = 5.0;  

// hj factor
const CDouble CGPUCostModelParams::DHJFactorVal = 0.5;  

// hash building factor
const CDouble CGPUCostModelParams::DHashFactorVal = 0.5;  

// default cost
const CDouble CGPUCostModelParams::DDefaultCostVal = 50.0;  

// largest estimation risk for which we don't penalize index join
const CDouble CGPUCostModelParams::DIndexJoinAllowedRiskThreshold = 3;

// default bitmap IO co-efficient when NDV is larger
const CDouble CGPUCostModelParams::DBitmapIOCostLargeNDV(0.0082);

// default bitmap IO co-efficient when NDV is smaller
const CDouble CGPUCostModelParams::DBitmapIOCostSmallNDV(0.2138);

// default bitmap page cost when NDV is larger
const CDouble CGPUCostModelParams::DBitmapPageCostLargeNDV(83.1651);

// default bitmap page cost when NDV is larger
const CDouble CGPUCostModelParams::DBitmapPageCostSmallNDV(204.3810);

// default bitmap page cost with no assumption about NDV
const CDouble CGPUCostModelParams::DBitmapPageCost(10);

// default threshold of NDV for bitmap costing
const CDouble CGPUCostModelParams::DBitmapNDVThreshold(200);

// cost of a bitmap scan rebind
const CDouble CGPUCostModelParams::DBitmapScanRebindCost(0.06);

// see CCostModelGPDB::CostHashJoin() for why this is needed
const CDouble CGPUCostModelParams::DPenalizeHJSkewUpperLimit(10.0);

#define GPOPT_COSTPARAM_NAME_MAX_LENGTH 80

// parameter names in the same order of param enumeration
const CHAR rgszCostParamNames[CGPUCostModelParams::EcpSentinel]
							 [GPOPT_COSTPARAM_NAME_MAX_LENGTH] = {
								 "SeqIOBandwidth",
								 "RandomIOBandwidth",
								 "TupProcBandwidth",
								 "OutputBandwidth",
								 "InitScanFacor",
								 "TableScanCostUnit",
								 "InitIndexScanFactor",
								 "IndexBlockCostUnit",
								 "IndexFilterCostUnit",
								 "IndexScanTupCostUnit",
								 "IndexScanTupRandomFactor",
								 "FilterColCostUnit",
								 "OutputTupCostUnit",
								 "GatherSendCostUnit",
								 "GatherRecvCostUnit",
								 "RedistributeSendCostUnit",
								 "RedistributeRecvCostUnit",
								 "BroadcastSendCostUnit",
								 "BroadcastRecvCostUnit",
								 "NoOpCostUnit",
								 "JoinFeedingTupColumnCostUnit",
								 "JoinFeedingTupWidthCostUnit",
								 "JoinOutputTupCostUnit",
								 "HJSpillingMemThreshold",
								 "HJHashTableInitCostFactor",
								 "HJHashTableColumnCostUnit",
								 "HJHashTableWidthCostUnit",
								 "HJHashingTupWidthCostUnit",
								 "HJFeedingTupColumnSpillingCostUnit",
								 "HJFeedingTupWidthSpillingCostUnit",
								 "HJHashingTupWidthSpillingCostUnit",
								 "HashAggInputTupColumnCostUnit",
								 "HashAggInputTupWidthCostUnit",
								 "HashAggOutputTupWidthCostUnit",
								 "SortTupWidthCostUnit",
								 "TupDefaultProcCostUnit",
								 "MaterializeCostUnit",
								 "TupUpdateBandwidth",
								 "NetworkBandwidth",
								 "Segments",
								 "NLJFactor",
								 "HJFactor",
								 "HashFactor",
								 "DefaultCost",
								 "IndexJoinAllowedRiskThreshold",
								 "BitmapIOLargerNDV",
								 "BitmapIOSmallerNDV",
								 "BitmapPageCostLargerNDV",
								 "BitmapPageCostSmallerNDV",
								 "BitmapNDVThreshold",
};


CGPUCostModelParams::CGPUCostModelParams(CMemoryPool *mp) : m_mp(mp)
{
	GPOS_ASSERT(NULL != mp);

	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		m_rgpcp[ul] = NULL;
	}

	// populate param array with default param values
	m_rgpcp[EcpSeqIOBandwidth] = GPOS_NEW(mp)
		SCostParam(EcpSeqIOBandwidth, DSeqIOBandwidthVal,
				   DSeqIOBandwidthVal * 0.8, DSeqIOBandwidthVal * 1.2);
	m_rgpcp[EcpRandomIOBandwidth] = GPOS_NEW(mp)
		SCostParam(EcpRandomIOBandwidth, DRandomIOBandwidthVal,
				   DRandomIOBandwidthVal * 0.8, DRandomIOBandwidthVal * 1.2);
	m_rgpcp[EcpTupProcBandwidth] = GPOS_NEW(mp)
		SCostParam(EcpTupProcBandwidth, DTupProcBandwidthVal,
				   DTupProcBandwidthVal * 0.8, DTupProcBandwidthVal * 1.2);
	m_rgpcp[EcpOutputBandwidth] = GPOS_NEW(mp)
		SCostParam(EcpOutputBandwidth, DOutputBandwidthVal,
				   DOutputBandwidthVal * 0.8, DOutputBandwidthVal * 1.2);
	m_rgpcp[EcpInitScanFactor] = GPOS_NEW(mp)
		SCostParam(EcpInitScanFactor, DInitScanFacorVal,
				   DInitScanFacorVal * 0.5, DInitScanFacorVal * 1.5);
	m_rgpcp[EcpTableScanCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpTableScanCostUnit, DTableScanCostUnitVal,
				   DTableScanCostUnitVal * 0.5, DTableScanCostUnitVal * 2.0);
	m_rgpcp[EcpInitIndexScanFactor] = GPOS_NEW(mp) 
		SCostParam(EcpInitIndexScanFactor, DInitIndexScanFactorVal,
		DInitIndexScanFactorVal * 0.5, DInitIndexScanFactorVal * 1.5);
	m_rgpcp[EcpIndexBlockCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpIndexBlockCostUnit, DIndexBlockCostUnitVal,
				   DIndexBlockCostUnitVal * 0.5, DIndexBlockCostUnitVal * 2.0);
	m_rgpcp[EcpIndexFilterCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpIndexFilterCostUnit, DIndexFilterCostUnitVal,
		DIndexFilterCostUnitVal * 0.5, DIndexFilterCostUnitVal * 2.0);
	m_rgpcp[EcpIndexScanTupCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpIndexScanTupCostUnit, DIndexScanTupCostUnitVal,
		DIndexScanTupCostUnitVal * 0.5, DIndexScanTupCostUnitVal * 2.0);
	m_rgpcp[EcpIndexScanTupRandomFactor] = GPOS_NEW(mp) 
		SCostParam(EcpIndexScanTupRandomFactor, DIndexScanTupRandomFactorVal,
		DIndexScanTupRandomFactorVal * 0.5, DIndexScanTupRandomFactorVal * 1.5);
	m_rgpcp[EcpFilterColCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpFilterColCostUnit, DFilterColCostUnitVal,
				   DFilterColCostUnitVal * 0.5, DFilterColCostUnitVal * 2.0);
	m_rgpcp[EcpOutputTupCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpOutputTupCostUnit, DOutputTupCostUnitVal,
				   DOutputTupCostUnitVal * 0.5, DOutputTupCostUnitVal * 2.0);
	m_rgpcp[EcpGatherSendCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpGatherSendCostUnit, DGatherSendCostUnitVal,
				   DGatherSendCostUnitVal * 0.5, DGatherSendCostUnitVal * 2.0);
	m_rgpcp[EcpGatherRecvCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpGatherRecvCostUnit, DGatherRecvCostUnitVal,
				   DGatherRecvCostUnitVal * 0.5, DGatherRecvCostUnitVal * 2.0);
	m_rgpcp[EcpRedistributeSendCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpRedistributeSendCostUnit, DRedistributeSendCostUnitVal,
		DRedistributeSendCostUnitVal * 0.5, DRedistributeSendCostUnitVal * 2.0);
	m_rgpcp[EcpRedistributeRecvCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpRedistributeRecvCostUnit, DRedistributeRecvCostUnitVal,
		DRedistributeRecvCostUnitVal * 0.5, DRedistributeRecvCostUnitVal * 2.0);
	m_rgpcp[EcpBroadcastSendCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpBroadcastSendCostUnit, DBroadcastSendCostUnitVal,
		DBroadcastSendCostUnitVal * 0.5, DBroadcastSendCostUnitVal * 2.0);
	m_rgpcp[EcpBroadcastRecvCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpBroadcastRecvCostUnit, DBroadcastRecvCostUnitVal,
		DBroadcastRecvCostUnitVal * 0.5, DBroadcastRecvCostUnitVal * 2.0);
	m_rgpcp[EcpNoOpCostUnit] =
		GPOS_NEW(mp) SCostParam(EcpNoOpCostUnit, DNoOpCostUnitVal,
								0.0, DNoOpCostUnitVal + 1.0e-6);
	m_rgpcp[EcpJoinFeedingTupColumnCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpJoinFeedingTupColumnCostUnit, DJoinFeedingTupColumnCostUnitVal,
		DJoinFeedingTupColumnCostUnitVal * 0.5, DJoinFeedingTupColumnCostUnitVal * 2.0);
	m_rgpcp[EcpJoinFeedingTupWidthCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpJoinFeedingTupWidthCostUnit, DJoinFeedingTupWidthCostUnitVal,
		DJoinFeedingTupWidthCostUnitVal * 0.5, DJoinFeedingTupWidthCostUnitVal * 2.0);
	m_rgpcp[EcpJoinOutputTupCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpJoinOutputTupCostUnit, DJoinOutputTupCostUnitVal,
		DJoinOutputTupCostUnitVal * 0.5, DJoinOutputTupCostUnitVal * 2.0);
	m_rgpcp[EcpHJSpillingMemThreshold] = GPOS_NEW(mp) 
		SCostParam(EcpHJSpillingMemThreshold, DHJSpillingMemThresholdVal,
		DHJSpillingMemThresholdVal * 0.5, DHJSpillingMemThresholdVal * 2.0);
	m_rgpcp[EcpHJHashTableInitCostFactor] = GPOS_NEW(mp)
		SCostParam(EcpHJHashTableInitCostFactor, DHJHashTableInitCostFactorVal,
				   DHJHashTableInitCostFactorVal * 0.5, DHJHashTableInitCostFactorVal * 1.5);
	m_rgpcp[EcpHJHashTableColumnCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpHJHashTableColumnCostUnit, DHJHashTableColumnCostUnitVal,
				   DHJHashTableColumnCostUnitVal * 0.5, DHJHashTableColumnCostUnitVal * 2.0);
	m_rgpcp[EcpHJHashTableWidthCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpHJHashTableWidthCostUnit, DHJHashTableWidthCostUnitVal,
		DHJHashTableWidthCostUnitVal * 0.5, DHJHashTableWidthCostUnitVal * 2.0);
	m_rgpcp[EcpHJHashingTupWidthCostUnit] = GPOS_NEW(mp)
		SCostParam(EcpHJHashingTupWidthCostUnit, DHJHashingTupWidthCostUnitVal,
				   DHJHashingTupWidthCostUnitVal * 0.5, DHJHashingTupWidthCostUnitVal * 2.0);
	m_rgpcp[EcpHJFeedingTupColumnSpillingCostUnit] =
		GPOS_NEW(mp) SCostParam(EcpHJFeedingTupColumnSpillingCostUnit,
								DHJFeedingTupColumnSpillingCostUnitVal,
								DHJFeedingTupColumnSpillingCostUnitVal * 0.5, 
								DHJFeedingTupColumnSpillingCostUnitVal * 2.0);
	m_rgpcp[EcpHJFeedingTupWidthSpillingCostUnit] =
		GPOS_NEW(mp) SCostParam(EcpHJFeedingTupWidthSpillingCostUnit,
								DHJFeedingTupWidthSpillingCostUnitVal,
								DHJFeedingTupWidthSpillingCostUnitVal * 0.5, 
								DHJFeedingTupWidthSpillingCostUnitVal * 2.0);
	m_rgpcp[EcpHJHashingTupWidthSpillingCostUnit] =
		GPOS_NEW(mp) SCostParam(EcpHJHashingTupWidthSpillingCostUnit,
								DHJHashingTupWidthSpillingCostUnitVal,
								DHJHashingTupWidthSpillingCostUnitVal * 0.5, 
								DHJHashingTupWidthSpillingCostUnitVal * 2.0);
	m_rgpcp[EcpHashAggInputTupColumnCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpHashAggInputTupColumnCostUnit, DHashAggInputTupColumnCostUnitVal,
		DHashAggInputTupColumnCostUnitVal * 0.5, DHashAggInputTupColumnCostUnitVal * 2.0);
	m_rgpcp[EcpHashAggInputTupWidthCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpHashAggInputTupWidthCostUnit, DHashAggInputTupWidthCostUnitVal,
		DHashAggInputTupWidthCostUnitVal * 0.5, DHashAggInputTupWidthCostUnitVal * 2.0);
	m_rgpcp[EcpHashAggOutputTupWidthCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpHashAggOutputTupWidthCostUnit, DHashAggOutputTupWidthCostUnitVal,
		DHashAggOutputTupWidthCostUnitVal * 0.5, DHashAggOutputTupWidthCostUnitVal * 2.0);
	m_rgpcp[EcpSortTupWidthCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpSortTupWidthCostUnit, DSortTupWidthCostUnitVal,
		DSortTupWidthCostUnitVal * 0.5, DSortTupWidthCostUnitVal * 2.0);
	m_rgpcp[EcpTupDefaultProcCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpTupDefaultProcCostUnit, DTupDefaultProcCostUnitVal,
		DTupDefaultProcCostUnitVal * 0.5, DTupDefaultProcCostUnitVal * 2.0);
	m_rgpcp[EcpMaterializeCostUnit] = GPOS_NEW(mp) 
		SCostParam(EcpMaterializeCostUnit, DMaterializeCostUnitVal,
		DMaterializeCostUnitVal * 0.5, DMaterializeCostUnitVal * 2.0);
	m_rgpcp[EcpTupUpdateBandwith] = GPOS_NEW(mp) 
		SCostParam(EcpTupUpdateBandwith, DTupUpdateBandwidthVal,
		DTupUpdateBandwidthVal * 0.8, DTupUpdateBandwidthVal * 1.2);
	m_rgpcp[EcpNetBandwidth] = GPOS_NEW(mp)
		SCostParam(EcpNetBandwidth, DNetBandwidthVal, 
		           DNetBandwidthVal * 0.8, DNetBandwidthVal * 1.2);
	m_rgpcp[EcpSegments] = GPOS_NEW(mp) 
		SCostParam(EcpSegments, DSegmentsVal, 
		           DSegmentsVal * 0.5, DSegmentsVal * 2.0);
	m_rgpcp[EcpNLJFactor] = GPOS_NEW(mp) 
		SCostParam(EcpNLJFactor, DNLJFactorVal, 
		           DNLJFactorVal * 0.8, DNLJFactorVal * 1.2);
	m_rgpcp[EcpHJFactor] = GPOS_NEW(mp) 
		SCostParam(EcpHJFactor, DHJFactorVal, 
		           DHJFactorVal * 0.5, DHJFactorVal * 1.5);
	m_rgpcp[EcpHashFactor] =
		GPOS_NEW(mp) SCostParam(EcpHashFactor, DHashFactorVal,
								DHashFactorVal * 0.5, DHashFactorVal * 1.5);
	m_rgpcp[EcpDefaultCost] =
		GPOS_NEW(mp) SCostParam(EcpDefaultCost, DDefaultCostVal,
								DDefaultCostVal * 0.5, DDefaultCostVal * 2.0);
	m_rgpcp[EcpIndexJoinAllowedRiskThreshold] = GPOS_NEW(mp)
		SCostParam(EcpIndexJoinAllowedRiskThreshold,
				   DIndexJoinAllowedRiskThreshold, 0, gpos::ulong_max);
	m_rgpcp[EcpBitmapIOCostLargeNDV] = GPOS_NEW(mp) 
		SCostParam(EcpBitmapIOCostLargeNDV, DBitmapIOCostLargeNDV,
		DBitmapIOCostLargeNDV * 0.5, DBitmapIOCostLargeNDV * 2.0);
	m_rgpcp[EcpBitmapIOCostSmallNDV] = GPOS_NEW(mp) 
		SCostParam(EcpBitmapIOCostSmallNDV, DBitmapIOCostSmallNDV,
		DBitmapIOCostSmallNDV * 0.5, DBitmapIOCostSmallNDV * 2.0);
	m_rgpcp[EcpBitmapPageCostLargeNDV] = GPOS_NEW(mp) 
		SCostParam(EcpBitmapPageCostLargeNDV, DBitmapPageCostLargeNDV,
		DBitmapPageCostLargeNDV * 0.5, DBitmapPageCostLargeNDV * 2.0);
	m_rgpcp[EcpBitmapPageCostSmallNDV] = GPOS_NEW(mp) 
		SCostParam(EcpBitmapPageCostSmallNDV, DBitmapPageCostSmallNDV,
		DBitmapPageCostSmallNDV * 0.5, DBitmapPageCostSmallNDV * 2.0);
	m_rgpcp[EcpBitmapPageCost] =
		GPOS_NEW(mp) SCostParam(EcpBitmapPageCost, DBitmapPageCost,
								DBitmapPageCost * 0.5, DBitmapPageCost * 2.0);
	m_rgpcp[EcpBitmapNDVThreshold] = GPOS_NEW(mp)
		SCostParam(EcpBitmapNDVThreshold, DBitmapNDVThreshold,
				   DBitmapNDVThreshold * 0.5, DBitmapNDVThreshold * 2.0);
	m_rgpcp[EcpBitmapScanRebindCost] = GPOS_NEW(mp)
		SCostParam(EcpBitmapScanRebindCost, DBitmapScanRebindCost,
				   DBitmapScanRebindCost * 0.5, DBitmapScanRebindCost * 2.0);
	m_rgpcp[EcpPenalizeHJSkewUpperLimit] = GPOS_NEW(mp) 
		SCostParam(EcpPenalizeHJSkewUpperLimit, DPenalizeHJSkewUpperLimit,
		DPenalizeHJSkewUpperLimit * 0.5, DPenalizeHJSkewUpperLimit * 2.0);
}


CGPUCostModelParams::~CGPUCostModelParams()
{
	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		GPOS_DELETE(m_rgpcp[ul]);
		m_rgpcp[ul] = NULL;
	}
}


CGPUCostModelParams::SCostParam *
CGPUCostModelParams::PcpLookup(ULONG id) const
{
	ECostParam ecp = (ECostParam) id;
	GPOS_ASSERT(EcpSentinel > ecp);

	return m_rgpcp[ecp];
}


CGPUCostModelParams::SCostParam *
CGPUCostModelParams::PcpLookup(const CHAR *szName) const
{
	GPOS_ASSERT(NULL != szName);

	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		if (0 == clib::Strcmp(szName, rgszCostParamNames[ul]))
		{
			return PcpLookup((ECostParam) ul);
		}
	}

	return NULL;
}


void
CGPUCostModelParams::SetParam(ULONG id, CDouble dVal, CDouble dLowerBound,
							   CDouble dUpperBound)
{
	ECostParam ecp = (ECostParam) id;
	GPOS_ASSERT(EcpSentinel > ecp);

	GPOS_DELETE(m_rgpcp[ecp]);
	m_rgpcp[ecp] = NULL;
	m_rgpcp[ecp] =
		GPOS_NEW(m_mp) SCostParam(ecp, dVal, dLowerBound, dUpperBound);
}


void
CGPUCostModelParams::SetParam(const CHAR *szName, CDouble dVal,
							   CDouble dLowerBound, CDouble dUpperBound)
{
	GPOS_ASSERT(NULL != szName);

	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		if (0 == clib::Strcmp(szName, rgszCostParamNames[ul]))
		{
			GPOS_DELETE(m_rgpcp[ul]);
			m_rgpcp[ul] = NULL;
			m_rgpcp[ul] =
				GPOS_NEW(m_mp) SCostParam(ul, dVal, dLowerBound, dUpperBound);

			return;
		}
	}
}



IOstream &
CGPUCostModelParams::OsPrint(IOstream &os) const
{
	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		SCostParam *pcp = PcpLookup((ECostParam) ul);
		os << rgszCostParamNames[ul] << " : " << pcp->Get() << "  ["
		   << pcp->GetLowerBoundVal() << "," << pcp->GetUpperBoundVal() << "]"
		   << std::endl;
	}
	return os;
}

BOOL
CGPUCostModelParams::Equals(ICostModelParams *pcm) const
{
	CGPUCostModelParams *pcmgOther = dynamic_cast<CGPUCostModelParams *>(pcm);
	if (NULL == pcmgOther)
		return false;

	for (ULONG ul = 0U; ul < GPOS_ARRAY_SIZE(m_rgpcp); ul++)
	{
		if (!m_rgpcp[ul]->Equals(pcmgOther->m_rgpcp[ul]))
			return false;
	}

	return true;
}

const CHAR *
CGPUCostModelParams::SzNameLookup(ULONG id) const
{
	ECostParam ecp = (ECostParam) id;
	GPOS_ASSERT(EcpSentinel > ecp);
	return rgszCostParamNames[ecp];
}

// EOF
