#pragma once


namespace AFOpKeys { 
    inline constexpr auto CLogicalPartialSelect = "CLogicalPartialSelect";
    inline constexpr auto CPhysicalGPUJoin = "CPhysicalGPUJoin"; 
    inline constexpr auto CPhysicalGPUPartialSelect = "CPhysicalGPUPartialSelect"; 
    inline constexpr auto CPhysicalGPUFullSelect = "CPhysicalGPUFullSelect";
    inline constexpr auto CPhysicalGPUProject = "CPhysicalGPUProject";
}

namespace AFTransformKeys {
    inline constexpr auto CXformGbSelect2SelectGather = "CXformGbSelect2SelectGather";
    inline constexpr auto CXformGbLogicalSelectGather2Select = "CXformGbLogicalSelectGather2Select";
    inline constexpr auto CXformLogicalJoin2PhysicalGPUJoin = "CXformLogicalJoin2PhysicalGPUJoin";
    inline constexpr auto CXformGbPostFilterLeft = "CXformGbPostFilterLeft";
    inline constexpr auto CXformGbPostFilterRight = "CXformGbPostFilterRight";
    inline constexpr auto CXformGbFSelectConjunct2NestedFSelect = "CXformGbFSelectConjunct2NestedFSelect";
    inline constexpr auto CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect = "CXformGbPartialSelect2GPUPhysicalSelect";
    inline constexpr auto CXformGbLogicalSelect2PhysicalGPUSelect = "CXformGbLogicalSelect2PhysicalGPUSelect";
    inline constexpr auto CXformGbSwapGPUSelect = "CXformGbSwapGPUSelect";
}

namespace AFEngineKeys {
    inline constexpr auto ArrayFire = "ArrayFire";
    inline constexpr auto Velox = "Velox";
}