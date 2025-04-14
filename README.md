# Multi-Kernel BOSS Benchmarks

## Pre-requisites:
* NVIDIA driver
* Docker

## Preparation on the host for CUDA support
```
follow instructions to install docker https://docs.docker.com/engine/install/
follow instructions to install nvidia-container-toolkit https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
sudo apt update
sudo apt install nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```

## Run the container image
```
docker run -ti --runtime=nvidia --gpus all --workdir / ghcr.io/hmohrdaurat/buildenvironment/velox-docker-image:latest
```

## Download and install CUDA 12
> only the CUDA Toolkit must be installed, not the driver
```
./cuda_12.0.1_525.85.12_linux.run
PATH=$PATH:/usr/local/cuda-12.0/bin
ldconfig
```

## Get the benchmark code
```
git clone https://github.com/lsds/MultiKernelBOSS.git
cd MultiKernelBOSS
```

## Generate TPC-H dataset
```
chmod u+x generate_tpch_data.sh
./generate_tpch_data.sh
```

* Generate TPC-H for SF 1 only:
```
./generate_tpch_data.sh 1 1
```

## Compile the code
```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX:PATH=.. -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_BUILD_TYPE=Release -DVELOX_DIRS=/usr/local/velox -B. ..
cd ..
cmake --build build --target install
```

## Run the benchmarks
```
cd bin
```

* Velox with SF 1:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --disable-constraints --library libBOSSArrowStorage.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --disable-constraints --library libBOSSArrowStorage.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --disable-constraints --library libBOSSArrowStorage.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --disable-constraints --library libBOSSArrowStorage.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --disable-constraints --library libBOSSArrowStorage.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/BOSS/1000MB
```

* MonetDB and DuckDB with SF 1:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --fixed-point-numeric-type --benchmark_filter=TPC-H_Q[0-9]+/MonetDB/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --fixed-point-numeric-type --benchmark_filter=TPC-H_Q[0-9]+/DuckDB/1000MB
```

* BOSS (CPU) with SF 1:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineCPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineCPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineCPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineCPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineCPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/BOSS/1000MB
```

* BOSS (GPU) with SF 1:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/BOSS/1000MB
```

* ArrayFire partial evaluation with SF 1:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --benchmark_filter=TPC-H_Q1V_POST-FILTER/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/BOSS/1000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --benchmark_filter=TPC-H_Q18/BOSS/1000MB
```

* BOSS (GPU) with SF 100:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 12000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 8500 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-1JOIN/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 11000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 12000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 12000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/100000MB
```

* BOSS (GPU) with various maximum allowed GPU memory on TPC-H Q6 with SF 100:
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 0 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 1000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 2000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 3000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 4000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 5000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 6000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 7000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 8000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 9000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 10000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --max-gpu-memory-cache 11000 --all-strings-as-integers --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6V_NESTED-SELECT2/100000MB
```

## Ablation Study

* BOSS (GPU) with SF 10 (with data copy from Storage to ArrayFire):
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/10000MB
```

* BOSS (GPU) with SF 10 (with data copy from ArrayFire To Velox):
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/10000MB
```

* BOSS (GPU) with SF 10 (with all data copies):
```
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q1/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q3V_POST-FILTER-2JOINS/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q6/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q9V_POST-FILTER-AND-PRIORITY/10000MB
LD_LIBRARY_PATH=../lib ./Benchmarks --default-storage-block-size 268435456 --all-strings-as-integers --benchmark-data-copy-in --benchmark-data-copy-out --library libBOSSArrowStorage.so --library libBOSSArrayFireEngineGPU.so --library libBOSSVeloxEngine.so --benchmark_filter=TPC-H_Q18/10000MB
```

* BOSS without fast path:

requires to re-compile the code with the line 21 in BOSS/Source/Expression.hpp modified as:
```
#define ABLATION_NO_FAST_PATH
```
