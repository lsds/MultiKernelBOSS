#~/bin/sh

index=(         1               2               3       )
MB=(            1000            10000           100000  )
scale=(         1.0             10.0            100.0   )

echo "compile dbgen..."
cd dbgen
make
if [[ $? -ne 0 ]]; then
   echo "check the compilation options in dbgen/Makefile and try again." >&2
      exit 1
fi

rm -rf *.tbl
for i in $(seq ${1-1} ${2-3}); do
   echo "[$i / ${2-3}] generate TPC-H SF ${scale[$i - 1]}..."
   mkdir -p ../data/tpch_${MB[$i - 1]}MB
   ./dbgen -s ${scale[$i - 1]}
   mv -f *.tbl ../data/tpch_${MB[$i - 1]}MB/
done