PMEM_IS_PMEM_FORCE=1 ./bin/pmemkv_bench --histogram=1 --db=/mnt/pmem/nvlsm/pmemkv.pool --engine=nvlsm --value_size=128 --db_size_in_gb=10 --benchmarks=fillrandom,readrandom,readseq --num=100000
