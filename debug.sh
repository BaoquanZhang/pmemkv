PMEM_IS_PMEM_FORCE=1 ./bin/pmemkv_bench --histogram=0 --db=/mnt/pmem/nvlsm/pmemkv.pool --engine=nvlsm --num=1000000 --value_size=128 --db_size_in_gb=1 --benchmarks=fillrandom

