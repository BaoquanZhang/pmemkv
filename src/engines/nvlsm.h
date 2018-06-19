/*
 * Copyright 2017-2018, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once
/* utility */
#include <pthread.h>
#include <queue>
#include <map>
#include <unistd.h>
#include <algorithm>
/* thread pool headers */
#include "nvlsm/threadpool.h"
/* pmdk headers */
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/transaction.hpp>

#include "../pmemkv.h"

using namespace std;
/* pmdk namespace */
using namespace pmem::obj;

namespace pmemkv {
namespace nvlsm {

#define RUN_SIZE 4096
#define LAYER_DEPTH 4
#define COM_RATIO 4
#define PERSIST_POOL_SIZE 1
#define COMPACT_POOL_SIZE 1
const string ENGINE = "nvlsm";                         // engine identifier
class Run;
class PRun;
class MemTable;
class NVLsm;

/* PMEM structures */
struct LSM_Root {                 // persistent root object
    persistent_ptr<Run> head;   // head of the vector of levels
};

/* KVRange : range for runs */
struct KVRange {
    string start_key;
    string end_key;
    KVRange() {}
    KVRange(string start, string end) {
        start_key = start;
        end_key = end;
    }

    bool const operator==(const KVRange &kvrange) const {
        return  start_key == kvrange.start_key 
                && end_key == kvrange.end_key;
    }

    bool const operator<(const KVRange &kvrange) const {
        return  start_key < kvrange.start_key 
                || (start_key == kvrange.start_key && end_key < kvrange.end_key);
    }

    bool const operator>(const KVRange &kvrange) const {
        return  start_key > kvrange.start_key 
                || (start_key == kvrange.start_key && end_key > kvrange.end_key);
    }
};

/* comparator for key ranges */
/*
struct RangeComparator {
    bool operator()(const KVRange &range1, const KVRange &range2) const {
        return range1.start_key <= range2.start_key;
    }
};*/

/* KVPair : key-value pair in DRAM */
class KVPair {
    public:
        string key;
        string val;
        KVPair();
        KVPair(string init_key, string init_val);
        ~KVPair();
};

/* KVPair : persistent key-value pair on NVM */
class PKVPair {
    public:
        persistent_ptr<string> key;
        persistent_ptr<string> val;
        PKVPair();
        PKVPair(string init_key, string init_val);
        ~PKVPair();
};

/* comparator for kv pairs */
struct PairComparator {
    bool operator()(const KVPair &pair1, const KVPair &pair2) {
        return pair1.key <= pair2.key;
    }
};


/* MemTable: the write buffer in DRAM */
class MemTable {
    private:
        Run * buffer;        // write buffer container
        pthread_rwlock_t rwlock;        // rw lock for write/read
        queue<Run *> persist_queue; // persist queue for memtable
        size_t buf_size;
    public:
        MemTable(int size); // buffer size
        ~MemTable();
        bool append(KVPair &kv_pair);
        void push_queue();
        Run * pop_queue();
        string search(string key);
};

/* Metadata table for sorted runs */
class MetaTable {
    private:
        pthread_rwlock_t rwlock;
        map<KVRange, persistent_ptr<PRun>> ranges;
        vector< persistent_ptr<PRun> > old_run;
        size_t next_compact;  // index for the run of the last compaction
    public:
        MetaTable();
        ~MetaTable();
        size_t getSize(); // get the size of ranges
        void add(vector<persistent_ptr<PRun>> runs);
        void add(persistent_ptr<PRun> run);
        void del(persistent_ptr<PRun> runs);
        void del(vector<persistent_ptr<PRun>> runs);
        bool search(string &key, string &value);
        void del_data();
        void display();
        persistent_ptr<PRun> getCompact(); // get the run for compaction
        void searchRun(KVRange &range, vector<persistent_ptr<PRun>> &runs);
};

/* Run: container for storing kv_pairs on DRAM*/
class Run {
    private:
        pthread_rwlock_t rwlock;
        KVPair array[RUN_SIZE];
        size_t size;
        KVRange range;
    public:
        Run();
        ~Run();
        size_t getSize();
        KVRange getRange();
        KVPair* getArray();
        void append(KVPair &kv_pair);
        bool search(string &req_key, string &req_val);
        void sort_array();
};

/* Run: container for storing kv_pairs on DRAM*/
class PRun {
    private:
        pthread_rwlock_t rwlock;
        persistent_ptr<PKVPair[]> array;
        size_t size;
        KVRange range;
    public:
        PRun();
        ~PRun();
        size_t getSize();
        KVRange getRange();
        persistent_ptr<PKVPair[]> getArray();
        void write(Run &run);
        void append(PKVPair &kv_pair);
        bool search(string &req_key, string &req_val);
};

/* the basic unit of a compaction */
class CompactionUnit {
    public:
        size_t index;   // index for the current component   
        persistent_ptr<PRun> up_run;
        vector< persistent_ptr<PRun> > low_runs;
        vector< persistent_ptr<PRun> > new_runs;
        CompactionUnit();
        ~CompactionUnit();
        void display();
};

class NVLsm : public KVEngine {
    private:
        ThreadPool * persist_pool;
        ThreadPool * compact_pool;
    public:
        size_t run_size;                                     // the number of kv pairs
        size_t layer_depth;
        size_t com_ratio;
        NVLsm(const string& path, const size_t size);        // default constructor
        ~NVLsm();                                          // default destructor
        // internal structure
        MemTable * mem_table;
        vector<MetaTable> meta_table;
        // utility
        CompactionUnit * plan_compaction(size_t index);
        void merge_sort(CompactionUnit * unit);
        void displayMeta();
        // public interface
        string Engine() final { return ENGINE; }               // engine identifier
        KVStatus Get(int32_t limit,                            // copy value to fixed-size buffer
                     int32_t keybytes,
                     int32_t* valuebytes,
                     const char* key,
                    char* value) final;
        KVStatus Get(const string& key,                        // append value to std::string
                     string* value) final;
        KVStatus Put(const string& key,                        // copy value from std::string
                     const string& value) final;
        KVStatus Remove(const string& key) final;              // remove value for key
};

} // namespace nvlsm
} // namespace pmemkv
