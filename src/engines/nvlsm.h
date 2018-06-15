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
#include <pthread.h>
#include <queue>
#include <set>
#include <unistd.h>
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
const string ENGINE = "nvlsm";                         // engine identifier
class KVPair;
class Run;
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
};

/* comparator for key range */
struct RangeComparator {
    bool operator()(const pair< KVRange, persistent_ptr<Run> > &pair1, 
            const pair< KVRange, persistent_ptr<Run> > &pair2) const {
        return pair1.first.start_key <= pair2.first.start_key;
    }
};

/* KVPair : key-value pair */
class KVPair {
    public:
        string key;
        string val;
        KVPair();
        KVPair(string init_key, string init_val);
        ~KVPair();
};

/* MemTable: the write buffer in DRAM */
class MemTable {
    private:
        Run * buffer;        // write buffer container
        pthread_rwlock_t rwlock;        // rw lock for write/read
        queue<Run *> persist_queue; // persist queue for memtable
        int buf_size;
    public:
        MemTable(int size);
        ~MemTable();
        void append(KVPair &kv_pair);
        string search(string key);
};

/* Metadata table for sorted runs */
class MetaTable {
    private:
        pthread_rwlock_t rwlock;
        set< pair<KVRange, persistent_ptr<Run> >, RangeComparator > ranges;
        vector< persistent_ptr<Run> > old_run;
    public:
        MetaTable();
        ~MetaTable();
        void add(vector<persistent_ptr<Run>> runs);
        void del(vector<persistent_ptr<Run>> runs);
        bool search(string &key, string &value);
        void del_data();
};

/* Run: container for storing kv_pairs on NVM */
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
        void write(vector<KVPair> &kv_pairs, size_t len);
        void append(KVPair &kv_pair);
        bool search(string &req_key, string &req_val);
};

class NVLsm : public KVEngine {
  public:
    NVLsm(const string& path, const size_t size);        // default constructor
    ~NVLsm();                                          // default destructor
    size_t run_size;                                     // the number of kv pairs
    size_t layer_depth;
    size_t com_ratio;
    MemTable * mem_table;

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
