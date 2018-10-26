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
#include <ctime>
#include <cmath>
#include <string>
/* thread pool headers */
#include "nvlsm/threadpool.h"
/* pmdk headers */
#include <libpmemlog.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/shared_mutex.hpp>
#include <libpmemobj++/mutex.hpp>

#include "../pmemkv.h"

using namespace std;
/* pmdk namespace */
using namespace pmem::obj;

namespace pmemkv {
namespace nvlsm2 {

#define RUN_SIZE 14400
#define KEY_SIZE 16
#define VAL_SIZE 128
#define MAX_DEPTH 4
#define COM_RATIO 4
#define PERSIST_POOL_SIZE 1
#define COMPACT_POOL_SIZE 1
#define SLOW_DOWN_US 2

const string ENGINE = "nvlsm2"; // engine identifier
class Run;
class PRun;
class PSegment;
class MemTable;
class NVLsm2;

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

    void display() const {
        cout << "<" << start_key << "," << end_key << "> ";;
    }
};

/* log entry for kvlog and compaction log */
class Log {
    private:
        char ops[100];
    public:
        void append(string str);
};

/* Run: container for storing kv_pairs on DRAM*/
class Run {
    public:
        pthread_rwlock_t rwlock;
        map<string, string> kv;
        size_t size;
        size_t iter;
        KVRange range;
        Run();
        ~Run();
        void append(const string &key, const string& val);
        bool search(string &req_key, string &req_val);
};

/* MemTable: the write buffer in DRAM */
class MemTable {
    private:
        Run * buffer;        // write buffer container
        pthread_rwlock_t rwlock;        // rw lock for write/read
        queue<Run *> persist_queue; // persist queue for memtable
        size_t buf_size;
        persistent_ptr<Log> kvlog;
    public:
        MemTable(int size); // buffer size
        ~MemTable();
        size_t getSize(); // get queue size
        bool append(const string &key, const string &val);
        void push_queue();
        Run * pop_queue();
        bool search(const string &key, string &val);
};

/* PRun: container for storing kv_pairs on pmem*/
struct KeyEntry {
    char key[KEY_SIZE];
    size_t val_len;
    char* p_val;
};
class PRun {
    public:
        PRun();
        ~PRun();
        //persistent_ptr<KeyEntry[RUN_SIZE]> key_entry;
        //persistent_ptr<char[VAL_SIZE * RUN_SIZE]> vals;
        KeyEntry key_entry[RUN_SIZE];
        char vals[VAL_SIZE * RUN_SIZE];
        size_t size;
        size_t seg_count;
        void get_range(KVRange& range);
};

/* persistent segment in a PRun */
class PSegment {
    public:
        persistent_ptr<PRun> pRun;
        KVRange allRange;
        size_t start;
        size_t end;
        size_t search(string key, string& value);
        persistent_ptr<PSegment> next_seg;
        void get_localRange(KVRange& kvRange);
        PSegment(persistent_ptr<PRun> p_run, size_t start_i, size_t end_i);
        ~PSegment();
};

/* Metadata table for sorted runs */
class MetaTable {
    public:
        pthread_rwlock_t rwlock;
        size_t next_compact;  // index for the run of the last compaction
        map<KVRange, persistent_ptr<PRun>> ranges;
        map<KVRange, persistent_ptr<PSegment>> segRanges;
        MetaTable();
        ~MetaTable();
        size_t getSize(); // get the size of ranges
        /* functions for segment ops in multiple layers */
        void add(vector<persistent_ptr<PSegment>> segs);
        void add(persistent_ptr<PSegment> seg);
        void del(vector<persistent_ptr<PSegment>> segs);
        void del(persistent_ptr<PSegment> seg);
        bool search(const string& key, string& val);
        void search(KVRange& kvRange, vector<persistent_ptr<PSegment>>& segs);
        void build_layer(persistent_ptr<PSegment> seg);
};

class NVLsm2 : public KVEngine {
    private:
        ThreadPool * persist_pool;
        ThreadPool * compact_pool;
    public:
        size_t run_size;                                     // the number of kv pairs
        size_t layer_depth;
        size_t com_ratio;
        NVLsm2(const string& path, const size_t size);        // default constructor
        ~NVLsm2();                                          // default destructor
        // internal structure
        MemTable * mem_table;
        vector<MetaTable> meta_table;
        persistent_ptr<Log> meta_log; // log for meta table
        // utility
        void displayMeta();
        void copy_kv(persistent_ptr<PRun> des_run, int des_i, persistent_ptr<PRun> src_run, int src_i);
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

} // namespace nvlsm2
} // namespace pmemkv
