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
#include <cstdlib>
#include <ctime>
#include <queue>
#include <map>
#include <stack>
#include <unistd.h>
#include <algorithm>
#include <ctime>
#include <cmath>
#include <string>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <list>
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
#define COM_RATIO 10
#define C0_COUNT 4
#define PERSIST_POOL_SIZE 1
#define COMPACT_POOL_SIZE 1
#define SLOW_DOWN_US 2

const string ENGINE = "nvlsm2"; // engine identifier
class Run;
class PRun;
class PSegment;
class MemTable;
class NVLsm2;
struct RunIndex;
struct KVRange;

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
    bool valid = true;
    size_t next_key = -1; // offset of the bottom key, default -1
    persistent_ptr<PRun> next_run = NULL;
};
class PRun {
    public:
        PRun();
        ~PRun();
        KeyEntry key_entry[RUN_SIZE];
        int id; // random id
        char vals[VAL_SIZE * RUN_SIZE];
        size_t size;
        int iter;
        int refered;
        void seek(char* key);
        bool next(RunIndex& runIndex);
        char* get_key(int index);
        void get_range(KVRange& range);
        void display();
        int find_key(const string& key, string& val, int left, int right, int& mid);
};
/* segment iterator */
struct RunIndex {
    persistent_ptr<PRun> pRun;
    int index;
    RunIndex(persistent_ptr<PRun> cur_run, int cur_index) {
        pRun = cur_run;
        index = cur_index;
    };
    RunIndex() {
        pRun = NULL;
        index = 0;
    };
    void display() {
        cout << pRun->key_entry[index].key << endl;
    }
    inline char* const get_key() const {
        return pRun->key_entry[index].key;
    }
    bool const operator == (const RunIndex& runIndex) const {
        return strncmp(get_key(), runIndex.get_key(), KEY_SIZE) == 0;
    };
    bool const operator < (const RunIndex& runIndex) const {
        return strncmp(get_key(), runIndex.get_key(), KEY_SIZE) < 0;
    };
    bool const operator>(const RunIndex runIndex) const {
        return strncmp(get_key(), runIndex.get_key(), KEY_SIZE) > 0;
    };
};
/* persistent segment in a PRun */
class PSegment {
    public:
        /* variable */
        list<persistent_ptr<PRun>> pRuns; // included runs, the front() is the top
        set<persistent_ptr<PRun>> runSet; // all of the runs in a segment
        size_t start;
        size_t end;
        int depth;
        int iter;
        map<RunIndex, int> search_stack;
        persistent_ptr<PRun> get_run(); // return the top run
        /* utilities */
        bool isInclude(persistent_ptr<PRun> run);
        void addRuns(list<persistent_ptr<PRun>> runs);
        void addRun(persistent_ptr<PRun> run);
        void seek(char* key);
        bool next(RunIndex& runIndex);
        bool search(const string& key, string& value);
        char* get_key(int index);
        void get_localRange(KVRange& kvRange);
        void display();
        PSegment(PSegment* old_seg, persistent_ptr<PRun> p_run, size_t start_i, size_t end_i);
        PSegment(vector<PSegment*> old_segs, persistent_ptr<PRun> p_run, size_t start_i, size_t end_i);
        ~PSegment();
};

/* Metadata table for sorted runs */
class MetaTable {
    public:
        int id;
        pthread_rwlock_t rwlock;
        size_t next_compact;  // index for the run of the last compaction
        map<KVRange, PSegment*> segRanges;
        MetaTable();
        MetaTable(int comp_index);
        ~MetaTable();
        size_t getSize(); // get the size of ranges
        void rdlock();
        void wrlock();
        void unlock();
        /* functions for segment ops in multiple layers */
        PSegment* getMerge(int id);
        void merge(PSegment* seg, vector<persistent_ptr<PRun>>& runs);
        void add(vector<PSegment*> segs);
        void add(PSegment* seg);
        void del(vector<PSegment*> segs);
        void del(PSegment* seg); 
        bool search(const string& key, string& val);
        bool seq_search(const string& key, string& val);
        void search(const string& key, vector<PSegment*>& segs);
        void search(KVRange& kvRange, vector<PSegment*>& segs);
        void build_layer(persistent_ptr<PRun> run);
        void do_build(vector<PSegment*>& overlap_segs, persistent_ptr<PRun> run);
        void build_link(persistent_ptr<PRun> src, int src_i, persistent_ptr<PRun> des, int des_i);
        void display();
        void copy_kv(persistent_ptr<PRun> des_run, int des_i, 
                        persistent_ptr<PRun> src_run, int src_i);
        PSegment* create_pseg(vector<PSegment*> old_segs, persistent_ptr<PRun> run, 
                int start, int end, int depth);
        PSegment* create_pseg(PSegment* old_seg, persistent_ptr<PRun> run, 
                int start, int end, int depth);
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
        void display();
        void compact(int comp, vector<persistent_ptr<PRun>>& runs);
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
