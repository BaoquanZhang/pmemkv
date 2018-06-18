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

#include <iostream>
#include "nvlsm.h"

#define DO_LOG 0
#define LOG(msg) if (DO_LOG) std::cout << "[nvlsm] " << msg << "\n"

namespace pmemkv {
namespace nvlsm {

pool<LSM_Root> pmpool;
size_t pmsize;
/* #####################static functions for multiple threads ####################### */
/* persist: persist a mem_table to c0
 * v_mem_table: an instance of */
static void persist(void * v_nvlsm) {
    cout << "persistted a mem_table!" << endl;
    NVLsm * nvlsm = (NVLsm *) v_nvlsm;
    // get the queue head from mem_table
    auto mem_table = nvlsm->mem_table;
    auto run = mem_table->pop_queue();
    // get the targeting meta_table[0] 
    if (nvlsm->meta_table.size() == 0) {
        MetaTable new_tbl;
        nvlsm->meta_table.push_back(new_tbl);
    }
    auto meta_table = &(nvlsm->meta_table[0]);
    // allocate space from NVM and copy data from mem_table
    persistent_ptr<PRun> persist_run;
    make_persistent_atomic<PRun>(pmpool, persist_run);
    persist_run->write(*run);
    meta_table->add(persist_run);
    delete run;
    //nvlsm->meta_table[0].display();
}

/* ######################### CompactionUnit ############################# */
void CompactionUnit::display() {
    auto start_key = up_run->getRange().start_key;
    auto end_key = up_run->getRange().end_key;
    cout << "up run:" << "<" << start_key << "," << end_key << ">" << endl;
    cout << "low runs: ";
    for (auto it : low_runs) {
        start_key = it->getRange().start_key;
        end_key = it->getRange().end_key;
        cout << "<" << start_key << "," << end_key << "> ";
    }
    cout << endl;
    return;
}

/* ######################## Implementations for NVLsm #################### */
NVLsm::NVLsm(const string& path, const size_t size) {
    run_size = RUN_SIZE;
    layer_depth = LAYER_DEPTH;
    com_ratio = COM_RATIO;
    // create a mem_table
    mem_table = new MemTable(run_size);
    // reserve space for the meta_table of 10 components 
    meta_table.emplace_back();
    // Create/Open pmem pool
    if (access(path.c_str(), F_OK) != 0) {
        LOG("Creating filesystem pool, path=" << path << ", size=" << to_string(size));
        pmpool = pool<LSM_Root>::create(path.c_str(), LAYOUT, size, S_IRWXU);
        pmsize = size;
    } else {
        LOG("Opening filesystem pool, path=" << path);
        pmpool = pool<LSM_Root>::open(path.c_str(), LAYOUT);
        struct stat st;
        stat(path.c_str(), &st);
        pmsize = (size_t) st.st_size;
    }
    LOG("Create/open pool done");
    // create the thread pool for pesisting mem_tables
    persist_pool = new ThreadPool(PERSIST_POOL_SIZE);
    if (persist_pool->initialize_threadpool() == -1) {
        cout << "Fail to initialize the thread pool!" << endl;
        exit(-1);
    }
    LOG("Opened ok");
}

NVLsm::~NVLsm() {
    persist_pool->destroy_threadpool();
    delete mem_table;
    delete persist_pool;
    LOG("Closing persistent pool");
    pmpool.close();
    LOG("Closed ok");
}

KVStatus NVLsm::Get(const int32_t limit, const int32_t keybytes, int32_t* valuebytes,
                        const char* key, char* value) {
    LOG("Get for key=" << key);
    return NOT_FOUND;
}

KVStatus NVLsm::Get(const string& key, string* value) {
    LOG("Get for key=" << key.c_str());
    return NOT_FOUND;
}

KVStatus NVLsm::Put(const string& key, const string& value) {
    LOG("Put key=" << key.c_str() << ", value.size=" << to_string(value.size()));
    KVPair kv_pair(key, value);
    if (mem_table->append(kv_pair)) {
        // write buffer is filled up
        mem_table->push_queue();
        Task * persist_task = new Task(&persist, (void *) this);
        persist_pool->add_task(persist_task);
    }

    if (meta_table[0].getSize() >= com_ratio) {
        auto unit = plan_compaction(0);
        unit->display();
    }
    return OK;
}

KVStatus NVLsm::Remove(const string& key) {
    LOG("Remove key=" << key.c_str());
    return OK;
}

/* plan a compaction */
CompactionUnit * NVLsm::plan_compaction(size_t index) {
    CompactionUnit * unit = new CompactionUnit();
    unit->index = index;
    unit->up_run = meta_table[index].getCompact(); 
    if (meta_table.size() > index && meta_table[index + 1].getSize() > 0) {
        auto range = unit->up_run->getRange();
        meta_table[index + 1].searchRun(range, unit->low_runs);
    }
    return unit;
}

/* ############## Implementation for KVPair ###################*/
KVPair::KVPair() {}

KVPair::KVPair(string init_key, string init_val) 
    : key(init_key), val(init_val) {}

KVPair::~KVPair() {}

/* ############## Implementaions for MemTable #################*/
MemTable::MemTable(int size) 
    : buf_size(size) {
    buffer = new Run();
    pthread_rwlock_init(&rwlock, NULL);
}

MemTable::~MemTable() {
    pthread_rwlock_destroy(&rwlock);
    delete buffer;
}

/* push_que: push the write buffer to the persist queue
 *           and allocate a new buffer
 * */
void MemTable::push_queue() {
    buffer->sort_array();
    persist_queue.push(buffer);
    //cout << "persist queue size: " << persist_queue.size() << endl;
    buffer = new Run();
    return;
}

/* pop_que: pop an element from the queue head */
Run * MemTable::pop_queue() {
    auto head = persist_queue.front();
    persist_queue.pop();
    return head;
}

/* append kv pair to write buffer 
 * return: true - the write buffer is filled up
 *         false - the write buffer still has space 
 * */
bool MemTable::append(KVPair &kv_pair) {
    buffer->append(kv_pair);
    if (buffer->getSize() >= RUN_SIZE) {
        return true;
    }
    return false;
}

/* ################### MetaTable ##################################### */
MetaTable::MetaTable() : next_compact(0) {
    ranges.clear();
    pthread_rwlock_init(&rwlock, NULL);
}

MetaTable::~MetaTable() {
    pthread_rwlock_destroy(&rwlock);
}

/* getSize: get the size of ranges */
size_t MetaTable::getSize() {
    return ranges.size();
}

/* display: show the current elements */
void MetaTable::display() {
    for (auto it : ranges) {
        cout << "<" << it.first.start_key << "," << it.first.end_key << ">";
        cout << "(" << it.second->getSize() << ") ";
    }
    cout << endl;
    return;
}

/* add: add new runs into meta_table */
void MetaTable::add(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    if (runs.size() == 0) 
        return;
    for (auto it : runs) {
        ranges.emplace(it->getRange(), it);
    }
    pthread_rwlock_unlock(&rwlock);
    return;
}

void MetaTable::add(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    ranges.emplace(run->getRange(), run);
    pthread_rwlock_unlock(&rwlock);
    return;
}
/* get a run for compaction */
persistent_ptr<PRun> MetaTable::getCompact() {
    auto it = ranges.begin();
    advance(it, next_compact);
    auto run = it->second;
    next_compact++;
    if (next_compact == ranges.size())
        next_compact = 0;
    return run;
}

/* searchRun: get all run within a kv range */
void MetaTable::searchRun(KVRange &kvrange, vector<persistent_ptr<PRun>> &runs) {
    if (ranges.empty())
        return;
    KVRange start_range(kvrange.start_key, kvrange.start_key);
    KVRange end_range(kvrange.end_key, kvrange.end_key);
    cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
    cout << "range size: " << ranges.size() << endl;
    auto it_low = ranges.lower_bound(start_range);
    auto it_high = ranges.upper_bound(end_range);
    cout << "mark" << endl;
    if (it_low != ranges.end()) {
        auto range = it_low->first;
        if (range.end_key >= kvrange.start_key)
            runs.push_back(it_low->second);
        it_low++;
        while (it_low != it_high) {
            runs.push_back(it_low->second);
            it_low++;
        }
    }
    return;
}

/* #################### Implementations of Run ############################### */
Run::Run() 
    : size(0) {
    pthread_rwlock_init(&rwlock, NULL);
}

Run::~Run() {
    pthread_rwlock_destroy(&rwlock);
}

KVRange Run::getRange() {
    return range;
}

size_t Run::getSize() {
    return size;
}

KVPair * Run::getArray() {
    return array;
}


/* search: binary search kv pairs in a run */
bool Run::search(string &req_key, string &req_val) {
    int start = 0;
    int end = size - 1;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        if (array[mid].key == req_key) {
            req_val = array[mid].val;
            return true;
        } else if (array[mid].key < req_key) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return false;
}

/* append: append a kv pair to run
 * this will only be called for write buffer 
 * */
void Run::append(KVPair & kv_pair) {
    pthread_rwlock_wrlock(&rwlock);
    // put kv_pair into buffer
    array[size].key = kv_pair.key;
    array[size].val = kv_pair.val;
    size++;
    // update range of buffer 
    if (range.start_key.empty() || range.start_key > kv_pair.key)
        range.start_key = kv_pair.key;
    if (range.end_key.empty() || range.end_key < kv_pair.key)
        range.end_key = kv_pair.key;
    pthread_rwlock_unlock(&rwlock);
}

/* sort_array: sort the kv array before it is persisted to NVM */
void Run::sort_array() {
    sort(array, array + size, PairComparator());
    return;
}

/* #################### PRun ############################### */
PRun::PRun() 
    : size(0) {
    pthread_rwlock_init(&rwlock, NULL);
    make_persistent_atomic<KVPair[]>(pmpool, array, RUN_SIZE);
}

PRun::~PRun() {
    pthread_rwlock_destroy(&rwlock);
}

KVRange PRun::getRange() {
    return range;
}

size_t PRun::getSize() {
    return size;
}

persistent_ptr<KVPair[]> PRun::getArray() {
    return array;
}

/* write: persist kv pairs to NVM */
void PRun::write(Run &run) {
    //cout << "persisting run to NVM" << endl;
    pthread_rwlock_wrlock(&rwlock);
    auto kv_pairs = run.getArray();
    int len = run.getSize();
    for (int i = 0; i < len; i++) {
        array[i].key = kv_pairs[i].key;
        array[i].val = kv_pairs[i].val;
    }
    size = len;
    range = run.getRange();
    pthread_rwlock_unlock(&rwlock);
}

/* search: binary search kv pairs in a run */
bool PRun::search(string &req_key, string &req_val) {
    int start = 0;
    int end = size - 1;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        if (array[mid].key == req_key) {
            req_val = array[mid].val;
            return true;
        } else if (array[mid].key < req_key) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return false;
}

} // namespace nvlsm
} // namespace pmemkv
