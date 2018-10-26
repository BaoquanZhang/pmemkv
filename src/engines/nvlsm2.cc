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
#include "nvlsm2.h"

#define DO_LOG 0
#define LOG(msg) if (DO_LOG) std::cout << "[nvlsm] " << msg << "\n"

namespace pmemkv {
namespace nvlsm2 {

pool<LSM_Root> pmpool;
size_t pmsize;
/* #####################static functions for multiple threads ####################### */
/* persist: persist a mem_table to c0
 * v_nvlsm: an instance of nvlsm
 * */
static void persist(void * v_nvlsm) {
    //cout << "persisting a mem_table!" << endl;
    NVLsm2 * nvlsm = (NVLsm2 *) v_nvlsm;
    /* get the targeting meta_table[0] */ 
    auto meta_table = &(nvlsm->meta_table[0]);
    /* get the queue head from mem_table */
    auto mem_table = nvlsm->mem_table;
    auto run = mem_table->pop_queue();
    /* allocate space from NVM and copy data from mem_table */
    persistent_ptr<PRun> p_run;
    int i = 0;
    make_persistent_atomic<PRun>(pmpool, p_run);
    auto key_entry = p_run->key_entry;
    auto vals = p_run->vals;
    for (auto it = run->kv.begin(); it != run->kv.end(); it++) {
        strncpy(key_entry[i].key, it->first.c_str(), KEY_SIZE);
        strncpy(&vals[i * VAL_SIZE], it->second.c_str(), it->second.size());
        key_entry[i].val_len = it->second.size();
        key_entry[i].p_val = &vals[i * VAL_SIZE];
        i++;
    }
    p_run->size = i;
    p_run.persist();
    // to-do build layers
    delete run;
    //cout << "persist stop" << endl;
}
/* ######################## Log #########################################*/
void Log::append(string str) {
    strncpy(ops, str.c_str(), str.size());
    return;
}

/* ######################## Implementations for NVLsm2 #################### */
NVLsm2::NVLsm2(const string& path, const size_t size) 
    : run_size(RUN_SIZE), layer_depth(MAX_DEPTH), com_ratio(COM_RATIO) {
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
    // create a mem_table
    mem_table = new MemTable(run_size);
    // reserve space for the meta_table of first components 
    meta_table.emplace_back();
    make_persistent_atomic<Log>(pmpool, meta_log);
    // create the thread pool for compacting runs
    compact_pool = new ThreadPool(COMPACT_POOL_SIZE);
    if (compact_pool->initialize_threadpool() == -1) {
        cout << "Fail to initialize the thread pool!" << endl;
        exit(-1);
    }
    LOG("Opened ok");
}

NVLsm2::~NVLsm2() {
    while (mem_table->getSize() > 0)
        usleep(500);
    displayMeta();
    persist_pool->destroy_threadpool();
    delete_persistent_atomic<Log>(meta_log);
    delete mem_table;
    delete persist_pool;
    LOG("Closing persistent pool");
    pmpool.close();
    LOG("Closed ok");
}

KVStatus NVLsm2::Get(const int32_t limit, const int32_t keybytes, int32_t* valuebytes,
                        const char* key, char* value) {
    LOG("Get for key=" << key);
    return NOT_FOUND;
}

KVStatus NVLsm2::Get(const string& key, string* value) {
    LOG("Get for key=" << key.c_str());
    //cout << "start to get key: " << key << endl;
    string val;
    LOG("Searching in memory buffer");
    if (mem_table->search(key, val)) {
        value->append(val);
        return OK;
    }

    for (int i = 0; i < meta_table.size(); i++) {
        //cout << "total " << meta_table.size() << " component";
        //cout << ": searchng in compoent " << i << endl;
    }
    //cout << key << " not found" << endl;
    return NOT_FOUND;
}

KVStatus NVLsm2::Put(const string& key, const string& value) {
    LOG("Put key=" << key.c_str() << ", value.size=" << to_string(value.size()));
    //cout << "Put key=" << key.c_str() << ", value.size=" << to_string(value.size()) << endl;;
    while (mem_table->getSize() > com_ratio)
        usleep(SLOW_DOWN_US); 
    if (mem_table->append(key, value)) {
        /* write buffer is filled up if queue size is larger than 4, wait */
        mem_table->push_queue();
        //cout << "memTable: " << mem_table->getSize() << endl; 
        Task * persist_task = new Task(&persist, (void *) this);
        persist_pool->add_task(persist_task);
        //cout << "started a persist thread " << endl;
    }
    return OK;
}

KVStatus NVLsm2::Remove(const string& key) {
    LOG("Remove key=" << key.c_str());
    //cout << "Remove key=" << key.c_str() << endl;;
    return OK;
}

/* copy data between PRuns */
void NVLsm2::copy_kv(persistent_ptr<PRun> des_run, int des_i, persistent_ptr<PRun> src_run, int src_i) {
    auto des_entry = des_run->key_entry;
    auto des_vals = des_run->vals;
    auto src_entry = src_run->key_entry;
    auto src_vals = src_run->vals;
    strncpy(des_entry[des_i].key, src_entry[src_i].key, KEY_SIZE);
    strncpy(&des_vals[des_i * VAL_SIZE], &src_vals[src_i * VAL_SIZE], src_entry[src_i].val_len);
    des_entry[des_i].val_len = src_entry[src_i].val_len;
    des_entry[des_i].p_val = &des_vals[des_i * VAL_SIZE];
    return;
}

/* display the meta tables */
void NVLsm2::displayMeta() {
    cout << "=========== start displaying meta table ======" << endl;
    cout << "=========== end displaying meta table ========" << endl;
}

/* ############## MemTable #################*/
MemTable::MemTable(int size) 
    : buf_size(size) {
    buffer = new Run();
    make_persistent_atomic<Log>(pmpool, kvlog);
    pthread_rwlock_init(&rwlock, NULL);
}

MemTable::~MemTable() {
    pthread_rwlock_destroy(&rwlock);
    delete_persistent_atomic<Log>(kvlog);
    delete buffer;
}

/* getSize: return the queue size 
 * */
size_t MemTable::getSize() {
    return persist_queue.size();
}

/* push_que: push the write buffer to the persist queue
 *           and allocate a new buffer
 * */
void MemTable::push_queue() {
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
bool MemTable::append(const string & key, const string &val) {
    buffer->append(key, val);
    kvlog->append(key + val);
    kvlog.persist();
    if (buffer->size >= RUN_SIZE) {
        return true;
    }
    return false;
}

/* search a key in the memory buffer */
bool MemTable::search(const string &key, string &val) {
    pthread_rwlock_rdlock(&rwlock);
    auto it = buffer->kv.find(key);
    if (it != buffer->kv.end()) {
        val = it->second;
        pthread_rwlock_unlock(&rwlock);
        return true;
    }
    pthread_rwlock_unlock(&rwlock);
    return false;
}

/* ################### MetaTable ##################################### */
MetaTable::MetaTable() : next_compact(0) {
    pthread_rwlock_init(&rwlock, NULL);
}

MetaTable::~MetaTable() {
    pthread_rwlock_destroy(&rwlock);
}

/* getSize: get the size of ranges */
size_t MetaTable::getSize() {
    return ranges.size();
}

/* add: add metadata for a segment/segments */
void MetaTable::add(persistent_ptr<PSegment> seg) {
    pthread_rwlock_wrlock(&rwlock);
    segRanges[seg->allRange] = seg;
    pthread_rwlock_unlock(&rwlock);
    return;
}
void MetaTable::add(vector<persistent_ptr<PSegment>> segs) {
    pthread_rwlock_wrlock(&rwlock);
    for (auto seg : segs) {
        segRanges[seg->allRange] = seg;
    }
    pthread_rwlock_unlock(&rwlock);
    return;
}

/* del: delete metadata (data if needed) for a seg/segs */
void MetaTable::del(vector<persistent_ptr<PSegment>> segs) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    for (auto seg : segs) {
        count += segRanges.erase(seg->allRange);
    }
    pthread_rwlock_unlock(&rwlock);
    if (count != segs.size()) {
        cout << "delete error: delete " << count << " of " << segs.size();
        exit(-1);
    }
    return;
}
void MetaTable::del(persistent_ptr<PSegment> seg) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    segRanges.erase(seg->allRange);
    pthread_rwlock_unlock(&rwlock);
}

/* search: search a key / overlapped ranges in a component */
bool MetaTable::search(const string& key, string& value) {
    pthread_rwlock_wrlock(&rwlock);
    // to-do
    pthread_rwlock_unlock(&rwlock);
    return true;
}
void MetaTable::search(KVRange& kvRange, vector<persistent_ptr<PSegment>>& segs) {
    if (segRanges.empty() || segRanges.size() == 0)
        return;
    // binary search for component i > 1
    KVRange end_range(kvRange.end_key, kvRange.end_key);
    //cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
    //cout << "range size2: " << ranges.size() << endl;
    auto it_high = segRanges.upper_bound(end_range);
    while (it_high == segRanges.end() || kvRange.start_key <= it_high->first.end_key) {
        if (it_high == segRanges.end()) {
            it_high--;
            continue;
        }
        if (kvRange.end_key >= it_high->first.start_key)
            segs.push_back(it_high->second);
        if (it_high == segRanges.begin())
            break;
        it_high--;
    }
    //cout << "search done, low_run " << runs.size() << endl;
    if (segs.size() > 0)
        reverse(segs.begin(), segs.end());
    return;
}

/* build_layer: build a new layer using a seg */
void build_layer(persistent_ptr<PSegment> seg) {
    vector<persistent_ptr<PSegment>> overlapped_segs;
    search(seg->allRange, overlapped_segs)
}
/* ###################### PRun ######################### */
PRun::PRun(): size(0), seg_count(1) {
}
PRun::~PRun() {
}
/* get kvrange for the current run */
void PRun::get_range(KVRange& range) {
    range.start_key.assign(key_entry[0].key, KEY_SIZE);
    range.end_key.assign(key_entry[size - 1].key, KEY_SIZE);
    return;
}
/* ##################### PSegment ############################################# */
PSegment::PSegment(persistent_ptr<PRun> p_run, size_t start_i, size_t end_i)
    : pRun(p_run), start(start_i), end(end_i) {
}
PSegment::~PSegment() {
    if (pRun->seg_count == 0) {
        cout << "delete a pRun" << endl;
        delete_persistent_atomic<PRun>(pRun);
    }
}

/* search: search a key in a seg 
 * para: key - key to search
 *       value - the value of the key
 * return: index of the key in the prun
 * */
size_t PSegment::search(string key, string& value) {
    int left = start;
    int right = end;
    auto key_entry = pRun->key_entry;
    while (left < right) {
        int mid = left + (right - left) / 2;
        int res = strncmp(key.c_str(), key_entry[mid].key, KEY_SIZE);
        if (res == 0) {
            value.assign(key_entry[mid].p_val, key_entry[mid].val_len);
            return mid;
        } else if (res > 0) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1;
}

/* get_range: get the kvrange of current seg */
void PSegment::get_localRange(KVRange& kvRange) {
    auto key_entry = pRun->key_entry;
    kvRange.start_key.assign(key_entry[start].key, KEY_SIZE);
    kvRange.end_key.assign(key_entry[end].key, KEY_SIZE);
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

/* search: binary search kv pairs in a run */
bool Run::search(string &req_key, string &req_val) {
    pthread_rwlock_rdlock(&rwlock);
    auto it = kv.find(req_key);
    if (it != kv.end()) {
        req_val = it->second;
        pthread_rwlock_unlock(&rwlock);
        return true;
    }
    pthread_rwlock_unlock(&rwlock);
    return false;
}

/* append: append a kv pair to run
 * this will only be called for write buffer 
 * */
void Run::append(const string &key, const string &val) {
    pthread_rwlock_wrlock(&rwlock);
    // put kv_pair into buffer
    auto it = kv.find(key);
    if (it == kv.end()) {
        size++;
    }
    kv[key] = val;
    // update range of buffer 
    if (range.start_key.empty() || range.start_key > key)
        range.start_key.assign(key);
    if (range.end_key.empty() || range.end_key < key)
        range.end_key.assign(key);
    pthread_rwlock_unlock(&rwlock);
}

} // namespace nvlsm
} // namespace pmemkv
