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
static void persist(void * v_mem_table) {
    MemTable * mem_table = (MemTable *) v_mem_table;
}
/* ######################## Implementations for NVLsm #################### */
NVLsm::NVLsm(const string& path, const size_t size) {
    run_size = RUN_SIZE;
    layer_depth = LAYER_DEPTH;
    com_ratio = COM_RATIO;
    mem_table = new MemTable(run_size);
    /* Create/Open pmem pool */
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

    LOG("Opened ok");
}

NVLsm::~NVLsm() {
    delete mem_table;
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
    mem_table->append(kv_pair);
    return OK;
}

KVStatus NVLsm::Remove(const string& key) {
    LOG("Remove key=" << key.c_str());
    return OK;
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
/* append kv pair to write buffer */
void MemTable::append(KVPair &kv_pair) {
    buffer->append(kv_pair);
    if (buffer->getSize() >= RUN_SIZE) {
        persist_queue.push(buffer);
        //cout << "persist queue size: " << persist_queue.size() << endl;
        buffer = new Run();
    }
}

/* ################### MetaTable ##################################### */
MetaTable::MetaTable() {
    pthread_rwlock_init(&rwlock, NULL);
}

MetaTable::~MetaTable() {
    pthread_rwlock_destroy(&rwlock);
}

/* add new runs into meta_table */
void MetaTable::add(vector<persistent_ptr<Run>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    if (runs.size() == 0) 
        return;
    for (auto it : runs) {
        ranges.emplace(make_pair(it->getRange(), it));
    }
    pthread_rwlock_unlock(&rwlock);
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

/* write: persist kv pairs to NVM */
void Run::write(vector<KVPair> &kv_pairs, size_t len) {
    pthread_rwlock_wrlock(&rwlock);
    for (int i = 0; i < len; i++) {
        array[i].key = kv_pairs[i].key;
        array[i].val = kv_pairs[i].val;
    }
    size = len;
    pthread_rwlock_unlock(&rwlock);
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
    if (range.start_key > kv_pair.key)
        range.start_key = kv_pair.key;
    if (range.end_key < kv_pair.key)
        range.end_key = kv_pair.key;
    pthread_rwlock_unlock(&rwlock);
}
} // namespace nvlsm
} // namespace pmemkv
