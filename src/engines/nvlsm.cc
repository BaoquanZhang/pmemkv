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
static void compact(void * v_nvlsm);
/* #####################static functions for multiple threads ####################### */
/* persist: persist a mem_table to c0
 * v_nvlsm: an instance of nvlsm
 * */
static void persist(void * v_nvlsm) {
    cout << "persisting a mem_table!" << endl;
    NVLsm * nvlsm = (NVLsm *) v_nvlsm;
    // get the targeting meta_table[0] 
    auto meta_table = &(nvlsm->meta_table[0]);
    // get the queue head from mem_table
    auto mem_table = nvlsm->mem_table;
    auto run = mem_table->pop_queue();
    // allocate space from NVM and copy data from mem_table
    persistent_ptr<PRun> persist_run;
    make_persistent_atomic<PRun>(pmpool, persist_run);
    persist_run->write(*run);
    meta_table->add(persist_run);
    delete run;
    nvlsm->compact(0);
    cout << "persist stop" << endl;
}

/* ####################### compaction function ###########################
 * v_nvlsm: an instance of nvlsm
 * */
static void compact(void * v_nvlsm) {
    NVLsm * nvlsm = (NVLsm *) v_nvlsm;
    // start to compact
    int comp_index = 0;
    cout << "start compaction " << endl;
    while (true) {
        int comp_size = nvlsm->meta_table.size();
        int current_size = nvlsm->meta_table[comp_index].getSize();
        int max_size = (comp_index + 1) * nvlsm->com_ratio;
        cout << "current_size: " << current_size << " max size:" << max_size << endl;
        if (current_size < max_size)
            break;
        // plan a compaction for the component i
        if (comp_size == comp_index + 1) {
            nvlsm->meta_table.emplace_back();
        }
        auto unit = nvlsm->plan_compaction(comp_index);
        // merge sort the runs
        nvlsm->merge_sort(unit);
        // delete the old meta data
        nvlsm->meta_table[comp_index].del(unit->up_run);
        nvlsm->meta_table[comp_index + 1].del(unit->low_runs);
        // add new meta data
        nvlsm->meta_table[comp_index + 1].add(unit->new_runs);
        // delete the old data
        //unit->display();
        delete unit;
        comp_index++;
        nvlsm->displayMeta();
    }
    cout << "stop compactiom" << endl;
}

/* ######################### CompactionUnit ############################# */
CompactionUnit::CompactionUnit(){}
CompactionUnit::~CompactionUnit(){
    //delete old data
    delete_persistent_atomic<PRun>(up_run);
    for(int i = 0; i < low_runs.size(); i++)
        delete_persistent_atomic<PRun>(low_runs[i]);
}
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
    cout << "new runs: ";
    for (auto it : new_runs) {
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
    // reserve space for the meta_table of first components 
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
    // create the thread pool for compacting runs
    compact_pool = new ThreadPool(COMPACT_POOL_SIZE);
    if (compact_pool->initialize_threadpool() == -1) {
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
        // if queue size is larger than 4, wait
        while (mem_table->getSize() >= com_ratio)
            usleep(SLOW_DOWN_US); 
        mem_table->push_queue();
        cout << "memTable: " << mem_table->getSize() << endl; 
        Task * persist_task = new Task(&persist, (void *) this);
        persist_pool->add_task(persist_task);
        cout << "started a persist thread " << endl;
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

void NVLsm::compact(int comp_index) {
    cout << "start compaction " << endl;
    int comp_count = meta_table.size();
    int current_size = meta_table[comp_index].getSize();
    int max_size = (comp_index + 1) * com_ratio;
    if (current_size > max_size) {
        cout << "current_size: " << current_size << " max size:" << max_size << endl;
        // plan a compaction for the component i
        if (comp_count == comp_index + 1) {
            meta_table.emplace_back();
        }
        auto unit = plan_compaction(comp_index);
        // merge sort the runs
        merge_sort(unit);
        // delete the old meta data
        meta_table[comp_index].del(unit->up_run);
        meta_table[comp_index + 1].del(unit->low_runs);
        // add new meta data
        meta_table[comp_index + 1].add(unit->new_runs);
        unit->display();
        // delete the old data
        delete unit;
        displayMeta();
        compact(comp_index + 1);
    }
    cout << "stop compactiom" << endl;
}

/* merge_sort: merge sort the runs during a compaction */
void NVLsm::merge_sort(CompactionUnit * unit) {
    auto up_run = unit->up_run;
    auto low_runs = unit->low_runs;
    // if no runs from lower component 
    if (low_runs.empty()) {
        unit->new_runs.push_back(up_run);
        return;
    }
    persistent_ptr<PRun> new_run;
    make_persistent_atomic<PRun>(pmpool, new_run);
    int low_index = 0;
    int up_index = 0;
    auto up_len = up_run->getSize();
    auto up_array = up_run->getArray();

    for (auto low_run : low_runs) {
        auto low_array = low_run->getArray();
        auto low_len = low_run->getSize();
        low_index = 0;
        while (low_index < low_len) {
            if (up_index < up_len) {
                // if up run has kv pairs
                auto up_key = up_array[up_index].key;
                auto res = up_key->compare(*(low_array[low_index].key));
                if (res <= 0) {
                    // up key is smaller 
                    new_run->append(up_array[up_index]);
                    up_index++;
                    if (res == 0)
                        low_index++;
                } else {
                    // low key is smaller
                    new_run->append(low_array[low_index]);
                    low_index++;
                }
            } else {
                // if up key has no kv pairs
                new_run->append(low_array[low_index]);
                low_index++;
            }
            if (new_run->getSize() == RUN_SIZE) {
                unit->new_runs.push_back(new_run);
                make_persistent_atomic<PRun>(pmpool, new_run);
            }
        }
    }
}

/* display the meta tables */
void NVLsm::displayMeta() {
    cout << "=========== start displaying meta table ======" << endl;
    int index = 0;
    for (auto component : meta_table) {
        cout << " ***component " << index++ << ": ";
        component.display();
    }
    cout << "=========== end displaying meta table ========" << endl;
}

/* ############## KVPair ###################*/
KVPair::KVPair() {}

KVPair::KVPair(string init_key, string init_val) 
    : key(init_key), val(init_val) {}

KVPair::~KVPair() {}

/* ############## PKVPair ###################*/
PKVPair::PKVPair() {
    make_persistent_atomic<string>(pmpool, key);
    make_persistent_atomic<string>(pmpool, val);
}

PKVPair::PKVPair(string init_key, string init_val) {
    make_persistent_atomic<string>(pmpool, key, init_key);
    make_persistent_atomic<string>(pmpool, val, init_val);
}

PKVPair::~PKVPair() {
    delete_persistent_atomic<string>(key);
    delete_persistent_atomic<string>(val);
}

/* ############## MemTable #################*/
MemTable::MemTable(int size) 
    : buf_size(size) {
    buffer = new Run();
    pthread_rwlock_init(&rwlock, NULL);
}

MemTable::~MemTable() {
    pthread_rwlock_destroy(&rwlock);
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
    cout << ranges.size() << endl;
    /*
    for (auto it : ranges) {
        cout << "<" << it.first.start_key << "," << it.first.end_key << ">";
        cout << "(" << it.second->getSize() << ") ";
    }
    */
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
    if (run == NULL)
        return;
    ranges.emplace(run->getRange(), run);
    pthread_rwlock_unlock(&rwlock);
    return;
}

/* delete run/runs from the metadata */
void MetaTable::del(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    auto kvrange = run->getRange();
    //cout << "deleting key range:";
    //cout << kvrange.start_key << " " << kvrange.end_key << endl;
    int count = 0;
    count += ranges.erase(run->getRange());
    //cout << "erase " << count << " out of 1" << endl;
    pthread_rwlock_unlock(&rwlock);
}

void MetaTable::del(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    for (auto run : runs) { 
        count += ranges.erase(run->getRange());
    }
    //cout << "erase " << count << " out of " << runs.size() << endl;
    pthread_rwlock_unlock(&rwlock);
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
    pthread_rwlock_rdlock(&rwlock);
    KVRange start_range(kvrange.start_key, kvrange.start_key);
    KVRange end_range(kvrange.end_key, kvrange.end_key);
    //cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
    //cout << "range size: " << ranges.size() << endl;
    auto it_low = ranges.lower_bound(start_range);
    auto it_high = ranges.upper_bound(end_range);
    //cout << "mark" << endl;
    if (it_low != ranges.end()) {
        auto range = it_low->first;
        if (range.end_key >= kvrange.start_key)
            runs.push_back(it_low->second);
        it_low++;
        while (it_low != it_high && it_low != ranges.end()) {
            runs.push_back(it_low->second);
            it_low++;
        }
    }
    cout << "search done, low_run " << runs.size() << endl;
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
    sort(array, array + size);
    return;
}

/* #################### PRun ############################### */
PRun::PRun() 
    : size(0) {
    pthread_rwlock_init(&rwlock, NULL);
    make_persistent_atomic<PKVPair[]>(pmpool, array, RUN_SIZE);
}

PRun::~PRun() {
    delete_persistent_atomic<PKVPair[]>(array, RUN_SIZE);
    pthread_rwlock_destroy(&rwlock);
}

KVRange PRun::getRange() {
    return range;
}

size_t PRun::getSize() {
    return size;
}

persistent_ptr<PKVPair[]> PRun::getArray() {
    return array;
}

/* append: append a kv pair to run
 * this will only be called for compaction
 * */
void PRun::append(PKVPair & kv_pair) {
    pthread_rwlock_wrlock(&rwlock);
    // put kv_pair into buffer
    array[size].key->assign(*(kv_pair.key));
    array[size].val->assign(*(kv_pair.val));
    size++;
    // update range of buffer 
    if (range.start_key.empty() || range.start_key.compare(*(kv_pair.key)) > 0)
        range.start_key = *(kv_pair.key);
    if (range.end_key.empty() || range.end_key.compare(*(kv_pair.key)) < 0)
        range.end_key = *(kv_pair.key);
    pthread_rwlock_unlock(&rwlock);
}

/* write: persist kv pairs to NVM */
void PRun::write(Run &run) {
    //cout << "persisting run to NVM" << endl;
    pthread_rwlock_wrlock(&rwlock);
    auto kv_pairs = run.getArray();
    int len = run.getSize();
    for (int i = 0; i < len; i++) {
        array[i].key->assign(kv_pairs[i].key);
        array[i].val->assign(kv_pairs[i].val);
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
        if (array[mid].key->compare(req_key) == 0) {
            req_val = *array[mid].val;
            return true;
        } else if (array[mid].key->compare(req_key) < 0) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return false;
}

} // namespace nvlsm
} // namespace pmemkv
