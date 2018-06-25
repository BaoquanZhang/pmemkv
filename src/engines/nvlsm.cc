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
 * v_nvlsm: an instance of nvlsm
 * */
static void persist(void * v_nvlsm) {
    //cout << "persisting a mem_table!" << endl;
    NVLsm * nvlsm = (NVLsm *) v_nvlsm;
    // get the targeting meta_table[0] 
    auto meta_table = &(nvlsm->meta_table[0]);
    // get the queue head from mem_table
    auto mem_table = nvlsm->mem_table;
    auto run = mem_table->pop_queue();
    //cout << "range of memory buffer :";
    // allocate space from NVM and copy data from mem_table
    persistent_ptr<PRun> persist_run;
    make_persistent_atomic<PRun>(pmpool, persist_run);
    auto p_array = persist_run->array;
    int i = 0;
    for (auto it = run->kv.begin(); it != run->kv.end(); it++) {
        strncpy(p_array[i].kv, it->second.c_str(), KEY_SIZE + VAL_SIZE);
        i++;
    }
    persist_run->size = i;
    persist_run->range.start_key.assign(persist_run->array[0].kv, KEY_SIZE);
    persist_run->range.end_key.assign(persist_run->array[i - 1].kv, KEY_SIZE);
    //cout << "persisted range: ";
    //persist_run->range.display();
    // add meta data to component 0
    meta_table->add(persist_run);
    delete run;
    if (meta_table->ranges.size() > nvlsm->com_ratio)
        nvlsm->compact(0);
    //cout << "persist stop" << endl;
}

/* ####################### compaction function ###########################
 * v_nvlsm: an instance of nvlsm
 * */
/*
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
        cout << "planning a compaction "<< endl;
        auto unit = nvlsm->plan_compaction(comp_index);
        // merge sort the runs
        cout << "merge sorting " << endl;
        nvlsm->merge_sort(unit);
        // delete the old meta data
        cout << "deleting old meta " << endl;
        nvlsm->meta_table[comp_index].del(unit->up_run);
        nvlsm->meta_table[comp_index + 1].del(unit->low_runs);
        // add new meta data
        cout << "adding new data " << endl;
        nvlsm->meta_table[comp_index + 1].add(unit->new_runs);
        // delete the old data
        unit->display();
        delete unit;
        comp_index++;
        nvlsm->displayMeta();
    }
    cout << "stop compactiom" << endl;
}
*/

/* ######################### CompactionUnit ############################# */
CompactionUnit::CompactionUnit(){}
CompactionUnit::~CompactionUnit(){
    //delete old data
    delete_persistent_atomic<PRun>(up_run);
    for(int i = 0; i < low_runs.size(); i++)
        delete_persistent_atomic<PRun>(low_runs[i]);
}

void CompactionUnit::display() {
    auto start_key = up_run->range.start_key;
    auto end_key = up_run->range.end_key;
    cout << "up run:" << "<" << start_key << "," << end_key << ">" << endl;
    cout << "low runs: ";
    for (auto it : low_runs) {
        start_key = it->range.start_key;
        end_key = it->range.end_key;
        cout << "<" << start_key << "," << end_key << "> ";
    }
    cout << "new runs: ";
    for (auto it : new_runs) {
        start_key = it->range.start_key;
        end_key = it->range.end_key;
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
    while (mem_table->getSize() > 0)
        usleep(500);
    displayMeta();
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
    while (mem_table->getSize() > com_ratio)
        usleep(SLOW_DOWN_US); 
    if (mem_table->append(key, value)) {
        // write buffer is filled up
        // if queue size is larger than 4, wait
        mem_table->push_queue();
        //cout << "memTable: " << mem_table->getSize() << endl; 
        Task * persist_task = new Task(&persist, (void *) this);
        persist_pool->add_task(persist_task);
        //cout << "started a persist thread " << endl;
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
        auto range = unit->up_run->range;
        meta_table[index + 1].search(range, unit->low_runs);
    }
    return unit;
}

void NVLsm::compact(int comp_index) {
    //cout << "start compaction " << endl;
    int comp_count = meta_table.size();
    int current_size = meta_table[comp_index].getSize();
    int max_size = 0;
    if (comp_index == 0 || comp_index == 1) 
        max_size = com_ratio;
    else
        max_size = (int) pow(com_ratio, comp_index);
    if (current_size > max_size) {
        //int start_s = clock();
        //cout << "current_size: " << current_size << " max size:" << max_size << endl;
        // plan a compaction for the component i
        if (comp_count == comp_index + 1) {
            meta_table.emplace_back();
            //displayMeta();
        }
        //cout << "planing a compaction" << endl;
        auto unit = plan_compaction(comp_index);
        // merge sort the runs
        cout << "C " << comp_index << " merge sorting " << endl;
        cout << "before merge sorting" << endl;
        displayMeta();
        merge_sort(unit);
        cout << "after  merge sorting" << endl;
        displayMeta();
        // delete the old meta data
        //cout << "deleting old meta data " << endl;
        if (!(meta_table[comp_index].del(unit->up_run))) {
            cout << "delete meta " << comp_index << " error! " << endl; 
            unit->display();
            exit(1);
        }
        if (!(meta_table[comp_index + 1].del(unit->low_runs))) {
            cout << "delete meta in C " << comp_index + 1 << " error! " << endl; 
            unit->display();
            exit(1);
        }
        cout << "after deleting old meta " << endl;
        displayMeta();
        // add new meta data
        //cout << "adding new  meta data " << endl;
        cout << "after adding new meta " << endl;
        meta_table[comp_index + 1].add(unit->new_runs);
        //unit->display();
        // delete the old data
        //cout << "deleting old data " << endl;
        delete unit;
        displayMeta();
        compact(comp_index + 1);
        //int stop_s=clock();
        //cout << "time: " << (stop_s - start_s) / double(CLOCKS_PER_SEC) * 1000 << endl;
    }
    //cout << "stop compactiom" << endl;
}

/* merge_sort: merge sort the runs during a compaction */
void NVLsm::merge_sort(CompactionUnit * unit) {
    auto meta_up = &(meta_table[unit->index]);
    auto meta_low = &(meta_table[unit->index + 1]);
    // acquire two locks
    // follow the strick order to avoid deadlock
    pthread_rwlock_rdlock(&(meta_up->rwlock));
    pthread_rwlock_rdlock(&(meta_low->rwlock));
    auto up_run = unit->up_run;
    auto low_runs = unit->low_runs;
    // if no runs from lower component 
    if (low_runs.empty()) {
        unit->new_runs.push_back(up_run);
        pthread_rwlock_unlock(&(meta_up->rwlock));
        pthread_rwlock_unlock(&(meta_low->rwlock));
        return;
    }
    persistent_ptr<PRun> new_run;
    make_persistent_atomic<PRun>(pmpool, new_run);
    int low_index = 0;
    int up_index = 0;
    int new_index = 0;
    auto up_len = up_run->size;
    auto up_array = up_run->array;

    cout << "before merge for both component " << endl;
    displayMeta();
    for (auto low_run : low_runs) {
        auto low_array = low_run->array;
        auto low_len = low_run->size;
        low_index = 0;
        while (low_index < low_len) {
            auto low_kv = low_array[low_index].kv;
            if (up_index < up_len) {
                // if up run has kv pairs
                auto up_kv = up_array[up_index].kv;
                auto res = strncmp(up_kv, low_kv, KEY_SIZE);
                if (res <= 0) {
                    // up key is smaller 
                    strncpy(new_run->array[new_index].kv, up_kv, KEY_SIZE + VAL_SIZE);
                    up_index++;
                    if (res == 0)
                        low_index++;
                } else {
                    // low key is smaller
                    strncpy(new_run->array[new_index].kv, low_kv, KEY_SIZE + VAL_SIZE);
                    low_index++;
                }
            } else {
                // if up key has no kv pairs
                strncpy(new_run->array[new_index].kv, low_kv, KEY_SIZE + VAL_SIZE);
                low_index++;
            }

            new_index++;

            if (new_index == RUN_SIZE) {
                new_run->range.start_key.assign(new_run->array[0].kv, KEY_SIZE);
                new_run->range.end_key.assign(new_run->array[new_index - 1].kv, KEY_SIZE);
                new_run->size = RUN_SIZE;
                unit->new_runs.push_back(new_run);
                cout << "before allocate new p run" << endl;
                displayMeta();
                make_persistent_atomic<PRun>(pmpool, new_run);
                new_index = 0;
                cout << "allocate new p run" << endl;
                displayMeta();
            }
        }
    }
    cout << "before merge for up component " << endl;
    displayMeta();

    // run out of low but up remains
    while (up_index < up_len) {
        auto up_kv = up_array[up_index].kv;
        strncpy(new_run->array[new_index].kv, up_kv, KEY_SIZE + VAL_SIZE);
        up_index++;
        new_index++;
        if (new_index == RUN_SIZE) {
            new_run->range.start_key.assign(new_run->array[0].kv, KEY_SIZE);
            new_run->range.end_key.assign(new_run->array[new_index - 1].kv, KEY_SIZE);
            new_run->size = RUN_SIZE;
            unit->new_runs.push_back(new_run);
            make_persistent_atomic<PRun>(pmpool, new_run);
            new_index = 0;
        }
    }

    cout << "before push rest of the new run" << endl;
    displayMeta();
    // run out of both up and low but new is not filled up
    if (new_index > 0 && new_index < RUN_SIZE) {
        new_run->range.start_key.assign(new_run->array[0].kv, KEY_SIZE);
        new_run->range.end_key.assign(new_run->array[new_index - 1].kv, KEY_SIZE);
        new_run->size = new_index;
        //new_run->range.display();
        unit->new_runs.push_back(new_run);
    } else {
        delete_persistent_atomic<PRun>(new_run);
    }
    cout << "after merge sort, the unit: ";
    unit->display();
    pthread_rwlock_unlock(&(meta_up->rwlock));
    pthread_rwlock_unlock(&(meta_low->rwlock));
    return;
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
    if (buffer->size >= RUN_SIZE) {
        return true;
    }
    return false;
}

/* ################### MetaTable ##################################### */
MetaTable::MetaTable() : next_compact(0) {
    ranges.clear();
    compact_mutex = new mutex();
    pthread_rwlock_init(&rwlock, NULL);
}

MetaTable::~MetaTable() {
    //delete compact_mutex;
    pthread_rwlock_destroy(&rwlock);
}

/* getSize: get the size of ranges */
size_t MetaTable::getSize() {
    return ranges.size();
}

/* display: show the current elements */
void MetaTable::display() {
    cout << ranges.size() << endl;
    for (auto it : ranges) {
        cout << "<" << it.first.start_key << "," << it.first.end_key << ">";
        cout << "(" << it.second->size << ") ";
        if (it.first.start_key != it.second->range.start_key
                || it.first.end_key != it.second->range.end_key) {
            cout << "kvrange inconsistent! " << endl;
            cout << "kvrange in meta table: ";
            cout << "<" << it.first.start_key << "," << it.first.end_key << ">" << endl;
            cout << "kvrange in run: ";
            cout << "<" << it.second->range.start_key << "," << it.second->range.end_key << ">" << endl;
            //cout << it.second->array[0].kv << "," << it.second->array[it.second->size - 1].kv << endl;
            exit(1);
        }
    }
    cout << endl;
    return;
}

/* add: add new runs into meta_table */
void MetaTable::add(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    if (runs.size() == 0) 
        return;
    for (auto it : runs) {
        ranges[it->range] = it;
        count++;
    }
    pthread_rwlock_unlock(&rwlock);
    return;
}

void MetaTable::add(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    if (run == NULL)
        return;
    ranges[run->range] = run;
    pthread_rwlock_unlock(&rwlock);
    return;
}

/* delete run/runs from the metadata */
bool MetaTable::del(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    count += ranges.erase(run->range);
    if (count != 1) {
        cout << "erase " << count << " out of 1" << endl;
        cout << "deleting range failed: ";
        run->range.display();
        return false;
    }
    return true;
    pthread_rwlock_unlock(&rwlock);
}

bool MetaTable::del(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    for (auto run : runs) { 
        if (ranges.erase(run->range) != 1) {
            cout << "deleting range failed: ";
            run->range.display();
        } else {
            count++;
        }
    }

    if (count != runs.size()) {
        cout << "erase " << count << " out of " << runs.size() << endl;
        return false;
    }
    return true;
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

/* search: get all run within a kv range */
void MetaTable::search(KVRange &kvrange, vector<persistent_ptr<PRun>> &runs) {
    if (ranges.empty())
        return;
    runs.clear();
    pthread_rwlock_rdlock(&rwlock);
    // binary search for component i > 1
    KVRange end_range(kvrange.end_key, kvrange.end_key);
    //cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
    //cout << "range size: " << ranges.size() << endl;
    auto it_high = ranges.upper_bound(end_range);

    if (it_high == ranges.end())
        it_high--;

    while (it_high != ranges.begin() && it_high->first.start_key > kvrange.end_key)
        it_high--;

    while (kvrange.start_key <= it_high->first.end_key) {
        if (!(it_high->first.start_key > kvrange.end_key
                || it_high->first.end_key < kvrange.start_key)) {
            runs.push_back(it_high->second);
        } else {
            break;
        }

        if (it_high == ranges.begin()) {
            break;
        } else {
            it_high--;
        }
    }

    reverse(runs.begin(), runs.end());
    //cout << "search done, low_run " << runs.size() << endl;
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
    kv[key] = key + val;
    // update range of buffer 
    if (range.start_key.empty() || range.start_key > key)
        range.start_key.assign(key);
    if (range.end_key.empty() || range.end_key < key)
        range.end_key.assign(key);
    pthread_rwlock_unlock(&rwlock);
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

/* search: binary search kv pairs in a run */
bool PRun::search(string &req_key, string &req_val) {
    int start = 0;
    int end = size - 1;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        int res = strncmp(array[mid].kv, req_key.c_str(), KEY_SIZE);
        if (res == 0) {
            req_val.assign(array[mid].kv, KEY_SIZE, VAL_SIZE);
            return true;
        } else if (res < 0) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return false;
}

} // namespace nvlsm
} // namespace pmemkv
