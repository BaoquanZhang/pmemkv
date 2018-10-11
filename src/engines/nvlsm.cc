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
    // allocate space from NVM and copy data from mem_table
    persistent_ptr<PRun> p_run;
    int i = 0;
    //cout << "step" << endl;
    make_persistent_atomic<PRun>(pmpool, p_run);
    //cout << "step2" << endl;
    auto keys = p_run->keys;
    auto vals = p_run->vals;
    auto val_lens = p_run->val_lens;
    auto val_offs = p_run->val_offs;
    //cout << "before persist" << endl;
    for (auto it = run->kv.begin(); it != run->kv.end(); it++) {
        strncpy(&vals[p_run->val_end], it->second.c_str(), it->second.size());
        strncpy(&keys[i * KEY_SIZE], it->first.c_str(), KEY_SIZE);
        val_lens[i] = it->second.size();
        p_run->val_end += it->second.size();
        i++;
    }
    p_run->key_num = i;
    //cout << "done" << endl;
    // add meta data to component 0
    p_run.persist();
    meta_table->add(p_run);
    delete run;
    if (meta_table->ranges.size() > nvlsm->com_ratio)
        nvlsm->compact(0);
    //cout << "persist stop" << endl;
}

/* ######################### CompactionUnit ############################# */
CompactionUnit::CompactionUnit(){}
CompactionUnit::~CompactionUnit(){
    //delete old data
    if (low_runs.size() == 0)
        return;
    delete_persistent_atomic<PRun>(up_run);
    for(int i = 0; i < low_runs.size(); i++) {
        delete_persistent_atomic<PRun>(low_runs[i]);
    }
}

void CompactionUnit::display() {
    KVRange kv_range;
    int key_num = up_run->key_num;
    auto keys = up_run->keys;
    up_run->get_range(kv_range);
    cout << "up run: ";
    kv_range.display();
    cout << endl;
    cout << "low runs: ";
    for (auto low_run : low_runs) {
        low_run->get_range(kv_range);
        kv_range.display();
    }
    cout << endl;
    cout << "new runs: ";
    for (auto new_run : new_runs) {
        new_run->get_range(kv_range);
        kv_range.display();
    }
    cout << endl;
    return;
}

/* ######################## Implementations for NVLsm #################### */
NVLsm::NVLsm(const string& path, const size_t size) {
    run_size = RUN_SIZE;
    layer_depth = LAYER_DEPTH;
    com_ratio = COM_RATIO;
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
    make_persistent_atomic<PRun>(pmpool, meta_log);
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
    delete_persistent_atomic<PRun>(meta_log);
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
    //cout << "start to get key: " << key << endl;
    string val;
    LOG("Searching in memory buffer");
    if (mem_table->search(key, val)) {
        value = new string(val);
        return OK;
    }

    for (int i = 0; i < meta_table.size(); i++) {
        //cout << "total " << meta_table.size() << " component";
        //cout << ": searchng in compoent " << i << endl;
        if (i == 0) {
            if (meta_table[i].seq_search(key, val)) {
                value->append(val);
                //cout << "get success" << endl;
                return OK;
            }
        } else {
            if (meta_table[i].search(key, val)) {
                //cout << "get success" << endl;
                value->append(val);
                return OK;
            }
        }
    }
    //cout << key << " not found" << endl;
    return NOT_FOUND;
}

KVStatus NVLsm::Put(const string& key, const string& value) {
    LOG("Put key=" << key.c_str() << ", value.size=" << to_string(value.size()));
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

KVStatus NVLsm::Remove(const string& key) {
    LOG("Remove key=" << key.c_str());
    return OK;
}

/* plan a compaction */
CompactionUnit * NVLsm::plan_compaction(size_t cur_comp) {
    CompactionUnit * unit = new CompactionUnit();
    unit->index = cur_comp;
    unit->up_run = meta_table[cur_comp].getCompact(); 
    if (meta_table.size() > cur_comp && meta_table[cur_comp + 1].getSize() > 0) {
        KVRange range;
        unit->up_run->get_range(range);
        meta_table[cur_comp + 1].search(range, unit->low_runs);
    }
    return unit;
}

void NVLsm::compact(int comp_index) {
    LOG("start compaction ");
    int comp_count = meta_table.size();
    int current_size = meta_table[comp_index].getSize();
    int max_size = 0;
    if (comp_index == 0 || comp_index == 1) 
        max_size = com_ratio;
    else
        max_size = (int) pow(com_ratio, comp_index);
    if (current_size > max_size) {
        LOG("plan a compaction for the component" << comp_index);
        if (comp_count == comp_index + 1) {
            meta_table.emplace_back();
        }
        auto unit = plan_compaction(comp_index);
        /* merge sort the runs */
        merge_sort(unit);
        /* delete the old meta data */
        LOG("deleting old meta data");
        meta_log->append("delete", "old meta data");
        if (!(meta_table[comp_index].del(unit->up_run))) {
            cout << "delete meta " << comp_index << " error! " << endl; 
            //unit->display();
            exit(1);
        }
        if (!(meta_table[comp_index + 1].del(unit->low_runs))) {
            cout << "delete meta in C " << comp_index + 1 << " error! " << endl; 
            //unit->display();
            exit(1);
        }
        LOG("adding new meta data");
        meta_log->append("add", "new metadata");
        meta_table[comp_index + 1].add(unit->new_runs);
        meta_log->append("commit", "compaction");
        LOG("deleting old data");
        delete unit;
        displayMeta();
        compact(comp_index + 1);
    }
    cout << "compaction done" << endl;
    LOG("stop compactiom");
}

/* merge_sort: merge sort the runs during a compaction */
void copy_kv(persistent_ptr<PRun> new_run, int new_index, int new_end,
        persistent_ptr<PRun> old_run, int old_index) {
    auto new_key = new_run->keys;
    auto old_key = old_run->keys;
    auto new_val = new_run->vals;
    auto old_val = old_run->vals;
    strncpy(&(new_key[new_index * KEY_SIZE]), &(old_key[old_index * KEY_SIZE]), KEY_SIZE);
    new_run->val_lens[new_index] = old_run->val_lens[old_index];
    strncpy(&(new_val[new_end]), &(old_val[old_run->val_offs[old_index]]), old_run->val_lens[old_index]);
    new_run->val_offs[new_index] = new_end;
    return;
} 

void NVLsm::merge_sort(CompactionUnit * unit) {
    auto meta_up = &(meta_table[unit->index]);
    auto meta_low = &(meta_table[unit->index + 1]);
    /* acquire two locks follow the strick order to avoid deadlock */
    pthread_rwlock_rdlock(&(meta_up->rwlock));
    pthread_rwlock_rdlock(&(meta_low->rwlock));
    auto up_run = unit->up_run;
    auto low_runs = unit->low_runs;
    /* if no runs from lower component */ 
    if (low_runs.empty()) {
        unit->new_runs.push_back(up_run);
        pthread_rwlock_unlock(&(meta_up->rwlock));
        pthread_rwlock_unlock(&(meta_low->rwlock));
        return;
    }

    persistent_ptr<PRun> new_run;
    make_persistent_atomic<PRun>(pmpool, new_run);
    int new_index = 0;
    int new_end = 0;
    int up_index = 0;
    auto up_len = up_run->key_num;
    auto up_key = up_run->keys;

    for (auto low_run : low_runs) {
        auto low_len = low_run->key_num;
        int low_index = 0;
        auto low_key = low_run->keys;
        while (low_index < low_len) {
            if (up_index < up_len) {
                /* if up run has kv pairs */
                auto res = strncmp(&up_key[up_index * KEY_SIZE], &low_key[low_index * KEY_SIZE], KEY_SIZE);
                if (res <= 0) {
                    /* up key is smaller */
                    copy_kv(new_run, new_index, new_end, up_run, up_index);
                    up_index++;
                    new_end += up_run->val_lens[up_index];
                    if (res == 0)
                        low_index++;
                } else {
                    /* low key is smaller */
                    copy_kv(new_run, new_index, new_end, low_run, low_index);
                    new_end += low_run->val_lens[low_index];
                    low_index++;
                }
            } else {
                /* if up key has no kv pairs */
                copy_kv(new_run, new_index, new_end, low_run, low_index);
                new_end += low_run->val_lens[low_index];
                low_index++;
            }
            new_index++;
            if (new_index == RUN_SIZE) {
                new_run->key_num = new_index;
                unit->new_runs.push_back(new_run);
                new_run.persist();
                make_persistent_atomic<PRun>(pmpool, new_run);
                new_index = 0;
            }
        }
    }

    /* run out of low but up remains */
    while (up_index < up_len) {
        copy_kv(new_run, new_index, new_end, up_run, up_index);
        up_index++;
        new_index++;
        if (new_index == RUN_SIZE) {
            new_run->key_num = new_index;
            unit->new_runs.push_back(new_run);
            new_run.persist();
            make_persistent_atomic<PRun>(pmpool, new_run);
            new_index = 0;
        }
    }
    /* run out of both up and low but new is not filled up */
    if (new_index > 0 && new_index < RUN_SIZE) {
        new_run->key_num = new_index;
        unit->new_runs.push_back(new_run);
    } else if (new_index == 0) {
        delete_persistent_atomic<PRun>(new_run);
    }
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
    make_persistent_atomic<PRun>(pmpool, kvlog);
    pthread_rwlock_init(&rwlock, NULL);
}

MemTable::~MemTable() {
    pthread_rwlock_destroy(&rwlock);
    delete_persistent_atomic<PRun>(kvlog);
    kvlog->key_num = 0;
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
    kvlog->append(key, val);
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

/* display: show the current elements */
void MetaTable::display() {
    cout << ranges.size() << endl;
    for (auto it : ranges) {
        it.first.display();
        cout << "(" << it.second->key_num << ") ";
        KVRange kv_range;
        it.second->get_range(kv_range);
        if (!(it.first == kv_range)) {
            cout << "kvrange inconsistent! " << endl;
            cout << "kvrange in meta table: ";
            it.first.display();
            cout << "kvrange in run: ";
            kv_range.display();
            exit(1);
        }
    }
    cout << endl;
    return;
}

/* add: add new runs into meta_table */
void MetaTable::add(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    if (runs.size() == 0) {
        pthread_rwlock_unlock(&rwlock);
        return;
    }
    for (auto it : runs) {
        KVRange kvrange;
        it->get_range(kvrange);
        ranges[kvrange] = it;
    }
    pthread_rwlock_unlock(&rwlock);
    return;
}

void MetaTable::add(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    KVRange kvrange;
    run->get_range(kvrange);
    ranges[kvrange] = run;
    pthread_rwlock_unlock(&rwlock);
    return;
}

/* delete run/runs from the metadata */
bool MetaTable::del(persistent_ptr<PRun> run) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    KVRange kvrange;
    run->get_range(kvrange);
    count += ranges.erase(kvrange);
    if (count != 1) {
        cout << "erase " << count << " out of 1" << endl;
        cout << "deleting range failed: ";
        kvrange.display();
        pthread_rwlock_unlock(&rwlock);
        return false;
    }
    pthread_rwlock_unlock(&rwlock);
    return true;
}

bool MetaTable::del(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    int count = 0;
    for (auto run : runs) {
        KVRange kvrange;
        run->get_range(kvrange);
        if (ranges.erase(kvrange) != 1) {
            cout << "deleting range failed: ";
            kvrange.display();
        } else {
            count++;
        }
    }

    if (count != runs.size()) {
        cout << "erase " << count << " out of " << runs.size() << endl;
        pthread_rwlock_unlock(&rwlock);
        return false;
    }
    pthread_rwlock_unlock(&rwlock);
    return true;
}

/* get a run for compaction */
persistent_ptr<PRun> MetaTable::getCompact() {
    auto it = ranges.begin();
    if (next_compact >= ranges.size())
        next_compact = 0;
    advance(it, next_compact);
    auto run = it->second;
    next_compact++;
    return run;
}

/* search: get all run within a kv range and store them in runs */
void MetaTable::search(KVRange &kvrange, vector<persistent_ptr<PRun>> &runs) {
    //cout << "range size1: " << ranges.size() << endl;
    if (ranges.empty() || ranges.size() == 0)
        return;
    // binary search for component i > 1
    KVRange end_range(kvrange.end_key, kvrange.end_key);
    cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
    //cout << "range size2: " << ranges.size() << endl;
    auto it_high = ranges.upper_bound(end_range);
    while (it_high == ranges.end() || kvrange.start_key <= it_high->first.end_key) {
        if (it_high == ranges.end()) {
            it_high--;
            continue;
        }
        if (kvrange.end_key >= it_high->first.start_key)
            runs.push_back(it_high->second);
        if (it_high == ranges.begin())
            break;
        it_high--;
    }
    cout << "search done, low_run " << runs.size() << endl;
    if (runs.size() > 0)
        reverse(runs.begin(), runs.end());
    return;
}

/* search for a key in a component > 0 */
bool MetaTable::search(const string &key, string &val) {
    KVRange range(key, key);
    vector<persistent_ptr<PRun>> runs;
    pthread_rwlock_rdlock(&rwlock);
    search(range, runs);
    for (auto run : runs) {
        int start = 0;
        int end = run->key_num - 1;
        auto keys = run->keys;
        //cout << key << endl;
        //cout << run->index[start].key << "," << run->index[end].key << endl;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            int res = strncmp(key.c_str(), &keys[mid * KEY_SIZE], KEY_SIZE);
            if (res == 0) {
                auto val_off = run->val_offs[mid];
                auto val_len = run->val_lens[mid];
                val.assign(&(run->vals[val_off]), val_len);
                pthread_rwlock_unlock(&rwlock);
                //cout << "true" << endl;
                return true;
            } else if (res < 0) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        //cout << "false" << endl;
    }
    pthread_rwlock_unlock(&rwlock);
    return false;
}

/* search a key in component 0 */ 
bool MetaTable::seq_search(const string &key, string &val) {
    //cout << "acquiring the lock" << endl;
    pthread_rwlock_rdlock(&rwlock);
    //cout << "get the lock" << endl;
    for (auto range : ranges) {
        if (key > range.first.end_key || key < range.first.start_key)
            continue;
        int start = 0;
        int end = range.second->key_num - 1;
        auto keys = range.second->keys;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            int res = strncmp(key.c_str(), &keys[mid * KEY_SIZE], KEY_SIZE);
            if (res == 0) {
                auto val_off = range.second->val_offs[mid];
                auto val_len = range.second->val_lens[mid];
                val.assign(&range.second->vals[val_off], val_len);
                pthread_rwlock_unlock(&rwlock);
                return true;
            } else if (res < 0) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
    }
    pthread_rwlock_unlock(&rwlock);
    return false;
}

/* ###################### PRun ######################### */
void PRun::append(const string &key, const string &val) {
    auto proot = pmpool.get_root();
    try {
        // take locks and start a transaction
        transaction::exec_tx(pmpool, [&]() {
                strncpy(&keys[key_num * KEY_SIZE], key.c_str(), key.size());
                strncpy(&vals[val_end], val.c_str(), val.size());
                key_num++;
                val_end += val.size();
                if (key_num == RUN_SIZE) {
                    key_num = 0;
                    val_end = 0;
                }
        }, proot->pmutex, proot->shared_pmutex);
    } catch (pmem::transaction_error &te) {

    }
}

void PRun::get_range(KVRange& kv_range) {
    kv_range.start_key.assign(&keys[0], KEY_SIZE);
    kv_range.end_key.assign(&keys[(key_num - 1) * KEY_SIZE], KEY_SIZE);
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
