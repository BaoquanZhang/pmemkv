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
long long write_count = 0;
long long write_unit = 1;
long long read_count = 0;
long long read_unit = 1;
long long count_unit = 10000000;
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
    //cout << "allocating new run" << endl;
    make_persistent_atomic<PRun>(pmpool, p_run);
    auto key_entry = p_run->key_entry;
    auto vals = p_run->vals;
    for (auto it = run->kv.begin(); it != run->kv.end(); it++) {
#ifdef AMP
        write_count++;
        if (write_count > write_unit * count_unit) {
            write_unit++;
            cout << "write_count: " << write_count << "times" << endl;
        }
#endif
        strncpy(key_entry[i].key, it->first.c_str(), KEY_SIZE);
        strncpy(&vals[i * VAL_SIZE], it->second.c_str(), it->second.size());
        key_entry[i].val_len = it->second.size();
        key_entry[i].p_val = &vals[i * VAL_SIZE];
        i++;
    }
    p_run->size = i;
    // add meta data to component 0
    p_run.persist();
    meta_table->add(p_run);
    delete run;
    if (meta_table->ranges.size() > nvlsm->com_ratio)
        nvlsm->compact(0);
    //nvlsm->compact(p_run, 0);
    //nvlsm->displayMeta();
    //cout << "C0 has ";
    //meta_table->display();
    //cout << endl;
    //cout << "persist stop" << endl;
}
/* ######################## Log #########################################*/
void Log::append(string str) {
    strncpy(ops, str.c_str(), str.size());
    return;
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
    string start_key;
    string end_key;
    KVRange range;
    up_run->get_range(range);
    cout << "up run: ";
    range.display();
    cout << endl;
    cout << "low runs: ";
    for (auto low_run : low_runs) {
        low_run->get_range(range);
        range.display();
    }
    cout << endl;
    cout << "new runs: ";
    for (auto new_run : new_runs) {
        new_run->get_range(range);
        range.display();
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
    make_persistent_atomic<Log>(pmpool, meta_log);
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
    delete_persistent_atomic<Log>(meta_log);
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
    //cout << "Put key=" << key.c_str() << ", value.size=" << to_string(value.size()) << endl;;
    if (mem_table->append(key, value)) {
        /* write buffer is filled up if queue size is larger than 4, wait */
        while (mem_table->getSize() > com_ratio);
        mem_table->push_queue();
        //cout << "memTable: " << mem_table->getSize() << endl; 
        Task * persist_task = new Task(&persist, (void *) this);
        persist_pool->add_task(persist_task);
        //cout << "started a persist thread " << endl;
    }
    return OK;
}
/* Seek to location*/
KVStatus NVLsm::Seek(const string& key) {
    //cout << "start to seek " << endl;
    if (meta_table.size() == 0)
        return OK;
    search_stack.clear();
    iters.resize(meta_table.size());
    meta_table[0].seq_seek(key, search_stack);
    for (int i = 1; i < meta_table.size(); i++) {
        //cout << "seeking comp " << i << endl;
        RunIndex runIndex;
        auto iter = meta_table[i].seek(key);
        if (iter != meta_table[i].ranges.end()) {
            iters[i] = iter;
            runIndex.pRun = iter->second;
            runIndex.index = iter->second->seek(key);
            runIndex.range = iter;
            search_stack.emplace(runIndex, i);
        }
    }
    //cout << "seek done. Seaching stack size: " << search_stack.size() << endl;
    return OK;
}
KVStatus NVLsm::Next(string& key, string& val) {
    if (search_stack.size() == 0)
        return OK;
    auto runIndex = search_stack.begin()->first;
    auto index = search_stack.begin()->first.index;
    auto comp = search_stack.begin()->second;
    search_stack.erase(search_stack.begin());
    //cout << "get key from stack" << endl;
    if (runIndex.pRun == NULL || runIndex.index >= runIndex.pRun->size)
        return OK;
    key.assign(runIndex.get_key(), KEY_SIZE);
    val.assign(runIndex.get_val(), VAL_SIZE);
    //cout << "index++" << endl;
    index++;
    if (index >= runIndex.pRun->size) {
        //cout << "go to next run" << endl;
        if (comp != 0) {
            auto iter = runIndex.range;
            iter++;
            if (iter != meta_table[comp].ranges.end()) {
                runIndex.pRun = iter->second;
                runIndex.index = 0;
                runIndex.range = iter;
                search_stack.emplace(runIndex, comp);
            }
            iters[comp] = iter;
        }
    } else {
        //cout << "stay in the same run" << endl;
        runIndex.index = index;
        search_stack.emplace(runIndex, comp);
    }
    return OK;
}

KVStatus NVLsm::Remove(const string& key) {
    LOG("Remove key=" << key.c_str());
    //cout << "Remove key=" << key.c_str() << endl;;
    return OK;
}

/* plan a compaction */
CompactionUnit * NVLsm::plan_compaction(size_t index) {
    CompactionUnit * unit = new CompactionUnit();
    unit->index = index;
    unit->up_run = meta_table[index].getCompact(); 
    if (meta_table.size() > index && meta_table[index + 1].getSize() > 0) {
        KVRange range;
        unit->up_run->get_range(range);
        meta_table[index + 1].search(range, unit->low_runs);
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
        meta_log->append("delete old meta data");
        meta_log.persist();
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
        LOG("adding new meta data");
        meta_log->append("add new metadata");
        meta_log.persist();
        if (!(meta_table[comp_index + 1].add(unit->new_runs))) {
            cout << "add meta in C " << comp_index + 1 << " error! " << endl; 
            unit->display();
            exit(1);
        }
        meta_log->append("commit compaction");
        meta_log.persist();
        LOG("deleting old data");
        delete unit;
        compact(comp_index + 1);
    }
    LOG("stop compactiom");
}

void NVLsm::compact(persistent_ptr<PRun> run, int index) {
    LOG("start compaction ");
    CompactionUnit* unit = new CompactionUnit();
    unit->up_run = run;
    KVRange kvRange;
    run->get_range(kvRange);
    meta_table[index].search(kvRange, unit->low_runs);
    /* merge sort the runs */
    merge_sort(unit);
    /* delete the old meta data */
    LOG("deleting old meta data");
    meta_log->append("delete old meta data");
    meta_log.persist();
    meta_table[index].del(unit->low_runs);
    meta_log->append("add new metadata");
    meta_log.persist();
    if (!(meta_table[index].add(unit->new_runs))) {
        cout << "add meta in C " << index << " error! " << endl; 
        unit->display();
        exit(1);
    }
    meta_log->append("commit compaction");
    meta_log.persist();
    LOG("deleting old data");
    delete unit;
    LOG("stop compactiom");
}

/* copy data between PRuns */
void NVLsm::copy_kv(persistent_ptr<PRun> des_run, int des_i, persistent_ptr<PRun> src_run, int src_i) {
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

/* merge_sort: merge sort the runs during a compaction */
void NVLsm::merge_sort(CompactionUnit * unit) {
    auto meta_up = &(meta_table[unit->index]);
    auto meta_low = &(meta_table[unit->index + 1]);
    /* acquire two locks follow the strick order to avoid deadlock */
    pthread_rwlock_rdlock(&(meta_up->rwlock));
    pthread_rwlock_rdlock(&(meta_low->rwlock));
    //cout << "unit before compaction" << endl;
    //unit->display();
    auto up_run = unit->up_run;
    auto low_runs = unit->low_runs;
    /* if no runs from lower component */ 
    if (low_runs.empty()) {
        unit->new_runs.push_back(up_run);
        //cout << "unit after compaction" << endl;
        //unit->display();
        pthread_rwlock_unlock(&(meta_up->rwlock));
        pthread_rwlock_unlock(&(meta_low->rwlock));
        return;
    }
    //cout << "merging step1" << endl;
    unit->new_runs.emplace_back();
    make_persistent_atomic<PRun>(pmpool, unit->new_runs.back());
    auto new_run = unit->new_runs.back();
    int new_index = 0;
    int up_index = 0;
    int up_len = up_run->size;
    for (auto low_run : low_runs) {
        int low_index = 0;
        int low_len = low_run->size;
        //cout << "merging step1.5" << endl;
        while (low_index < low_len) {
#ifdef AMP
            write_count++;
            if (write_count > write_unit * count_unit) {
                write_unit++;
                cout << "write_count: " << write_count << "times" << endl;
            }
            read_count++;
            if (read_count > read_unit * count_unit) {
                read_unit++;
                cout << "read_count: " << read_count << "times" << endl;
            }
#endif
            if (up_index < up_len) {
                /* if up run has kv pairs */
                auto up_key = up_run->key_entry[up_index].key;
                auto low_key = low_run->key_entry[low_index].key;
                auto res = strncmp(up_key, low_key, KEY_SIZE);
                if (res <= 0) {
                    /* up key is smaller */
                    copy_kv(new_run, new_index, up_run, up_index);
                    up_index++;
                    if (res == 0)
                        low_index++;
                } else {
                    /* low key is smaller */
                    copy_kv(new_run, new_index, low_run, low_index);
                    low_index++;
                }
            } else {
                /* if up key has no kv pairs */
                copy_kv(new_run, new_index, low_run, low_index);
                low_index++;
            }
            new_index++;
            if (new_index == RUN_SIZE) {
                new_run->size = RUN_SIZE;
                unit->new_runs.emplace_back();
                make_persistent_atomic<PRun>(pmpool, unit->new_runs.back());
                new_run = unit->new_runs.back();
                new_index = 0;
            }
        }
    }

    //cout << "merging step2" << endl;
    /* run out of low but up remains */
    while (up_index < up_len) {
        copy_kv(new_run, new_index, up_run, up_index);
        up_index++;
        new_index++;
        if (new_index == RUN_SIZE) {
            new_run->size = RUN_SIZE;
            unit->new_runs.emplace_back();
            make_persistent_atomic<PRun>(pmpool, unit->new_runs.back());
            new_run = unit->new_runs.back();
            new_index = 0;
        }
    }
    /* run out of both up and low but new is not filled up */
    if (new_index > 0 && new_index < RUN_SIZE) {
        new_run->size = new_index;
    } else if (new_index == 0) {
        delete_persistent_atomic<PRun>(unit->new_runs.back());
        unit->new_runs.pop_back();
    }
    //cout << "unit after compaction" << endl;
    //unit->display();
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

/* Seek to locations */
map<KVRange, persistent_ptr<PRun>>::iterator MetaTable::seek(const string& key) {
    KVRange kvrange(key, key);
    auto it = ranges.lower_bound(kvrange);
    while (it != ranges.end() && it != ranges.begin() && it->first.start_key > key)
        it--;
    return it;
}
/* put every run to search stack 
 * this is only called for comp 0*/
void MetaTable::seq_seek(const string& key, map<RunIndex, int>& search_stack) {
    for (auto it = ranges.begin(); it != ranges.end(); it++) {
        RunIndex runIndex;
        runIndex.range = it;
        runIndex.pRun = it->second;
        runIndex.index = it->second->seek(key);
        search_stack.emplace(runIndex, 0);
    }
    return;
}

/* display: show the current elements */
void MetaTable::display() {
    cout << ranges.size() << endl;
    for (auto it : ranges) {
        auto meta_range = it.first;
        meta_range.display();
        cout << "(" << it.second->size << ") ";
        KVRange range;
        it.second->get_range(range);
        if (!(meta_range == range)) {
            cout << "kvrange inconsistent! " << endl;
            cout << "kvrange in meta table: ";
            meta_range.display();
            cout << endl;
            cout << "kvrange in run: ";
            range.display();
            exit(1);
        }
    }
    cout << endl;
    return;
}

/* add: add new runs into meta_table */
bool MetaTable::add(vector<persistent_ptr<PRun>> runs) {
    pthread_rwlock_wrlock(&rwlock);
    if (runs.size() == 0) {
        pthread_rwlock_unlock(&rwlock);
        return true;
    }
    for (auto it : runs) {
        KVRange kvrange;
        it->get_range(kvrange);
        ranges[kvrange] = it;
    }
    pthread_rwlock_unlock(&rwlock);
    return true;
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
    advance(it, next_compact);
    auto run = it->second;
    next_compact++;
    if (next_compact == ranges.size())
        next_compact = 0;
    return run;
}

/* search: get all run within a kv range and store them in runs */
void MetaTable::search(KVRange &kvrange, vector<persistent_ptr<PRun>> &runs) {
    //cout << "range size1: " << ranges.size() << endl;
    if (ranges.empty() || ranges.size() == 0)
        return;
    // binary search for component i > 1
    KVRange end_range(kvrange.end_key, kvrange.end_key);
    //cout << "kvrange:" << kvrange.start_key << "," << kvrange.end_key << endl;
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
    //cout << "search done, low_run " << runs.size() << endl;
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
        int end = run->size - 1;
        auto key_entry = run->key_entry;
        while (start <= end) {
#ifdef AMP
            read_count++;
            if (read_count > read_unit * count_unit) {
                read_unit++;
                cout << "write_count: " << read_count << "times" << endl;
            }
#endif
            int mid = start + (end - start) / 2;
            int res = strncmp(key.c_str(), key_entry[mid].key, KEY_SIZE);
            if (res == 0) {
                val.assign(run->key_entry[mid].p_val, run->key_entry[mid].val_len);
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

/* search a key in component 0*/ 
bool MetaTable::seq_search(const string &key, string &val) {
    //cout << "acquiring the lock" << endl;
    pthread_rwlock_rdlock(&rwlock);
    //cout << "get the lock" << endl;
    for (auto range : ranges) {
        auto run = range.second;
        if (key > range.first.end_key || key < range.first.start_key)
            continue;
        int start = 0;
        int end = run->size - 1;
        auto key_entry = run->key_entry;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            int res = strncmp(key.c_str(), key_entry[mid].key, KEY_SIZE);
            if (res == 0) {
                val.assign(&run->vals[mid], VAL_SIZE);
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
PRun::PRun() {
    //make_persistent_atomic<KeyEntry[RUN_SIZE]>(pmpool, key_entry);
    //make_persistent_atomic<char[VAL_SIZE * RUN_SIZE]>(pmpool, vals);
}
PRun::~PRun() {
    //delete_persistent_atomic<KeyEntry[RUN_SIZE]>(key_entry);
    //delete_persistent_atomic<char[VAL_SIZE * RUN_SIZE]>(vals);
}
/* get kvrange for the current run */
void PRun::get_range(KVRange& range) {
    range.start_key.assign(key_entry[0].key, KEY_SIZE);
    range.end_key.assign(key_entry[size - 1].key, KEY_SIZE);
    return;
}
/* seek to index */
int PRun::seek(const string& key) {
    int start = 0;
    int end = size - 1;
    int mid = 0;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        int res = strncmp(key_entry[mid].key, key.c_str(), KEY_SIZE);
        if (res < 0) 
            start = mid + 1;
        else if (res > 0)
            end = mid - 1;
        else
            break;
    }
    while (strncmp(key_entry[mid].key, key.c_str(), KEY_SIZE) >= 0)
        mid--;
    return mid;
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
