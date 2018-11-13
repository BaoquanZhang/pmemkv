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
    p_run->valid = i;
    p_run.persist();
    delete run;
    /* start to build layers */ 
    //cout << "before building new layers: " << endl;
    //nvlsm->display();
    vector<persistent_ptr<PRun>> runs;
    runs.push_back(p_run);
    nvlsm->compact(0, runs);
    cout << "after building new layers: " << endl;
    nvlsm->display();
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
        //cout << ": searchng " << key << " in compoent " << i << endl;
        meta_table[i].rdlock();
        if (meta_table[i].search(key, val)) {
            value->append(val);
            meta_table[i].unlock();
            return OK;
        }
        meta_table[i].unlock();
    }
    //cout << key << " not found" << endl;
    //exit(-1);
    return NOT_FOUND;
}

KVStatus NVLsm2::Put(const string& key, const string& value) {
    LOG("Put key=" << key.c_str() << ", value.size=" << to_string(value.size()));
    //cout << "Put key=" << key.c_str() << ", value.size=" << to_string(value.size()) << endl;;
    if (mem_table->append(key, value)) {
        /* write buffer is filled up if queue size is larger than 4, wait */
        while (mem_table->getSize() > 0);
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

/* display the meta tables */
void NVLsm2::display() {
    cout << "=========== start displaying meta table ======" << endl;
    for (int i = 0; i < meta_table.size(); i++) {
        cout << "Component " << i << ": " << endl;
        meta_table[i].display();
    }
    cout << "=========== end displaying meta table ========" << endl;
}
/* compact: compact runs to a component 
 * return: the segs need to be merged 
 * */
void NVLsm2::compact(int comp, vector<persistent_ptr<PRun>>& runs) {
    cout << "start to compact" << endl;
    if (runs.size() == 0)
        return;
    if (meta_table.size() == comp) 
        meta_table.emplace_back();
    cout << "start to build layers" << endl;
    meta_table[comp].wrlock();
    for (auto run : runs) {
        meta_table[comp].build_layer(run);
    }
    cout << "finish build layers in component " << comp << endl;
    display();
    vector<persistent_ptr<PRun>> mergeRes;
    meta_table[comp].merge(mergeRes);
    meta_table[comp].unlock();
    if (mergeRes.size() > 0) {
        compact(comp + 1, mergeRes);
    }
    cout << "finish merging in component " << comp << endl;
    return;
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
    return segRanges.size();
}

/* add: add metadata for a segment/segments */
void MetaTable::add(PSegment* seg) {
    KVRange kvRange;
    seg->get_localRange(kvRange);
    segRanges[kvRange] = seg;
    return;
}
void MetaTable::add(vector<PSegment*> segs) {
    for (auto seg : segs) {
        KVRange kvRange;
        seg->get_localRange(kvRange);
        segRanges[kvRange] = seg;
    }
    return;
}
/* del: delete metadata (data if needed) for a seg/segs */
void MetaTable::del(vector<PSegment*> segs) {
    int count = 0;
    for (auto seg : segs) {
        KVRange kvRange;
        seg->get_localRange(kvRange);
        count += segRanges.erase(kvRange);
        delete seg;
    }
    if (count != segs.size()) {
        cout << "delete error: delete " << count << " of " << segs.size();
        exit(-1);
    }
    return;
}
void MetaTable::del(PSegment* seg) {
    int count = 0;
    KVRange kvRange;
    seg->get_localRange(kvRange);
    count += segRanges.erase(kvRange);
    if (count != 1) {
        cout << "delete error: delete " << count << " of 1" << endl;
        cout << "debug info: ";
        cout << "seg to delete: ";
        kvRange.display();
        cout << endl;
        cout << "current component: ";
        display();
        cout << endl;
        exit(-1);
    }
    delete seg;
    return;
}
/* getMerge: get the segment needs to be merged based on the current depths
 * */
PSegment* MetaTable::getMerge() {
    cout << "getting seg to merge " << endl;
    int len = segRanges.size();
    auto it = segRanges.begin();
    int cur = 0;
    if (next_compact < len) {
        advance(it, cur);
        cur = next_compact;
    }
    next_compact = cur;
    bool end_flag = false;
    while (it->second->depth < MAX_DEPTH) {
        //cout << "check seg " << cur << endl;
        if (cur == next_compact && end_flag) {
            cout << "no seg to merge" << endl;
            return NULL;
        }
        //cout << "checking seg index " << cur << endl;
        it++;
        cur++;
        if (it == segRanges.end()) {
            cur = 0;
            it = segRanges.begin();
            end_flag = true;
        }
    }
    next_compact = cur + 1;
    cout << "merging seg is ";
    it->second->display();
    cout << endl;
    return it->second;
}
/* copy_key: copy kv from src run to des run */
void MetaTable::copy_kv(persistent_ptr<PRun> des_run, int des_i, 
        persistent_ptr<PRun> src_run, int src_i) {
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
/* merge: merge all layers of a seg 
 * @param: seg -- the segment to merge 
 *         runs -- merge results
 * */
/* copy data between PRuns */
void MetaTable::merge(vector<persistent_ptr<PRun>>& runs) {
    auto seg = getMerge();
    if (seg == NULL) {
        return;
    }
    cout << "start to merge seg ";
    seg->display();
    cout << endl;
    persistent_ptr<PRun> new_run;
    make_persistent_atomic<PRun>(pmpool, new_run);
    int new_index = 0;
    RunIndex runIndex;
    long count = 0;
    char* last_key = NULL;
    vector<persistent_ptr<PRun>> to_del;
    seg->seek_begin();
    while (seg->next(runIndex)) {
        auto new_entry = runIndex.pRun->key_entry[runIndex.index];
        char* new_key = new_entry.key;
        //cout << "iterating: " << new_key << endl;
        if (last_key == NULL || strncmp(last_key, new_key, KEY_SIZE) < 0) {
            count++;
            copy_kv(new_run, new_index, runIndex.pRun, runIndex.index);
            last_key = new_run->key_entry[new_index].key;
            new_index++;
            if (new_index == RUN_SIZE) {
                new_run->size = new_index;
                new_run->valid = new_index;
                runs.push_back(new_run);
                make_persistent_atomic<PRun>(pmpool, new_run);
                new_index = 0;
            }
        }
        //runIndex.pRun->display();
        //cout << "valid from " << runIndex.pRun->valid;
        runIndex.pRun->valid--;
        new_entry.valid = false;
        //cout << " to " << runIndex.pRun->valid << endl;
        if (runIndex.pRun->valid == 0) {
            to_del.push_back(runIndex.pRun);
        }
    }
    if (new_index > 0) {
        new_run->size = new_index;
        new_run->valid = new_index;
        runs.push_back(new_run);
    } else {
        delete_persistent_atomic<PRun>(new_run);
    }
    cout << "merge results: " << runs.size() << "runs and " << count << "kvs" << endl;
    for (auto run : runs) {
        run->display();
    }
    cout << endl;
    cout << "max stack: " << seg->max_stack << endl;
    del(seg);
    cout << "delete " << to_del.size() << " runs" << endl; 
    for (auto run : to_del) {
        //delete_persistent_atomic<PRun>(run);
    }
    return;
}
/* display: display the ranges in the current component */
void MetaTable::display() {
    //cout << segRanges.size() << " ranges." << endl;
    for (auto segRange : segRanges) {
        segRange.first.display();
        auto pRun = segRange.second->pRun;
        cout << "(" << pRun->size << ",";
        cout << pRun->valid << ")";
        cout << " ";
        KVRange kvRange;
        segRange.second->get_localRange(kvRange);
        if (!(kvRange == segRange.first)) {
            cout << "inconsistency detected!" << endl;
            cout << "metadata: "; 
            segRange.first.display();
            cout << endl;
            cout << "data: ";
            kvRange.display();
            cout << endl;
            exit(-1);
        }
    }
    cout << endl;
    return;
}
/* lock/unlock: lock operations of metaTable */
void MetaTable::rdlock() {
    pthread_rwlock_rdlock(&rwlock);
    return;
}
void MetaTable::wrlock() {
    pthread_rwlock_wrlock(&rwlock);
    return;
}
void MetaTable::unlock() {
    pthread_rwlock_unlock(&rwlock);
    return;
}
/* search: search a key / overlapped ranges in a component */
bool MetaTable::search(const string& key, string& value) {
    KVRange kvRange;
    kvRange.start_key = key;
    kvRange.end_key = key;
    vector<PSegment*> segs;
    search(kvRange, segs);
    //if (segs.size() == 0)
    //    cout << "No range found for key: " << key << endl; 
    for (auto seg : segs) {
        if (seg->search(key, value)) {
            return true;
        }
    }
    return false;
}
void MetaTable::search(KVRange& kvRange, vector<PSegment*>& segs) {
    if (segRanges.empty() || segRanges.size() == 0)
        return;
    // binary search for component i > 1
    KVRange end_range(kvRange.end_key, kvRange.end_key);
    //cout << "kvrange:" << kvRange.start_key << "," << kvRange.end_key << endl;
    //cout << "current commopont: ";
    //display();
    auto it_high = segRanges.upper_bound(end_range);
    if (it_high == segRanges.end()) {
        /* every start key of range is smaller than the target key 
         * then go to the last element 
         * */
        it_high--; 
    }
    while (kvRange.start_key <= it_high->first.end_key) {
        //cout << "find a range for key: " << kvRange.start_key << endl;
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
PSegment* create_pseg(persistent_ptr<PRun> run, int start, int end, int depth) {
    auto new_seg = new PSegment(run, start, end);
    new_seg->depth = depth;
    return new_seg;
}
void MetaTable::do_build(vector<PSegment*>& overlapped_segs, persistent_ptr<PRun> run) {
    cout << "start to build a new layer" << endl;
    if (overlapped_segs.size() == 0 || run == NULL)
        return;
    vector<PSegment*> new_segs;
    PSegment* new_seg = NULL;
    int up_right = 0;
    int up_left = 0;
    int btm_left = 0;
    int btm_right = 0;
    int max_depth = 1;
    auto begin_seg = overlapped_segs.front();
    btm_left = begin_seg->start;
    btm_right = btm_left;
    auto up_begin = run->key_entry[0].key;
    /* step 1: skip the non-overlapped keys at the beginning */
    while (btm_right <= begin_seg->end) {
        auto btm_end = begin_seg->get_key(btm_right);
        if (strncmp(btm_end, up_begin, KEY_SIZE) >= 0)
            break;
        btm_right++;
    }
    /* check if the skipped keys are enough to build a new seg */
    if (btm_right > btm_left) {
        new_seg = create_pseg(begin_seg->pRun, btm_left, btm_right - 1, begin_seg->depth);
        new_segs.push_back(new_seg);
        btm_left = btm_right;
    }
    /* step 2: add the overlap segs */
    for (int i = 0; i < overlapped_segs.size(); i++) {
        auto overlap_seg = overlapped_segs[i];
        if (up_right == run->size) {
            /* almost impossible */
            new_segs.push_back(overlap_seg);
            continue;
        }
        if (i >= 1) {
            /* we need to remember if we skipped some keys at the beginning*/
            btm_left = overlap_seg->start;
            btm_right = btm_left;
        }
        while (up_right < run->size && btm_right <= overlap_seg->end) {
            auto btm_key = overlap_seg->pRun->key_entry[btm_right].key;
            auto up_key = run->key_entry[up_right].key;
            if (strncmp(up_key, btm_key, KEY_SIZE) <= 0) {
                run->key_entry[up_right].next_key = btm_right;
                run->key_entry[up_right].next_run = overlap_seg->pRun;
                int cur_depth = overlap_seg->depth + 1;
                max_depth = max_depth > cur_depth ? max_depth : cur_depth;
                up_right++;
            } else {
                btm_right++;
            }
        }
    }
    auto last_seg = overlapped_segs.back();
    auto btm_end = last_seg->get_key(btm_right); 
    /* if we have up keys remain, we will run out of btm key */
    if (up_right < run->size) {
        while (up_right < run->size) {
            auto cur_up = run->key_entry[up_right].key;
            if (strncmp(cur_up, btm_end, KEY_SIZE) > 0)
                break;
            /* if the subsequent key are in the range */
            run->key_entry[up_right].next_key = last_seg->end;
            run->key_entry[up_right].next_run = last_seg->pRun;
            int cur_depth = last_seg->depth + 1;
            max_depth = max_depth > cur_depth ? max_depth : cur_depth;
            up_right++;
        } 
    } else {
        /* if we have btm keys remain */
        if (btm_right <= last_seg->end) {
            auto up_end = run->key_entry[run->size - 1].key; 
            while (btm_right >= 0) {
                auto btm_key = last_seg->pRun->key_entry[btm_right].key;
                if (strncmp(btm_key, up_end, KEY_SIZE) <= 0)
                    break;
                btm_right--;
            }
        }
    }
    new_seg = create_pseg(run, up_left, run->size - 1, max_depth);
    new_segs.push_back(new_seg);
    /* step 3 add the non-overlapped keys at the end */
    if (btm_right < last_seg->end) {
        new_seg = create_pseg(last_seg->pRun, btm_right + 1, last_seg->end, last_seg->depth);
        new_segs.push_back(new_seg);
        btm_right = last_seg->end;
    }
    /* built a new layer */
    cout << "up run: ";
    run->display();
    cout << "overlapped segs: ";
    for (auto overlap_seg : overlapped_segs) {
        overlap_seg->display();
    }
    cout << "new layers: ";
    for (auto new_seg : new_segs) {
        new_seg->display();
    }
    cout << endl;
    del(overlapped_segs);
    add(new_segs);
    cout << "finish building a new layer" << endl;
    return;
}
void MetaTable::build_layer(persistent_ptr<PRun> run) {
    vector<PSegment*> overlapped_segs;
    KVRange kvRange;
    PSegment* seg = new PSegment(run, 0, run->size - 1);
    seg->get_localRange(kvRange);
    search(kvRange, overlapped_segs);
    if (overlapped_segs.size() == 0) {
        seg->depth = 1;
        add(seg);
    } else {
        cout << "have overlaps do build" << endl;
        do_build(overlapped_segs, run);
    }
    return;
}
/* ###################### PRun ######################### */
PRun::PRun(): size(0) {
}
PRun::~PRun() {
}
/* display: display the range of the current PRun */ 
void PRun::display() {
    KVRange kvRange;
    get_range(kvRange);
    kvRange.display();
    return;
}
/* get kvrange for the current run */
void PRun::get_range(KVRange& range) {
    range.start_key.assign(key_entry[0].key, KEY_SIZE);
    range.end_key.assign(key_entry[size - 1].key, KEY_SIZE);
    return;
}

/* find_key: looking for a key in a run within the start -- end
 * return: 0 -- we find the key
 *         1 -- can not find and the mid is larger than key
 *         -1 -- can not find and the mid is smaller than the key
 * */
int PRun::find_key(const string& key, string& value, int left, int right, int& mid) {
    //cout << key << ": find key in the prun: ";
    //display();
    //cout << " left = " << left << ", right = " << right;
    //cout << endl;
    if (left > right) {
        cout << "error happends! left > right when binary searching" << endl;
        exit(-1);
    }
    int res = 0;
    while (left <= right) {
        mid = left + (right - left) / 2;
        res = strncmp(key_entry[mid].key, key.c_str(), KEY_SIZE);
        if (res == 0) {
            value.assign(key_entry[mid].p_val, key_entry[mid].val_len);
            return res;
        } else if (res > 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return res;
}

/* ##################### PSegment ############################################# */
PSegment::PSegment(persistent_ptr<PRun> p_run, size_t start_i, size_t end_i)
    : pRun(p_run), start(start_i), end(end_i), depth(0), max_stack(0) {
}
PSegment::~PSegment() {
}
/* search: search a key in a seg 
 * para: key - key to search
 *       value - the value of the key
 * return: true if we find the key
 * */
bool PSegment::search(const string& key, string& value) {
    auto cur_run = pRun;
    int left = start;
    int right = end;
    int mid = 0;
    while (cur_run) {
        auto res = cur_run->find_key(key, value, left, right, mid);
        if (res == 0)
            return true;
        else {
            persistent_ptr<PRun> left_run;
            persistent_ptr<PRun> right_run;
            auto next_run = cur_run->key_entry[mid].next_run;
            auto next_key = cur_run->key_entry[mid].next_key;
            if (next_run == NULL)
                break;
            if (res < 0) {
                left = next_key;
                left_run = next_run;
                right = left;
                right_run = left_run;
                if (mid < cur_run->size - 1) {
                    right = cur_run->key_entry[mid + 1].next_key;
                    right_run = cur_run->key_entry[mid + 1].next_run;
                }
            } else {
                right = next_key;
                right_run = next_run;
                left = right;
                left_run = right_run;
                if (mid > 0) {
                    left = cur_run->key_entry[mid - 1].next_key;
                    left_run = cur_run->key_entry[mid - 1].next_run;
                }
            }
            /* check if left_run and right_run are the same */
            if (left_run == right_run) {
                cur_run = left_run;
            } else {
                int left_end = left_run->size - 1;
                if (strncmp(key.c_str(), left_run->key_entry[left_end].key, KEY_SIZE) > 0) {
                    cur_run = right_run;
                    left = 0;
                } else {
                    cur_run = left_run;
                    right = left_run->size - 1;
                }
            }
        }
    }
    return false;
}

/* get_range: get the kvrange of the top layer */
void PSegment::get_localRange(KVRange& kvRange) {
    auto key_entry = pRun->key_entry;
    kvRange.start_key.assign(key_entry[start].key, KEY_SIZE);
    kvRange.end_key.assign(key_entry[end].key, KEY_SIZE);
    return;
}
/* seek: seek a key and build the search stack */
void PSegment::seek_begin() {
    //cout << "seek to begin of a segment" << endl;
    //cout << "put " << pRun->key_entry[start].key << "into stack" << endl;
    RunIndex runIndex(pRun, start, 1);
    search_stack.emplace(runIndex, 1);
    max_stack = 1;
    return;
}
/* next: get the next key
 * description: get the top element from search stack
 *              and put right/next key into the stack
 * */
void PSegment::check_push(map<RunIndex, int>& search_stack, RunIndex runIndex) {
    auto cur_entry = runIndex.pRun->key_entry[runIndex.index];
    if (cur_entry.valid 
            && strncmp(get_key(end), cur_entry.key, KEY_SIZE) >= 0) {
        search_stack[runIndex] = 1;
    }
    /* if it is a new run, we need to check the previous keys */
    if (run_set.count(runIndex.pRun) == 0) {
        run_set.emplace(runIndex.pRun);
        int cur_index = runIndex.index;
        char* cur_key = runIndex.pRun->key_entry[cur_index].key;
        char* start_key = pRun->key_entry[start].key;
        while (cur_index >= 0 && runIndex.pRun->key_entry[cur_index].valid
                && strncmp(start_key, cur_key, KEY_SIZE) <= 0) {
            runIndex.index = cur_index;
            search_stack[runIndex] = 1;
            cur_index--;
        }
    }
    return;
}
bool PSegment::next(RunIndex& runIndex) {
    if (search_stack.size() == 0)
        return false;
    runIndex = search_stack.begin()->first;
    search_stack.erase(search_stack.begin());
    auto cur_run = runIndex.pRun;
    auto cur_index = runIndex.index;
    auto cur_dep = runIndex.depth;
    auto cur_key = cur_run->key_entry[cur_index].key;
    if (cur_index < cur_run->size - 1) {
        RunIndex runIndex(cur_run, cur_index + 1, cur_dep);
        check_push(search_stack, runIndex);
    }
    auto next_run = cur_run->key_entry[cur_index].next_run;
    auto next_index = cur_run->key_entry[cur_index].next_key;
    if (next_run != NULL) {
        RunIndex runIndex(next_run, next_index, cur_dep + 1);
        check_push(search_stack, runIndex);
    }
    int size = search_stack.size();
    max_stack = max(max_stack, size);
    return true;
}

/* get_key: return a key by the index */
char* PSegment::get_key(int index) {
    return pRun->key_entry[index].key;
}
/* display: display the key range */
void PSegment::display() {
    KVRange kvRange;
    get_localRange(kvRange);
    kvRange.display();
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
