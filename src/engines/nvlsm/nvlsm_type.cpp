#include "nvlsm_types.h"
#include <algorithm>
#include <mutex>
#include <iostream>

using namespace std;
using namespace nv_lsm;

pool<LSM_Root> pmpool;
size_t pmsize;

/* This function will run in a sub-thread
 * 1. keep checking persist_queue with an infinite loop
 * 2. copy array from persist_queue to level0
 * */
void * persist_memTable(void * virtual_p) 
{
    PlsmStore * plsmStore = (PlsmStore *) virtual_p;
    auto p_memTable = plsmStore->memTable;
    auto p_queue = &(p_memTable->persist_queue);
    auto p_mutex = &(p_memTable->queue_mutex);
    auto p_cond = &(p_memTable->queue_cond);
    auto level0 = plsmStore->level_head;
    auto p_start_lock = &(plsmStore->start_persist_mutex);
    LOG("Persist thread start");
    while (true) {
        
        pthread_mutex_lock(p_mutex);
        while (p_queue->empty()) {

            LOG("Persisting thread waiting candidates");
            pthread_cond_wait(p_cond, p_mutex);
            pthread_mutex_unlock(p_mutex);

            /* Stop when main thread ask
             * we assume that main thread will wait the pesist 
             * thread finish its job before stop it 
             * */
            pthread_mutex_lock(p_start_lock);
            if (!plsmStore->start_persist) {
                pthread_mutex_unlock(p_start_lock);
                LOG("Persisting thread exits");
                return NULL;
            } else {
                pthread_mutex_unlock(p_start_lock);
            }
        }

        /* start to copy data
         * 1. Create a new run at the end of Level 0 
         * 2. copy kvs into new run 
         * */
        LOG("persist thread copying kv pairs");
        auto p_queue_range = &(plsmStore->metaTable->queue_range);
        LOG("Persisting <" + 
                p_queue_range->front()->start_key + "," + 
                p_queue_range->front()->end_key + ">");
        persistent_ptr<Run> new_run;
        pthread_mutex_lock(p_mutex);
        LOG("queue front: " << p_queue->front()->size());
        make_persistent_atomic<Run>(pmpool, new_run, p_queue->front());
        pthread_mutex_unlock(p_mutex);

        level0->run_count = level0->run_count + 1; 
        /* add new range for memtadata of level 0 */
        vector< pair<KeyRange, persistent_ptr<Run> > > add_range;
        add_range.push_back(make_pair(*(p_queue_range->front()), new_run));
        plsmStore->metaTable->meta_update(0, 1, add_range);
        LOG("Persisting thread add range to level 1 done");
        /* pop the first memtable from persist queue */
        pthread_mutex_lock(p_mutex);
        delete p_queue->front();
        p_queue->pop_front();
        p_queue_range->pop_front();
        pthread_mutex_unlock(p_mutex);
        plsmStore->buffer_log->reclaim(RUN_SIZE);

        if (level0->run_count > plsmStore->level_base) {
            pthread_mutex_lock(&(plsmStore->planner_mutex));
            plsmStore->compact_planner(0);
        }
    }
    LOG("Persist thread exit");

}

/*normal_compaction: traditional compaction function
 * 1. get first run from upper level
 * 2. break related runs from lower level (put to broken_list)
 * 3. read and merge
 * 4. update meta data
 * 5. delete old runs and add new runs
 * */
void display_opvec(vector< pair<KeyRange, persistent_ptr<Run> > > &op_vec) 
{
    for (auto it : op_vec) {
        KeyRange key_range = it.first;
        auto p_run = it.second;
        KeyRange real_range = p_run->get_KeyRange();
        cout << "<" << key_range.start_key << "," << key_range.end_key << ">";
        cout << "(" << real_range.start_key << "," << real_range.end_key << ")";
    }

    cout << endl;
}

void normal_compaction(void * unit) 
{
    LOG("Start a normal compaction");
    auto p_unit = (CompactionUnit *) unit;
    auto plsmStore = p_unit->plsmStore;
    int level_id = p_unit->level_id;
    auto p_up_runs = p_unit->up_runs;
    auto p_low_runs = p_unit->low_runs;
    vector< pair< KeyRange, persistent_ptr<Run> > > new_run;
    
    if (p_up_runs.empty()) {
        /* do nothing if up level is empty */
        LOG("up run is empty");
        return;
    }

    if (p_low_runs.empty()) {
        /* only up level has runs -- no need to merge */
        LOG("low run is empty");
        new_run = p_up_runs;
    } else {
        /* merge sort */
        LOG("Merge sorting");
        new_run = plsmStore->merge_sort(p_unit);
    }

    /* add runs to next level */
    persistent_ptr<Level> p_level = plsmStore->find_level(level_id);
    LOG("Find current level by id: " << p_level->level_id);
    if (p_level->next_level == NULL) {
        make_persistent_atomic<Level>(pmpool, p_level->next_level, level_id + 1);
    }

    /* update metadata first */ 
    // 1. delete old metadata for up level
    if (DO_LOG) {
        cout << "delete range for up level: ";
        display_opvec(p_up_runs);
    }

    plsmStore->metaTable->meta_update(level_id, 0, p_up_runs);
    p_level->run_count = p_level->run_count - p_up_runs.size();

    // 2.delete old metadata for low level
    if (DO_LOG) {
        cout << "delete range for low level: ";
        display_opvec(p_low_runs);
    }
    p_level->next_level->run_count = p_level->next_level->run_count - p_low_runs.size();
    plsmStore->metaTable->meta_update(level_id + 1, 0, p_low_runs);

    // 3. add new metatdata for low level
    if (DO_LOG) {
        cout << "add range for low level: ";
        display_opvec(new_run);
    }
    plsmStore->metaTable->meta_update(level_id + 1, 1, new_run);
    p_level->next_level->run_count = p_level->next_level->run_count + new_run.size();
    
    /* then update data */
    // 1. delete old runs of upper level
    p_unit->del_runs(0);
    // 2. delete old runs of lower level
    p_unit->del_runs(1);
    delete p_unit;

    int next_count = p_level->next_level->run_count;
    int level_base = plsmStore->level_base;
    int level_ratio = plsmStore->level_ratio;
    if (next_count > p_level->next_level->level_id * level_base * level_ratio)
        plsmStore->compact_planner(level_id + 1);
    if (level_id == 0)
        pthread_mutex_unlock(&(plsmStore->planner_mutex));
    return;
}

/* Implementations for PlsmStore */

PlsmStore::PlsmStore (const string& path, const size_t size, int level_base_val, int level_ratio_val) 
    : level_base(level_base_val), 
    level_ratio(level_ratio_val) 
{
    /* Create mem structures */
    memTable = new MemTable();
    metaTable = new MetaTable();
    metaTable->level_range.resize(7); // we reserve metadata for 7 levels
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
    /* Create persist log */
    make_persistent_atomic<Log>(pmpool, buffer_log, RUN_SIZE * 5);

    /* Create Level 0 */
    make_persistent_atomic<Level>(pmpool, level_head, 0);
    level_tail = level_head;
    pthread_mutex_init(&planner_mutex, NULL);
    /* Start persist thread for copy kv pairs from memtable to persistent levels */
    pthread_mutex_init(&start_persist_mutex, NULL);
    pthread_mutex_lock(&start_persist_mutex);
    start_persist = true;
    pthread_mutex_unlock(&start_persist_mutex);
    
    if (pthread_create(&persist_thread_id, NULL, persist_memTable,  this) < 0) {
        cout << "create persisting thread error!" << endl;
        exit(-1);
    }
    /* create thread pool for compaction */
    LOG("Creating thread pool for compaction");
    thread_pool = new ThreadPool(POOL_SIZE); // current POOL_SIZE is 1
    if (thread_pool->initialize_threadpool() == -1) {
        cout << "Fail to initialize the thread pool!" << endl;
        exit(-1);
    }
}

PlsmStore::~PlsmStore()
{
    while (true) {
        pthread_mutex_lock(&(memTable->queue_mutex));
        if (memTable->persist_queue.empty()) {
            pthread_mutex_unlock(&(memTable->queue_mutex));
            LOG("Stop persisting thread");
            break;
        } else {
            pthread_mutex_unlock(&(memTable->queue_mutex));
            LOG("Waiting persisting thread");
            sleep(1);
        }
    }
    
    pthread_mutex_lock(&start_persist_mutex);
    start_persist = false;
    pthread_mutex_unlock(&start_persist_mutex);
    pthread_cond_signal(&(memTable->queue_cond));
    thread_pool->destroy_threadpool();
    delete memTable;
    delete metaTable;
    LOG("Closing persistent pool");
    pmpool.close();
    LOG("Closed ok");
}
/* find_level: find level pointer by id
 * @ level_id: given id
 * return: corresponding pointer
 * */
persistent_ptr<Level> PlsmStore::find_level(int level_id) 
{
    auto current_level = level_head;
    while (current_level) {
        if (current_level->level_id == level_id)
            return current_level;
        current_level = current_level->next_level;
    }

    return NULL;
}

/* compare two kv pairs */
bool PlsmStore::compare(pair<string, string> kv1, pair<string, string> kv2) 
{
    return kv1.first < kv2.first;
}

/* insert a key value pair to memory buffer */
void PlsmStore::put(string key, string  value) 
{
    if (memTable->buffer == NULL) {
        memTable->buffer = new vector< pair<string, string> >();
        memTable->buffer->reserve(RUN_SIZE);
        metaTable->buffer_range = new KeyRange(KEY_SIZE);
    }
    
    /* put kv pair into memory buffer */
    buffer_log->add_entry(key, value);
    memTable->buffer->insert(memTable->buffer->end(), make_pair(key, value));
    // update key range for mem buffer
    int len = memTable->buffer->size();
    auto p_buffer_range = metaTable->buffer_range;
    if (len == 1) {
        p_buffer_range->start_key = key;
        p_buffer_range->end_key = key;
    } else {
        if (p_buffer_range->start_key.compare(key) > 0)
            p_buffer_range->start_key = key;
        else if (p_buffer_range->end_key.compare(key) < 0)
            p_buffer_range->end_key = key;
    }

    if (len == RUN_SIZE) {
        /* put memory buffer into persist queue (after sort) 
         * and allocate new buffer */
        sort(memTable->buffer->begin(), memTable->buffer->end(), compare);
        
        pthread_mutex_lock(&(memTable->queue_mutex));
        memTable->persist_queue.insert(memTable->persist_queue.end(), memTable->buffer);
        metaTable->queue_range.insert(metaTable->queue_range.end(), metaTable->buffer_range);
        pthread_mutex_unlock(&(memTable->queue_mutex));
        pthread_cond_signal(&(memTable->queue_cond));

        memTable->buffer = new vector< pair<string, string> >();
        memTable->buffer->reserve(RUN_SIZE);
        metaTable->buffer_range = new KeyRange(KEY_SIZE);
    }
    return;
}
/* get: find a given key 
 * @key: given key
 * return: the corresponding value
 * */
string PlsmStore::get(string key) 
{
    /* check memory buffer 
     * we do not add lock for buffer 
     * */
    //LOG("Searching memTable");
    auto buffer = memTable->buffer;
    auto buf_range = metaTable->buffer_range;
    if (buf_range->start_key <= key && buf_range->end_key >= key) {
        for (auto it = buffer->begin(); it != buffer->end(); it++) {
            if (it->first == key) 
                return it->second;
        }
    }
    /* check persistent memory 
     * */
    //LOG("Searching persist queue");
    pthread_mutex_lock(&memTable->queue_mutex);
    auto range = metaTable->queue_range;
    auto queue_it = memTable->persist_queue.rbegin();
    for (auto range_it = range.rbegin(); range_it != range.rend(); range_it++) {
        if ((*range_it)->start_key <= key && (*range_it)->end_key >= key) {
            auto vec = *(queue_it);
            int start = 0;
            int end = vec->size() - 1;
            while (start <= end) {
                int mid = start + (end - start) / 2;
                if (vec->at(mid).first > key)
                    end = mid - 1;
                else if (vec->at(mid).first < key)
                    end = mid + 1;
                else if (vec->at(mid).first == key) 
                    return vec->at(mid).second;
            }
        }
        queue_it++;
    } 
    pthread_mutex_unlock(&memTable->queue_mutex);
    /* check levels */
    //LOG("Searching levels");
    vector< pair< KeyRange, persistent_ptr<Run> > > res;
    auto current_level = level_head;
    while (current_level) {
        //LOG("Searching Level " << current_level->level_id);
        res.clear();
        metaTable->meta_search(res, current_level->level_id, key);
        //LOG("Find related reanges");
        for (auto it : res) {
            auto p_kv = it.second->single_search(const_cast<char*>(key.c_str()));
            if (p_kv) 
                return p_kv->value;
        }
        current_level = current_level->next_level;
    }

    return "";
}
vector< pair<string, string> > PlsmStore::range(string start, string end) 
{
    // to-do
}

/* utility for displaying CompactionUnit */
void display_unit(CompactionUnit * unit) 
{
    cout << "Displaying unit" << endl;
    auto p_up = unit->up_runs;
    cout << "up_runs: ";
    for (auto it : p_up){
        cout << "<" << it.first.start_key << "," << it.first.end_key << "> ";
    }
    cout << endl;

    auto p_low = unit->low_runs;
    cout << "low runs: ";
    for (it : p_low) {
        cout << "<" << it.first.start_key << "," << it.first.end_key << "> ";
    }
    cout << endl;
}

/* compact_planner: function to start compactions
 * 1. start a compaction at level i (start from 0)
 *     a. select runs from level i
 *     b. select runs from level i + 1
 *     c. construct a CompactionUnit
 *     d. start a compaction thread
 * 2. check if level i+1 needs a compaction.
 *     a. quit it no compaction needed
 *     b. goto 1 if needs to compact
 *     */
void PlsmStore::compact_planner(int level_id)
{
    LOG("Planning a compaction");
    auto p_range = &(metaTable->level_range[level_id]);
    CompactionUnit * unit = new CompactionUnit(level_id);
    unit->plsmStore = this;
    // select compaction candidate from current level
    metaTable->select_up_run(level_id, unit);

    // select compaction candidate from next level
    LOG("Lower level is not empty");
    for (auto it = unit->up_runs.begin(); it != unit->up_runs.end(); it++) {
        metaTable->meta_search(unit->low_runs, level_id + 1, it->first);
    }

    LOG("Compacting from level " << level_id );
    if (DO_LOG)
        display_unit(unit);
    Task * compact_task = new Task(&normal_compaction, (void *) unit);
    thread_pool->add_task(compact_task);
}

/* merge_sort: merge sort runs from two levels
 * typically upper level will only have one run
 * */
vector< pair< KeyRange, persistent_ptr<Run> > > PlsmStore::merge_sort(CompactionUnit* p_unit) 
{
    int level_id = p_unit->level_id;
    auto p_up_runs = p_unit->up_runs;
    auto p_low_runs = p_unit->low_runs;
    persistent_ptr<Run> new_run;
    make_persistent_atomic<Run>(pmpool, new_run);
    vector< pair< KeyRange, persistent_ptr<Run> > > new_run_vec;

    auto up_it = p_up_runs.begin();
    auto low_it = p_low_runs.begin();

    auto up_array = up_it->second->local_array;
    auto low_array = low_it->second->local_array;
    auto new_array = new_run->local_array;

    int pos = 0, up_pos = 0, low_pos = 0;
    /* both level have runs -- merge sort */
    while (up_it != p_up_runs.end() || low_it != p_low_runs.end()) {
        if (up_it != p_up_runs.end() && low_it != p_low_runs.end()) {
            if (strcmp(low_array[low_pos].key, up_array[up_pos].key) < 0) {
                strcpy(new_array[pos].key, low_array[low_pos].key);
                strcpy(new_array[pos].value, low_array[low_pos].value);
                low_pos++;
                pos++;
            } else if (strcmp(low_array[low_pos].key, up_array[up_pos].key) > 0) {
                strcpy(new_array[pos].key, up_array[up_pos].key);
                strcpy(new_array[pos].value, up_array[up_pos].value);
                up_pos++;
                pos++;
            } else {
                low_pos++;
            }
        } else if (up_it != p_up_runs.end()) {
            strcpy(new_array[pos].key, up_array[up_pos].key);
            strcpy(new_array[pos].value, up_array[up_pos].value);
            up_pos++;
            pos++;
        } else if (low_it != p_low_runs.end()) {
            strcpy(new_array[pos].key, low_array[low_pos].key);
            strcpy(new_array[pos].value, low_array[low_pos].value);
            low_pos++;
            pos++;
        }

        if (up_pos == RUN_SIZE) {
            /* upper level will only have one run to compact */
            up_it++;
            up_pos = 0;
            if (up_it != p_up_runs.end()) {
                up_array = up_it->second->local_array;
            } 
        }

        if (low_pos == RUN_SIZE) {
            low_it++;
            low_pos = 0;
            if (low_it != p_low_runs.end()) {
                low_array = low_it->second->local_array;
            }
        }

        if (pos == RUN_SIZE) {
            new_run->len = RUN_SIZE;
            if (up_it != p_up_runs.end() || low_it != p_low_runs.end()) {
                pos = 0;
                KeyRange range(new_run->local_array[0].key, new_run->local_array[RUN_SIZE - 1].key);
                new_run_vec.push_back(make_pair(range, new_run));
                make_persistent_atomic<Run>(pmpool, new_run);
                new_array = new_run->local_array;
            }
        }
    }
    
    new_run->len = pos;
    KeyRange range(new_run->local_array[0].key, new_run->local_array[pos - 1].key);
    new_run_vec.push_back(make_pair(range, new_run));

    return new_run_vec;
}


void PlsmStore::lazy_compaction(void * unit) 
{
    // to-do
}

/* Implementations for MemTable */
MemTable::MemTable() : queue_count(0) 
{
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_cond_init(&queue_cond, NULL);
}

MemTable::~MemTable() 
{
    pthread_cond_destroy(&queue_cond);
    pthread_mutex_destroy(&queue_mutex);
    delete(buffer);
}

/* Implementations for MetaTable */
MetaTable::MetaTable() 
{
    pthread_rwlock_init(&level_lock, NULL);
}
MetaTable::~MetaTable()
{
    pthread_rwlock_destroy(&level_lock);
}

/* Display the ranges for a given level 
 * @level_id: given level id
 * */
void MetaTable::display_range(int level_id) {
    cout << "level " << level_id << " ranges: ";
    for (auto it : level_range[level_id]) {
        cout << "<" << it.first.start_key << "," << it.first.end_key << "> ";
    }
    cout << endl;
}

/* select_up_run: select the compaction candidate from current level 
 * @ level_id: the id of current level
 * Description: select key range based on round robin
 * */
void MetaTable::select_up_run(int level_id, CompactionUnit * unit)
{
    // if current level is empty
    // almost impossible
    if (level_range.size() < level_id + 1 || level_range[level_id].size() == 0) 
        return;

    if (last_comp.size() < level_id + 1) {
        // if this is the first compaction in this level
        // select the smalleset key range
        unit->up_runs.push_back(level_range[level_id][0]);
        last_comp.push_back(0);
    } else {
        // select the next range
        auto last_run = last_comp[level_id];
        int next_run = (last_run + 1) % level_range[level_id].size();
        unit->up_runs.push_back(level_range[level_id][next_run]);
        last_comp[level_id] = last_run + 1;
    }
}

/* meta_search: search meta data for a given key/key range
 * @level_id: target postion to search a key, including levels (>0)
 * @key: key to search
 * return: pointer to runs that potentially includes the key*/
void MetaTable::meta_search(vector< pair< KeyRange, persistent_ptr<Run> > > &res, int level_id, string &key) 
{
    pthread_rwlock_rdlock(&level_lock);
    auto p_ranges = &level_range[level_id];
    int start = 0;
    int end = p_ranges->size() - 1;
    
    while (start <= end) {
        if (level_id == 0) {
            string start_key = p_ranges->at(start).first.start_key;
            string end_key = p_ranges->at(start).first.end_key;
            if (key >= start_key && key <= end_key)
                res.push_back(p_ranges->at(start));
            start++;
        } else {
            int mid = start + (end - start) / 2;
            string start_key = p_ranges->at(mid).first.start_key;
            string end_key = p_ranges->at(mid).first.end_key;
            if (key >= start_key && key <= end_key) {
                res.push_back(p_ranges->at(mid));
                break;
            } else if (key < start_key) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
    }
    pthread_rwlock_unlock(&level_lock);
    return;
}

void MetaTable::meta_search(vector< pair< KeyRange, persistent_ptr<Run> > > &res, int level_id, KeyRange &key_range)
{
    LOG("Searching meta data for keyrange: <" << 
            key_range.start_key + "," + key_range.end_key + ">");
    if (DO_LOG) {
        display_range(level_id);
    }
    pthread_rwlock_rdlock(&level_lock);
    auto p_ranges = &level_range[level_id];
    int start = 0;
    int end = p_ranges->size() - 1; 
    int mid = 0;
    while (start <= end) {
        mid = start + (end - start) / 2;
        string start_key = p_ranges->at(mid).first.start_key;
        string end_key = p_ranges->at(mid).first.end_key;
        if (key_range.start_key == end_key) {
            break;
        } else if (key_range.start_key > end_key) {
            start = mid + 1;
        } else if (key_range.start_key < end_key) {
            end = mid - 1;
        }
    }
    while (mid < p_ranges->size()) {
        string start_key = p_ranges->at(mid).first.start_key;
        string end_key = p_ranges->at(mid).first.end_key;
        if (key_range.end_key < start_key)
            break;
        else if (key_range.start_key <= end_key && key_range.end_key > start_key)
            res.push_back(p_ranges->at(mid));
        mid++;
    }
    pthread_rwlock_unlock(&level_lock);
}

/* meta_update: update metatable 
 * @level_id: target level to update 
 * @op: operation type 
 *      0 - delete : the vector (3rd paremeter) will include only one range. 
 *                   if level_id > 0, All of the metadate included in the range 
 *                   will be deleted.
 *                   if level_id == 0, only one metadata will be deteleted
 *      1 - add    : vector includes multiple ranges. All of the ranges will be added
 *                   one by one.
 * @vector: target deleted or added elements 
 * */
void MetaTable::meta_update(int level_id, int op, vector< pair<KeyRange, persistent_ptr<Run> > > &op_ranges)
{
    pthread_rwlock_wrlock(&level_lock);
    auto p_ranges = &level_range[level_id];
    int start = 0;
    int end = p_ranges->size() - 1;
    int mid = start + (end - start) / 2;
    
    if (op_ranges.empty())
        return;

    string range_start = op_ranges.front().first.start_key;
    string range_end = op_ranges.back().first.end_key; 
    
    if (op == 0) {
        if (DO_LOG) { 
            cout << "delete range at level " << level_id << ": ";
            cout << "before delete: "; 
            display_range(level_id);
            cout << endl;
        }
        auto p_erase_run = op_ranges[0].second;
        /* do delete */
        if (level_id == 0) {
            /* sequential searching for level 0 */
            while (start <= end) {
                string start_key = p_ranges->at(start).first.start_key;
                string end_key = p_ranges->at(start).first.end_key;
                auto p_meta_run = p_ranges->at(start).second;
                if (start_key == range_start && end_key == range_end && p_erase_run == p_meta_run) {
                    p_ranges->erase(p_ranges->begin() + start);
                    break;
                } else {
                    start++;
                }
            }
        } else {
            /* binary search for other levels */
            while (start <= end) {
                mid = start + (end - start) / 2;
                string start_key = p_ranges->at(mid).first.start_key;
                string end_key = p_ranges->at(mid).first.end_key;
                
                if (range_start == end_key || range_start == start_key) {
                    break;
                } else if (range_start > end_key) {
                    start = mid + 1;
                } else if (range_start < end_key) {
                    end = mid - 1;
                }
            }

            auto erase_pos = p_ranges->begin() + mid;
            while (erase_pos != p_ranges->end()) {
                if (range_end < erase_pos->first.start_key)
                    break;
                else
                    erase_pos = p_ranges->erase(erase_pos);
            }
        }
        if (DO_LOG) {
            cout << "after delete: ";
            display_range(level_id);
            cout << end;
        }
    } else if (op == 1) {
        if (DO_LOG) { 
            cout << "add range at level " << level_id << ": ";
            cout << "before add: "; 
            display_range(level_id);
            cout << endl;
        }
        if (level_id == 0) {
            p_ranges->insert(p_ranges->end(), op_ranges.begin(), op_ranges.end());
        } else {
            auto it = p_ranges->begin();
            while (it != p_ranges->end()) {
                if (it->first.start_key > range_start)
                    break;
                it++;
            }
            p_ranges->insert(it, op_ranges.begin(), op_ranges.end());
        }
        if (DO_LOG) {
            cout << "after add: ";
            display_range(level_id);
            cout << end;
        }
    }
    pthread_rwlock_unlock(&level_lock);
}

/* Implememtations for Level */
Level::Level(int id) : level_id(id), run_count(0) 
{
    pthread_rwlock_init(&level_lock, NULL);
    pthread_rwlock_init(&unit_lock, NULL);
}

Level::~Level() 
{
    pthread_rwlock_destroy(&level_lock);
    pthread_rwlock_destroy(&unit_lock);
}

/* Implementations for Run */
Run::Run() : len(0) 
{
    make_persistent_atomic<KVPair[]>(pmpool, local_array, RUN_SIZE);
}

Run::Run(vector< pair<string, string> > * array) 
{
    make_persistent_atomic<KVPair[]>(pmpool, local_array, RUN_SIZE);
    for (int i = 0; i < array->size(); i++) {
        array->at(i).first.copy(local_array[i].key, array->at(i).first.length(), 0);
        array->at(i).second.copy(local_array[i].value, array->at(i).second.length(), 0);
    }
    len = array->size();
}

Run::~Run() {}

KeyRange Run::get_KeyRange()
{
    string start_key(local_array[0].key);
    string end_key(local_array[len - 1].key);
    KeyRange range(start_key, end_key);
    return range;
}

persistent_ptr<KVPair> Run::single_search(char * key) 
{
    //LOG("binary searching in run");
    int start = 0;
    int end = len - 1;
    while (start <= end) {
        int mid = start + (end - start) / 2;
        if (strcmp(local_array[mid].key, key) == 0)
            return &local_array[mid];
        else if (strcmp(local_array[mid].key, key) > 0)
            end = mid - 1;
        else
            start = mid + 1;
    }
    //LOG("binary searching done");
    return NULL;
}

/* Implementation for Compaction Unit */
CompactionUnit::CompactionUnit(int id) : level_id(id) {}
CompactionUnit::~CompactionUnit() {};

void CompactionUnit::del_runs(int up_low)
{
    vector< pair< KeyRange, persistent_ptr<Run> > > p_run;
    if (up_low == 0)
        p_run = up_runs;
    else
        p_run = low_runs;

    for (auto it : p_run) {
        delete_persistent_atomic<Run>(it.second);
    }
}

/* Implementation for Log */
Log::Log(int size) {
    make_persistent_atomic<KVPair[]>(pmpool, entry, size);
    head = &entry[0];
    tail = &entry[0];
    start = &entry[0];
    end = &entry[size - 1];
    log_size = size;
}

Log::~Log() {}

void Log::add_entry(string key, string value) {
    if (tail == end)
        tail = start;
    key.copy(tail->key, key.length(), 0);
    value.copy(tail->value, value.length(), 0);
    tail++;
}

void Log::reclaim(int size) {
    if (head == end)
        head = start;
    head = head + size;
}
