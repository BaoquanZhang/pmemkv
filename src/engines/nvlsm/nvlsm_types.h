#ifndef _NVLSM_TYPE_H_
#define _NVLSM_TYPE_H_

#include <vector>
#include <list>
#include <unistd.h>
#include <utility>
#include <string>
#include <pthread.h>
#include "global_conf.h"
#include "threadpool.h"

using namespace std;
/* pmdk namespace */
using namespace pmem::obj;

namespace nv_lsm {
    class PlsmStore;
    class MemTable;
    class MetaTable;
    class Level;
    class Run;
    class CompactionUnit;
    class Log;
    struct KeyRange;

    class CompactionUnit {
        public:
            int level_id;
            PlsmStore * plsmStore;
            vector< pair< KeyRange, persistent_ptr<Run> > > up_runs;
            vector< pair< KeyRange, persistent_ptr<Run> > > low_runs;
        
            CompactionUnit(int id);
            CompactionUnit();
            ~CompactionUnit();
            void del_runs(int up_low);
    };

    class PlsmStore {
        private:
            pthread_t persist_thread_id;
            /* internal operations */
            //void normal_compaction(void * unit);
            void lazy_compaction(void * unit);
            static bool compare(pair<string, string> kv1, pair<string, string> kv2);
            ThreadPool * thread_pool;

        public:
            bool start_persist;
            pthread_mutex_t start_persist_mutex;
            pthread_mutex_t planner_mutex;
            p<int> level_base;
            p<int> level_ratio;
            persistent_ptr<Level> level_head;
            persistent_ptr<Level> level_tail;
            MemTable * memTable;
            MetaTable * metaTable;
            persistent_ptr<Log> buffer_log;
            /* interface */
            void put(string key, string value);
            string get(string key);
            vector< pair<string, string> > range(string start_key, string end_key);
            persistent_ptr<Level> find_level(int level_id);
            vector< pair< KeyRange, persistent_ptr<Run> > > merge_sort(CompactionUnit * unit);
            void compact_planner(int level_id);

            PlsmStore(const string &path, const size_t size, int level_base_val, int level_ratio_val);
            ~PlsmStore();
    };

    /* DRAM structures */
    class MemTable {
        public:
            vector< pair<string, string> > * buffer;
            pthread_mutex_t queue_mutex; // pretect queue count
            pthread_cond_t queue_cond; // indicating if queue has candidates
            int queue_count;
            list< vector< pair<string, string> > * > persist_queue;
            MemTable();
            ~MemTable();
    };

    struct KeyRange {
        string start_key;
        string end_key;
        KeyRange(string key1, string key2) {
            start_key = key1;
            end_key = key2;
        }

        KeyRange(int size) {
            start_key.reserve(size);
            end_key.reserve(size);
        }
    };

    class MetaTable {
        public:
            KeyRange * buffer_range; // key ranges for buffer
            list<KeyRange *> queue_range; // key ranges for persist list
            vector<int> last_comp; // key ranges selected in last compact
            pthread_rwlock_t level_lock;
            vector< vector< pair<KeyRange, persistent_ptr<Run> > > > level_range; // key ranges for level

            persistent_ptr<Run> meta_search(int level_id, string key);
            void meta_search(vector< pair< KeyRange, persistent_ptr<Run> > > &res, int level_id, KeyRange &range);
            void meta_search(vector< pair< KeyRange, persistent_ptr<Run> > > &res, int level_id, string &key);
            void meta_update(int level_id, int op, vector< pair<KeyRange, persistent_ptr<Run> > > &op_ranges);
            void display_range(int level_id);
            void select_up_run(int level_id, CompactionUnit * unit);
            MetaTable();
            ~MetaTable();

    };

    /* PMEM structures */
    struct LSM_Root {                 // persistent root object
        persistent_ptr<Level> head;   // head of the vector of levels
    };

    class Level {
        public:

            pthread_rwlock_t level_lock;
            pthread_rwlock_t unit_lock;
            p<int> level_id;
            p<int> run_count;
            
            persistent_ptr<Level> next_level;
            persistent_ptr<Level> pre_level;

            Level(int id);
            ~Level();
    };

    struct KVPair {
        char key[KEY_SIZE];
        char value[VALUE_SIZE];
    };

    class Run {
        public:
            p<int> len;
            p<bool> is_compact;
            persistent_ptr<KVPair[]> local_array;
            persistent_ptr<Run> next_run;
            persistent_ptr<Run> pre_run;
            Run();
            Run(vector< pair<string, string> > * array);
            ~Run();
            KeyRange get_KeyRange();
            persistent_ptr<KVPair> single_search(char * key);
    };

    class Log {
        public:
            int log_size;
            persistent_ptr<KVPair> head;
            persistent_ptr<KVPair> tail;
            persistent_ptr<KVPair> start;
            persistent_ptr<KVPair> end;
            persistent_ptr<KVPair[]> entry;
            void add_entry(string key, string value);
            void reclaim(int size);

            Log(int size);
            ~Log();
    };
};
#endif // _NVLSM_TYPE_H_
