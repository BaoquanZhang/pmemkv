#ifndef _GLOBAL_CONF_H_
#define _GLOBAL_CONF_H_

/* pmdk headers */
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/transaction.hpp>

#define RUN_SIZE 4096
#define MAX_ARRAY 3
#define LAYOUT "plsmStore"
#define KEY_SIZE 16
#define VALUE_SIZE 128
#define POOL_SIZE 1


#define DO_LOG true
#define LOG(msg) if (DO_LOG) std::cout << "[nvlsm] " << msg << "\n"

#endif // _GLOBAL_CONF_H_
