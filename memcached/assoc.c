/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;


typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

unsigned int temp_hashpower = 16;
int temp_expanding = 0;
/* how many powers of 2's worth of buckets we use */
static unsigned int *hashpower = &temp_hashpower;


#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*********************************************
 * Modified author: ChenTao
 */
typedef struct HashTable_Array{
	item** primary_hashtable;
	item** old_hashtable;
	unsigned int *hash_items;
	unsigned int *hashpower;
	int *expanding;

}ht_array;

#define HTARRAY_LENGTH 100000

static ht_array *hashtable_array =0;
/**********************************************/

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable = 0;

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable = 0;

/* Number of items in the hash table. */
static unsigned int *hash_items = 0;

/* Flag: Are we in the middle of expanding now? */
static int *expanding = &temp_expanding;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;

/*
 * modified by Chen Tao
 */

/*
 * rs hash algorithm
 */
static unsigned int RSHash(const char *str) {
        unsigned int b = 378551;
        unsigned int a = 63689;
        unsigned int hash = 0;
        while (*str){

        hash = hash * a + (*str++);
        a *= b;
        }
        return (hash & 0x7FFFFFF);
}

void assoc_init(void) {
	hashtable_array = calloc(HTARRAY_LENGTH, sizeof(ht_array));
	  if(! hashtable_array) {
		  fprintf(stderr, "Failed to init hashtable array.\n");
	  }
}

/*
 * added by Chen Tao
 */
void ct_get_hashtable(const char *key){
	uint32_t ht = RSHash(key);
	ht_array *ht_node = hashtable_array + ht % HTARRAY_LENGTH;
	if ( !ht_node->primary_hashtable ){
		ht_node->primary_hashtable = calloc(hashsize(*hashpower), sizeof(void *));
		ht_node->hash_items = calloc(1, sizeof(unsigned int));
		ht_node->expanding = calloc(1, sizeof(int));
		ht_node->hashpower = calloc(1, sizeof(unsigned int));
		*(ht_node->hashpower ) = 16;
		*(ht_node->expanding) = 0;
		 if (! ht_node->primary_hashtable) {
			 fprintf(stderr, "Failed to init hashtable.\n");
			 exit(EXIT_FAILURE);
		 }
	}
	primary_hashtable = ht_node->primary_hashtable;
	old_hashtable = ht_node->old_hashtable;
	hash_items = ht_node->hash_items;
	expanding = ht_node->expanding;
	hashpower = ht_node->hashpower;
}

item **assoc_find_hashtable(int ht_num){
	item **ret = NULL;
	ht_array *ht_node = hashtable_array + ht_num;

	if(*(ht_node->expanding) == 0)
		ret = ht_node->primary_hashtable;
	else
		ret = ht_node->old_hashtable;

	return ret;

}

unsigned int assoc_get_hashpower(int ht_num){
	ht_array *ht_node = hashtable_array + ht_num;
	return *ht_node->hashpower;
}

item *assoc_find(const char *key, const size_t nkey) {
	ct_get_hashtable(key);
    uint32_t hv = hash(key, nkey, 0);
    item *it;
    unsigned int oldbucket;

    if (*expanding &&
        (oldbucket = (hv & hashmask((*hashpower) - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        it = primary_hashtable[hv & hashmask(*hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item** _hashitem_before (const char *key, const size_t nkey) {
    uint32_t hv = hash(key, nkey, 0);
    item **pos;
    unsigned int oldbucket;

    if (*expanding &&
        (oldbucket = (hv & hashmask((*hashpower) - 1))) >= expand_bucket)
    {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(*hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize((*hashpower) + 1), sizeof(void *));
    if (primary_hashtable) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        (*hashpower)++;
        *expanding = 1;
        expand_bucket = 0;
        pthread_cond_signal(&maintenance_cond);
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it) {
	ct_get_hashtable(ITEM_key(it));
    uint32_t hv;
    unsigned int oldbucket;

    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    hv = hash(ITEM_key(it), it->nkey, 0);
    if (*expanding &&
        (oldbucket = (hv & hashmask((*hashpower) - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        it->h_next = primary_hashtable[hv & hashmask(*hashpower)];
        primary_hashtable[hv & hashmask(*hashpower)] = it;
    }

    (*hash_items)++;
    if (! *expanding && *hash_items > (hashsize(*hashpower) * 3) / 2) {
        assoc_expand();
    }
    printf("insert it. hash_items = %d, vnode = %d\n",*hash_items,  RSHash(ITEM_key(it)) % HTARRAY_LENGTH);
    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, *hash_items);
    return 1;
}

void assoc_delete(const char *key, const size_t nkey) {
	ct_get_hashtable(key);
    item **before = _hashitem_before(key, nkey);

    if (*before) {
        item *nxt;
        (*hash_items)--;
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, *hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}

static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void *arg) {

    while (do_run_maintenance_thread) {
        int ii = 0;

        /* Lock the cache, and bulk move multiple buckets to the new
         * hash table. */
        pthread_mutex_lock(&cache_lock);

        for (ii = 0; ii < hash_bulk_move && *expanding; ++ii) {
            item *it, *next;
            int bucket;

            for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                next = it->h_next;

                bucket = hash(ITEM_key(it), it->nkey, 0) & hashmask(*hashpower);
                it->h_next = primary_hashtable[bucket];
                primary_hashtable[bucket] = it;
            }

            old_hashtable[expand_bucket] = NULL;

            expand_bucket++;
            if (expand_bucket == hashsize((*hashpower) - 1)) {
                (*expanding) = 0;
                free(old_hashtable);
                if (settings.verbose > 1)
                    fprintf(stderr, "Hash table expansion done\n");
            }
        }

        if (!(*expanding)) {
            /* We are done expanding.. just wait for next invocation */
            pthread_cond_wait(&maintenance_cond, &cache_lock);
        }

        pthread_mutex_unlock(&cache_lock);
    }
    return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_assoc_maintenance_thread() {
    pthread_mutex_lock(&cache_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    pthread_mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


