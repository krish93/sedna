/* associative array */
void assoc_init(void);
item *assoc_find(const char *key, const size_t nkey);
int assoc_insert(item *item);
void assoc_delete(const char *key, const size_t nkey);
void do_assoc_move_next_bucket(void);
void ct_get_hashtable(const char *key);
int start_assoc_maintenance_thread(void);
void stop_assoc_maintenance_thread(void);
item **assoc_find_hashtable(int ht_num);
unsigned int assoc_get_hashpower(int ht_num);
