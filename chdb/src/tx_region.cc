#include "tx_region.h"
#include <cstdio>

int tx_region::put(const int key, const int val) {
    mtx.lock();
    is_read_only = false;
    int shard_num = db->shards.size();
    int num = db->vserver->dispatch(key, shard_num) - 1; //分配到的shard
    chdb_protocol::operation_var var;
    var.tx_id = tx_id;
    var.key = key;
    var.value = val;
    int r = 0;
//    printf("tx_region::put key=%d val=%d tx_id=%d\n", key, val, tx_id);

    if (BIG_LOCK == 0) {
//        printf("tx_region::lock, key=%d tx_id=%d\n", key, tx_id);
        db->lock(key, tx_id);
        db->shards[num]->put(var, r);
        keys.insert(key);
    } else {
        db->shards[num]->put(var, r);
    }
//    printf("tx_region::put completed key=%d val=%d tx_id=%d\n", key, val, tx_id);
    mtx.unlock();
    return 0;
}

int tx_region::get(const int key) {
    mtx.lock();
    int shard_num = db->shards.size();
    int num = db->vserver->dispatch(key, shard_num) - 1;
    chdb_protocol::operation_var var;
    var.tx_id = tx_id;
    var.key = key;
    var.value = 0;
    int r = 0;
//    printf("tx_region::get key=%d tx_id=%d\n", key, tx_id);

    if (BIG_LOCK == 0) {
//        printf("tx_region::lock, key=%d tx_id=%d\n", key, tx_id);
        db->lock(key, tx_id);
        db->shards[num]->get(var, r);
        keys.insert(key);
    } else {
        db->shards[num]->get(var, r);
    }
//    printf("tx_region::get completed key=%d tx_id=%d\n", key, tx_id);
    mtx.unlock();
    return r;
}

int tx_region::tx_can_commit() {
    if (is_read_only) {
        return chdb_protocol::prepare_ok;
    }
    int shard_num = db->shards.size();
    chdb_protocol::check_prepare_state_var var;
    var.tx_id = tx_id;
    for (int i = 0; i < shard_num; i++) {
        int r = 0;
        db->shards[i]->check_prepare_state(var, r);
        if (r == chdb_protocol::prepare_not_ok) return r;
    }
    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    printf("tx[%d] begin\n", tx_id);
    keys.clear();
    return 0;
}

int tx_region::tx_commit() {
    mtx.lock();
    printf("tx[%d] commit\n", tx_id);
    chdb_protocol::commit_var var;
    var.tx_id = tx_id;
    int shard_num = db->shards.size();
    for (int i = 0; i < shard_num; i++) {
        int r = 0;
        db->shards[i]->commit(var, r);
    }

    if (BIG_LOCK == 0) {
        for (auto key: keys) {
//            printf("tx_region::unlock, key=%d\n", key);
            db->unlock(key);
        }
    }

    mtx.unlock();
    return 0;
}

int tx_region::tx_abort() {
    mtx.lock();
    printf("tx[%d] abort\n", tx_id);
    chdb_protocol::rollback_var var;
    var.tx_id = tx_id;
    int shard_num = db->shards.size();
    for (int i = 0; i < shard_num; i++) {
        int r = 0;
        db->shards[i]->rollback(var, r);
    }

    if (BIG_LOCK == 0) {
        for (auto key: keys) {
//            printf("tx_region::unlock, key=%d\n", key);
            db->unlock(key);
        }
    }
    mtx.unlock();
    return 0;
}
