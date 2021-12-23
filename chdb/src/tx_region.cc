#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    mtx.lock();
    is_read_only = false;
    int shard_num = db->shards.size();
    int num = db->vserver->dispatch(key, shard_num)-1; //分配到的shard
    chdb_protocol::operation_var var;
    var.tx_id = tx_id;
    var.key = key;
    var.value = val;
    int r = 0;
    std::cout << "tx_region::put " << key<<" "<<val<<std::endl;
    db->shards[num]->put(var, r);
    mtx.unlock();
    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    mtx.lock();
    int shard_num = db->shards.size();
    int num = db->vserver->dispatch(key, shard_num)-1;
    chdb_protocol::operation_var var;
    var.tx_id = tx_id;
    var.key = key;
    var.value = 0;
    int r = 0;
    std::cout << "tx_region::get " << key<<std::endl;
    db->shards[num]->get(var, r);
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
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    mtx.lock();
    printf("tx[%d] commit\n", tx_id);
    chdb_protocol::commit_var var;
    var.tx_id = tx_id;
    int shard_num = db->shards.size();
    for (int i = 0; i < shard_num; i++) {
        int r = 0;
        db->shards[i]->commit(var, r);
    }
    mtx.unlock();
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    mtx.lock();
    printf("tx[%d] abort\n", tx_id);
    chdb_protocol::rollback_var var;
    var.tx_id = tx_id;
    int shard_num = db->shards.size();
    for (int i = 0; i < shard_num; i++) {
        int r = 0;
        db->shards[i]->rollback(var, r);
    }
    mtx.unlock();
    return 0;
}
