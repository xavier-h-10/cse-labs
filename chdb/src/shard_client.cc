#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    mtx.lock();
    int store_id = get_store_id(var.tx_id);
    store[store_id][var.key] = var.value;

    mtx.unlock();
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    mtx.lock();
    int store_id = get_store_id(var.tx_id);
    if(store[store_id].find(var.key)!=store[store_id].end()) {
        r=store[store_id][var.key];
    }
    else if (store[0].find(var.key) != store[0].end()) {
        r = store[0][var.key];
    } else {
        r = 0;
    }
    mtx.unlock();
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    mtx.lock();
    int tx_id = var.tx_id;
    int store_id = get_store_id(tx_id);
    for (auto &kv: store[store_id]) {
        int key = kv.first;
        int val = kv.second;
        store[0][key] = val;
    }
    store[store_id].clear();
    mtx.unlock();
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    mtx.lock();
    int tx_id = var.tx_id;
    int store_id = get_store_id(tx_id);
    store[store_id].clear();
    mtx.unlock();
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    int tx_id = var.tx_id;
//    if (!active || prepare_state.find(tx_id) == prepare_state.end() || prepare_state[tx_id] == false) {
//        r = chdb_protocol::prepare_not_ok;
//    } else {
//        r = chdb_protocol::prepare_ok;
//    }
    if (!active) {
        r = chdb_protocol::prepare_not_ok;
    } else {
        r = chdb_protocol::prepare_ok;
    }
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    int tx_id = var.tx_id;
    if (active) {
        r = chdb_protocol::prepare_ok;
        prepare_state[tx_id] = true;
    } else {
        r = chdb_protocol::prepare_not_ok;
        prepare_state[tx_id] = false;
    }
    return 0;
}

shard_client::~shard_client() {
    delete node;
}