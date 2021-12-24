#ifndef db_server_h
#define db_server_h

#include "common.h"
#include "shard_client.h"

#include <cstdio>


using shard_dispatch = int (*)(int key, int shard_num);
using chdb_raft = raft<chdb_state_machine, chdb_command>;
using chdb_raft_group = raft_group<chdb_state_machine, chdb_command>;

/**
 * Master node
 * */
class view_server {
public:
    rpc_node *node;
    shard_dispatch dispatch;            /* Dispatch requests to the target shard */
    chdb_raft_group *raft_group;

    view_server(const int base_port,
                shard_dispatch dispatch,
                const int num_raft_nodes = 3) :
            dispatch(dispatch),
            node(new rpc_node(base_port)) {
#if RAFT_GROUP
        raft_group = new chdb_raft_group(num_raft_nodes);
#endif
    };

    chdb_raft *leader() const {
        int leader = this->raft_group->check_exact_one_leader();
        return this->raft_group->nodes[leader];
    }


    /**
     * Add the shard client for rpc communication
     * */
    int add_shard_client(shard_client *shard) {
        int port = shard->node->port();
        int view_server_port = this->node->port();
        shard->bind_view_server(view_server_port);
        return this->node->bind_remote_node(port);
    }

    /**
     * Shard num that the view_server manage now
     * */
    int shard_num() const {
        return this->node->rpc_clients.size();
    }

    /**
     * Dispatch the request to specific shard client(s)
     * Sync return when use normal view server (single node)
     * Async return when use raft group vie server, since the command log should be distributed first.
     * */
    int
    execute(unsigned int query_key,
            unsigned int proc,
            const chdb_protocol::operation_var &var,
            int &r);


    ~view_server();

};


/*
 * chdb: One KV storage
 * */
class chdb {
public:
    chdb(const int shard_num, const int cluster_port, shard_dispatch dispatch = default_dispatch)
            : max_tx_id(0),
              vserver(new view_server(cluster_port, dispatch)) {
        for (int i = 1; i <= shard_num; ++i) {
            shard_client *shard = new shard_client(i, i + cluster_port);
            vserver->add_shard_client(shard);
            this->shards.push_back(shard);
        }

        std::srand(std::time(nullptr));

    }

    ~chdb() {
        for (auto &shard: shards) delete shard;
        delete vserver;
    }

    void set_shard_down(const int offset) {
        const int len = shards.size();
        assert(len > 0);
        shards[offset % len]->set_active(false);
    }

    void set_shards_down(const std::set<int> shards_offset) {
        const int len = shards.size();
        assert(len > 0);

        for (auto offset: shards_offset) {
            shards[offset % len]->set_active(false);
        }
    }

    void set_shards_up(const std::set<int> shards_offset) {
        const int len = shards.size();
        assert(len > 0);

        for (auto offset: shards_offset) {
            shards[offset % len]->set_active(true);
        }
    }

    /**
     * Generate one unique transaction id
     * */
    int next_tx_id() {
        int res;
        {
            tx_id_mtx.lock();
            res = max_tx_id++;
            tx_id_mtx.unlock();
        }
        return res;
    }

    view_server *vserver;

    std::vector<shard_client *> shards;
    int max_tx_id;
    std::mutex tx_id_mtx;
    std::mutex map_mtx;

    std::map<int, std::mutex> key_mtx;
    std::map<int, int> key_master;

    void lock_all() {
        tx_id_mtx.lock();
    }

    void unlock_all() {
        tx_id_mtx.unlock();
    }

    //solve dead_lock
    void lock(int key, int tx_id) {
//        printf("ch_db::lock called, key=%d, tx_id=%d\n", key, tx_id);
        while (true) {
//            printf("ch_db::lock checkpoint\n");
            if (key_master.find(key) == key_master.end() || tx_id == key_master[key]) {
                if (tx_id == key_master[key]) return;
//                printf("ch_db::lock start to wait, key=%d, tx_id=%d\n", key, tx_id);
                key_mtx[key].lock();
                key_master[key] = tx_id;
                return;
            } else {
                int sleep_time = std::rand() % 100 + 50;
//                printf("ch_db::lock sleep_time=%d, key=%d, tx_id=%d\n", sleep_time, key, tx_id);
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
            }
        }
    }

    void unlock(int key) {
        key_mtx[key].unlock();
        key_master.erase(key);
    }


private:
    static int default_dispatch(const int key, int shard_num) {
        int shard_offset = key % shard_num;
        if (0 == shard_offset)++shard_offset;
        return shard_offset;
    }
};

#endif // db_server_h
