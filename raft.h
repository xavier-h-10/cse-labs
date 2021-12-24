#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <random>
#include <vector>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

    static_assert(std::is_base_of<raft_state_machine, state_machine>(),

    "state_machine must inherit from raft_state_machine");

    static_assert(std::is_base_of<raft_command, command>(),

    "command must inherit from raft_command");


    friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
            rpcs *rpc_server,
            std::vector<rpcc *> rpc_clients,
            int idx,
            raft_storage<command> *storage,
            state_machine *state
    );

    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage;              // To persist the raft log
    state_machine *state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;  //当前任期


    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    std::chrono::time_point<std::chrono::system_clock> current_time;
    std::chrono::time_point<std::chrono::system_clock> last_received_RPC_time;

    //persistent states
    int voted_for;     //给谁投票
    int votes;         //获得的选票数

    int commit_index;
    int leader_commit_index;
    int last_applied;
    int leader_id;

    std::vector<log_entry<command> > logs;

    std::vector<int> next_index;
    std::vector<int> match_index;

    std::mutex log_mtx;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);

    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);

    void
    handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);

    void
    handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);


private:
    bool is_stopped();

    int num_nodes() { return rpc_clients.size(); }

    // background workers    
    void run_background_ping();

    void run_background_election();

    void run_background_commit();

    void run_background_apply();

    // Your code here:
    void update_term(int term);       //更新现在的term

    void turn_to_follower(int term);  //变为follower

    void ping();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state) :
        storage(storage),
        state(state),
        rpc_server(server),
        rpc_clients(clients),
        my_id(idx),
        stopped(false),
        role(follower),
        current_term(0),
        background_election(nullptr),
        background_ping(nullptr),
        background_commit(nullptr),
        background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    current_time = std::chrono::system_clock::now();
    last_received_RPC_time = current_time;

    voted_for = -1;
    leader_id = -1;
    votes = 0;
    last_applied = 0;
    leader_commit_index = 0;
    commit_index = 0;
    logs.clear();

    int size = num_nodes();
    next_index.assign(size, 0);
    match_index.assign(size, 0);
    logs.assign(1, log_entry<command>());

}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/*****************************************************************

Public Interfaces

******************************************************************/


template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
    RAFT_LOG("stop");
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
//    RAFT_LOG("start");
//    mtx.lock(); //think
    log_mtx.lock();
    std::vector <log_entry<command>> log;
    storage->read_log(log);
    storage->read_metadata(current_term, voted_for);
    voted_for = -1;
    RAFT_LOG("start");
    int size = log.size();
    for (int i = 0; i < size; i++) {
        state->apply_log(log[i].cmd);
        logs.push_back(log[i]);
    }

    size = this->logs.size();
    commit_index = last_applied = size - 1;
    log_mtx.unlock();
//    mtx.unlock(); //think

    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
//    std::unique_lock <std::mutex> lock(mtx); // need to check

    if (role == leader) {
        term = current_term;
        log_mtx.lock();
        log_entry<command> entry = log_entry<command>(current_term, cmd);
        storage->write_log(entry);
        log_mtx.unlock();
        return true;
    } else {
        log_entry<command> entry = log_entry<command>(current_term, cmd);
        storage->write_log(entry);
        return false;
    }
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}


/*****************************************************************

RPC Related

******************************************************************/

template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Your code here:
    std::unique_lock <std::mutex> lock(mtx);
    log_mtx.lock();
    int last_log_index = logs.size() - 1;
    int last_log_term = logs[last_log_index].term;
    log_mtx.unlock();
//    RAFT_LOG("raft::request_vote called arg.id=%d arg.term=%d current_term=%d", args.candidate_id, args.term,
//             current_term);
    if (args.term <= current_term) reply.vote_granted = false;
    else {
//        if ( voted_for != -1 && voted_for != args.candidate_id) {
//            reply.vote_granted = false;
//        } else {
        //当term改变时,无论是否投过票,都应该转变为follower并且重置voted_for
        update_term(args.term);
        if (last_log_term < args.last_log_term ||
            (last_log_term == args.last_log_term && last_log_index <= args.last_log_index)) {
            voted_for = args.candidate_id;
            reply.vote_granted = true;
            turn_to_follower(args.term);
        } else {
            reply.vote_granted = false;
        }
    }
//    RAFT_LOG("request_vote called, arg.can_id=%d, arg.term=%d, arg.last_log_term=%d, last_log_term=%d, current_term=%d, voted_for=%d, reply.granted=%d  reply.term=%d",
//             args.candidate_id, args.term, args.last_log_term, last_log_term, current_term, voted_for, reply.vote_granted, reply.term);
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Your code here:
    std::unique_lock <std::mutex> lock(mtx);
    if (reply.vote_granted && role == candidate) {
        votes++;
        int size = num_nodes();
        if (votes >= (size + 1) / 2) {
            RAFT_LOG("become leader, votes=%d id=%d", votes, my_id);
            role = leader;
            voted_for = -1;
            votes = 0;

            //每次选举后,需要更新leader的nextIndex和matchIndex
            log_mtx.lock();
            int tmp = logs.size();
            std::fill(match_index.begin(), match_index.end(), last_applied);
            std::fill(next_index.begin(), next_index.end(), tmp);
            log_mtx.unlock();

            ping();
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Your code here:
    std::unique_lock <std::mutex> lock(mtx);
    if (arg.term >= current_term) {
        //收到leader的heartbeat时,更新rpc time
        last_received_RPC_time = std::chrono::system_clock::now();
        if (my_id != arg.leader_id && arg.leader_id != leader_id) {
            turn_to_follower(arg.term);
            leader_id = arg.leader_id;
        }
        reply.success = true;
        reply.action = arg.action;
        reply.index = 0;
        current_term = arg.term;
        switch (arg.action) {
            //heartbeat
            case 0: {
                break;
            }
                //append
            case 1: {
                log_mtx.lock();
                int size = logs.size();
                int idx = arg.prev_log_index;
                // contain an entry at prev_log_index, term=prev_log_term
                if (idx == 0 || (size > idx && logs[idx].term == arg.prev_log_term)) {
                    //删除冲突的log
                    logs.resize(idx + 1);
                    int size_1 = arg.entries.size();

                    for (int i = 0; i < size_1; i++) {
                        logs.push_back(arg.entries[i]);
                    }
                } else {
                    reply.success = false;
                }
                reply.index = logs.size() - 1;

                log_mtx.unlock();
                break;
            }
                //commit
            case 2: {
                commit_index = std::max(commit_index, arg.prev_log_index);
                reply.index = arg.prev_log_index;
                break;
            }
        }
    } else {
        reply.success = false;
        reply.action = arg.action;
        reply.index = 0;
    }
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Your code here:
    std::unique_lock <std::mutex> lock(mtx);
    int size = num_nodes();
    if (role != leader) return;
//    if (reply.action != 0)
//        RAFT_LOG("raft::append_entries_reply my_id=%d target=%d reply.index=%d reply.action=%d reply.success=%d", my_id,
//                 target, reply.index, reply.action, reply.success);
    if (!reply.success) {
        //如果因为log inconsistency, append失败, 则nextIndex--并重试
        log_mtx.lock();
        if (target < size && next_index[target] > 0) next_index[target]--;
        log_mtx.unlock();
    } else {
        switch (reply.action) {
            case 0: {
                break;
            }
            case 1: {
                log_mtx.lock();
                if (target < size) next_index[target] = reply.index + 1;
                int tot = 1;
//                for (int i = 0; i < size; i++) {
//                    if (i == my_id) continue;
//                    if (next_index[i] == commit_index + 1) tot++;
//                }

                int index = logs.size() - 1;
                for (; index >= commit_index; index--) {
                    tot = 1;
                    if (logs[index].term != current_term) continue;
                    for (int i = 0; i < size; i++) {
                        if (i == my_id) continue;
                        if (next_index[i] - 1 >= index) tot++;
                    }
                    if (tot >= (size + 1) / 2) break;
                }
                //已经收到大多数follower的回复, 可以apply logs: last_applied+1 - commit_index
                if (tot >= (size + 1) / 2) {
//                    RAFT_LOG("tot=%d commit_index=%d", tot, index);
//                    for (int i = 0; i < size; i++) {
//                        RAFT_LOG("next_index[%d]=%d", i, next_index[i]);
//                        RAFT_LOG("match_index[%d]=%d", i, match_index[i]);
//                    }
                    commit_index = index;
                    int tmp = logs.size();
                    if (tmp > commit_index) {
                        for (int i = last_applied + 1; i <= commit_index; i++) {
//                            RAFT_LOG("raft::append_entries_reply leader apply log my_id=%d i=%d", my_id, i);
                            storage->write_log(logs[i]);
                            state->apply_log(logs[i].cmd);
                        }
//                        leader_commit_index = commit_index;
                        last_applied = commit_index;
                    }
                }
//                RAFT_LOG("raft::append_entries_reply my_id=%d target=%d tot=%d last_applied=%d commit_index=%d", my_id,
//                         target, tot, last_applied, commit_index);
                log_mtx.unlock();
                break;
            }
            case 2: {
                log_mtx.lock();
                match_index[target] = reply.index;
                log_mtx.unlock();
                break;
            }
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Your code here:
//    std::unique_lock <std::mutex> lock(mtx);
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Your code here:
//    std::unique_lock <std::mutex> lock(mtx);
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/*****************************************************************

Background Workers

******************************************************************/


template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> distrib(300.0, 500.0);

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        current_time = std::chrono::system_clock::now();
        double dr_ms = std::chrono::duration<double, std::milli>(current_time - last_received_RPC_time).count();
        double timeout = distrib(gen);
        // follower->candidate, 开始一轮选举
        if (role == follower && dr_ms > timeout) {
            mtx.lock();
            role = candidate;
            last_received_RPC_time = current_time;  //需要考虑正确性
//            RAFT_LOG("raft::run_background_election start election, new_term=%d, timeout=%f", current_term + 1,
//                     timeout);
            update_term(current_term + 1);
            votes = 1;  //给自己投票
            voted_for = my_id;

            int size = num_nodes();
            request_vote_args args;
            args.term = current_term;
            args.candidate_id = my_id;
            args.last_log_index = logs.size() - 1;
            args.last_log_term = logs[args.last_log_index].term;
            mtx.unlock();

            for (int i = 0; i < size; i++)
                if (i != (my_id)) {
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                }
        }
            // 没有收到足够选票, candidate->follower
        else if (role == candidate && dr_ms > 1000.0) {
            mtx.lock();
            turn_to_follower(current_term);
            mtx.unlock();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }


    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if (role == leader) {
            log_mtx.lock();
            mtx.lock();
            int index = logs.size() - 1;
            int size = num_nodes();
            //之前的操作已经完成,有新的log加入,需要commit
            // need to check: a majority of match_index[i]>=n
//            if (last_applied == commit_index && index > last_applied && logs[index].term == current_term) {
//                commit_index = index;
//            }

//             for(;index>=commit_index;index--) {
//                 int tot=0;
//                 for(int j=0;j<size;j++) {
//                     if(match_index[j]>=index) tot++;
//                 }
//                 if(tot>=(size+1)/2 && logs[index].term==current_term) break;
//             }
//             commit_index=index;


            for (int i = 0; i < size; i++) {
                if (i == my_id) continue;
                if (next_index[i] <= index) {
//                    mtx.lock(); //think

                    append_entries_args<command> args;
                    args.term = current_term;
                    args.leader_id = my_id;
                    args.action = 1;
                    args.prev_log_index = next_index[i] - 1;
                    args.prev_log_term = logs[args.prev_log_index].term;
                    args.entries.clear();
                    args.leader_commit = commit_index;
                    for (int j = next_index[i]; j <= index; j++) {
//                        if(logs[j].term==current_term)      //figure8 think
                        args.entries.push_back(logs[j]);
                    }
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);

//                    mtx.unlock(); //think
                }

//                }
            }

            for (int i = 0; i < size; i++) {
                if (i == my_id) continue;
                if (match_index[i] < last_applied) {
//                    mtx.lock(); //think
                    append_entries_args<command> args;
                    args.term = current_term;
                    args.leader_id = my_id;
                    args.action = 2;
                    args.prev_log_index = commit_index;
                    args.leader_commit = commit_index;
//                    RAFT_LOG("send apply, my_id=%d, target=%d", my_id, i);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
//                    mtx.unlock(); //think
                }
            }
            mtx.unlock();
            log_mtx.unlock();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index


    while (true) {
        if (is_stopped()) return;
        // Your code here:
        log_mtx.lock();
        int tmp;
        tmp = commit_index;
        if (tmp > last_applied) {
            int size = logs.size();
            if (size > tmp) {
                for (int i = last_applied + 1; i <= tmp; i++) {
                    storage->write_log(logs[i]);
                    state->apply_log(logs[i].cmd);
                }
                last_applied = tmp;
            }

        }
        log_mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if (role == leader) {
            ping();
        }


        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::update_term(int term) {
    int old_term = this->current_term;
    if (term > old_term) {
        this->current_term = term;
        storage->write_metadata(term, this->voted_for);
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::turn_to_follower(int term) {
    voted_for = -1;
    votes = 0;
    update_term(term);
    if (role == leader || role == candidate) {
        commit_index = last_applied;
    }
    role = follower;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::ping() {
    append_entries_args<command> args;
    args.term = current_term;
    args.leader_id = my_id;
    args.action = 0;

    int size = num_nodes();
    for (int i = 0; i < size; i++) {
        if (role == leader && i != my_id) {
//            RAFT_LOG("raft::run_background_ping my_id=%d target=%d", my_id, i);
            thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
        }
    }
}


/*****************************************************************

Other functions

******************************************************************/


#endif // raft_h
