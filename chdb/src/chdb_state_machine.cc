#include "chdb_state_machine.h"

chdb_command::chdb_command() {
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id) {
    res->start = std::chrono::system_clock::now();
    res->key = key;
    res->tx_id = tx_id;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
}


void chdb_command::serialize(char *buf, int size) const {
    sprintf(buf, "%d %d %d %d", (int) cmd_tp, key, value, tx_id);
}

void chdb_command::deserialize(const char *buf, int size) {
    int tmp;
    sscanf(buf, "%d %d %d %d", tmp1, key, value, tx_id);
    cmd_tp = (chdb_command::command_type) tmp;
    key = tmp2;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    m << (int) cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    m << cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    int tmp;
    u >> tmp;
    u >> cmd.key;
    u >> cmd.value;
    u >> cmd.tx_id;
    cmd.cmd_tp = (chdb_command::command_type) tmp;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    chdb_command &chdb_cmd = dynamic_cast<chdb_command &>(cmd);
    std::unique_lock <std::mutex> lock(chdb_cmd.res->mtx);
    switch (chdb_cmd.cmd_tp) {
        case chdb_command::CMD_GET: {
            if (store.find(chdb_cmd.key) != store.end()) {
                chdb_cmd.res->value = store[chdb_cmd.key];
                chdb_cmd.res->succ = true;
            } else {
                chdb_cmd.res->value = -1;
                chdb_cmd.res->succ = false;
            }
//            std::cout << "chdb_state_machine: get, key=" << chdb_cmd.key << " val=" << chdb_cmd.res->value << std::endl;
            break;
        }
        case chdb_command::CMD_PUT: {
            store[chdb_cmd.key] = chdb_cmd.value;
//            std::cout << "chdb_state_machine: put, key=" << chdb_cmd.key << " val=" << chdb_cmd.value << std::endl;
            chdb_cmd.res->value = chdb_cmd.value;
            chdb_cmd.res->succ = true;
            break;
        }
        case chdb_command::CMD_NONE: {
            chdb_cmd.res->value = chdb_cmd.value;
            chdb_cmd.res->succ = true;
            break;
        }
    }
    chdb_cmd.res->key = chdb_cmd.key;
    chdb_cmd.res->done = true;
    chdb_cmd.res->cv.notify_all();
    return;
}