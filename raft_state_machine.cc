#include "raft_state_machine.h"

#define N 105

kv_command::kv_command() : kv_command(CMD_NONE, "", "") {}

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) :
        cmd_tp(tp), key(key), value(value), res(std::make_shared<result>()) {
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() {}

int kv_command::size() const {
    // Your code here:
    return key.size() + value.size();
}


void kv_command::serialize(char *buf, int size) const {
    // Your code here:
    sprintf(buf, "%d key:%s value:%s", (int) cmd_tp, key.c_str(), value.c_str());
//    std::cout<<"kv_command::serialize buf="<<buf<<std::endl;
    return;
}

void kv_command::deserialize(const char *buf, int size) {
    // Your code here:
    int tmp1;
    char tmp2[N],tmp3[N];
    sscanf(buf, "%d key:%s value:%s", &tmp1, tmp2, tmp3);
    cmd_tp = (kv_command::command_type) tmp1;
    key = tmp2;
    value = tmp3;
//    std::cout<<"kv_command::deserialize key="<<key<<" val="<<value<<" tmp1="<<tmp1<<std::endl;
//    std::cout<<"kv_command::deserialize buf="<<buf<<std::endl;
    return;
}

marshall &operator<<(marshall &m, const kv_command &cmd) {
    // Your code here:
    m << (int) cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    return m;
}

unmarshall &operator>>(unmarshall &u, kv_command &cmd) {
    // Your code here:
    int tmp;
    u >> tmp;
    u >> cmd.key;
    u >> cmd.value;
    cmd.cmd_tp = (kv_command::command_type) tmp;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command &>(cmd);
    std::unique_lock <std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_GET: {
            std::string val = store[kv_cmd.key];
            std::cout<<"kv_state_machine: get, key="<<kv_cmd.key<<" val="<<val<<std::endl;
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = val;
            if (val == "") {
                kv_cmd.res->succ = false;
            } else {
                kv_cmd.res->succ = true;
            }
            break;
        }
        case kv_command::CMD_PUT: {
            std::string val = store[kv_cmd.key];
            kv_cmd.res->key = kv_cmd.key;
            std::cout<<"kv_state_machine: put, key="<<kv_cmd.key<<" val="<<val<<" kv_cmd.val="<<kv_cmd.value<<std::endl;
            if (val == "") {
                kv_cmd.res->succ = true;
                store[kv_cmd.key] = kv_cmd.value;
                kv_cmd.res->value = kv_cmd.value;
            } else {
                kv_cmd.res->succ = false;
                store[kv_cmd.key] = kv_cmd.value;
                kv_cmd.res->value = val;
            }
            break;
        }
        case kv_command::CMD_DEL: {
            std::string val = store[kv_cmd.key];
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = val;
            std::cout<<"kv_state_machine del, key="<<kv_cmd.key<<" val="<<val<<std::endl;
            if (val == "") {
                kv_cmd.res->succ = false;
            } else {
                kv_cmd.res->succ = true;
            }
            store.erase(kv_cmd.key);
            break;
        }
        case kv_command::CMD_NONE: {
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = kv_cmd.value;
            kv_cmd.res->succ = true;
            break;
        }
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char> &snapshot) {
    // Your code here:
    return;
}
