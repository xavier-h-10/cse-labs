#include "ch_db.h"
#include <cstdio>

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
//    printf("view_server::execute called key=%d, val=%d, tx_id=%d\n", var.key, var.value, var.tx_id);
    chdb_command::command_type type;
    switch (proc) {
        case chdb_protocol::Get:
            type = chdb_command::CMD_GET;
            break;
        case chdb_protocol::Put:
            type = chdb_command::CMD_PUT;
            break;
        default:
            type = chdb_command::CMD_NONE;
            break;
    }
    if (type != chdb_command::CMD_NONE) {
        chdb_command tmp;
        tmp.key = var.key;
        tmp.value = var.value;
        tmp.tx_id = var.tx_id;
        tmp.cmd_tp = type;
        int term = 0, index = 0;
//        chdb_raft *lead = leader();
//        lead->new_command(tmp, term, index);
        send_command(tmp);
    }

    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());
    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}