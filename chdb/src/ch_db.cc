#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
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
        chdb_command cmd(type, var.key, var.value, var.tx_id);
        int term, index;
        leader()->new_command(cmd, term, index);
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