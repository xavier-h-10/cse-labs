#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <string>
#include <fstream>
#include <vector>

const std::string log_name = "/log.txt";
const std::string meta_name = "/meta.txt";

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);

    ~raft_storage();
    // Your code here

    void read_metadata(int &term, int &voted_for);

    void write_metadata(int term, int voted_for);

    void read_log(std::vector <log_entry<command>> &log);

    void write_log(const log_entry<command> &log);

    command deserialize(const std::string &str);

    std::string serialize(const command &cmd);

private:
    std::mutex meta_mtx;
    std::mutex log_mtx;
    std::ofstream log_of;
    std::ofstream meta_of;
    std::ifstream log_in;
    std::ifstream meta_in;
    std::string dir;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir): dir(dir) {
//    log_of.open(dir + log_name, std::ios::app | std::ios::out);
//    meta_in.open(dir + meta_name, std::ios::in);
    log_in.open(dir + log_name, std::ios::in);
    log_of.open(dir + log_name, std::ios::app | std::ios::out);

}

template<typename command>
raft_storage<command>::~raft_storage() {
    // Your code here
    if (log_of.is_open()) log_of.close();
    if (meta_of.is_open()) meta_of.close();
    if (log_in.is_open()) log_in.close();
    if (meta_in.is_open()) meta_in.close();
}

template<typename command>
void raft_storage<command>::read_metadata(int &term, int &voted_for) {
    meta_mtx.lock();
//    meta_in.open(dir + meta_name, std::ios::in);
//    meta_in >> term;
//    meta_in >> voted_for;
    meta_in.open(dir + meta_name, std::ios::in);
    std::string str, str1;
    while(getline(meta_in,str)) {
        if(getline(meta_in,str1)) {
            term=atoi(str.c_str());
            voted_for=atoi(str1.c_str());
//            std::cout<<"read_metadata term="<<str<<" voted_for="<<str1<<std::endl;
        }
    }
//    term=atoi(str.c_str());
//    voted_for=atoi(str1.c_str());
//    std::cout<<"read_metadata term="<<term<<" voted_for="<<voted_for<<std::endl;
    meta_in.close();
    meta_mtx.unlock();
}

template<typename command>
void raft_storage<command>::write_metadata(int term, int voted_for) {
    meta_mtx.lock();
//    meta_of.open(dir + meta_name, std::ios::trunc | std::ios::out);
    meta_of.open(dir + meta_name, std::ios::ate | std::ios::out);
    meta_of << term << std::endl;
    meta_of << voted_for << std::endl;
    meta_of.close();
    meta_mtx.unlock();
}

template<typename command>
void raft_storage<command>::read_log(std::vector <log_entry<command>> &log) {
    log_mtx.lock();
//    log_in.open(dir + log_name, std::ios::in);
    log.clear();

    std::string str;
    int term;
    while (getline(log_in,str)) {
//        log_in>>term;
        term=atoi(str.c_str());
        getline(log_in,str);
        log.push_back(log_entry<command>(term, deserialize(str)));
    }

//    log_in.close();
    log_mtx.unlock();
}

template<typename command>
void raft_storage<command>::write_log(const log_entry<command> &log) {
    log_mtx.lock();
//    log_of.open(dir + log_name, std::ios::app | std::ios::out);
    log_of << log.term << std::endl;
    log_of << serialize(log.cmd) << std::endl;
//    std::cout << "raft_storage::write_log log=" << serialize(log.cmd) << std::endl;
//    log_of.close();
    log_mtx.unlock();
}

template<typename command>
command raft_storage<command>::deserialize(const std::string &str) {
    char *tmp = (char *) str.c_str();
    command cmd;
//    std::cout<<"deserialize:"<<tmp<<std::endl;
    int size = str.length();
    cmd.deserialize(tmp, size);
    return cmd;
}

template<typename command>
std::string raft_storage<command>::serialize(const command &cmd) {
    int size = cmd.size();
    char tmp[size];
    cmd.serialize(tmp, size);
//    std::cout<<"serialize:"<<tmp<<std::endl;
    return tmp;
}

#endif // raft_storage_h