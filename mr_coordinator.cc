#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
    int taskType;     // should be either Mapper or Reducer
    bool isAssigned;  // has been assigned to a worker
    bool isCompleted; // has been finished by a worker
    int index;        // index to the file
    time_t fromTime;
};

class Coordinator {
public:
    Coordinator(const vector <string> &files, int nReduce);

    mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);

    mr_protocol::status submitTask(int taskType, int index, bool &success);

    bool isFinishedMap();

    bool isFinishedReduce();

    bool Done();

private:
    vector <string> files;
    vector <Task> mapTasks;
    vector <Task> reduceTasks;

    mutex mtx;

    long completedMapCount;
    long completedReduceCount;
    bool isFinished;

    string getFile(int index);

    bool assignTask(Task &task);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int id, mr_protocol::AskTaskResponse &reply) {
    // Lab2 : Your code goes here.
    this->mtx.lock();
    Task task;
    bool res = assignTask(task);
    if (res) {
        reply.index = task.index;
        reply.taskType = task.taskType;
        reply.filename = getFile(task.index);
        reply.tot = files.size();
    } else {
        reply.taskType = mr_tasktype::NONE;
    }
    this->mtx.unlock();
    return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
    // Lab2 : Your code goes here.
    this->mtx.lock();
//    printf("coordinator::submittask called %d %d\n", taskType, index);
    if (taskType == mr_tasktype::MAP) {
        if (!this->mapTasks[index].isCompleted) {
            this->completedMapCount++;
        }
        this->mapTasks[index].isAssigned = false;
        this->mapTasks[index].isCompleted = true;
    } else if (taskType == mr_tasktype::REDUCE) {
        if (!this->reduceTasks[index].isCompleted) {
            this->completedReduceCount++;
        }
        this->reduceTasks[index].isAssigned = false;
        this->reduceTasks[index].isCompleted = true;
//        printf("coordinator::submittask completedMapCount=%d completedReduceCount=%d totalmap=%d totalreduce=%d\n",
//               this->completedMapCount, this->completedReduceCount, this->mapTasks.size(), this->reduceTasks.size());
        if (isFinishedReduce() && isFinishedMap()) {
            this->isFinished = true;
        }
    }
    success = true;
//    printf("coordinator::submittask end!\n", taskType, index);
    this->mtx.unlock();
    return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
//	this->mtx.lock();
    string file = this->files[index];
//	this->mtx.unlock();
    return file;
}

bool Coordinator::isFinishedMap() {
    bool isFinished = false;
//	this->mtx.lock();
    if (this->completedMapCount >= long(this->mapTasks.size())) {
        isFinished = true;
    }
//	this->mtx.unlock();
    return isFinished;
}

bool Coordinator::isFinishedReduce() {
    bool isFinished = false;
//	this->mtx.lock();
    if (this->completedReduceCount >= long(this->reduceTasks.size())) {
        isFinished = true;
    }
//	this->mtx.unlock();
    return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
    bool r = false;
    this->mtx.lock();
    r = this->isFinished;
    this->mtx.unlock();
    return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector <string> &files, int nReduce) {
    this->files = files;
    this->isFinished = false;
    this->completedMapCount = 0;
    this->completedReduceCount = 0;

    int filesize = files.size();
    for (int i = 0; i < filesize; i++) {
        this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i, 0});
    }
    for (int i = 0; i < nReduce; i++) {
        this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i, 0});
    }
}

int main(int argc, char *argv[]) {
    int count = 0;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
        exit(1);
    }
    char *port_listen = argv[1];

    setvbuf(stdout, NULL, _IONBF, 0);

    char *count_env = getenv("RPC_COUNT");
    if (count_env != NULL) {
        count = atoi(count_env);
    }

    vector <string> files;
    char **p = &argv[2];
    while (*p) {
        files.push_back(string(*p));
        ++p;
    }

    rpcs server(atoi(port_listen), count);

    Coordinator c(files, REDUCER_COUNT);

    //
    // Lab2: Your code here.
    // Hints: Register "askTask" and "submitTask" as RPC handlers here
    //
    server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
    server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

    while (!c.Done()) {
        sleep(1);
    }

    return 0;
}

//先分配map,map全部做完再分配reduce
bool Coordinator::assignTask(Task &task) {
    bool res = false;
    time_t nowTime = time(NULL);
    if (!isFinishedMap()) {
        long size = this->mapTasks.size();
//        cout << "corrdinator::assigntask size=" << size << endl;
        for (long i = 0; i < size; i++) {
            if (!this->mapTasks[i].isAssigned && !this->mapTasks[i].isCompleted) {
                this->mapTasks[i].isAssigned = true;
                this->mapTasks[i].fromTime = time(NULL);
                task = this->mapTasks[i];
                res = true;
                break;
            }
            if (this->mapTasks[i].isAssigned && !this->mapTasks[i].isCompleted) {
                //时间太长 重新分配
                if (difftime(nowTime, this->mapTasks[i].fromTime) > 60.0) {
                    this->mapTasks[i].fromTime = time(NULL);
                    task = this->mapTasks[i];
                    res = true;
                    break;
                }
            }
        }
    } else if (!isFinishedReduce()) {
        long size = this->reduceTasks.size();
        for (long i = 0; i < size; i++) {
            if (!this->reduceTasks[i].isAssigned && !this->reduceTasks[i].isCompleted) {
                this->reduceTasks[i].isAssigned = true;
                this->reduceTasks[i].fromTime = time(NULL);
                task = this->reduceTasks[i];
                res = true;
                break;
            }
            if (this->reduceTasks[i].isAssigned && !this->reduceTasks[i].isCompleted) {
                //时间太长 重新分配
                if (difftime(nowTime, this->reduceTasks[i].fromTime) > 60.0) {
                    this->reduceTasks[i].fromTime = time(NULL);
                    task = this->reduceTasks[i];
                    res = true;
                    break;
                }
            }
        }
    }
    return res;
}