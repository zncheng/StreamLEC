#ifndef __AFS_WORKER_HPP_INCLUDED__
#define __AFS_WORKER_HPP_INCLUDED__

#include <stdint.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/stat.h> /* For mode constants */
#include <fcntl.h> /* For O_* constants */ 
#include <errno.h>

#include <map>
#include <vector>
#include <string>
#include <deque>

//#include <zookeeper.h>

#include "up_thread.hpp"
#include "up_thread_net.hpp"
#include "main_thread.hpp"
#include "down_thread.hpp"
#include "../queues/zerocopy_ringbuffer.hpp"
#include "../util.hpp"
#include "../wrap_item.hpp"

//using namespace std;
#include "../controller/zk_worker_client.hpp"

//zhandle_t* ZkWorkerClient::zh = NULL;
//std::string ZkWorkerClient::server_name = "";
//clientid_t ZkWorkerClient::myid;
//set<string> ZookeeperClient::upstream_workers;
//deque<ControlMsg> ZookeeperClient::remoteCmd;

/**
 * Parent process of a computation node, consisting of: one dispatcher,
 * one collector, multiple compute_threads and one Zookeeper client
 * @tparam T1 output class of dispatcher, input class of compute_threads
 * @tparam T2 output class of compute_threads, input class of collector
 * @tparam T3 output class of collector, input class of next-hop dispatcher
 */

namespace afs {

enum WindowType {WIN_NO = 0, WIN_HOP, WIN_SLIDE, WIN_DECAY};

template <class InT, class OutT, class RInT=NullClass, class ROutT=NullClass>
class Worker {

typedef WrapItem<InT> WInT;
typedef WrapItem<OutT> WOutT;
typedef WrapItem<RInT> WRInT;
typedef WrapItem<ROutT> WROutT;

private:
    int worker_id_;
    std::string worker_name_;

    /**
     * number of compute_threads (We supports multiple compute_threads within one worker)
     */
    int num_compute_thread_;

    /**
     * references to the compute_threads
     */
    //ComputeThread<InT, OutT, RInT, ROutT>** compute_threads_;
    std::vector<MainThread<InT, OutT, RInT, ROutT>*> compute_threads_;

    UpThread<InT, ROutT>* up_thread_;

    DownThread<OutT, RInT>* down_thread_;

    // New PinCPU interface: from config file not function
    void ParseCPUAffinity(char* str);

public:
    /**
     * Constructor
     * @param worker_name The unique name of the worker
     * @param addr The IP address of Zookeeper server
     */
    Worker(string worker_name, int id, string zkAddr, int num_compute_thread);

    /******************************************************************
     *
     * IMPORTANT Hint:
     * The following three functions add threads to the Worker.
     * DON'T initialize resources when creating the instances
     * Allocate in their ThreadInitHandler() functions
     *
     ****************************************************************/

    void AddUpThread(UpThread<InT, ROutT>* up_thread);
    void AddDownThread(DownThread<OutT, RInT>* down_thread);
    void AddMainThread(MainThread<InT, OutT, RInT, ROutT>* main_thread);

    /**
     * execution all threads (upstream thread, compute_threads, downstream thread)
     */
    void Start(int recovery = 0);
};

/*********************************************************************
 *
 * Implementation
 *
 ********************************************************************/

template <class InT, class OutT, class RInT, class ROutT>
Worker<InT, OutT, RInT, ROutT>::Worker(string worker_name, int id, string zkAddr, int num_compute_thread) :
    worker_id_(id),
    worker_name_(worker_name),
    num_compute_thread_(num_compute_thread),
    //compute_threads_(NULL),
    up_thread_(NULL),
    down_thread_(NULL) {
}

template <class InT, class OutT, class RInT, class ROutT>
void Worker<InT, OutT, RInT, ROutT>::AddUpThread(UpThread<InT, ROutT>* up_thread) {
    up_thread_ = up_thread;
}

template <class InT, class OutT, class RInT, class ROutT>
void Worker<InT, OutT, RInT, ROutT>::AddDownThread(DownThread<OutT, RInT>* down_thread) {
    down_thread_ = down_thread;
}

template <class InT, class OutT, class RInT, class ROutT>
void Worker<InT, OutT, RInT, ROutT>::AddMainThread(MainThread<InT, OutT, RInT, ROutT>* new_thread) {
    compute_threads_.push_back(new_thread);
}

template <class InT, class OutT, class RInT, class ROutT>
void Worker<InT, OutT, RInT, ROutT>::ParseCPUAffinity(char* str) {

    std::vector<std::string> strings;
    char* tok = strtok(str, ";");
    while (tok) {
        strings.push_back(tok);
        tok = strtok(NULL, ";");
    }
    int size = strings.size();
    if (size > 0) {
        // worker
        int cpu = atoi(strings[0].c_str());
        pin_to_cpu(cpu);
    }
    if (size > 1) {
        size_t start = 0;
        size_t found = strings[1].find(',', start);
        int index = 0;
        while (found != std::string::npos) {
            int cpu = atoi(strings[1].substr(start, found-start).c_str());
            compute_threads_[index++]->AssignCPU(cpu);
            start = found+1;
            found = strings[1].find(',', start);
        }
    }
    if (size > 2) {
        int cpu = atoi(strings[2].c_str());
        up_thread_->AssignCPU(cpu);
    }
    if (size > 3) {
        int cpu = atoi(strings[3].c_str());
        down_thread_->AssignCPU(cpu);
    }
}

template <class InT, class OutT, class RInT, class ROutT>
void Worker<InT, OutT, RInT, ROutT>::Start(int recovery) {
    Config* config = Config::getInstance();

    char* zk_addr = config->getstring("sys.zookeeper_server", NULL);
    ZkWorkerClient* zk_client = NULL;
    if (zk_addr == NULL) {
        LOG_WARN("No ZooKeeper server\n")
    }
    else {
        zk_client = new ZkWorkerClient(worker_name_, zk_addr);
        zk_client->initialize();
    }

    LOG_SYS(HLINE "[Worker] Setup inter-thread channels...\n");

    LOG_MSG("    thread IDs...\n");
    up_thread_->set_tid(num_compute_thread_);
    up_thread_->set_wid(worker_id_);
    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->set_tid(i);
        compute_threads_[i]->set_wid(worker_id_);
    }
    if (down_thread_) {
        down_thread_->set_tid(num_compute_thread_+1);
        down_thread_->set_wid(worker_id_);
    }

    LOG_MSG("    CPU affinity...\n");
    char* cpu_affinity = config->getstring("sys.cpu_affinity", NULL);
    if (cpu_affinity) {
        ParseCPUAffinity(cpu_affinity);
    }

    LOG_MSG("    inter-thread channels...\n");
    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->ConnectUpThread(up_thread_);
    }
    if (down_thread_) {
        for (int i=0; i<num_compute_thread_; i++) {
            compute_threads_[i]->ConnectDownThread(down_thread_);
        }
    }

    LOG_SYS(HLINE "[Worker] Create threads...\n");

    up_thread_->Create();
    up_thread_->WaitThread(afs_zmq::command_t::ready);

    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->Create();
        compute_threads_[i]->WaitThread(afs_zmq::command_t::ready);
    }

    if (down_thread_) {
        down_thread_->Create();
        down_thread_->WaitThread(afs_zmq::command_t::ready);
    }

    //if (recovery > 0) {
    //    LOG_MSG("    Recovery mode...\n");
    //    
    //    for(int i = 0; i < num_compute_thread_; i++) {
    //        compute_threads_[i]->ComputeThreadRecovery();
    //    }
    //    
    //}

    LOG_SYS(HLINE "[Worker] Start running...\n");
    up_thread_->NotifyThread(afs_zmq::command_t::type_t::ready);
    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->NotifyThread(afs_zmq::command_t::type_t::ready);
    }
    if (down_thread_) {
        down_thread_->NotifyThread(afs_zmq::command_t::type_t::ready);
    }

    size_t stop = 0, max_stop = num_compute_thread_ + 1;
    afs_zmq::command_t cmd;
    while (1) {
        int rc = -1;
        rc = up_thread_->WaitThread(&cmd, false);
        if (rc == -1) {
        }
        else if (rc == afs_zmq::command_t::type_t::finish) {
            stop++;
        }
        else {
            afs_assert(0, "In_Thread WaitThread unknown type %d\n", rc);
        }

        for (int i=0; i<num_compute_thread_; i++) {
            rc = compute_threads_[i]->WaitThread(&cmd, false);
            if (rc == -1) {
            }
            else if (rc == afs_zmq::command_t::type_t::finish) {
                stop++;
            }
            else if (rc == afs_zmq::command_t::type_t::backup) {
                usleep(cmd.len);
                compute_threads_[i]->NotifyThread(afs_zmq::command_t::type_t::done);
            }
            else {
                afs_assert(0, "Analzyer WaitThread unknown type %d\n", rc);
            }
        }

        if (stop == max_stop) {
            break;
        }
    }

    if (down_thread_) {
        down_thread_->WaitThread(afs_zmq::command_t::finish);
    }

    LOG_SYS(HLINE "[Worker] Clean and output...\n");
    up_thread_->NotifyThread(afs_zmq::command_t::type_t::clean);
    up_thread_->WaitThread(afs_zmq::command_t::finish);
    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->NotifyThread(afs_zmq::command_t::type_t::clean);
        compute_threads_[i]->WaitThread(afs_zmq::command_t::finish);
    }
    if (down_thread_) {
        down_thread_->NotifyThread(afs_zmq::command_t::type_t::clean);
        down_thread_->WaitThread(afs_zmq::command_t::finish);
    }

    LOG_SYS(HLINE "[Worker] Finish...\n");
    up_thread_->Destroy();
    for (int i=0; i<num_compute_thread_; i++) {
        compute_threads_[i]->Destroy();
    }
    if (down_thread_) {
        down_thread_->Destroy();
    }

    if (zk_client) {
        zk_client->finish();
    }
}

}

#endif 
