#include <stdlib.h>
#include <string>
#include "decode.hpp"
#include "sink_interface.hpp"

int main(int argc, char* argv[]) {
    // worker name is used to find out the corresponding config section in the config file
    if (argc != 3) {
        LOG_MSG("Requires: Command [Config file] [Worker Name]\n");
        return -1;
    }

    char* config_name = argv[1];    
    char* worker_name = argv[2];

    Config *config = Config::getInstance(config_name, worker_name);

    int num_compute_thread = 1; 

    // create worker
    int worker_id = config->getint("worker_id", -1);
    afs_assert(worker_id>=0, "worker id is not specified in config file\n");

    afs::Worker<Message, struct afs::NullClass, struct afs::NullClass, Message>* worker = 
        new afs::Worker<Message, struct afs::NullClass, struct afs::NullClass, Message>(std::string(worker_name), worker_id, std::string(""), num_compute_thread);


    //Create up_thread and add to worker
    int num_in = config->getint("num_upstream",-1);
    afs_assert(num_in>0, "number of upstream worker is not specified in config file\n");
    char * addr = config->getstring("listen_addr", NULL);
    if(addr == NULL){
        LOG_MSG("Find not [listen addr] in the config file\n");
        return -1;
    }
    afs::UpThreadNet<Message, Message>* i_thread = new afs::UpThreadNet<Message, Message>(num_in, num_compute_thread);
    i_thread->AddSource(addr);
    worker->AddUpThread(i_thread);

    // use placment new to guarantee 32 byte algined to support SIMD
    void *buf = aligned_alloc(32, sizeof(RSDecodeThread)); 
    RSDecodeThread* main_thread = new (buf) RSDecodeThread(num_in);
    worker->AddMainThread(main_thread);

    // Start the worker
    worker->Start(0);

    return 0;
}
