#include <stdlib.h>
#include <string>
#include "encode.hpp"
#include "source_interface.hpp"

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

    char* data_source = config->getstring("data_source", NULL);
    if (data_source == NULL) {
        LOG_MSG("[data source] must be specified as `trace` input! \n");
        return -1;
    }

    std::string spout = data_source;

    if (spout == "trace"){

        // create worker
        int worker_id = config->getint("worker_id", -1);
        afs_assert(worker_id>=0, "worker id is not specified in config file!\n");
        afs::Worker<struct afs::DoubleItem, Message, Message, struct afs::NullClass>* worker = 
            new afs::Worker<struct afs::DoubleItem, Message, Message, struct afs::NullClass>(std::string(worker_name), worker_id, std::string(""), num_compute_thread);

        //Create up_thread and add to worker
        afs::RouterRR* router =  new afs::RouterRR(num_compute_thread);
        afs::UpThreadTraceAFS<afs::DoubleItem>* i_thread = new afs::UpThreadTraceAFS<afs::DoubleItem>(router); 
        worker->AddUpThread(i_thread);

        //create compute_thread and add to Worker 
        int num_out = config->getint("num_downstream", -1);
        afs_assert(num_out>=0, "[num_downstream] is not specified in config file!\n");

        // using placement new to ensure aligned 32 byte for SIMD optimization
        void *buf = aligned_alloc(32, sizeof(RSEncodeThread));
        RSEncodeThread* main_thread = new (buf) RSEncodeThread(num_out);
        worker->AddMainThread(main_thread);

        //create down_thread and add to Worker
        afs::DownThreadNet<Message, Message>* o_thread = new afs::DownThreadNet<Message, Message>(num_compute_thread, num_out);
        std::string prefix("downstream_addr");
        for (int i = 0; i < num_out; i++){
            char tmp[10];
            sprintf(tmp, "%d", i);
            std::string key = prefix + tmp;
            char* addr = config->getstring(key.c_str(), NULL);
            o_thread->AddDest(i, addr);
        }
        worker->AddDownThread(o_thread);

        // start worker
        worker->Start(0);

    } else {
        // error configuration to specify input source
        LOG_MSG("Currently only support trace input!\n");
        return -1;
    }

    return 0;
}
