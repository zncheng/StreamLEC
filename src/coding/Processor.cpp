#include "Processor.hpp"
#include "processor_interface.hpp"

int main(int argc, char* argv[]) {
    // worker name is used to find out the corresponding config section in the config file
    if (argc != 3) {
        LOG_MSG("Requires: Command [Config file] [Worker Name]\n");
        return -1;
    }

    char* config_name = argv[1];
    char* worker_name = argv[2];

    Config *config = Config::getInstance(config_name, worker_name);

    int num_compute_thread = config->getint("num_compute_threads", 0);
    if (num_compute_thread == 0) {
        LOG_MSG("[num_compute_threads] must be specified in the config file: at least 1\n");
        return -1;
    }

    // create worker
    int worker_id = config->getint("worker_id", -1);
    afs_assert(worker_id>=0, "worker id is not specified in config file! \n");
    afs::Worker<struct CodingItem, struct O_CodingItem, struct O_CodingItem, struct afs::NullClass>* worker = 
        new afs::Worker<struct CodingItem, struct O_CodingItem, struct O_CodingItem, struct afs::NullClass>(std::string(worker_name), worker_id, std::string(""), num_compute_thread);
    char* addr = config->getstring("listen_addr", NULL);
    if (addr == NULL) {
        LOG_MSG("[listen addr] must be specified in the config file!\n");
        return -1;
    }

    int num_in = config->getint("num_upstream", -1);
	afs_assert(num_in>0, "[num_upstream] should be spcified in config file!\n");
    int num_out = config->getint("num_downstream", -1);
	afs_assert(num_out>0, "[num_downstream] should be spcified in config file!\n");
    
    afs::UpThreadNet<struct CodingItem, struct afs::NullClass>* i_thread = new afs::UpThreadNet<struct CodingItem, struct afs::NullClass>(num_in, num_compute_thread);

    i_thread->AddSource(addr);
    worker->AddUpThread(i_thread);

    for (int i=0; i<num_compute_thread; i++) {
        ProcessorThread* compute_thread =
            new ProcessorThread(num_in, num_out);
        worker->AddComputeThread(compute_thread);
    }

    afs::DownThreadNet<struct O_CodingItem, struct O_CodingItem> * o_thread = new afs::DownThreadNet<struct O_CodingItem, struct O_CodingItem>(num_compute_thread, num_out);
    std::string prefix("downstream_addr");
    for (int i=0; i<num_out; i++)
    {
        char tmp[10];
        sprintf(tmp, "%d", i);
        std::string key = prefix +tmp;
        char * addr = config->getstring(key.c_str(), NULL);
        o_thread->AddDest(i, addr);
    }       
    worker->AddDownThread(o_thread);


    // Start
    worker->Start(0);

    return 0;
}
