#include <stdlib.h>
#include <string>
#include "decode_default.hpp"
#include "decode_interface.hpp"

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
    afs_assert(worker_id>=0, "worker id is not specified in config file\n");

    afs::Worker<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>* worker = 
        new afs::Worker<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>(std::string(worker_name), worker_id, std::string(""), num_compute_thread);


    //Create up_thread and add to worker
    int num_in = config->getint("num_upstream",-1);
    afs_assert(num_in>0, "number of upstream worker is not specified in config file\n");
    char * addr = config->getstring("listen_addr", NULL);
    if(addr == NULL){
        LOG_MSG("Find not [listen addr] in the config file\n");
        return -1;
    }
    afs::UpThreadNet<struct O_CodingItem, struct CodingItem>* i_thread = new afs::UpThreadNet<struct O_CodingItem, struct CodingItem>(num_in, num_compute_thread);
    i_thread->AddSource(addr);
    worker->AddUpThread(i_thread);

    //create compute_thread and add to Worker 
    int num_out = config->getint("num_downstream", -1);
    afs_assert(num_out>=0, "downstream number is not specified in config file\n");
    char* type = config->getstring("coding_type", NULL);
    if (type == NULL) {
        LOG_MSG("[data source] must be specified as `trace` input! \n");
    }
    int parameter_r = config->getint("parameter_r", -1);
    int parameter_k = config->getint("parameter_k", -1); 
    afs_assert(parameter_r > 0, "[parameter_r] is not specefied\n"); 
    afs_assert(parameter_k > 0, "[parameter_k] is not specefied\n"); 
    afs_assert(parameter_k + parameter_r == num_in - 1, "[parameter_k+parameter_r] should equal to processor numbers\n"); 

    std::string code_type = type;
    if (code_type == "Customized") {
        // use placment new to guarantee 32 byte algined to support SIMD
        void *buf = aligned_alloc(32, sizeof(CustomizedDecodeThread)); 
        CustomizedDecodeThread* compute_thread = new (buf) CustomizedDecodeThread(num_in, num_out);
        worker->AddComputeThread(compute_thread);
    }
    else if (code_type == "RAID5" || parameter_r == 1){
        for (int i=0; i<num_compute_thread; i++) {
            // use placment new to guarantee 32 byte algined to support SIMD
            void *buf = aligned_alloc(32, sizeof(RAID5DecodeThread)); 
            RAID5DecodeThread* compute_thread = new (buf) RAID5DecodeThread(num_in, num_out);
            worker->AddComputeThread(compute_thread);
        }
    }
    else if (code_type == "RAID6" || parameter_r == 2){
        for (int i=0; i<num_compute_thread; i++){
            // use placment new to guarantee 32 byte algined to support SIMD
            void *buf = aligned_alloc(32, sizeof(RAID6DecodeThread)); 
            RAID6DecodeThread* compute_thread = new (buf) RAID6DecodeThread(num_in, num_out);
            worker->AddComputeThread(compute_thread);
        }

    }
    else if (code_type == "RS-code" || parameter_r > 2) {
        for (int i = 0; i < num_compute_thread ; i++) {
            // use placment new to guarantee 32 byte algined to support SIMD
            void *buf = aligned_alloc(32, sizeof(RSDecodeThread)); 
            RSDecodeThread* compute_thread = new (buf) RSDecodeThread(num_in, num_out);
            worker->AddComputeThread(compute_thread);
        }
    }
    else{
        LOG_MSG("Unkonwn coding type, currently, CodedStream only supports RAID5, RAID6 and RS_code\n");
    }


    //create down_thread and add to Worker
    afs::DownThreadNet<struct CodingItem, struct CodingItem>* o_thread = new afs::DownThreadNet<struct CodingItem, struct CodingItem>(num_compute_thread, num_out);
    std::string prefix("downstream_addr");
    for (int i = 0; i < num_out; i++){
        char tmp[10];
        sprintf(tmp, "%d", i);
        std::string key = prefix + tmp;
        char * addr = config->getstring(key.c_str(), NULL);
        o_thread->AddDest(i, addr);
    }
    worker->AddDownThread(o_thread);


    // Start the worker
    worker->Start(0);

    return 0;
}
