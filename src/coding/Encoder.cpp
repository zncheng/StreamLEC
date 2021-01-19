#include <stdlib.h>
#include <string>
#include "encode_default.hpp"
#include "encode_interface.hpp"

int main(int argc, char* argv[]) {

    // worker name is used to find out the corresponding config section in the config file
    if (argc != 3) {
        LOG_MSG("Requires: Command [Config file] [Operator Name]\n");
        return -1;
    }

    char* config_name = argv[1];    
    char* worker_name = argv[2];

    Config *config = Config::getInstance(config_name, worker_name);

    int num_compute_thread = config->getint("num_compute_threads", 0);
    if (num_compute_thread == 0) {
        LOG_MSG("[num_compute_threads] must be specified in the config file: at least 1!\n");
        return -1;
    }

    char* data_source = config->getstring("data_source", NULL);
    if (data_source == NULL) {
        LOG_MSG("[data source] must be specified as `trace` input! \n");
        return -1;
    }

    std::string spout = data_source;

    if (spout == "trace"){

        // create operator
        int worker_id = config->getint("worker_id", -1);
        afs_assert(worker_id>=0, "worker id is not specified in config file!\n");
        afs::Worker<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>* worker = 
            new afs::Worker<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>(std::string(worker_name), worker_id, std::string(""), num_compute_thread);

        //Create up_thread and add to worker
        afs::RouterRR* router =  new afs::RouterRR(num_compute_thread);
        afs::UpThreadTraceAFS<afs::DoubleItem>* i_thread = new afs::UpThreadTraceAFS<afs::DoubleItem>(router); 
        worker->AddUpThread(i_thread);

        //create compute_thread and add to Worker 
        int num_out = config->getint("num_downstream", -1);
        afs_assert(num_out>=0, "[num_downstream] is not specified in config file!\n");
        char* type = config->getstring("coding_type", NULL);
        if (type == NULL) {
            LOG_MSG("[coding_type] must be specified in the coding file, currently CodedStream supports RAID5 and RAID6!\n");
        }
        
        int parameter_r = config->getint("parameter_r", -1);
        int parameter_k = config->getint("parameter_k", -1); 
        afs_assert(parameter_r > 0, "[parameter_r] is not specefied\n"); 
        afs_assert(parameter_k > 0, "[parameter_k] is not specefied\n"); 
        afs_assert(parameter_k + parameter_r == num_out - 1, "[parameter_k+parameter_r] should equal to processor numbers\n"); 
      
        std::string code_type = type;
        if (code_type == "customized") {
            for (int i = 0; i < num_compute_thread; i++) {
                // using placement new to ensure aligned 32 byte for SIMD optimization
                void *buf = aligned_alloc(32, sizeof(CustomizedEncodeThread));
                CustomizedEncodeThread* compute_thread = new (buf) CustomizedEncodeThread(num_out);
                worker->AddComputeThread(compute_thread);
            }
        }
        else if (code_type == "RAID5" || parameter_r == 1){
            for (int i = 0; i < num_compute_thread; i++) {
                // using placement new to ensure aligned 32 byte for SIMD optimization
                void *buf = aligned_alloc(32, sizeof(RAID5EncodeTraceThread));
                RAID5EncodeTraceThread* compute_thread = new (buf) RAID5EncodeTraceThread(num_out);
                worker->AddComputeThread(compute_thread);
            }
        } 
        else if (code_type == "RAID6" || parameter_r == 2){
            for (int i = 0; i < num_compute_thread; i++){
                // using placement new to ensure aligned 32 byte for SIMD optimization
                void *buf = aligned_alloc(32, sizeof(RAID6EncodeTraceThread));
                RAID6EncodeTraceThread* compute_thread = new (buf) RAID6EncodeTraceThread(num_out);
                //cout<<"compute thread size"<< sizeof(RAID6EncodeTraceThread)<<endl;
                worker->AddComputeThread(compute_thread);
            }
        }
        else if (code_type == "RS-code" || parameter_r > 2) {
            for (int i = 0; i < num_compute_thread; i++ ){
                // using placement new to ensure aligned 32 byte for SIMD optimization
                void *buf = aligned_alloc(32, sizeof(RSEncodeTraceThread));
                RSEncodeTraceThread* compute_thread = new (buf) RSEncodeTraceThread(num_out);
                worker->AddComputeThread(compute_thread);
            }
        }
        else {
            LOG_MSG("Unkonwn coding_type, currently, CodedStream only supports RAID5, RAID6, and RS-code\n");
        }

        //create down_thread and add to Worker
        afs::DownThreadNet<struct CodingItem, struct CodingItem>* o_thread = new afs::DownThreadNet<struct CodingItem, struct CodingItem>(num_compute_thread, num_out);
        std::string prefix("downstream_addr");
        for (int i = 0; i < num_out; i++){
            char tmp[10];
            sprintf(tmp, "%d", i);
            std::string key = prefix + tmp;
            char* addr = config->getstring(key.c_str(), NULL);
            o_thread->AddDest(i, addr);
        }
        worker->AddDownThread(o_thread);
        // start the opeartor
        worker->Start(0);

    } else {
        // error configuration to specify input source
        LOG_MSG("Currently only support tracd input!\n");
        return -1;
    }

    return 0;
}
