#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <sys/time.h>
#include <immintrin.h>
#include <vector>
#include <map>
#include <streamlec.h>
#include "parameters.hpp"

class RSEncodeThread : public afs::MainThread<struct afs::DoubleItem, Message, Message, struct afs::NullClass> {
    public:
        RSEncodeThread(int num_downstreams) :
            afs::MainThread<struct afs::DoubleItem, Message, Message, struct afs::NullClass>(0, num_downstreams) {}

    private:

        /**
        * Internal global variables
        **/
        alignas(32) double Parity[MAX_R_VALUE][MAX_DATA_LENGTH];
        double cache[MAX_MICRO_BATCH_SIZE][MAX_DATA_LENGTH];
        uint32_t downstream_num;
        uint32_t RS_k = -1;
        uint32_t RS_r = -1;
        uint32_t counter = 0;
        uint32_t batch_size = 0;
        uint32_t item_size = 0;
        uint32_t worker_id = -1;
        uint64_t watermark = 0;
        bool enable_adaptive = false;
        bool run_coded = true;    
        // for non-block communication
        vector<pair<int, Item*>> nonblock;

        /**
        *  extensible interfaces and data structures
        **/
        void Encode(int sIndex, Item& data, double *parity);
        void ProcessACK(ACK& ack);
        // users can fill in this feedback, which will be send to each processors at the end of each micro-batch

        /**
        *  Initialization and clean for main thread
        **/
        void MainThreadInit() {

            Config* config = Config::getInstance();

            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");

            RS_k = config->getint("RS_k",0);
            afs_assert(RS_k > 0, "[RS_k] is not set!\n");

            RS_r = config->getint("RS_r",0);
            afs_assert(RS_r > 0, "[RS_r] is not set!\n");

            batch_size = config->getint("batch_size", -1);
            afs_assert(batch_size >= 0, "[batch_size] should be indicated in the config file!\n");

            item_size = config->getint("item_size", -1);
            afs_assert(item_size >= 0, "[item_size] should be indicated in the config file!\n");
            
            enable_adaptive = config->getint("enable_adaptive", -1);
            run_coded = true;
            worker_id = get_wid();
            // feedback = new Feedback;
            for (uint32_t i = 0; i < RS_r; i++) 
                for (uint32_t j = 0; j < item_size; j++) 
                    Parity[i][j] = 0;

        }

        void MainThreadFinish() { }
        
        void WorkerBackwardEntry(int worker, int thread, struct Message &msg) {
        }

        /**
        *   Main entry for each item produce in source
        **/
        void WorkerMainEntry(uint32_t worker, uint32_t thread, uint64_t seq, struct afs::DoubleItem &tuple) {

            // uncoded mode
            if (enable_adaptive && !run_coded) {
                Item* data = new Item;
                data->push_array(&tuple.raw_data[0], item_size);
                data->key.sourceID = worker_id;
                data->key.sinkID = worker_id;
                data->key.stripeID = counter;
                data->key.sIndex = counter % batch_size;
                data->key.adaptive = true;
                int offset = data->key.sIndex % (RS_k + RS_r);
                Emit(offset, *data);
                delete data;

                // cache for re-transmit
                memcpy(cache[counter % batch_size], &tuple.raw_data[0], sizeof(double) * item_size); 

                counter ++;

                if (counter % batch_size == 0) {
                    ACK ack = Recv(worker_id);
                    ProcessACK(ack);
                }
                return;
            }

            // run coded mode
            Item* data = new Item;
            data->push_array(&tuple.raw_data[0], item_size);
            data->key.sourceID = worker_id;
            data->key.sinkID = worker_id;
            data->key.stripeID = counter / RS_k;
            data->key.sIndex = (counter % batch_size ) % RS_k;
            data->key.adaptive = false;

            //// block communication
            //Emit(data->key.sIndex, *data);
            //Encode(data->key.sIndex, *data, &Parity[0][0]);

            // non-block communication
            if (Nonblock_Emit(data->key.sIndex,0, *data) == 1) {
                nonblock.push_back(make_pair((int)data->key.sIndex, data)); 
                Encode(data->key.sIndex, *data, &Parity[0][0]);
            }
            else {
                Encode(data->key.sIndex, *data, &Parity[0][0]);
                delete data;
            }

            if ((uint32_t)data->key.sIndex == RS_k -1) {
                for (uint32_t i = 0; i < RS_r; i++) {
                    Item* parity = new Item;
                    parity->push_array(&Parity[i][0], item_size);
                    parity->key.sourceID = worker_id;
                    parity->key.sinkID = worker_id;
                    parity->key.stripeID = counter / RS_k;
                    parity->key.sIndex = RS_k + i;
                    parity->key.adaptive = false;

                    // block communication
                    //Emit(RS_k+i, *parity);
                    //delete parity;
                    
                    //non-block communication
                    if (Nonblock_Emit(RS_k+i, 0, *parity) == 1) {
                        nonblock.push_back(make_pair(RS_k+i, parity)); 
                    }
                    else {
                        delete parity;
                    }

                    for (uint32_t j = 0; j < item_size; j++)
                        Parity[i][j] = 0;
                }

                while(nonblock.size() > RS_r) {
                    for(auto it = nonblock.begin(); it != nonblock.end(); it++)
                    {
                            int seq = (*it).first;
                            Item *send = (*it).second;
                            int ret = Nonblock_Emit(seq, 0, *send);
                            if (ret == 0) {
                                delete send;
                                nonblock.erase(it--);
                            }
                    }
                }

                while(nonblock.size() > 0) {
                    delete nonblock.front().second;
                    nonblock.erase(nonblock.begin());
                }

                 //LOG_MSG("Finish stripe %d msg\n", counter / RS_k);
            }
            counter ++;
            
            //if(counter == batch_size) {
            //    data->key.sIndex = -1;
            //    Emit(RS_k + RS_r, *data);
            //    // counter = 0;
            //}

            // wait for ACK
            if (counter % batch_size == 0) {
                LOG_MSG("Finish micro_batch %d!\n", counter / batch_size);
                ACK ack = Recv(worker_id);
                ProcessACK(ack);
            }
        }
};


