#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <unistd.h>
#include <sys/time.h>
#include <algorithm>
#include <streamlec.h>
#include "parameters.hpp"

class RSDecodeThread : public afs::MainThread<Message, struct afs::NullClass, struct afs::NullClass, Message> {
    public:
        RSDecodeThread(int num_upstreams) :
            afs::MainThread<Message, struct afs::NullClass, struct afs::NullClass, Message>(num_upstreams, 0) {}

    private:
       
        /**
        * Internal global variables
        **/ 
        alignas(32) double received[MAX_MICRO_BATCH_SIZE][MAX_DATA_LENGTH];
        alignas(32) double decoded[MAX_WORKER_NUM][MAX_DATA_LENGTH];
        //struct KEY cache[MAX_MICRO_BATCH_SIZE];
        uint32_t counter[MAX_MICRO_BATCH_SIZE];
        bool uncoded_visited[MAX_MICRO_BATCH_SIZE];
        bool coded_visited[MAX_MICRO_BATCH_SIZE * 2];
        uint32_t RS_k = -1;
        uint32_t RS_r = -1;
        uint32_t batch_size = 0;
        uint32_t item_size = 0;
        bool enable_adaptive = false;
        bool run_coded = true;
        bool next_run_coded = true;
        int upstream_num;
        int upstream_id[MAX_WORKER_NUM]; 
        double processor_progress[MAX_WORKER_NUM];
        double slide_window[SLIDING_WINDOW_SIZE];
        int slide_count = 0;
        uint32_t batch_progress_counter = 0;
        uint64_t watermark = 0;
        struct timeval m_start, m_end;
        double micro_batch_interval = 1000;
        uint32_t current_stripe_id = 0;
        Message * feedback;
        bool trigger_monitoring = true;

        // application-specific
        // double model[MAX_MODEL_SIZE] = {0};
        uint32_t aggregate_count = 0;
        // int attr_num = 0;

        /**
        * extensible interfaces
        **/
 
        Result Recompute(Item &decoded);
        void Aggregate(Result &result);
        void Decode(double *receive, double *decoded);

        /**
        * Initialization and clean for main thread
        **/
        void MainThreadInit() {

            Config* config = Config::getInstance();

            upstream_num = config->getint("num_upstream",0);
            afs_assert(upstream_num, "[num_upstream] is not set!\n");
            RS_k = config->getint("RS_k",0);
            afs_assert(RS_k > 0, "[RS_k] is not set!\n");

            RS_r = config->getint("RS_r",0);
            afs_assert(RS_r > 0, "[RS_r] is not set!\n");

            char *upid = config->getstring("upstream_id", NULL);
            if (upid == NULL) {
                LOG_ERR("[upstream_id] must be specified in the coding file!\n");
            }
            char* tmp;
            char* token = strtok_r(upid, ",", &tmp);
            int index = 0;
            while (token) {
                upstream_id[index++] = atoi(token);
                token = strtok_r(NULL, ",", &tmp);
            }        

            // for decoding
            init_origin(RS_k);

            batch_size = config->getint("batch_size", -1);
            afs_assert(batch_size >= 0, "[batch_size] should be indicated in the config file!\n");

            item_size = config->getint("item_size", -1);
            afs_assert(item_size >= 0, "[item_size] should be indicated in the config file!\n");
            
            enable_adaptive = config->getint("enable_adaptive", -1);
            batch_progress_counter = 0;
            gettimeofday(&m_start, NULL);
            feedback = new Message;
        }

        void MainThreadFinish() { }

        void TimerCallback() {
            // handle timeout
            if (true) { // crash
                Message *ack = new Message;
                ack->key.adaptive = next_run_coded;
                EmitFeedback(upstream_id[0], *ack);
                for (uint32_t i = 0; i < batch_size; i++) {
                    if (!uncoded_visited[i]){
                        watermark = i;
                        break;
                    }
                }
                ack->key.sIndex = -1;
                ack->key.stripeID = watermark;
            }
        }

        bool check_if_lose(uint32_t stripe_id) {
            uint32_t base = stripe_id * (RS_r + RS_k);
            for (uint32_t i = base; i < base + RS_k; i++) {
                if (!coded_visited[i])
                    return true;
            }
            return false; 
        }

        void WorkerMainEntry(uint32_t worker, uint32_t thread, uint64_t seq, Message &msg) {
            
            //if(msg.key.sIndex == -1) {
            //    return;
            //}

            int processor_id = 0;
            /* 
                start to process received results
            */
            if (enable_adaptive && !run_coded) { // uncoded mode
                if (!uncoded_visited[msg.key.sIndex]) {
                    // repeated in this micro-batch, drop it.
                    uncoded_visited[msg.key.sIndex] = true;
                }
                else return;
                Result rn = msg; 
                Aggregate(rn);
                processor_id = msg.key.sIndex % (RS_k + RS_r);
                batch_progress_counter ++;
                watermark ++;
            }
            else { // coded mode
                processor_id = msg.key.sIndex;
                int stripe_offset = msg.key.stripeID % (batch_size / RS_r);
                int offset = stripe_offset * (RS_k + RS_r) + msg.key.sIndex;
                if ((((uint32_t)msg.key.stripeID <= watermark) || (counter[stripe_offset] > RS_k)) && watermark > 0) return;

                // LOG_MSG("Send stripe %d index %d, first value %lf stripe offset %d offset %d\n", msg.key.stripeID, msg.key.sIndex,msg.pop_value(1), stripe_offset, offset);

                Result rn = msg;
                // buffer data or parity
                memcpy(&received[offset][0], &rn.value[0], sizeof(double) * item_size);
                if ((uint32_t)rn.key.sIndex < RS_k) {
                    batch_progress_counter ++;
                    // extract results
                    memmove(&rn.value[0], &rn.value[item_size], rn.size() - item_size); 
                    Aggregate(rn);
                }
                coded_visited[offset] = true;
                counter[stripe_offset] ++;
                if (counter[stripe_offset] == RS_k) {
                    // RS_k items/parity for this stripe
                    if(check_if_lose(stripe_offset)) {
                        // some data loss, need to decode and rec-compute
                       current_stripe_id = stripe_offset;
                       Decode(&received[stripe_offset][0],&decoded[0][0]);
                       uint32_t base = stripe_offset * (RS_r + RS_k);
                       for (uint32_t i = base; i < base + RS_k; i++) {
                           if (!coded_visited[i]) {
                               Item * decode = new Item;
                               decode->push_array(&decoded[i][0], item_size);
                               decode->key.stripeID = stripe_offset;
                               decode->key.sIndex = i - base;
                               Result dec = Recompute(*decode);
                               Aggregate(dec);
                               batch_progress_counter ++;
                            }
                       }
                    }
                    watermark = rn.key.stripeID;
                    counter[stripe_offset] ++;
                }
            }
            
            /* 
                monitoring the progress of each processor
            */
            processor_progress[processor_id] += 1;
            if (enable_adaptive && !run_coded && (batch_progress_counter == 1)) { // for crash detection in uncoded mode
                    unsigned int timeout = micro_batch_interval * (STRAGGLER_THRESHOLD - 1) * 1000000;
                    afs_zmq::command_t cmd; 
                    cmd.type = afs_zmq::command_t::backup;
                    cmd.len = timeout;
                    ThreadBase::NotifyWorker(cmd);
             }

            if (enable_adaptive && trigger_monitoring && ((!run_coded && msg.key.stripeID >= (batch_size - RS_k - RS_r) ) || (run_coded && (msg.key.stripeID == batch_size / RS_k)) )) {
                // LOG_MSG("Monitoring\n");
                std::sort(processor_progress, (processor_progress + upstream_num -1));
                slide_window[slide_count] = processor_progress[RS_r] / processor_progress[RS_r - 1];
                if(slide_count == SLIDING_WINDOW_SIZE -1)
                    slide_count = 0;
                else
                    slide_count ++;
                double sum = 0;
                for (int i = 0; i < SLIDING_WINDOW_SIZE; i ++)
                    sum += slide_window[i];
                if ( sum / SLIDING_WINDOW_SIZE * SCALING_FACTOR > STRAGGLER_THRESHOLD) {
                    next_run_coded = false;
                    //for (int i = 0; i < SLIDING_WINDOW_SIZE; i ++)
                    //    slide_window[i] = -100;
                 }  
                else
                    next_run_coded = false;
                trigger_monitoring = false;
            }
            if (!enable_adaptive)
                next_run_coded = true;
            // LOG_MSG("progress is %ld\n", batch_progress_counter);
            if (batch_progress_counter == batch_size) {  // micro-batch finished
                // feedback and ack
                run_coded = next_run_coded;
                for (int i = 1; i < upstream_num; i++) {
                    EmitFeedback(upstream_id[i], *feedback);
                }
                delete feedback;
                feedback = new Message;
                Message *ack = new Message;
                ack->key.adaptive = next_run_coded;
                EmitFeedback(upstream_id[0], *ack);
                delete ack;

                // clear for next micro-batch
                gettimeofday(&m_end, NULL);
                micro_batch_interval = (m_end.tv_sec + m_end.tv_usec/1000000.0) - (m_start.tv_sec + m_start.tv_usec/1000000.0);
                memset(coded_visited, false, sizeof(coded_visited));
                memset(uncoded_visited, false, sizeof(uncoded_visited));
                batch_progress_counter = 0;
                for (int i = 0; i < MAX_WORKER_NUM; i++) {
                    processor_progress[i] = 0;
                }                
                for (int i = 0; i < MAX_MICRO_BATCH_SIZE; i++) {
                    counter[i] = 0;
                }
                if(enable_adaptive) {
                    trigger_monitoring = true;
                }
                LOG_MSG("Current watermark is %lu\n", watermark);  
            }
        }
};

