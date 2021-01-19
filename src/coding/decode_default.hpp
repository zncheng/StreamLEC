#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <unistd.h>
#include <sys/time.h>
#include <algorithm>

#include <codestream.h>

#define max_stale 150000
#define max_coding_length 256
#define max_upstream_size 50
#define slide_window_size 5
#define window_size 5
#define max_attrnum 205
#define max_node_size 50
#define max_node_tolerance 12
#define max_worker_num_rs 50
#define max_worker_num 50
#define window_size_rs 5

class RAID5DecodeThread : public afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem> {
    public:
        RAID5DecodeThread(int num_upstreams, int num_downstreams) :
            afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>(num_upstreams, num_downstreams) {}

    private:

        alignas(32) double Pdrive[max_stale + 1][max_coding_length];
        double coded[max_stale + 1][max_coding_length];
        int width = 1;
        int length;
        int downstream_num;
        int upstream_num;
        int count[max_stale + 1];
        int seq_sum[max_stale + 1];
        bool ready[max_stale + 1];
        long long progress = 0;
        long long watermark = 0;
        int micro_batch = 0;
        int syn_id = -1;
        std::string feedback_type;
        std::string syn_type;
        int feedback_count = 0;
        int upstream_id[100];
        int thread_id;
        int num_compute_thread;
        int enable_adaptive = 0;
        int do_encode = 0;
        int adaptive_count = 0;
        double current_record[max_upstream_size];
        double history[slide_window_size];
        int history_count;  
        double straggler_threshold;
        int erasure_node;
        int old = -1;
        int aggregate_count = 0;
        struct timeval m_start, m_end;
        struct timeval start_time, current_time, end_time;
        bool first, last;

        // application-specific
        double model[max_attrnum] = {0};

        // interfaces
        void Recompute(struct O_CodingItem &linear);
        void Aggregate(struct O_CodingItem &result); 
        void Initialization();
 
        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            num_compute_thread = config->getint("num_compute_threads", -1);
            afs_assert(num_compute_thread > 0, "[num_compute_threads] should be indicated in config file!\n");
            upstream_num = config->getint("num_upstream",0);
            afs_assert(upstream_num, "[num_upstream] is not set!\n");
            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");
            micro_batch  = config->getint("micro_batch", -1);
            afs_assert(micro_batch >= 0, "[micro_batch] should be specified to determine the working mode !\n");
            width = upstream_num - 2;
            length = config->getint("coding_length",0);
            afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
            afs_assert(length<=max_coding_length, "[coding_length] exceeds the maximum length!\n");
            char* feed_type = config->getstring("feedback_type", NULL);
            if (feed_type == NULL) {
                LOG_ERR("[feedback_type] must be specified in the coding file as [ALL] or [RR]!\n");
            }
            char * synchronize_type = config->getstring("syn_type", NULL);
            if (synchronize_type == NULL){
                LOG_MSG("[syn_type] is set as 'fast' default, changed to 'chain' in config file if necessary !\n");
                syn_type = "fast";
            }
            else {
                syn_type = synchronize_type;
            }
            feedback_type = feed_type;
            feedback_count = 0;
            char *upid = config->getstring("upstream_id", NULL);
            if (upid == NULL) {
                LOG_ERR("[upstream_id] must be specified in the coding file!\n");
            }
            char *tmp;
            char* token = strtok_r(upid, ",", &tmp);
            int index = 0;
            while (token) {
                upstream_id[index++] = atoi(token);
                token = strtok_r(NULL, ",\n", &tmp);
            }        
            for (int j = 0; j < max_stale+1; j++ ) {
                for (int i = 0; i < length; i++) {
                    Pdrive[j][i] = 0;
                    coded[j][i] = 0;                
                }
                count[j] = 0;
                ready[j] = false;
                seq_sum[j] = 0;
            }
            watermark = 0;
            progress = 0;
            syn_id = -1;       
            thread_id = get_tid();
            //adaptive coding
            enable_adaptive = config->getint("enable_adaptive", -1);
            afs_assert(enable_adaptive >= 0, "[enable_adaptive] should be indicated\n");
            do_encode = 0;
            adaptive_count = 0;
            history_count = 0;
            erasure_node = -1;
            old = -1;
            for (int i = 0; i < slide_window_size; i++) {
                history[i] = 1.0;
            }
            for (int i = 0; i < max_upstream_size; i++ ) {
                current_record[i] = 0;
            }
            if (enable_adaptive) {
                straggler_threshold  = config->getdouble("straggler_threshold", -1);
                afs_assert(straggler_threshold > 0, "[straggler_threshold] should be indicated in adaptive mode!\n");
            }
            first = false;
            last = false;

            // application-specific
            Initialization();
        }

        void ComputeThreadFinish() {

        }

        // interfaces
        //void Recompute(struct O_CodingItem &linear) {
        //    // add recompute part here
        //    for (int t = 0; t < 10; t++) {
        //        double ret = 0;
        //        for (int i = 0; i < length; i ++){
        //            ret += model[i] * linear.GetRaw(i);
        //        }
        //    }
        //}
        //
        //void Aggregate(struct O_CodingItem &result) {
        //    if (aggregate_count == 0) {
        //        gettimeofday(&start_time, NULL);
        //        gettimeofday(&m_start, NULL);
        //    }
        //    // add aggregate code here
        //    aggregate_count ++;
        //    
        //    if (aggregate_count % (micro_batch * width * 10) == 0) {
        //        gettimeofday(&end_time, NULL);
        //        double used_time;
        //        used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
        //        LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, aggregate_count/used_time);
        //    }

        //    if (aggregate_count % (micro_batch * width) == 0) {
        //        gettimeofday(&m_end, NULL);
        //        double used_time;
        //        used_time = (m_end.tv_sec + m_end.tv_usec/1000000.0) - (m_start.tv_sec + m_start.tv_usec/1000000.0);
        //        m_start  = m_end;
        //        LOG_MSG("Latency of %d micro-batch is: %lf ms\n", aggregate_count / micro_batch / width, used_time * 1000);
        //    }
        //} 


        void ComputeThreadRecovery() {}
        void TimerCallback(){
            // handle timeout
            if (!last) { // still no finish after timeout, send negative ack
                CodingItem * nak = new CodingItem;
                nak->id = -2;
                nak->sequence = -2;
                nak->progress = watermark;
                EmitReverseDataQuick(0,  *nak);
                delete nak;
            }
        }

        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct O_CodingItem &tuple) {

            // micro_batch synchronization 
            if (tuple.id == -1)	{
                if (syn_id  == -1) {
                    syn_id  = worker;
                    CodingItem * ack = new CodingItem;
                    ack->id = -1;
                    ack->sequence = -1;
                    EmitReverseDataQuick(syn_id, thread_id, *ack);
                    LOG_MSG("Thread %ld finish the first time synchronization from thread %d!\n", get_tid(), thread);
                    //struct timeval now;
                    //gettimeofday(&now, NULL);
                    for (int i = 0; i < max_upstream_size; i++){
                        current_record[i] = 1;
                    } 
                    first = true;
                    last = true;
                    delete ack;
                    return;
                }
                return;			
            }

            // uncoded mode
            if (enable_adaptive && !do_encode) {
                tuple.isAdaptive = true;
                Aggregate(tuple); 
                progress += tuple.progress;
                count[(tuple.id / num_compute_thread) % max_stale] ++;

                // failure estimation
                if (first) {  // receive first item in micro-batch
                    gettimeofday(&m_start, NULL);
                    first = false;
                }
                if (tuple.sequence >= 0 && last) {
                    current_record[tuple.sequence] = current_record[tuple.sequence + 1]; 
                }
                if (micro_batch && ((tuple.id / num_compute_thread % micro_batch) == 0) && last) { // first item in last stripe
                    gettimeofday(&m_end, NULL);
                    last = false; 
                    // start a timer
                    double interval = (m_end.tv_sec + m_end.tv_usec/1000000.0) - (m_start.tv_sec + m_start.tv_usec/1000000.0);
                    unsigned int tosleep = interval * (straggler_threshold - 1) * 1000000;
                    afs_zmq::command_t cmd; 
                    cmd.type = afs_zmq::command_t::backup;
                    cmd.len = tosleep;
                    ThreadBase::NotifyWorker(cmd);
                }
                // end failure estimation

                if (count[(tuple.id / num_compute_thread) % max_stale] == (upstream_num -1)) {
                    watermark += tuple.progress;
                    count[(tuple.id / num_compute_thread) % max_stale] = 0;

                    if (micro_batch && (watermark % micro_batch == 0)) { // finish micro-batch
                        std::sort(current_record, (current_record + upstream_num -2));
                        double S = current_record[upstream_num - 3]/current_record[upstream_num -2];
                        CodingItem *ack = new CodingItem;
                        if (S > straggler_threshold) {
                            ack->isAdaptive = true;
                            do_encode = 1;
                        }
                        else {
                            ack->isAdaptive = false;
                            do_encode = 0;
                        }

                        ack->isCoded = false;
                        ack->id = -1;
                        ack->sequence = -1;
                        if (syn_type == "fast") {
                            EmitReverseDataQuick(syn_id, thread_id, *ack);
                        }
                        delete ack;
                        // clear for next micro-batch
                        first = true;
                        last = true;
                        for (int i = 0; i < max_upstream_size; i++){
                            current_record[i] = 1;
                        }
                    }
                }

                return; 
            }

            if ((tuple.id / num_compute_thread < watermark) && (tuple.id  > -1)) {
                return;
            }
            if (tuple.id / num_compute_thread > watermark + max_stale) {
                LOG_ERR("ERROR, the number of stragglers or failures exceeds the RAID5\n");
                return;
            }

            // coded mode
            if (!tuple.isCoded) {
                // receiving the normal item
                Aggregate(tuple);
                progress ++;

                //SIMD optimization
                for (int i = 0; i < length / 4; i ++) {
                    __m256d r = _mm256_set_pd(tuple.GetRaw(4*i+3), tuple.GetRaw(4*i+2), tuple.GetRaw(4*i+1), tuple.GetRaw(4*i));
                    __m256d p = _mm256_load_pd(Pdrive[(tuple.id / num_compute_thread % max_stale)] + 4 *i);
                    __m256d ret = _mm256_add_pd(r, p);
                    _mm256_store_pd(Pdrive[(tuple.id / num_compute_thread % max_stale)] + 4 *i, ret);
                } 
                for (int i = length - length % 4; i < length; i++){
                    Pdrive[(tuple.id / num_compute_thread % max_stale)][i] += tuple.GetRaw(i);
                }

                count[(tuple.id / num_compute_thread)  % max_stale] ++;
                seq_sum[(tuple.id /num_compute_thread) % max_stale] += tuple.sequence;
                if ( enable_adaptive) {
                    current_record[tuple.sequence] = current_record[tuple.sequence] + 1; 
                }
            }
            else {
                // receiving the coded item
                int index = 0;
                while (index < length){
                    coded[(tuple.id / num_compute_thread) % max_stale][index] = tuple.GetRaw(index);
                    index ++;
                } 
                ready[(tuple.id / num_compute_thread) % max_stale] = true;
                count[(tuple.id / num_compute_thread) % max_stale] ++;
                // for straggler estimation
                if (enable_adaptive) {
                    current_record[upstream_num -2] = current_record[upstream_num - 2] + 1; 
                }
            }

            // receive k items, decode if necessary
            if (count[(tuple.id / num_compute_thread) % max_stale] == (upstream_num - 2)){
                O_CodingItem *current = new O_CodingItem;
                int straggler = -1;
                if (ready[(tuple.id / num_compute_thread) % max_stale]){
                    //need to decode now
                    for (int i = 0; i < length; i ++){
                        current->AppendItem(coded[(tuple.id / num_compute_thread) % max_stale][i] - Pdrive[(tuple.id / num_compute_thread) % max_stale][i]);
                    }
                    current->isCoded = true;
                    current->id = tuple.id;
                    straggler = ((upstream_num -2) * (upstream_num - 3) /2 - seq_sum[ (tuple.id / num_compute_thread) % max_stale]);
                    current->sequence = straggler;
                    Recompute(*current);
                    Aggregate(*current);
                    progress ++;
                }
                else {
                }

                delete current;

                // clear everything for next round
                watermark ++;
                for (int i = 0; i < length; i ++){
                    Pdrive[(tuple.id / num_compute_thread) % max_stale][i] = 0L;
                    coded[(tuple.id / num_compute_thread) % max_stale][i] = 0L;
                }
                ready[(tuple.id / num_compute_thread) % max_stale] = false;
                count[(tuple.id / num_compute_thread) % max_stale] = 0;
                seq_sum[(tuple.id / num_compute_thread) % max_stale] = 0;

                if (micro_batch && ((watermark % micro_batch) == 0) ) {
                    CodingItem *ack = new CodingItem;
                    if (enable_adaptive) {
                        std::sort(current_record, (current_record + upstream_num -2));
                        history[history_count] = current_record[1] * 1.0 / current_record[0];
                        for (int i = 0; i < upstream_num -1; i++){
                            current_record[i] = 0;
                        }
                        if (history_count == slide_window_size -1) {
                            history_count = 0;
                        }
                        else{ 
                            history_count ++;
                        }
                        double sum = 0;
                        for (int i = 0; i < slide_window_size; i++) {
                            sum += history[i];
                        }
                        //LOG_MSG("Coding mode %lld, straggler factor is %lf\n", watermark, sum/slide_window_size);
                        //end straggler finding
                        if (sum / slide_window_size > straggler_threshold) {
                            ack->isAdaptive = true;
                            do_encode = 1;
                        }
                        else {
                            ack->isAdaptive = false;
                            do_encode = 0;
                        }
                    }
                    ack->isCoded = false;
                    ack->id = -1;
                    ack->sequence = -1;
                    if (syn_type == "fast") {
                        EmitReverseDataQuick(syn_id, 0, *ack);
                    }
                    // send clear signal to stragglers
                    if (thread_id == 0 && straggler >= 0) {
                        EmitReverseDataQuick(upstream_id[straggler +1], 0, *ack);
                    }
                    delete ack;
                }
            }
        }

        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {
        }

        void ProcessPunc(){}

};

class RAID6DecodeThread : public afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem> {
    public:
        RAID6DecodeThread(int num_upstreams, int num_downstreams) :
            afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>(num_upstreams, num_downstreams) {}

    private:

        alignas(32) double Pdrive[max_stale + 1][max_coding_length];
        alignas(32) double Qdrive[max_stale + 1][max_coding_length]; 
        int width = 1;
        int length;
        int downstream_num;
        int upstream_num;
        struct timeval start_time, current_time, end_time;
        struct timeval m_start, m_end;
        double Pcoded[max_stale + 1][max_coding_length];
        double Qcoded[max_stale + 1][max_coding_length];
        
        int count[max_stale + 1];
        int seq_sum[max_stale + 1];
        int seq_mul[max_stale + 1];
        int ready[max_stale + 1];
        long long progress = 0;
        long long watermark = 0;
        int micro_batch = 0;
        int syn_id = -1;
        std::string feedback_type;
        std::string syn_type;
        int feedback_count = 0;
        int upstream_id[100]; 
        int parallel = 1;
        int thread_id = 0;

        int tot_sum = 0;
        int tot_mul = 0;

        //for adaptive coding
        int enable_adaptive = 0;
        int do_encode = 0;
        int adaptive_count = 0;
        double current_record[max_worker_num];
        double history[window_size];
        int history_count;
        double straggler_threshold;
        double normal_t, coded_t, decode_t, sent_t;
        int aggregate_count = 0;
        bool first, last;

        // application-specific
        double model[max_attrnum] = {0};

        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            parallel = config->getint("num_compute_threads", -1);
            afs_assert(parallel > 0, "[num_compute_threads] should be indicated!\n");
            upstream_num = config->getint("num_upstream",0);
            afs_assert(upstream_num >=5, "[num_upstream] should be at least 5 for RAID6 decode!\n");
            afs_assert(upstream_num, "[num_upstream] is not set!\n");
            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");
            micro_batch  = config->getint("micro_batch", -1);
            afs_assert(micro_batch >= 0, "[micro_batch] should be specified to determine the working mode !\n");
            length = config->getint("coding_length",0);
            afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
            afs_assert(length<=max_coding_length, "[coding_length] exceeds the maximum length!\n");
            char* feed_type = config->getstring("feedback_type", NULL);
            if (feed_type == NULL) {
                LOG_ERR("[feedback_type] must be specified in the coding file!\n");
            }
            char* synchronize_type = config->getstring("syn_type", NULL);
            if (synchronize_type == NULL) {
                LOG_MSG("[syn_type] is set as 'fast' default, changed to 'chain' in config file!\n");
                syn_type = "fast";
            }
            else {
                syn_type = synchronize_type;
            }
            feedback_type = feed_type;
            feedback_count = 0;
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
            for (int j = 0; j < max_stale+1; j++ ) {
                for (int i = 0; i < length; i++)
                {
                    Pdrive[j][i] = 0L;
                    Pcoded[j][i] = 0L;
                    Qdrive[j][i] = 0L;
                    Qcoded[j][i] = 0L; 
                }
                count[j] = 0;
                ready[j] = 0;
                seq_sum[j] = 0;
                seq_mul[j] = 0;
            }
            for ( int i = 1; i <= upstream_num - 3; i ++) {
                tot_sum += i;
                tot_mul += i * i;
            }
            watermark = 0;
            progress = 0;
            syn_id = -1;
            thread_id = get_tid();
            //for adaptive coding
            enable_adaptive = config->getint("enable_adaptive", -1);
            afs_assert(enable_adaptive >= 0, "[enable_adaptive] should be indicated!\n");
            do_encode = 0;
            adaptive_count = 0;
            history_count = 0;
            for (int i = 0; i < window_size; i++) {
                history[i] = 1;
            }
            for (int i = 0; i < max_worker_num; i++) {
                current_record[i] = 0;
            }
            if (enable_adaptive) {
                straggler_threshold = config->getdouble("straggler_threshold", -1);
                afs_assert(straggler_threshold > 0, "[straggler_threshold] should be indicated in adaptive mode!\n");
            }
            //for performance measurement, record the start time here
            normal_t = 0;
            coded_t = 0;
            decode_t = 0;
            sent_t = 0;
            first = true;
            last = true;
            width = upstream_num - 3;

            // application-specific
            Initialization();
        }

        void ComputeThreadFinish() {

        }

        void Recompute(struct O_CodingItem &linear);
        void Aggregate(struct O_CodingItem &result);
        void Initialization();

        //void Recompute(struct O_CodingItem &linear) {
        //    for (int t = 0; t < 10; t ++) {
        //        double ret = 0;
        //        for (int i = 0; i < length; i ++){
        //            ret += model[i] * linear.GetRaw(i);
        //        }
        //    }
        //}
        //
        //void Aggregate(struct O_CodingItem &result) {

        //    if (aggregate_count == 0) {
        //        gettimeofday(&start_time, NULL);
        //        gettimeofday(&m_start, NULL);
        //    }
        //        
        //    // add aggregate code here
        //    aggregate_count ++;
        //    
        //    if (aggregate_count % (micro_batch * width * 10) == 0) {
        //        gettimeofday(&end_time, NULL);
        //        double used_time;
        //        used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
        //        LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, aggregate_count/used_time);
        //    }

        //    if (aggregate_count % (micro_batch * width) == 0) {
        //        gettimeofday(&m_end, NULL);
        //        double used_time;
        //        used_time = (m_end.tv_sec + m_end.tv_usec/1000000.0) - (m_start.tv_sec + m_start.tv_usec/1000000.0);
        //        m_start  = m_end;
        //        LOG_MSG("Latency of %d micro-batch is: %lf ms\n", aggregate_count / micro_batch / width, used_time * 1000);
        //    }
        //} 

        void ComputeThreadRecovery() {}
        void TimerCallback(){
            // handle timeout
            if (!last) { // still no finish after timeout, send negative ack
                CodingItem * nak = new CodingItem;
                nak->id = -2;
                nak->sequence = -2;
                nak->progress = watermark;
                EmitReverseDataQuick(0,  *nak);
                delete nak;
            }
        }

        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct O_CodingItem &tuple) {

            if (tuple.id == -1) {
                if(syn_id  == -1) {
                    syn_id  = worker;
                    CodingItem * ack = new CodingItem;
                    ack->id = -1;
                    ack->sequence = -1;
                    EmitReverseDataQuick(syn_id, thread_id,  *ack);
                    LOG_MSG("Finish the first time synchronization!\n");
                    for (int i = 0; i < max_upstream_size; i++){
                        current_record[i] = 1;
                    } 
                    first = true;
                    last = true;
                    delete ack;
                    return;
                }
                return;			
            }

            // uncoded mode
            if (enable_adaptive && !do_encode) {
                tuple.isAdaptive = true;
                Aggregate(tuple);

                progress += tuple.progress;
                count[(tuple.id / parallel) % max_stale] ++;           
                // failure estimation
                if (first) {  // receive first item in micro-batch
                    gettimeofday(&m_start, NULL);
                    first = false;
                }
                if (tuple.sequence >= 0 && last) {
                    current_record[tuple.sequence] = current_record[tuple.sequence + 1]; 
                }
                if (micro_batch && ((tuple.id / parallel % micro_batch) == 0) && last) { // first item in last stripe
                    gettimeofday(&m_end, NULL);
                    last = false; 
                    // start a timer
                    double interval = (m_end.tv_sec + m_end.tv_usec/1000000.0) - (m_start.tv_sec + m_start.tv_usec/1000000.0);
                    unsigned int tosleep = interval * (straggler_threshold - 1) * 1000000;
                    afs_zmq::command_t cmd; 
                    cmd.type = afs_zmq::command_t::backup;
                    cmd.len = tosleep;
                    ThreadBase::NotifyWorker(cmd);
                }
                // end failure estimation

                if (count[(tuple.id / parallel) % max_stale] == (upstream_num -1)) {
                    watermark += tuple.progress;
                    count[(tuple.id / parallel) % max_stale] = 0;

                    if (micro_batch && (watermark % micro_batch == 0)) {
                        CodingItem *ack = new CodingItem;
                        std::sort(current_record, (current_record + upstream_num -2));
                        double S = current_record[upstream_num - 4]/current_record[upstream_num - 2];
                        if (S > straggler_threshold) {
                            ack->isAdaptive = true;
                            do_encode = 1;
                        }
                        else {
                            ack->isAdaptive = false;
                            do_encode = 0;
                        }
                        ack->isCoded = false;
                        ack->id = -1;
                        ack->sequence = -1;
                        if (syn_type == "fast")
                            EmitReverseDataQuick(syn_id, thread_id, *ack);
                        delete ack;
                        // clear for next micro-batch
                        first = true;
                        last = true;
                        for (int i = 0; i < max_upstream_size; i++){
                            current_record[i] = 1;
                        }
                    }
                }
                return;
            }

            if (((tuple.id / parallel) < watermark) && (tuple.id > -1)) {
                //LOG_MSG("Drop outdated items");
                return;
            }
            if ((tuple.id / parallel) > watermark + max_stale) {
                LOG_ERR("ERROR, the number of stragglers exceeds RAID6!\n");
                return;
            }

            // run coded mode
            if (!tuple.isCoded) {
                Aggregate(tuple);
                progress ++;

                // SIMD optimization
                for (int i = 0; i < length / 4; i++) {
                    __m256d r = _mm256_set_pd(tuple.GetRaw(4*i+3), tuple.GetRaw(4*i+2), tuple.GetRaw(4*i+1),  tuple.GetRaw(4*i));
                    __m256d p = _mm256_load_pd(Pdrive[(tuple.id / parallel % max_stale)] + 4*i);
                    __m256d q = _mm256_load_pd(Qdrive[(tuple.id / parallel % max_stale)] + 4*i);
                    __m256d a = _mm256_set1_pd(tuple.sequence + 1.0);
                    __m256d pd = _mm256_add_pd(p, r);
                    __m256d qd = _mm256_fmadd_pd(r, a, q);
                    _mm256_store_pd(Pdrive[(tuple.id / parallel % max_stale)] + 4*i, pd);
                    _mm256_store_pd(Qdrive[(tuple.id / parallel % max_stale)] + 4*i, qd);
                }
                for (int i = length - length % 4; i < length; i++){
                    Pdrive[(tuple.id / parallel) % max_stale][i] += tuple.GetRaw(i);
                    Qdrive[(tuple.id / parallel) % max_stale][i] += tuple.GetRaw(i) * (tuple.sequence + 1.0);
                } 

                seq_sum[(tuple.id / parallel) % max_stale] += (tuple.sequence + 1);
                seq_mul[(tuple.id / parallel) % max_stale] += (tuple.sequence + 1 ) * (tuple.sequence + 1);
                count[(tuple.id / parallel) % max_stale] ++;
                if ( enable_adaptive ) {
                    current_record[tuple.sequence] = current_record[tuple.sequence] + 1;
                }
            } 
            else  { //receive a coded item
                int index = 0;
                if(tuple.sequence == -1) {
                    while(index < length) {
                        Pcoded[(tuple.id / parallel)  % max_stale][index] = tuple.GetRaw(index);
                        index++;
                    }
                    ready[(tuple.id / parallel) % max_stale] -= 1;
                    count[(tuple.id / parallel) % max_stale] ++;

                    if (enable_adaptive) {
                        current_record[upstream_num - 3] = current_record[upstream_num - 3] + 1;
                    }

                } else if(tuple.sequence == -2) {
                    while(index < length) {
                        Qcoded[(tuple.id / parallel) % max_stale][index] = tuple.GetRaw(index);
                        index ++;
                    }
                    ready[(tuple.id / parallel)  % max_stale] -= 2;
                    count[(tuple.id / parallel) % max_stale] ++;

                    if (enable_adaptive) {
                        current_record[upstream_num - 2] = current_record[upstream_num - 2] + 1;
                    }
                } else {
                    LOG_ERR("Unkonwn sequence number in coded item!\n");
                }
            }

            // receie k items if necessary
            if(count[(tuple.id / parallel) % max_stale] == upstream_num - 3) {   
                O_CodingItem *current = new O_CodingItem;
                O_CodingItem *next =  new O_CodingItem;
                int straggler1 = -1;
                int straggler2 = -1;

                if(ready[(tuple.id / parallel)  % max_stale] == 0){
                    //decode case 1
                } 
                else if (ready[(tuple.id / parallel) % max_stale] == -1) {
                    //decode case 2, use first parity
                    for (int i = 0; i < length; i++) {
                        current->AppendItem(Pcoded[(tuple.id / parallel) % max_stale][i] - Pdrive[(tuple.id / parallel) % max_stale][i]);
                    }
                    current->isCoded = true;
                    current->id = tuple.id;
                    current->sequence = tot_sum - seq_sum[(tuple.id / parallel) % max_stale] - 1;
                    straggler1 = tot_sum - seq_sum[(tuple.id / parallel)  % max_stale];
                    Recompute(*current);
                    Aggregate(*current);
                    progress ++;
                } 
                else if(ready[(tuple.id / parallel) % max_stale] == -2) {
                    //decode case 3, use second parity
                    for(int i = 0; i < length; i++) {
                        current->AppendItem((Qcoded[(tuple.id / parallel) % max_stale][i] - Qdrive[(tuple.id / parallel) % max_stale][i])/(tot_sum - seq_sum[(tuple.id / parallel) % max_stale])); 
                    }
                    current->isCoded = true;
                    current->id = tuple.id;
                    current->sequence = tot_sum - seq_sum[(tuple.id / parallel) % max_stale] - 1;
                    straggler1 = tot_sum - seq_sum[(tuple.id / parallel)  % max_stale];
                    Recompute(*current);
                    Aggregate(*current);
                    progress ++;
                } 
                else if(ready[(tuple.id / parallel) % max_stale] == -3) {
                    //decode case 4, use two parity
                    /*  find the indices of the two missing rows
                        a + b = x
                        a^2 + b^2 = y
                        a < b
                        ------------------>
                        a = (x-sqrt(y*2-x*x))/2
                        b = (x+sqrt(y*2-x*x))/2
                     */
                    int x = tot_sum - seq_sum[(tuple.id / parallel) % max_stale];
                    int y = tot_mul - seq_mul[(tuple.id / parallel) % max_stale];
                    int num1 = (x - sqrt(y * 2 - x * x)) / 2;
                    int num2 = (x + sqrt(y * 2 - x * x)) / 2;
                    straggler1 = num1;
                    straggler2 = num2;
                    //recover the missing rows
                    /*
                       a + b =  x
                       num1 * a + num2 * b = y
                       a < b
                       ------------------->
                       b = (y - num1 * x)/(num2 -num1)
                       a = x - (y - num1 * x)/(num2 -num1)
                     */
                    double tmp1, tmp2;
                    for(int i  = 0; i < length; i++) {
                        tmp1 = Pcoded[(tuple.id / parallel) % max_stale][i] - Pdrive[(tuple.id / parallel) % max_stale][i];
                        tmp2 = Qcoded[(tuple.id / parallel) % max_stale][i] - Qdrive[(tuple.id / parallel) % max_stale][i];
                        current->AppendItem(tmp1 - (tmp2 - num1 * tmp1) / (num2 - num1) );
                        next->AppendItem((tmp2 - num1 * tmp1) / (num2 - num1));
                    }
                    current->isCoded = true;
                    next->isCoded = true;
                    current->id = tuple.id;
                    next->id = tuple.id;
                    current->sequence = num1 - 1;
                    next->sequence = num2 - 1;
                    Recompute(*current);
                    Aggregate(*current);
                    progress ++;
                    Recompute(*next);
                    Aggregate(*next);
                    progress ++;
                } 
                else {
                    LOG_ERR("Error during decoding, Unknown sequences!\n");
                }

                delete current;
                delete next;
                //clear everything for next round
                watermark ++;
                count[(tuple.id / parallel) % max_stale] = 0;
                seq_sum[(tuple.id / parallel) % max_stale] = 0;
                seq_mul[(tuple.id / parallel) % max_stale] = 0;
                ready[(tuple.id / parallel) % max_stale] = 0;
                for (int i = 0; i < length; i ++) {
                    Pdrive[(tuple.id / parallel) % max_stale][i] = 0L;
                    Pcoded[(tuple.id / parallel) % max_stale][i] = 0L;
                    Qdrive[(tuple.id / parallel) % max_stale][i] = 0L;
                    Qcoded[(tuple.id / parallel) % max_stale][i] = 0L;   
                }

                //feedback minibatch
                if(micro_batch && (watermark % micro_batch == 0)) {
                    //gettimeofday(&db_start, NULL);
                    CodingItem *ack = new CodingItem;
                    if (enable_adaptive) {
                        std::sort(current_record, (current_record + upstream_num -1));
                        history[history_count] =  current_record[1] *1.0 /current_record[0];
                        for(int i = 0; i < upstream_num -1; i++) {
                            current_record[i] = 1;
                        }
                        if (history_count == window_size -1) {
                            history_count = 0;
                        }
                        else {
                            history_count ++;
                        }
                        double sum = 0;
                        for (int i = 0; i < window_size; i++) {
                            sum += history[i];
                            LOG_MSG("%lf ", history[i]);
                        }
                        // LOG_MSG("Coding mode %lld, The final straggler factor is %lf\n", watermark, sum/window_size);
                        if (sum / window_size > straggler_threshold) {
                            ack->isAdaptive = true;
                            do_encode = 1;
                        }
                        else {
                            ack->isAdaptive = false;
                            do_encode = 0;
                        }
                    }
                    ack->isCoded = false;
                    ack->id = -1;
                    ack->sequence = -1;
                    if (syn_type == "fast") {
                        EmitReverseDataQuick(syn_id, thread_id, *ack);
                        if (straggler1 != -1) {
                            //        EmitReverseDataQuick(upstream_id[straggler1], 0, *ack);
                        }
                        if (straggler2 != -1) {
                            //        EmitReverseDataQuick(upstream_id[straggler2], 0, *ack);
                        }
                        delete ack;
                    }

                }
            }
        }

        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {

        }

        void ProcessPunc(){}

};


class RSDecodeThread : public afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem> {
public:
    RSDecodeThread(int num_upstreams, int num_downstreams) :
        afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>(num_upstreams, num_downstreams) {}

private:

    int width = 1;
    int length;
    int downstream_num;
    int upstream_num;
    struct timeval start_time, current_time, end_time;
    double drive[max_stale + 1][max_node_size + 15][max_coding_length];
    int flag[max_stale + 1][max_node_size + 15];
    int coding_count[max_stale + 1];
    int coded_count[max_stale + 1];
    int count[max_stale +1]; 
    long long progress = 0;
    long long watermark = 0;
	int micro_batch = 0;
    int syn_id = -1;
    std::string feedback_type;
    int feedback_count = 0;
    int upstream_id[100]; 
    int num_tolerance = 3;
    int node_size = 0;
    int parallel = 1;
    int thread_id = 0;    

    //for adaptive coding
    int enable_adaptive = 0;
    int do_encode = 0;
    int adaptive_count = 0;
    double current_record[max_worker_num_rs];
    double history[window_size_rs];
    int history_count;
    double straggler_threshold;

    void ComputeThreadInit() {

        Config* config = Config::getInstance();
        parallel = config->getint("num_compute_threads", -1);
        afs_assert(parallel > 0, "[num_compute_threads] must be indicated!\n");
	    upstream_num = config->getint("num_upstream",0);
        afs_assert(upstream_num >=5, "[num_upstream] should be at least 5 for RAID6 decode!\n");
        afs_assert(upstream_num, "[num_upstream] is not set!\n");
        downstream_num  = config->getint("num_downstream", 0);
        afs_assert(downstream_num, "[num_downstream] is not set!\n");
		micro_batch  = config->getint("micro_batch", -1);
		afs_assert(micro_batch >= 0, "[micro_batch] should be specified to determine the working mode !\n");
        length = config->getint("coding_length",0);
        afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
        afs_assert(length<=max_coding_length, "[coding_length] exceeds the maximum length!\n");
        char* feed_type = config->getstring("feedback_type", NULL);

        if (feed_type == NULL) {
            LOG_ERR("[feedback_type] must be specified in the coding file!\n");
        }
        feedback_type = feed_type;
        feedback_count = 0;
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
        num_tolerance = config->getint("num_m", -1);
        afs_assert(num_tolerance > 0, "[num_m] should be indicated in config file!\n");
        afs_assert(num_tolerance <= max_node_tolerance, "Exceeds maximum number tolerance!\n");
        for (int j = 0; j < max_stale + 1; j++ ) {
            for (int i = 0; i < max_node_size + 15; i++) {
                for (int k = 0; k < max_coding_length; k++) {
                    drive[j][i][k] = 0L;
                }
                flag[j][i] = 0; 
            }
            coding_count[j] = 0;
            coded_count[j] = 0;
            count[j] = 0;
        }
        watermark = 0;
        progress = 0;
        syn_id = -1;
        thread_id = get_tid();
        //initial an identity matrix for reverse
        node_size = upstream_num - num_tolerance - 1;
        init_origin(thread_id, node_size);
        enable_adaptive = config->getint("enable_adaptive", -1);
        afs_assert(enable_adaptive >= 0, "[enable_adaptive] should be indicated!\n");
        do_encode = 0;
        adaptive_count = 0;
        history_count = 0;
        for (int i = 0; i < window_size_rs; i++) {
            history[i] = 1;
        }
        for (int i = 0; i < max_worker_num_rs; i ++) {
            current_record[i] = 0;
        }
        if (enable_adaptive) {
            straggler_threshold = config->getdouble("straggler_threshold", -1);
            afs_assert(straggler_threshold > 0, "[straggler_threshold] should be indicated in adaptive mode!\n");
        }
        gettimeofday(&start_time, NULL);
        Initialization();
    }

    void Recompute(O_CodingItem &linear);
    void Aggregate(O_CodingItem &result);
    void Initialization();

    void ComputeThreadFinish() {
	    gettimeofday(&end_time, NULL);
	    double used_time, throughput;
	    used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
	    throughput = 11*1024*1.0/(used_time);
	    LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, throughput);
    }

    void ComputeThreadRecovery() {}

    void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct O_CodingItem &tuple) {
		if (tuple.id == -1) {
            if(syn_id  == -1) {
                syn_id  = worker;
                CodingItem * ack = new CodingItem;
			    ack->id = -1;
				ack->sequence = -1;
			    EmitReverseDataQuick(syn_id, thread_id, *ack);
                LOG_MSG("Finish the first time synchronization!\n");    
                delete ack;
                return;
            }
            //LOG_MSG("Receive an micro_batch synchronous item, do nothing!\n");    
			return;			
		}
        // uncoded mode
        if (enable_adaptive && !do_encode) {
            // EmitData(progress % downstream_num, tuple);
            progress ++;
            count[(tuple.id / parallel) % max_stale] ++;
            
            //beginning of batch, record times
            if (micro_batch && ((tuple.id / parallel) % micro_batch  == 0)) {
                struct timeval now;
                gettimeofday(&now, NULL);
                current_record[tuple.sequence] = now.tv_sec + now.tv_usec/1000000.0;
                //LOG_MSG("%d-th worker starts at %lf\n", tuple.sequence, current_record[tuple.sequence]);
            }
            //end of batch, record times
            if (micro_batch && ((tuple.id / parallel % micro_batch)  == (micro_batch -1)) && tuple.sequence >= 0) {
                struct timeval now;
                gettimeofday(&now, NULL);
                current_record[tuple.sequence] = (now.tv_sec + now.tv_usec/1000000.0) - current_record[tuple.sequence];
            }
            if (count[(tuple.id / parallel) % max_stale] == (upstream_num - 1)) {
                watermark ++;
                count[(tuple.id / parallel) % max_stale] = 0;

                if (micro_batch && (watermark % micro_batch == 0)) {
                    //find if there are stragglers
                    std::sort(current_record, (current_record + upstream_num -1), greater<double>());
                    history[history_count] = current_record[num_tolerance - 1] / current_record[num_tolerance];
                    for (int i = 0; i < upstream_num - 1; i++) {
                        current_record[i] = 0; 
                    }
                    if (history_count == (window_size_rs - 1 )) {
                        history_count = 0;
                    }
                    else {
                        history_count ++;
                    }
                    double sum = 0;
                    for (int i  = 0; i < window_size_rs; i++) {
                        sum += history[i];
                    }
                    CodingItem *ack = new CodingItem;
                    if ( sum / window_size_rs > straggler_threshold) {
                        ack->isAdaptive = true;
                        do_encode = 1;
                    }
                    else {
                        ack->isAdaptive = false;
                        do_encode = 0;
                    }
                    ack->isCoded = false;
                    ack->id = -1;
                    ack->sequence = -1;
                    EmitReverseDataQuick(syn_id, thread_id, *ack);
                    delete ack;
                }
            }
            return; 
        }

        // run coded mode
        if (((tuple.id / parallel) < watermark) && (tuple.id > -1)) {
            //LOG_MSG("Receive an outdated items!\n");
            return;
        }
        if ((tuple.id / parallel) > watermark + max_stale) {
            //LOG_ERR("ERROR, the number of stragglers exceeds num_m\n");
			return;
        }

        if (!tuple.isCoded) { // receiving a normal item
            //EmitData(progress % downstream_num, 0, tuple);
            progress ++;
            int index = 0;
            while (index < length) {
                drive[(tuple.id / parallel) % max_stale][tuple.sequence][index] = tuple.GetRaw(index);
                index ++;
            }
            flag[(tuple.id / parallel) % max_stale][tuple.sequence] = 1;
            coding_count[(tuple.id / parallel) % max_stale] ++;
            if (enable_adaptive) {
                current_record[tuple.sequence] = current_record[tuple.sequence] + 1;
            }
    	} 
        else  { //receive a coded item
            int index = 0;
            while (index < length) {
                drive[(tuple.id/parallel) % max_stale][max_node_size + coded_count[(tuple.id/parallel) % max_stale]][index] = tuple.GetRaw(index);
                index ++;
            }
            flag[(tuple.id / parallel) % max_stale][max_node_size + coded_count[(tuple.id / parallel) % max_stale]] = -tuple.sequence; 
            coded_count[(tuple.id / parallel) % max_stale] ++;

            if (enable_adaptive) {
                current_record[upstream_num + tuple.sequence ] = current_record[upstream_num + tuple.sequence] + 1;
            }
        }
        
        if ((coding_count[(tuple.id / parallel) % max_stale] + coded_count[(tuple.id / parallel) % max_stale]) == node_size) {   
            // receive k items, decode if necesary
            if(coded_count[(tuple.id / parallel) % max_stale] == 0) {
                // no decode necessary
            } 
            else {
                // require decode
                //makeup the data matrix and find reverse matrix
                int index = 0;
                for (int i = 0; i < node_size; i++) {
                    if (flag[(tuple.id / parallel) % max_stale][i] == 0) {
                        for (int j = 0; j < length; j ++) {
                            drive[(tuple.id / parallel) % max_stale][i][j] = drive[(tuple.id / parallel) % max_stale][max_node_size + index][j];
                        }
                        update_origin(thread_id, i, flag[(tuple.id / parallel) % max_stale][max_node_size + index], node_size );
                        index ++;
                    }
                }
                //find inverse matrix for decode
                inverse(origin[thread_id], revers[thread_id], node_size);
                //recover the missing rows
                for (int i = 0; i < node_size; i++) {
                    if(flag[(tuple.id / parallel) % max_stale][i] == 0) {
                        CodingItem *current = new CodingItem;
                        int ind = 0;
                        while (ind < length) {
                            double tmp = 0L;
                            for (int k = 0; k < node_size; k++) {
                                tmp += revers[thread_id][i][k] * drive[(tuple.id / parallel) % max_stale][k][ind];
                            }
                            current->AppendItem(tmp);
                            ind ++;
                        } 
                        current->isCoded = false;
                        current->id = tuple.id;
                        current->sequence = i;
                        //EmitData(progress % downstream_num, 0, *current);
                        progress ++;
                        delete current;
                        recover_origin(thread_id, i, node_size);
                    }
                }
            }

            // micro_batch feedback and clear for next round
            watermark ++;
            coding_count[(tuple.id / parallel) % max_stale] = 0;
            coded_count[(tuple.id / parallel) % max_stale] = 0;
            if (micro_batch && (watermark % micro_batch ==0)) {
                CodingItem *ack = new CodingItem;
                if (enable_adaptive) {
                    std::sort(current_record, (current_record + upstream_num -1));
                    history[history_count] = current_record[num_tolerance] / current_record[num_tolerance] - 1;
                    for ( int i = 0; i < upstream_num -1; i++) {
                        LOG_MSG("%lf ", current_record[i]);
                        current_record[i] = 0;
                    }
                    LOG_MSG("\n");
                    if (history_count == window_size_rs - 1) {
                        history_count = 0;
                    }
                    else {
                        history_count ++;
                    }
                    double sum = 0;
                    for (int i= 0; i < window_size_rs; i++) {
                        sum += history[i];
                    }
                    //LOG_MSG("\n Finally stragger factor is %lf\n", sum/window_size_rs);
                    if ( sum / window_size_rs > straggler_threshold) {
                        ack->isAdaptive = true;
                        do_encode = 1;
                        LOG_MSG("We need to encode the next batch!\n");
                    }
                    else {
                        ack->isAdaptive = false;
                        do_encode = 0;
                        LOG_MSG("We don't really need to encode the enxt batch!\n");
                    }
                }
                ack->isCoded = false;
                ack->id = -1;
                ack->sequence = -1;
                if(thread_id == 0) {
                    for (int i = 0; i < node_size; i++ ) {
                        if(flag[(tuple.id / parallel) % max_stale][i] == 0) {
                            EmitReverseDataQuick(upstream_id[ i + 1], 0, *ack); 
                       }
                    }
                }
                EmitReverseDataQuick(syn_id, thread_id, *ack);
                delete ack;
            }
            for (int i = 0; i < node_size; i++) {
                for (int j = 0; j < length; j++) {
                   drive[(tuple.id / parallel) % max_stale][i][j] = 0L;
                }
                flag[(tuple.id / parallel) % max_stale][i] = 0;
            }
        }
    }

    void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {
    }
    void ProcessPunc(){}

};
