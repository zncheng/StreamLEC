#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <sys/time.h>
#include <immintrin.h>
#include <vector>
#include <map>
#include <codestream.h>

#define max_micro_batch 1000000
#define max_coding_length 256
#define max_block_size 10
#define max_node_tolerance 12

class RAID5EncodeTraceThread : public afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass> {
    public:
        RAID5EncodeTraceThread(int num_downstreams) :
            afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>(0, num_downstreams) {}

    private:

        CodingItem* upstream[max_micro_batch];
        double upstreams[max_micro_batch][max_coding_length];
        alignas(32) double Pdrive[max_coding_length];
        alignas(32) double intermediate[max_coding_length];
        int width = 1;
        int length;
        int downstream_num;
        struct timeval start_time, current_time, end_time;
        int count = 0;
        int up_count = 0;
        long long coding_id = 0;
        int micro_batch = 0;
        int start = -1;
        int parallelism = 1;
        int thread_id = 0;
        int enable_adaptive = 0;
        int do_encode = 0;
        vector<pair<int, CodingItem*>> nonblock;

        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            parallelism = config->getint("num_compute_threads", -1);
            afs_assert(parallelism > 0, "[parallelisms] should be indicated in the config file!\n");
            enable_adaptive = config->getint("enable_adaptive",-1);
            afs_assert(enable_adaptive >= 0, "[enable_adaptive] is not indicated!\n");
            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");
            length = config->getint("coding_length",0);
            afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
            afs_assert(length<=max_coding_length, "Exceeds the maximum coding length!\n"); 
            micro_batch = config->getint("micro_batch", -1);
            afs_assert(micro_batch >= 0, "[micro_batch] should be indicated in the config file!\n");
            for (int i = 0; i < length; i++) {
                Pdrive[i] = 0L;
            }
            count = 0;
            start = -1;
            thread_id = get_tid();
            coding_id = get_tid();
            do_encode = 0;
            up_count  = 0;
            gettimeofday(&start_time, NULL);
        }

        void ComputeThreadFinish() {
            // report throughput
            gettimeofday(&end_time, NULL);
            double used_time, throughput;
            used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
            throughput = 11*1024*1.0/(used_time);
            LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, throughput);
        }

        void ComputeThreadRecovery() {}

        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct afs::DoubleItem &tuple) {

            // begining of the mcro-batch, do synchronization
            if (micro_batch && (start == -1)) {
                start = 0;
                LOG_MSG("Thread %ld synchronization!\n", get_tid());
                CodingItem * current = new CodingItem;
                current->id = -1;
                current->sequence = -1;
                EmitDataAndWaitFeedback(downstream_num-1, thread_id, *current);            
                delete current;
            }

            // run to uncoded mnode
            if (enable_adaptive && !do_encode) {
                CodingItem* toSend = new CodingItem;
                toSend->AppendItem(&tuple.raw_data[0], sizeof(double)*length);
                toSend->isAdaptive = true;
                toSend->isCoded = false;
                toSend->id = coding_id;
                toSend->sequence = count;
                toSend->progress  = 1;
                //EmitData(count, 0, *toSend);
                if(Nonblock_EmitData(count, 0, *toSend) ==1)
                    nonblock.push_back(make_pair(count, toSend));
                else delete toSend;

                // upstream backup for resend
                if(upstream[up_count % micro_batch] != NULL) {
                    upstream[up_count % micro_batch] = toSend;
                    memcpy(upstreams[up_count % (micro_batch*width)], &tuple.raw_data[0], sizeof(double)*length); 
                    up_count ++ ;
                }
                count ++;
                if (count == (downstream_num-1)) {
                    count = 0;
                    coding_id += parallelism;
                    if(micro_batch && ((coding_id / parallelism)  % micro_batch == 0)) {  
                        CodingItem * current = new CodingItem;
                        current->id = -1;
                        current->sequence = -1;
                        current->isAdaptive = true;
                        EmitDataAndWaitFeedback(downstream_num - 1, thread_id, *current); 
                        delete current;
                        up_count = 0;
                    }
                }
                return;
            }

            // run to coded mode
            CodingItem* toSend = new CodingItem;
            memcpy(intermediate, &tuple.raw_data[0], sizeof(double)*length); 
            toSend->AppendItem(intermediate, sizeof(double)*length);

            // SIMD-optimized incremental encoding
            for (int i = 0; i < length/4; i++){
                __m256d r = _mm256_load_pd(intermediate + 4*i);
                __m256d p = _mm256_load_pd(Pdrive + 4*i);
                __m256d ret = _mm256_add_pd(r, p);
               _mm256_store_pd(Pdrive + 4*i, ret);
            }	
            for (int i = length - length % 4; i < length; i++){
                Pdrive[i] += intermediate[i];
            }

            toSend->isCoded = false;
            toSend->id = coding_id;
            toSend->sequence = count;
            toSend->progress = 1;

            if (Nonblock_EmitData(count, 0, *toSend) == 1) {
                nonblock.push_back(make_pair(count, toSend));
            }
            else {
                delete toSend;
            }

            count ++;

            // finish of one round, i.e., reach k uncoded item
            if (count == (downstream_num-2) ) {
                CodingItem *current  = new CodingItem;
                for (int j = 0; j < length; j++) {
                    current->AppendItem(Pdrive[j]);
                    Pdrive[j] = 0L;
                }
                current->isCoded = true;
                current->id = coding_id;
                current->sequence = -1;

                if (Nonblock_EmitData(count, 0, *current) == 1) {
                    nonblock.push_back(make_pair(count, current));
                }
                else {
                    delete current;
                } 

                // non-block send, ensure at least send k items
                while(nonblock.size() > 1) {
                    for (auto it = nonblock.begin(); it != nonblock.end(); it++) {
                        int seq = (*it).first;
                        int ret = Nonblock_EmitData(seq, 0, *(*it).second);
                        if (ret == 0) {
                            delete (*it).second;
                            nonblock.erase(it--);
                        }
                    }
                }
                if (nonblock.size() == 1) {
                    delete nonblock.front().second;
                    nonblock.erase(nonblock.begin());
                }

                count = 0;

                coding_id += parallelism;   
                if(micro_batch && ((coding_id / parallelism)  % micro_batch == 0)) {  
                    CodingItem * current = new CodingItem;
                    current->id = -1;
                    current->sequence = -1;
                    EmitDataAndWaitFeedback(downstream_num - 1, thread_id, *current); 
                    delete current;
                    //Nonblock_Clear();
                }
            }
        }

        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {
            if((tuple.id == -1) && micro_batch) {
                // end of micro-batch, check run coded or uncoded
                if (tuple.isAdaptive) {
                    do_encode = 1;
                }
                else{
                    do_encode = 0;
                }
                LOG_MSG("Release the %lld mini batch block!\n", (coding_id+1)/micro_batch);
                return;    
            }
            // required resend from decoder
            if (tuple.id == -2 && tuple.sequence == -2) {  //re-send in coded mode
                int bid = tuple.progress * width;
                int count= 0; 
                //LOG_MSG("Goning to resned from %d to %d\n", bid, micro_batch * width);
                for(int i = bid * (downstream_num -1); i < micro_batch * (downstream_num -1); i++) {
                    memcpy(intermediate, upstreams[i], sizeof(double)*length);
                    for (int j = 0; j < length/4; j++){
                        __m256d r = _mm256_load_pd(intermediate + 4*j);
                        __m256d p = _mm256_load_pd(Pdrive + 4*j);
                        __m256d ret = _mm256_add_pd(r, p);
                       _mm256_store_pd(Pdrive + 4*j, ret);
                    }	
                    for (int j = length - length % 4; j < length; j++){
                        Pdrive[j] += intermediate[j];
                    }
                    count ++; 
                    if (count == downstream_num -2) {
                        for(int j = 0; j < count; j++) {
                            CodingItem* toSend = new CodingItem;
                            toSend->AppendItem(upstreams[i-count+1+j], sizeof(double)*length);
                            memcpy(intermediate, upstreams[i], sizeof(double)*length);
                            toSend->isCoded = false;
                            toSend->id = coding_id;
                            toSend->sequence = j;
                            toSend->progress = 1;
                            if (j !=0)
                            EmitData(j, 0, *toSend);
                            delete toSend;
                        }
                        CodingItem *current  = new CodingItem;
                        for (int j = 0; j < length; j++) {
                            current->AppendItem(Pdrive[j]);
                            Pdrive[j] = 0L;
                        }
                        current->isCoded = true;
                        current->id = coding_id;
                        current->sequence = -1;
                        EmitData(count, 0, *current);
                        delete current;
                        count = 0;
                    } 
                }
                //LOG_MSG("resned from %d to %d finish\n", bid, micro_batch * width);
                CodingItem *current  = new CodingItem;
                current->isCoded = true;
                current->id = -1;
                current->sequence = -1;
                EmitDataAndWaitFeedback(count, 0, *current);
                delete current;
                 
            }
 
        }

        void ProcessPunc(){}
};


class RAID6EncodeTraceThread : public afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass> {
    public:
        RAID6EncodeTraceThread(int num_downstreams) :
            afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>(0, num_downstreams) {}

    private:
        CodingItem* upstream[max_micro_batch];
        double upstreams[max_micro_batch][max_coding_length];
        alignas(32) double Pdrive[max_coding_length] = {0.0};
        alignas(32) double Qdrive[max_coding_length] = {0.0};
        alignas(32) double intermediate[max_coding_length] = {0.0};
        int width = 1;
        int length;
        int downstream_num;
        struct timeval start_time, current_time, end_time;
        int count = 0;
        int up_count = 0;
        int micro_batch = 0;
        long long coding_id = 0;
        int start = -1;
        int parallelism = 1;
        int thread_id = 0;
        int enable_adaptive = 0;
        int do_encode = 0;    
        vector<pair<int, CodingItem*>> nonblock;

        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");
            afs_assert(downstream_num >= 5, "[num_downstream] should be at lease 5 in RAID6!\n");
            length = config->getint("coding_length",0);
            afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
            afs_assert(length<=max_coding_length, "Exceeds the maximum coding length!\n"); 
            micro_batch = config->getint("micro_batch", -1);
            afs_assert(micro_batch >= 0, "[micro_batch] should be indicated in the config file!\n");
            parallelism = config->getint("num_compute_threads", -1);        
            afs_assert(parallelism > 0, "[num_compute_threads] should be specified in config file!\n");
            for (int i = 0; i < length; i++) {
                Pdrive[i] = 0L;
                Qdrive[i] = 0L;
                intermediate[i] = 0L;
            }
            count = 0;
            thread_id = get_tid();
            coding_id = get_tid();
            start = -1;
            enable_adaptive = config->getint("enable_adaptive", -1);
            afs_assert(enable_adaptive >= 0, "[enable_adaptive] should be indicated!\n");
            do_encode = 0;
            gettimeofday(&start_time, NULL);
        }

        void ComputeThreadFinish() {
            // report throughput
            gettimeofday(&end_time, NULL);
            double used_time, throughput;
            used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
            throughput = 11*1024*1.0/(used_time);
            LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, throughput);
        }

        void ComputeThreadRecovery() {}

        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct afs::DoubleItem &tuple) {
            // synchronization
            if (micro_batch && (start == -1)) {
                start = 0;
                LOG_MSG("synchronization!\n");
                CodingItem * current = new CodingItem;
                current->id = -1;
                current->sequence = -1;
                EmitDataAndWaitFeedback(downstream_num-1, thread_id, *current);        
                delete current;
            }       
            // uncoded mode
            if (enable_adaptive && !do_encode) {
                CodingItem* toSend = new CodingItem;
                toSend->AppendItem(&tuple.raw_data[0], sizeof(double)*length);
                toSend->isAdaptive = true;
                toSend->isCoded = false;
                toSend->id = coding_id;
                toSend->sequence = count;
                toSend->progress = 1;
                EmitData(count, 0, *toSend);

                if(Nonblock_EmitData(count, 0, *toSend) ==1)
                    nonblock.push_back(make_pair(count, toSend));
                else delete toSend;

                // upstream backup for resned
                memcpy(upstreams[up_count % (micro_batch*width)], &tuple.raw_data[0], sizeof(double)*length); 

               // if( upstream[up_count % micro_batch] != NULL) {
               //     toSend->isAdaptive = true;
               //     delete upstream[up_count % micro_batch];
               //     upstream[up_count % micro_batch] = toSend;
               //     up_count ++ ;
               // }
            
                count ++;
                if (count == (downstream_num - 1)) {
                    count = 0;
                    coding_id += parallelism;

                    while(nonblock.size() > 2){
                        for(auto it = nonblock.begin(); it != nonblock.end(); it++) {
                            int seq = (*it).first;
                            int ret = Nonblock_EmitData(seq, 0, *(*it).second); 
                            if (ret == 0) {
                                delete (*it).second;
                                nonblock.erase(it--);
                            }
                        }
                    }
                    while (nonblock.size() > 0) {
                        delete nonblock.front().second;
                        nonblock.erase(nonblock.begin());
                    }

                    if (micro_batch && ((coding_id / parallelism) % micro_batch == 0)) {
                        CodingItem * current = new CodingItem;
                        current->id = -1;
                        current->sequence = -1;
                        current->isAdaptive = true;
                        EmitDataAndWaitFeedback(downstream_num - 1, thread_id, *current);
                        delete current;
                        up_count = 0;
                    }
                }
                return;
            }

            // run coded mode
            CodingItem* toSend = new CodingItem;
            memcpy(intermediate, &tuple.raw_data[0], sizeof(double)*length);
            toSend->AppendItem(intermediate, sizeof(double)*length);

            // SIMD-optimized incremental encoding
            for (int i = 0; i < length / 4; i++) {
            	__m256d r = _mm256_load_pd(intermediate + 4*i);
            	__m256d p = _mm256_load_pd(Pdrive + 4*i);
            	__m256d q = _mm256_load_pd(Qdrive + 4*i);
            	__m256d a = _mm256_set1_pd(count + 1.0);
            	__m256d pd = _mm256_add_pd(p, r);
            	__m256d qd = _mm256_fmadd_pd(r, a, q);
            	_mm256_store_pd(Pdrive + 4*i, pd);
            	_mm256_store_pd(Qdrive + 4*i, qd);	
            }
            for (int i = length - length % 4; i < length; i++) {
            	Pdrive[i] += intermediate[i];
            	Qdrive[i] += intermediate[i] * (count + 1.0);
            }

            toSend->isCoded = false;
            toSend->id = coding_id;
            toSend->sequence = count;
            toSend->progress = 1;

            if (Nonblock_EmitData(count, 0, *toSend) == 1) {
                nonblock.push_back(make_pair(count, toSend));
            }
            else {
                delete toSend;
            }

            count ++;

            // finish of one round
            if (count == (downstream_num-3) ) {
                CodingItem *PItem = new CodingItem;
                CodingItem *QItem = new CodingItem;
                for (int j = 0; j < length; j++) {
                    PItem->AppendItem(Pdrive[j]);
                    QItem->AppendItem(Qdrive[j]);
                    Pdrive[j] = 0L;
                    Qdrive[j] = 0L;
                }
                PItem->isCoded = true;
                QItem->isCoded = true;
                PItem->id = coding_id;
                QItem->id = coding_id;
                // using -1 and -2 
                PItem->sequence = -1;
                QItem->sequence = -2;

                if (Nonblock_EmitData(count, 0, *PItem) == 1) {
                    nonblock.push_back(make_pair(count, PItem));
                }
                else {
                    delete PItem;
                }

                if (Nonblock_EmitData(count+1, 0, *QItem) == 1) {
                    nonblock.push_back(make_pair(count+1, QItem));
                }
                else {
                    delete QItem;
                }
    
                // ensure at least send k items
                while(nonblock.size() > 2) {
                    for (auto it = nonblock.begin(); it != nonblock.end(); it++) {
                        int seq = (*it).first;
                        CodingItem* send = (*it).second;
                        int ret = Nonblock_EmitData(seq, 0, *send);
                        if (ret == 0) {
                            delete send;
                            nonblock.erase(it--);
                        }
                    }
                }

                while (nonblock.size() > 0) {
                    delete nonblock.front().second;
                    nonblock.erase(nonblock.begin());
                }

                count = 0;
                coding_id += parallelism;   

                // end of micro batch
                if (micro_batch && ((coding_id / parallelism) % micro_batch == 0)) {  
                    //LOG_MSG("Run to micro_batch synchronous!\n");
                    CodingItem * current = new CodingItem;
                    current->id = -1;
                    current->sequence = -1;
                    EmitDataAndWaitFeedback(downstream_num-1, thread_id, *current);            
                    delete current;
                    //Nonblock_Clear();
                }
            }
        }

        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {

            // feedback from decoder, check uncoded or coded for next micro-batch
            if((tuple.id == -1) && micro_batch) { 
                if (tuple.isAdaptive) {
                    do_encode = 1;
                } 
                else do_encode = 0;
                LOG_MSG("Release the %lld mini batch block!\n", (coding_id+1)/micro_batch);
                return;    
            } 

            if (tuple.id == -2 && tuple.sequence == -2) // re-send
            {
                int bid = tuple.progress * width;
                int count= 0; 
                // LOG_MSG("Goning to resned from %d to %d\n", bid, micro_batch * width);
                for(int i = bid * (downstream_num -1); i < micro_batch * (downstream_num -1); i++){
                    memcpy(intermediate, upstreams[i], sizeof(double)*length);
                    //SIMD optimization
                    for (int t = 0; t < length / 4; t++){
                    	__m256d r = _mm256_load_pd(intermediate + 4*t);
                    	__m256d p = _mm256_load_pd(Pdrive + 4*t);
                    	__m256d q = _mm256_load_pd(Qdrive + 4*t);
                    	__m256d a = _mm256_set1_pd(count + 1.0);
                    	__m256d pd = _mm256_add_pd(p, r);
                    	__m256d qd = _mm256_fmadd_pd(r, a, q);
                    	_mm256_store_pd(Pdrive + 4*t, pd);
                    	_mm256_store_pd(Qdrive + 4*t, qd);	
                    }
                    for (int t = length - length % 4; t < length; t++){
                    	Pdrive[t] += intermediate[t];
                    	Qdrive[t] += intermediate[t] * (count + 1.0);
                    }

                    count ++; 
                    if (count == downstream_num -3) {
                        for(int j = 0; j < count; j++) {
                            CodingItem* toSend = new CodingItem;
                            toSend->AppendItem(upstreams[i-count+1+j], sizeof(double)*length);
                            memcpy(intermediate, upstreams[i], sizeof(double)*length);
                            toSend->isCoded = false;
                            toSend->id = coding_id;
                            toSend->sequence = j;
                            toSend->progress = 1;
                            if (j > 1)
                            EmitData(j, 0, *toSend);
                            delete toSend;
                        }
                        CodingItem *PItem = new CodingItem;
                        CodingItem *QItem = new CodingItem;
                        for (int t = 0; t < length; t++) {
                            PItem->AppendItem(Pdrive[t]);
                            QItem->AppendItem(Qdrive[t]);
                            Pdrive[t] = 0L;
                            Qdrive[t] = 0L;
                        }

                        PItem->isCoded = true;
                        QItem->isCoded = true;
                        PItem->id = coding_id;
                        QItem->id = coding_id;
                        PItem->sequence = -1;
                        QItem->sequence = -2;
                        EmitData(count, 0, *PItem);
                        EmitData(count + 1, 0, *QItem);
                        delete PItem;
                        delete QItem;
                        count = 0;
                    } 
                }
                LOG_MSG("resned from %d to %d finish\n", bid, micro_batch * width);
                CodingItem *current  = new CodingItem;
                current->isCoded = true;
                current->id = -1;
                current->sequence = -1;
                EmitDataAndWaitFeedback(count, 0, *current);
                delete current;
            }
        }

        void ProcessPunc(){}

};

class RSEncodeTraceThread : public afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass> {
    public:
        RSEncodeTraceThread(int num_downstreams) :
            afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>(0, num_downstreams) {}

    private:

        int width = 1;
        int length;
        int downstream_num;
        struct timeval start_time, current_time, end_time;
        double drive[max_node_tolerance][max_coding_length];
        int num_tolerance = 3;
        int count = 0;
        int micro_batch = 0;
        long long coding_id = 0;
        int start = -1;
        int parallel = 1;
        int thread_id = 0;
        int enable_adaptive = 0;
        int do_encode = 0;

        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            parallel = config->getint("num_compute_threads", -1);
            afs_assert(parallel > 0, "[num_compute_threads] should be indicated!\n");
            downstream_num  = config->getint("num_downstream", 0);
            afs_assert(downstream_num, "[num_downstream] is not set!\n");
            afs_assert(downstream_num >= 5, "[num_downstream] should be at lease 5 in RAID6!\n");
            length = config->getint("coding_length",0);
            afs_assert(length, "[coding_length] (i.e., row size) is not set!\n");
            afs_assert(length<=max_coding_length, "Exceeds the maximum coding length!\n"); 
            micro_batch = config->getint("micro_batch", -1);
            afs_assert(micro_batch >= 0, "[micro_batch] should be indicated in the config file!\n");
            num_tolerance = config->getint("num_m", -1);
            afs_assert(num_tolerance > 0, "[num_m] should be indicated in the config file for RS-code!\n");
            afs_assert(num_tolerance <= max_node_tolerance, "Exceeds the maximum node tolerant numbers!\n");
            for (int j = 0; j < num_tolerance; j++ ) {
                for (int i = 0; i < length; i++) {
                    drive[j][i] = 0L;
                }
            }
            count = 0;
            coding_id = get_tid();
            thread_id = get_tid();
            start = -1;
            enable_adaptive = config->getint("enable_adaptive", -1);
            afs_assert(enable_adaptive >= 0, "[enable_adaptive] should be indicated!\n");
            do_encode = 0;
            gettimeofday(&start_time, NULL);
        }

        void ComputeThreadFinish() {
            gettimeofday(&end_time, NULL);
            double used_time, throughput;
            used_time = (end_time.tv_sec + end_time.tv_usec/1000000.0) - (start_time.tv_sec + start_time.tv_usec/1000000.0);
            throughput = 11*1024*1.0/(used_time);
            LOG_MSG("The used time is %lf , and the throughput is: %lf MB/s\n", used_time, throughput);
        }

        void ComputeThreadRecovery() {}

        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct afs::DoubleItem &tuple) {

            // synchronization
            if (micro_batch && (start == -1)) {
                start = 0;
                LOG_MSG("First time synchronization!\n");
                CodingItem * current = new CodingItem;
                current->id = -1;
                current->sequence = -1;
                EmitDataAndWaitFeedback(downstream_num-1, thread_id, *current);            
                delete current;
            }       

            // uncoded mode
            if (enable_adaptive && !do_encode) {
                CodingItem *toSend = new CodingItem;
                int index = 0;
                double* line = tuple.raw_data;
                while (index < length){
                    toSend->AppendItem(line[index]);
                    index ++;
                }
                toSend->isAdaptive = true;
                toSend->isCoded = false;
                toSend->id = coding_id;
                toSend->sequence = count;
                EmitData(count, 0, *toSend);
                delete toSend;
                count++;
                if (count == (downstream_num -1)) {
                    count = 0;
                    coding_id += parallel;

                    if (micro_batch && ((coding_id /parallel) % micro_batch  == 0)) {
                        CodingItem *current = new CodingItem;
                        current->id = -1;
                        current->sequence = -1;
                        current->isAdaptive = true;
                        EmitDataAndWaitFeedback(downstream_num - 1, thread_id, *current);
                        delete current;
                    }
                }
                return;
            }
            
            // coded mode
            CodingItem* toSend = new CodingItem;
            int index = 0;
            double* line = tuple.raw_data;
            while (index < length) {
                for (int i = 0; i < num_tolerance; i++) {
                    drive[i][index] +=  line[index] * pow(i + 1, count);
                }
                toSend->AppendItem(line[index]);
                index ++;
            }

            toSend->isCoded = false;
            toSend->id = coding_id;
            toSend->sequence = count;
            EmitData(count, 0, *toSend);
            delete toSend;
            count ++;

            if (count == (downstream_num - num_tolerance - 1) ) {
                for (int i = 0; i <  num_tolerance; i++) {
                    CodingItem *current = new CodingItem;
                    for (int j = 0; j < length; j++) {
                        current->AppendItem(drive[i][j]);
                        drive[i][j] = 0L;
                    }
                    current->isCoded = true;
                    current->id = coding_id;
                    //using -(i + 1) to indicate this is the i-th parity
                    current->sequence = -( i + 1);
                    EmitData(count + i, 0, *current);
                    delete current;
                }

                count = 0;
                coding_id += parallel;   
                //end of mini batch, synchronize with DecoderWorker
                if (micro_batch && ((coding_id / parallel) % micro_batch == 0)) {  
                    LOG_MSG("Run to micro_batch synchronous!\n");
                    CodingItem * current = new CodingItem;
                    current->id = -1;
                    current->sequence = -1;
                    EmitDataAndWaitFeedback(downstream_num - 1, thread_id, *current);            
                    delete current;
                }
            }
        }

        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {

            if((tuple.id == -1) && micro_batch) {  
                LOG_MSG("Release micro_batch block!\n");
                if (tuple.isAdaptive) {
                    do_encode = 1;
                }
                else {
                    do_encode = 0;
                }
                return;    
            } 
        }

        void ProcessPunc(){}

};


