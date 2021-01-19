#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <sys/time.h>
#include <unistd.h>
#include <codestream.h>

#define maxnum_attr 256

class ProcessorThread : public afs::ComputeThread<struct CodingItem, struct O_CodingItem, struct O_CodingItem, struct afs::NullClass> {
    public:
        ProcessorThread(int num_upstreams, int num_downstreams) :
            afs::ComputeThread<struct CodingItem, struct O_CodingItem, struct O_CodingItem, struct afs::NullClass>(num_upstreams, num_downstreams) {}

        void Initialization();
        void ProcessLinear(CodingItem &tuple);
        void ProcessNonlinear(CodingItem &tuple);
        void ProcessData(CodingItem &tuple);
        void ProcessFeedback(int worker, int thread, struct O_CodingItem &tuple);

    private:

        // constant
        int downstream_num;
        int upstream_num;
        int parallel = 1;
        int enable_coded;
        struct timeval start_time, current_time, end_time;

        double model[maxnum_attr];
        double ret[maxnum_attr];
        int count = 0;
        int micro_batch = 0;	
        int attr_num = 0;


        void ComputeThreadInit() {

            Config* config = Config::getInstance();
            upstream_num = config->getint("num_upstream", -1);
            afs_assert(upstream_num > 0, "[num_upstream] must be specified in config file!\n");
            downstream_num = config->getint("num_downstream", -1);
            afs_assert(downstream_num > 0, "[num_downstream] must be specified in config file!\n");
            micro_batch = config->getint("micro_batch", -1);
            afs_assert(micro_batch > 0, "[micro_batch] must be specified in config file!\n");
            attr_num = config->getint("attr_num", -1);
            afs_assert(attr_num > 0, "[attr_num] must be specified in config file!\n");
            count  = 0;
            enable_coded = config->getint("enable_coded", -1);
            afs_assert(enable_coded >= 0, "[enable_ccoded] must be inidcated!\n");
            Initialization();
        }

        // main entry for handling each recieved item
        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct CodingItem &tuple) {

            O_CodingItem* current = new O_CodingItem;
            if (enable_coded) { // integrating coded computation
                if(tuple.isAdaptive ) {
                    ProcessLinear(tuple);
                    ProcessNonlinear(tuple);
                }
                else {
                    if(tuple.isCoded) {
                          ProcessLinear(tuple);
                    }
                    else {
                        ProcessLinear(tuple);
                        ProcessNonlinear(tuple);
                    }
                }
            } 
            else { // use piggybacking only
                current->AppendItem((double *)(&tuple.raw[0]), sizeof(double) * attr_num); 
                if (!tuple.isCoded)
                    ProcessData(tuple);
            }
            current->isCoded = tuple.isCoded;
            current->sid = tuple.sid;
            current->id = tuple.id;
            current->sequence = tuple.sequence;
            current->isAdaptive = tuple.isAdaptive;
            EmitData(tuple.sid, 0, *current);
            delete current; 
        }

        void ComputeThreadFinish(){}
        void ComputeThreadRecovery(){}
        void ProcessPunc(){}
};
