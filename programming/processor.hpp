#include <string>
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <sys/time.h>
#include <unistd.h>
#include <streamlec.h>

#define MAX_MODEL_SIZE 256

class ProcessorThread : public afs::MainThread<Message, Message, Message, struct afs::NullClass> {
    public:
        ProcessorThread(int num_upstreams, int num_downstreams) :
            afs::MainThread<Message, Message, Message, struct afs::NullClass>(num_upstreams, num_downstreams) {}

        /**
        * user-extensible interfaces
        **/
        Result ProcessLinear(Item &item);
        Result ProcessNonlinear(Result &linear, Item &item);
        Result ProcessData(Item &item);
        void ProcessFeedback(Feedback &fed);

    private:

        uint32_t downstream_num;
        uint32_t upstream_num;
        uint32_t count = 0;
        uint32_t batch_size = 0;	
        uint32_t attr_num = 0;
        int32_t RS_k = 0;
        int32_t RS_r = 0;
        bool enable_hybrid = false;
        double model[MAX_MODEL_SIZE];

        void MainThreadInit() {

            Config* config = Config::getInstance();

            upstream_num = config->getint("num_upstream", -1);
            afs_assert(upstream_num > 0, "[num_upstream] must be specified in config file!\n");

            downstream_num = config->getint("num_downstream", -1);
            afs_assert(downstream_num > 0, "[num_downstream] must be specified in config file!\n");

            batch_size = config->getint("batch_size", 0);
            afs_assert(batch_size > 0, "[batch_size] must be specified in config file!\n");

            attr_num = config->getint("attr_num", -1);
            afs_assert(attr_num > 0, "[attr_num] must be specified in config file!\n");

            enable_hybrid = config->getboolean("enable_hybrid", -1);
            RS_k = config->getint("RS_k", -1);
            afs_assert(RS_k > 0, "[RS_k] must be inidcated!\n");

            RS_r = config->getint("RS_r", -1);
            afs_assert(RS_r > 0, "[RS_r] must be inidcated!\n");

            count  = 0;

        }

        // main entry for handling each recieved item
        void WorkerMainEntry(uint32_t worker, uint32_t thread, uint64_t seq, Message &msg) {

            Item item = (Item)msg;
            Message* toSend = new Message;
            if (enable_hybrid) { // enable hybrid coded computation, call ProcessLinear and ProcessNonlinear
                if (item.key.adaptive) { //uncoded mode
                    Result rl = ProcessLinear(item);
                    Result rn = ProcessNonlinear(rl, item);
                    toSend->push_array(rn.pop_header(), rn.size());
                }
                else { // coded mode
                    Result rl = ProcessLinear(item);
                    toSend->push_array(rl.pop_header(), rl.size());
                    Result rn = ProcessNonlinear(rl,item);
                    if (item.key.sIndex < RS_k ) { // append the non-linear result 
                        toSend->push_array(rn.pop_header(), rn.size());
                    }
                }
            }
            else { // disable hybrid coded computation, call ProcessData
                if (item.key.adaptive) { // uncoded mode
                    Result r = ProcessData(item);
                    toSend->push_array(r.pop_header(), r.size());
                }
                else { // coded mode
                    // piggyback data or parity item
                    toSend->push_array(&item.value[0], item.size()); 
                    Result r = ProcessData(item); // do the computation anyway
                    if (item.key.sIndex < RS_k ) { // append the result
                        toSend->push_array(r.pop_header(), r.size());
                    }
                }
            }
            toSend->key.sourceID = item.key.sourceID;
            toSend->key.sinkID = item.key.sinkID;
            toSend->key.stripeID = item.key.stripeID;
            toSend->key.sIndex = item.key.sIndex;
            Emit(item.key.sinkID, *toSend);
            delete toSend;

            // LOG_MSG("Send stripe %d index %d, first value %lf\n", toSend->key.stripeID, toSend->key.sIndex,toSend->pop_value(1));
            // LOG_MSG("current stripe %d batch %d RS %d\n", item.key.stripeID, batch_size, RS_k);
            if ((!item.key.adaptive && ((item.key.stripeID + 1)  % (batch_size / RS_k) == 0)) || (item.key.adaptive && item.key.stripeID % batch_size >= batch_size - RS_r - RS_k)) {
                int batch_id = 0;
                if (item.key.adaptive)
                    batch_id = (item.key.stripeID + 1) / batch_size;
                else
                    batch_id = ((item.key.stripeID +1) * RS_k) / batch_size;
                LOG_MSG("Wait Feedback for batch %d!\n", batch_id);
                Feedback *fed = new Feedback;
                *fed = Recv(item.key.sinkID);
                ProcessFeedback(*fed);
                delete fed;
            }
        }
        void MainThreadFinish(){}
};
