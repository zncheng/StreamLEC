#include <string> 
#include <unordered_map>
#include <random>
#include <sstream>
#include <ctime>
#include <sys/time.h>
#include <math.h>
#include <immintrin.h>
#include <codestream.h>

class CustomizedEncodeThread : public afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass> {
    public:
        CustomizedEncodeThread(int num_downstreams) :
            afs::ComputeThread<struct afs::DoubleItem, struct CodingItem, struct CodingItem, struct afs::NullClass>(0, num_downstreams) {}
        
        // declare variables for encoding here

        void ProcessedEncode(afs::DoubleItem & tuple) {
            // handle encode here

        }
        void ProcessFeedback(int worker, int thread, struct CodingItem &tuple) {
            // handle feedback from decoder
        }


    private:
        void ComputeThreadInit() {
            // for initialization before receiving any items
        }
        void ComputeThreadFinish() {
            // for clear after finish of stream processig
        }
        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct afs::DoubleItem &tuple) {
            // receive an items
            // called user-specified encoding function
            ProcessedEncode(tuple);
        }


        void ProcessPunc(){}
        void ComputeThreadRecovery() {}
};


