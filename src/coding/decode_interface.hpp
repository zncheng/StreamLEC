#include <codestream.h>

// for r = 1
void RAID5DecodeThread::Recompute(O_CodingItem & linear) {
    // pure linear, no re-computation
}

void RAID5DecodeThread::Aggregate(O_CodingItem & result) {
    LOG_MSG("Aggregate round %lld offset %d \n", result.id, result.sequence);
}

void RAID5DecodeThread::Initialization() {
   for (int i = 0; i < length; i++) {
        model[i] = 1.0;
    } 
}

// for r = 2
void RAID6DecodeThread::Recompute(O_CodingItem & linear) {
    // pure linear, no re-computation
}

void RAID6DecodeThread::Aggregate(O_CodingItem & result) {
    LOG_MSG("Aggregate round %lld offset %d \n", result.id, result.sequence);
}

void RAID6DecodeThread::Initialization() {
   for (int i = 0; i < length; i++) {
        model[i] = 1.0;
    } 
}

// for r > 2
void RSDecodeThread::Recompute(O_CodingItem & results) {
    
}

void RSDecodeThread::Aggregate(O_CodingItem & raw) {
    
}

void RSDecodeThread::Initialization() {

}


// for customized decoder
class CustomizedDecodeThread : public afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem> {
    public:
        CustomizedDecodeThread(int num_upstreams, int num_downstreams) :
            afs::ComputeThread<struct O_CodingItem, struct CodingItem, struct CodingItem, struct CodingItem>(num_upstreams, num_downstreams) {}
        
        void ProcessedDecode(struct O_CodingItem & tuple) {
  
        }
        void Recompute(struct O_CodingItem & tuple) {

        }
        void Aggregate(struct O_CodingItem & tuple) {

        }


    private:

        // declare variables for encoding here

        void ComputeThreadInit() {
            // for initialization before receiving any items
        }
        void ComputeThreadFinish() {
            // for clear after finish of stream processig
        }
        void ProcessData(uint32_t worker, uint32_t thread, uint64_t seq, struct O_CodingItem &tuple) {
            // receive an items
            // called user-specified encoding function
            ProcessedDecode(tuple);
        }


        void ProcessPunc(){}
        void ComputeThreadRecovery() {}
};


