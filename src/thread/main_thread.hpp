#ifndef __AFS_MAIN_THREAD_HPP_INCLUDED__
#define __AFS_MAIN_THREAD_HPP_INCLUDED__

#include <stdio.h>
#include <stdint.h>

#include <string>

#include "../config.hpp"
#include "thread.hpp"
#include "up_thread.hpp"
#include "down_thread.hpp"

#include "../wrap_item.hpp"
#include "router_base.hpp"
#include "../queues/mpsc_channel.hpp"
#include "../queues/zerocopy_ringbuffer.hpp"

namespace afs {

/**
 * Processing events
 * @param InT class of input event from Dispatcher
 * @param OutT class of output event to Collector
 */
template <class InT, class OutT, class RInT, class ROutT>
class MainThread : public ThreadBase {

typedef WrapItem<InT> WInT;
typedef WrapItem<OutT> WOutT;
typedef WrapItem<RInT> WRInT;
typedef WrapItem<ROutT> WROutT;

public:
    MainThread(int num_upstream, int num_downstream);

    void ConnectUpThread(UpThread<InT, ROutT>* up_thread);
    void ConnectDownThread(DownThread<OutT, RInT>* down_thread);

protected:


    /**
     * Send Message to next-hop worker via output_thread
     */

    void Emit(int dest, OutT& msg) {
        //LOG_MSG("Dest %d with size %d\n", dest, down_writer_->GetReaderSize(dest));
        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        slot->set_type(ITEM_NORMAL);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->data() = msg;
        down_writer_->Write(dest, slot);
        //LOG_MSG("   end emit\n");
    }

    void Emit(int worker_dest, int thr_dest, OutT& msg) {
        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        slot->set_type(ITEM_NORMAL);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->set_thr_dest(thr_dest);
        slot->data() = msg;
        down_writer_->Write(worker_dest, slot);
        sent_cnt[worker_dest] ++;
        //LOG_MSG("   end emit\n");
    }
   
    int Nonblock_Emit(int dest, OutT& msg) {
        if (down_writer_->GetReaderSize(dest) < RB_SIZE/2 -10 ) {
            WOutT* slot = (WOutT*)down_writer_->GetSlot();
            slot->set_type(ITEM_NORMAL);
            slot->set_worker_source(get_wid());
            slot->set_thr_source(get_tid());
            slot->data() = msg;
            down_writer_->Write(dest, slot);
            return 0;
        }
        else {
            return 1;
        }
    }

    int Nonblock_Emit(int worker_dest, int thr_dest, OutT& msg) {

        int tmp = down_writer_->GetPushSize(worker_dest);
        if (ack_max < tmp)
            ack_max = tmp;
        if (ack_max - tmp > 1800)
            return 1;

        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        slot->set_type(ITEM_NORMAL);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->set_thr_dest(thr_dest);
        slot->data() = msg;
        down_writer_->Write(worker_dest, slot);
        return 0;
    }

    void EmitFeedback(int dest, ROutT& msg) {
        if (is_stop_[dest] == 1) {
            return;
        }
        afs_assert(up_writer_, "    reverse writer null\n");
        WROutT* slot = (WROutT*)up_writer_->GetSlot();
        slot->set_type(ITEM_EMERGENCE);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->data() = msg;
        up_writer_->Write(dest, slot);
        up_writer_->Flush();
        // LOG_MSG("   end emit reverse\n");
    }

    void EmitFeedback(int dest_worker,int dest_thr, ROutT& msg) {
        if (is_stop_[dest_worker] == 1) {
            return;
        }
        // LOG_MSG("To emit reverse\n");
        afs_assert(up_writer_, "    reverse writer null\n");
        WROutT* slot = (WROutT*)up_writer_->GetSlot();
        slot->set_type(ITEM_SHUFFLE);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->set_thr_dest(dest_thr);
        slot->data() = msg;
        up_writer_->Write(dest_worker, slot);
        up_writer_->Flush();
        // LOG_MSG("   end emit reverse\n");
    }

    RInT Recv(int dest_worker) {
        WRInT* r_tuple = NULL;
        //LOG_MSG("Go to wait ACK !\n")
        while (1) {
            if ((r_tuple=r_in_queue_->Extract()) == NULL) {
                continue;
            }
            r_in_queue_->Ack();
            break;
        }
        //LOG_MSG("Got ACK successfully!\n")
        return r_tuple->data();
    }

    void EmitDataAndWaitFeedback(int dest_worker, int dest_thr, OutT& msg) {
        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        slot->set_type(ITEM_SHUFFLE);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->set_thr_dest(dest_thr);
        slot->data() = msg;
        down_writer_->Write(dest_worker, slot);
        down_writer_->Flush();

        WRInT* r_tuple = NULL;
        while (1) {
            if ((r_tuple=r_in_queue_->Extract()) == NULL) {
                continue;
            }
            ITEM_TYPE t = r_tuple->get_type();
            // LOG_MSG("\tgot type %d\n", t);
            switch (t) {
                case ITEM_FINISH:
                    LOG_ERR("Unexpected event type %d\n", (int)t);
                    break;
                case ITEM_REVERSE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_EMERGENCE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_SHUFFLE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                default:
                    LOG_ERR("Unidentified event type %d\n", (int)t);
                    break;
            }
            r_in_queue_->Ack();
            break;
        } // end of while reverse
    }


    void EmitDataAndWaitFeedback(int dest, OutT& msg) {
        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        slot->set_type(ITEM_EMERGENCE);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->data() = msg;
        down_writer_->Write(dest, slot);
        down_writer_->Flush();

        WRInT* r_tuple = NULL;
        while (1) {
            if ((r_tuple=r_in_queue_->Extract()) == NULL) {
                continue;
            }
            ITEM_TYPE t = r_tuple->get_type();
            switch (t) {
                case ITEM_FINISH:
                    LOG_ERR("Unexpected event type %d\n", (int)t);
                    break;
                case ITEM_REVERSE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_EMERGENCE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_SHUFFLE:
                    r_event_++;
                    ProcessFeedback(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                default:
                    LOG_ERR("Unidentified event type %d\n", (int)t);
                    break;
            }
            r_in_queue_->Ack();
            break;
        } // end of while reverse
    }

    /**
    * APIs to query current worker state and ringbuffer state
    */

    uint64_t GetNumDownstream() {
        return num_downstream_; 
    }

    int GetRemainUpstream() {
        return remain_upstream_;
    }

    int GetRemaingSize(){
        return in_queue_->Size();
    }

    void ClearInput() {
        WInT* in_tuple = NULL;
        while ((in_tuple = in_queue_->Extract()) != NULL) {
            in_queue_->Ack();
        }
    }

    void ResetMonitor() {
        for (int i = 0; i < sent_cnt.size(); i++) {
            sent_cnt[i] = 0;
        }
    }

    void Nonblock_Clear() {
        down_writer_->reset();
    }

    void EmitReverseData(int dest, ROutT& msg) {
        //LOG_MSG("To emit reverse\n");
        afs_assert(up_writer_, "    reverse writer null\n");
        //int dest = r_router_->GetDestination(&msg, sizeof(ROutT));
        WROutT* slot = (WROutT*)up_writer_->GetSlot();
        slot->set_type(ITEM_REVERSE);
        slot->set_worker_source(get_wid());
        slot->set_thr_source(get_tid());
        slot->data() = msg;
        up_writer_->Write(dest, slot);
        //LOG_MSG("   end emit reverse\n");
    }

    void FlushReverseWriter() {
        up_writer_->Flush();
        up_writer_->Clean();
        up_writer_->Init();
    }
    
    void EmitReverseTimeout(int dest) {
        WROutT r_wrap_msg;
        r_wrap_msg.set_type(ITEM_REVERSE);
        r_wrap_msg.set_worker_source(get_wid());
        r_wrap_msg.set_thr_source(get_tid());
        EmitReverseWrapData(dest, r_wrap_msg);
    }

private:

    //  monitor number of process events
    uint64_t event_;
    uint64_t r_event_;

    int num_downstream_;
    int num_upstream_;
    int remain_upstream_;

    // forward queues
    ZeroRingBuffer<WInT>* in_queue_;
    MPSCWriter<WOutT>* down_writer_;

    // backward queue
    ZeroRingBuffer<WRInT>* r_in_queue_;
    MPSCWriter<WROutT>* up_writer_;

    uint32_t* is_stop_;

    // monitor nonblock emit
    vector<int> sent_cnt;
    int ack_min, ack_max;

    // derived from ThreadBase
    void ThreadInitHandler();
    void ThreadFinishHandler();
    void ThreadMainHandler();

    //  user-define interface to initialize and cleaning-up
    //  called by ThreadInitHandler and ThreadFinishHander respectively
    virtual void MainThreadInit() = 0;
    virtual void MainThreadFinish() = 0;

    //  user-define interface to process events of various types
    void TimerCallback(){};
    virtual void WorkerMainEntry(uint32_t src_worker, uint32_t src_thread, uint64_t seq, InT& tuple) = 0;
    //virtual void ProcessEncode(uint32_t src_worker, uint32_t src_thread, uint64_t seq, InT& tuple) = 0;
    //virtual void ProcessDecode(uint32_t src_worker, uint32_t src_thread, uint64_t seq, InT& tuple) = 0;
    //virtual void WorkerBackardEntry(int src_worker, int src_thread, RInT& tuple) {
    void WorkerBackardEntry(int src_worker, int src_thread, RInT& tuple) {
        //LOG_MSG("Function ProcessFeedback does not be implemented\n");
    }

    void EmitWrapData(int dest, WOutT& msg) {
        WOutT* slot = (WOutT*)down_writer_->GetSlot();
        *slot = msg;
        down_writer_->Write(dest, slot);
    }

    void EmitReverseWrapData(int dest, WROutT& msg) {
        WROutT* slot = (WROutT*)up_writer_->GetSlot();
        *slot = msg;
        up_writer_->Write(dest, slot);
    }

    void EmitFinish() {
        WOutT wrap_msg;
        if (down_writer_ != NULL) {
            wrap_msg.set_type(ITEM_FINISH);
            wrap_msg.set_worker_source(get_wid());
            wrap_msg.set_thr_source(get_tid());
            for (int i = 0; i < num_downstream_; i++) {
                EmitWrapData(i, wrap_msg);
            }
            down_writer_->Flush();
            LOG_MSG("MainThread: emit finish\n");
        }
    }
    
    void EmitReverseFinish() {
        WROutT r_wrap_msg;
        if (up_writer_ != NULL) {
            r_wrap_msg.set_type(ITEM_FINISH);
            r_wrap_msg.set_worker_source(get_wid());
            r_wrap_msg.set_thr_source(get_tid());
            for (int i=0; i < num_upstream_; i++) {
                LOG_MSG("Conpute thread: emit %d\n", i);
                EmitReverseWrapData(i, r_wrap_msg);
            }
            up_writer_->Flush();
        }
    }

};

template <class InT, class OutT, class RInT, class ROutT>
MainThread<InT, OutT, RInT, ROutT>::MainThread(
        int num_upstream,
        int num_downstream
        ) :
        ThreadBase(t_compute_thread),
        event_(0),
        r_event_(0),
        num_downstream_(num_downstream),
        num_upstream_(num_upstream),
        in_queue_(NULL),
        down_writer_(NULL),
        r_in_queue_(NULL),
        up_writer_(NULL)
    {
        LOG_MSG("compute_thread downstream %d, upstream %d\n", num_downstream, num_upstream);
        if (num_downstream > 0) {
            down_writer_ = new MPSCWriter<WOutT>();
        }
        if (num_upstream > 0) {
            LOG_MSG("create reverse write_\n");
            up_writer_ = new MPSCWriter<WROutT>();
        }
    }

template <class InT, class OutT, class RInT, class ROutT>
void MainThread<InT, OutT, RInT, ROutT>::ThreadInitHandler() {
    LOG_MSG(INDENT "initializing\n");
    if (down_writer_ != NULL) {
        down_writer_->Init();
    }
    if (up_writer_ != NULL) {
        up_writer_->Init();
    }
    MainThreadInit();
}

template <class InT, class OutT, class RInT, class ROutT>
void MainThread<InT, OutT, RInT, ROutT>::ThreadMainHandler() {
    
    WInT* tuple = NULL;
    WRInT* r_tuple = NULL;
    int up_stop = 0;
    int down_stop = 0;

    // no upstream implies external source
    if (num_upstream_ == 0) {
        num_upstream_++;
    }

    remain_upstream_ = num_upstream_;
    is_stop_ = (uint32_t*)calloc(num_upstream_, sizeof(uint32_t));

    while (up_stop<num_upstream_ || down_stop<num_downstream_) {
        //LOG_MSG("while\n");
        while (down_stop<num_downstream_ && (r_tuple=r_in_queue_->Extract()) != NULL) {
            ITEM_TYPE t = r_tuple->get_type();
            switch (t) {
                case ITEM_FINISH:
                    down_stop++;
                    break;
                case ITEM_REVERSE:
                    r_event_++;
                    WorkerBackardEntry(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_EMERGENCE:
                    r_event_++;
                    WorkerBackardEntry(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                case ITEM_SHUFFLE:
                    r_event_++;
                    WorkerBackardEntry(r_tuple->get_worker_source(), r_tuple->get_thr_source(), r_tuple->data());
                    break;
                default:
                    LOG_ERR("Unidentified event type %d\n", (int)t);
                    break;
            }
            r_in_queue_->Ack();
        } // end of while reverse

        //LOG_MSG("read forward queue\n");
        if (afs_unlikely(up_stop == num_upstream_)) {
            down_writer_->AttemptClean();
        }
        else {
            if ((tuple=in_queue_->Extract()) != NULL) {
                ITEM_TYPE t = tuple->get_type();
                uint32_t worker = tuple->get_worker_source();
                uint32_t thread = tuple->get_thr_source();
                uint64_t seq = tuple->get_seq();

                InT data = tuple->data();
                in_queue_->Ack();
                switch (t) {
                    case ITEM_FINISH:
                        up_stop++;
                        // send finish first
                        // but continue to wait for finish from downstream workers
                        is_stop_[worker] = 1;
                        remain_upstream_--;
                        if (up_stop == num_upstream_) {
                            EmitFinish();
                        }
                        LOG_MSG("compute thread: up_worker %d up_stop %d remain %d\n", worker, up_stop, remain_upstream_);
                        break;
                    case ITEM_TIMEOUT:
                        //ProcessPunc();
                        break;
                    case ITEM_NORMAL:
                        event_++;
                        WorkerMainEntry(worker, thread, seq, data);
                        break;
                    case ITEM_EMERGENCE:
                        event_++;
                        WorkerMainEntry(worker, thread, seq, data);
                        break;
                    case ITEM_SHUFFLE:
                        event_++;
                        WorkerMainEntry(worker, thread, seq, data);
                        break;
                    default:
                        LOG_ERR("Unidentified event type %d\n", (int)t);
                        break;
                }
            } // end of if
        }
        // handle timer finish
        afs_zmq::command_t cmd;
        int rc = -1;
        rc = ThreadBase::WaitWorker(&cmd, false);
        if (rc == -1) {
            // timer no finish, skip
        } else if (rc  == afs_zmq::command_t::type_t::done) { // timer finish
            TimerCallback();
        } 
        
    } // end of while

    LOG_MSG(" compute thread (out-of-while): up_stop %d, upstream %d, down_stop %d, downstream %d\n", up_stop, num_upstream_, down_stop, num_downstream_);

    if (down_writer_) {
        down_writer_->AttemptClean();
    }
    LOG_MSG("    compute thread send reverse end\n");
    EmitReverseFinish();
    if (up_writer_ != NULL) {
        up_writer_->Clean();
    }

    free(is_stop_);
    LOG_MSG("    compute thread end\n");
}

template <class InT, class OutT, class RInT, class ROutT>
void MainThread<InT, OutT, RInT, ROutT>::ThreadFinishHandler() {
    LOG_MSG(INDENT "process %lu tuples, %lu tuples\n", event_, r_event_);

    MainThreadFinish();
}

template <class InT, class OutT, class RInT, class ROutT>
void MainThread<InT, OutT, RInT, ROutT>::ConnectUpThread(UpThread<InT, ROutT>* up_thread) {
    in_queue_ = new ZeroRingBuffer<WInT>();
    up_thread->AddOutQueue(in_queue_);
    if (up_thread->IsReverse()) {
        // each upstream worker corresponds to an output buffer in up_thread
        // add this compute_thread as a writer for all the output buffers
        int num_upstream = up_thread->GetNumUpstream();
        int writer_id = get_tid();
        for (int i=0; i<num_upstream; i++) {
            MPSCReader<WROutT>* r_reader = (MPSCReader<WROutT>*)up_thread->GetReverseOutBufferReader(i);
            afs_assert(up_writer_, "reverse writer %d is not available\n", writer_id);
            afs_assert(r_reader, "reverse reader %d is not available\n", i);
            up_writer_->SetWriterId(writer_id);
            up_writer_->ConnectPeer(i, r_reader);
            r_reader->SetReaderId(i);
            r_reader->ConnectPeer(writer_id, up_writer_);
        }
    }
}

template <class InT, class OutT, class RInT, class ROutT>
void MainThread<InT, OutT, RInT, ROutT>::ConnectDownThread(DownThread<OutT, RInT>* down_thread) {
    int writer_id = get_tid();
    int num_down_thread_reader = down_thread->GetDownstream();
    for (int i=0; i<num_down_thread_reader; i++) {
        MPSCReader<WOutT>* reader = (MPSCReader<WOutT>*)down_thread->GetOutBufferReader(i);
        afs_assert(down_writer_, "writer %d is not available\n", writer_id);
        afs_assert(reader, "reader %d is not available\n", i);
        down_writer_->SetWriterId(writer_id);
        down_writer_->ConnectPeer(i, reader);
        reader->SetReaderId(i);
        reader->ConnectPeer(writer_id, down_writer_);
    }

    for (int i = 0; i < num_down_thread_reader; i++){
        sent_cnt.push_back(0);
    }   
    ack_min = 0;
    ack_max = 0;
 
    if (down_thread->IsReverse()) {
        r_in_queue_ = new ZeroRingBuffer<WRInT>();
        down_thread->AddReverseInBuffer(r_in_queue_);
    }
}

} // namespace

#endif 
