#ifndef __MPMC_QUEUE_MESH_HPP_INCLUDED__
#define __MPMC_QUEUE_MESH_HPP_INCLUDED__

#include <vector>
#include "zerocopy_ringbuffer.hpp"

template<class T>
class LockFreeQueue {
public:

	int Nonblock_push(int writer_index, T x) {
        // return 1 if fail
		return q_[writer_index]->Nonblock_Insert(&x);
	}

	void push(int writer_index, T x) {
		q_[writer_index]->Insert(&x);
		//p_[writer_index]++;
	}

    int GetSize(int writer_index) {
        return q_[writer_index]->Size();
    }

    bool pop(T& ret) {
        T* slot = NULL;
        for (int i=0; i<num_queue; i++) {
            if (cur >= num_queue) {
                cur = 0;
            }
            slot = q_[cur]->Extract();
            if (slot) {
                ret = *slot;
                q_[cur]->Ack();
                cur++;
                //send_cnt ++;
                return true;
            }
            cur++;
        }
		return false;
	}

    // pop specific buffer corspond to reader
    //int PopNum(){
    //    return send_cnt;
    //}

    void IncAck(int writer_id) {
        p_[writer_id] ++;
    }

    int AckNum(int writer_id) {
        return p_[writer_id];
    }

    void resetAck(){
        for (unsigned int i = 0; i < p_.size(); i++)
            p_[i] = 0;
    }
    //
    //int BufferSize(int writer_id) {
    //    return q_[writer_id]->Size();
    //}

    void flush() {
        for (int i=0; i<num_queue; i++) {
            q_[i]->Flush();
        }
    }

    void AddQueue() {
        num_queue++;
        q_.push_back(new ZeroRingBuffer<T>());
        p_.push_back(0);
    }

    LockFreeQueue() : num_queue(0), cur(0) {
        // send_cnt = 0;
        //q_ = (ZeroRingBuffer<T>**)calloc(num_writer, sizeof(ZeroRingBuffer<T>*));
        /*
        for (int i=0; i<num_writer; i++) {
            //q_[i] = new ZeroRingBuffer<T>();
            q_.push_back(new ZeroRingBuffer<T>());
        }
        */
    }

    ~LockFreeQueue() {
        for (int i=0; i<num_queue; i++) {
            delete q_[i];
        }
        //free(q_);
    }

private:
    //ZeroRingBuffer<T>** q_;
    std::vector<ZeroRingBuffer<T>*> q_;
    std::vector<long long int> p_;
    int num_queue;
    int cur;
    //long long int send_cnt;
};


#endif
