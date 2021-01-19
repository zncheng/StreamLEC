#ifndef __MPSC_WRITER_HPP_INCLUDED__
#define __MPSC_WRITER_HPP_INCLUDED__

#include "mpsc_object_indirect_mesh.hpp"

namespace afs {

template<class T>
class MPSCWriter : public MPSCObject<T> {

public:

    int GetPushSize(int reader) {
        //if (reader == 0) {
            //int orgin = MPSCObject<T>::own->AckNum(reader);
        //    return orgin - RB_SIZE + 1;
        //}
        //else
            return MPSCObject<T>::own->AckNum(reader);
    }

    void reset() {
        MPSCObject<T>::own->resetAck();
    }
    //int GetPopSize(int reader) {
    //    return MPSCObject<T>::peers[reader]->PopNum();
    //} 

    int GetReaderSize(int reader) {
        return MPSCObject<T>::peers[reader]->GetSize(writer_id);
    }

    T* GetSlot() {
        T* ret = NULL;
        while (!MPSCObject<T>::own->pop(ret)) {}
        return ret;
    }

    // if nonblock write fail, must free the slot
    int Nonblock_Write(int reader, T* addr) {
        // return 1 if write fail
        int ret = MPSCObject<T>::peers[reader]->Nonblock_push(writer_id, addr);
        return ret;
    }

    // free slot API for Nonblock_write
    void free_slot(int reader, T* slot) {
        MPSCObject<T>::own->push(reader, slot);
    }

    void Write(int reader, T* addr) {
        MPSCObject<T>::peers[reader]->push(writer_id, addr);
    }

    MPSCWriter<T>() :
        MPSCObject<T>(),
        //num_reader(num_readers),
        writer_id(-1)
    {
        slots = (T*) calloc(RB_SIZE * 2, sizeof(T));
        afs_assert(slots, "Slots allocate failure\n");
    }

    void SetWriterId(int w) {
        writer_id = w;
    }

    void Init() {
        afs_assert(writer_id != -1, "writer id is not set\n");
        for (auto d=0; d<RB_SIZE-1; d++) {
            MPSCObject<T>::own->push(0, slots + d);
        }
        clean_cnt++;
    }


    void Flush() {
        for (uint64_t i=0; i<MPSCObject<T>::peers.size(); i++) {
            MPSCObject<T>::peers[i]->flush();
        }
    }

    void AttemptClean() {
        T* ret = NULL;
        while (MPSCObject<T>::own->pop(ret)) {
            clean_cnt++;
        }
    }

    void Clean() {
        for (auto d=0; d<RB_SIZE-1-clean_cnt; d++) {
            T* ret = NULL;
            while (!MPSCObject<T>::own->pop(ret)) {};
        }
    }

private:
    T* slots;
    //int num_reader;
    int writer_id;
    int clean_cnt;
};

}

#endif
