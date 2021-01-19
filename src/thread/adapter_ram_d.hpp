#ifndef __AFS_ADAPTER_RAM_DOUBLE_HPP_INCLUDED__
#define __AFS_ADAPTER_RAM_DOUBLE_HPP_INCLUDED__

#include "adapter_base.hpp"

#include <stdio.h>
#include <string>

#include "../config.hpp" 
#include "../util.hpp"

namespace afs {

class AdapterRAM_d : public AdapterBase {

public:

    // derived from AdapterBase
    void Init();
    void Clean();
    //void AddSource(const char* source);

    AdapterRAM_d();

private:

    unsigned char* buf_;
    uint64_t ptr_;
    uint64_t tot_cnt_;
    uint64_t cur_cnt_;
    uint64_t tot_byte_;
    uint64_t cur_byte_;

    size_t print_freq_;
    uint64_t last_ts_;
   
    //attribute numbers per record
    int attr_num;
 
    // derived from AdapterBase
    void ReadRecord(void** data, uint32_t *len);
};

AdapterRAM_d::AdapterRAM_d() :
    buf_(NULL),
    ptr_(0),
    tot_cnt_(0),
    cur_cnt_(0),
    tot_byte_(0),
    cur_byte_(0) {
    }

void AdapterRAM_d::Init() {

    Config* config = Config::getInstance();
    unsigned long long int max = config->get_ull("adapter.max_data", 0);
        
    print_freq_ = config->get_ull("ram_adapter_print", 0);
    LOG_MSG("print freq %lu\n", print_freq_);

    size_t buf_size = config->get_ull("memory_buffer_size", 0);
    afs_assert(buf_size, "RAM buffer size is not set in config file\n");

    attr_num = config->getint("item_attribute_number", 0);
    afs_assert(attr_num, "Item attribute number is not set in config file\n");

    char* filename = config->getstring("trace_file", NULL);
    afs_assert(filename, "[trace_file] must be specified in the config file\n");

    FILE* file = fopen(filename, "r");
    afs_assert(file, "Input file %s not found, exit\n", filename);

    buf_ = (unsigned char*)calloc(buf_size, sizeof(unsigned char));
    afs_assert(buf_, "Allocate trace buffer error\n");
    ptr_ = 0;

    //char buf[MAX_RECORD_LENGTH];
    double buf;
    while (1) {
        if (fscanf(file, "%lf", &buf) == EOF) {
            tot_cnt_ = tot_cnt_ / attr_num;
            break;
        }
        else {
            if (buf_size-ptr_ < sizeof(double)) {
                break;
            }
            *((double*)(buf_+ptr_)) = buf;
            ptr_ += sizeof(double);
            //memcpy(buf_+ptr_, buf, len);
            //ptr_ += len;
            tot_cnt_++;
            tot_byte_ += sizeof(double);
            if (max && tot_cnt_/attr_num == max) {
                break;
            }
        }
    }

    ptr_ = 0;

    fclose(file);

    LOG_MSG("Complete read %lu messages, %lu bytes, %lf bytes/msg\n", tot_cnt_, tot_byte_, 1.0*tot_byte_/tot_cnt_);
}

void AdapterRAM_d::Clean() {
    free(buf_);
}

/*
void AdapterRAM::AddSource(const char* source) {
    LOG_ERR("We only consider single file for adapter_disk\n");
}
*/

// assume each record has fixed number of attributes
void AdapterRAM_d::ReadRecord(void** data, uint32_t *data_len) {
    if (cur_cnt_ == tot_cnt_) {
        *data = NULL;
        return;
    }

    //uint32_t len = *((uint32_t*)(buf_+ptr_));
    //ptr_ += sizeof(uint32_t);
    uint32_t len = sizeof(double) * attr_num;
    *data = (void*)(buf_ + ptr_);
    *data_len = len;
    ptr_ += len;
    cur_cnt_++;
    cur_byte_ += len;

    if (print_freq_) {
        if (afs_unlikely(cur_cnt_==1)) {
            last_ts_ = now_us();
        }
        else if (afs_unlikely(cur_cnt_ % print_freq_ == 0)) {
            uint64_t cur_ts = now_us();
            if (last_ts_ > 0) {
                LOG_MSG("AdapterRAM throughput: %.02lf msg/s, %.02lf MB/s by %lu messages\n",
                    1000000.0*print_freq_ / (cur_ts-last_ts_),
                    1.0*cur_byte_ / (cur_ts-last_ts_),
                    cur_cnt_
                    );
            }
            last_ts_ = cur_ts;
            cur_byte_ = 0;
        }
    }
}

}
#endif

