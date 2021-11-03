#ifndef __AFS_DATA_TYPE_HPP_INCLUDED__
#define __AFS_DATA_TYPE_HPP_INCLUDED__

#define MAX_MESSAGE_VALUE_SIZE 60

struct KEY {
    uint32_t sourceID;
    uint32_t sinkID;
    uint64_t stripeID;
    int32_t sIndex;
    bool adaptive;
} __attribute__ ((__packed__));

class Message {
    public:
        struct KEY key;
        double value[MAX_MESSAGE_VALUE_SIZE];
        uint32_t len;
        uint32_t offset;
        
        Message() {
            this->len = 0;
            this->offset = 0;
        }

        double pop_value(int index) {
            return value[index];
        }

        double push_value(double v) {
            value[offset] = v;
            offset ++;
            len ++;
        }

        void push_array(double *v, int size) {
            memcpy(&value[offset], v, size * sizeof(double));
            offset += size;
            len += size;
        } 
        int size() {
            return len;
        }

} __attribute__ ((__packed__));

class Item : public Message {
    public:

        Item(){}
        
        Item(Message msg) {
            this->key = msg.key;
            memcpy(this->value, msg.value, sizeof(double)*msg.len);
            this->len = msg.len;
            this->offset = msg.offset;
        }

        void push_value(double v) {
            value[offset] = v;
            offset ++;
            len ++;
        }

        double* pop_header() {
            return &value[0];
        }
 
} __attribute__ ((__packed__)); 

class  Result : public Message {
    public:
        Result(){}

        Result(Message msg) {
            this->key = msg.key;
            memcpy(this->value, msg.value, sizeof(double) * msg.len);
            this->len = msg.len;
            this->offset = msg.offset;
        }
        void push_value(double v) {
            value[offset] = v;
            offset ++;
            len ++;
        }

        double* pop_header() {
            return &value[0];
        }
        
} __attribute__ ((__packed__)); 


class Feedback : public Message {
    public:

        Feedback(){}
        
        Feedback(Message msg) {
            this->key = msg.key;
            memcpy(this->value, msg.value, sizeof(double)*msg.len);
            this->len = msg.len;
            this->offset = msg.offset;
        }
        void push_value(double v) {
            value[offset] = v;
            offset ++;
            len ++;
        }
        
} __attribute__ ((__packed__));

class ACK : public Message {
    public:
        ACK(Message msg) {
            this->key = msg.key;
        }

        void set_pairID(uint32_t id) {
            key.sourceID = id;
            key.sinkID = id;
        }

        /**
        * use sIndex = -1 to indicate re-transmit
        * use stripeID to to record watermark
        **/
        void set_watermark(uint64_t watermark) {
            key.sIndex = -1;
            key.stripeID = watermark;
        }

        bool check_retransmit() {
            if (key.sIndex == -1)
                return true;
            else return false;
       }       
 
        uint64_t get_watermark() {
            return key.stripeID;
        }

        /**
        * use adaptive to record uncoded/uncoded, true for coded moe
        **/
        void set_adaptive(bool coded) {
            key.adaptive = coded;
        }
        
        bool get_adaptive() {
            return key.adaptive;
        }

} __attribute__ ((__packed__));

#endif
