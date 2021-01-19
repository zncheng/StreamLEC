#ifndef _AFS_CODING_ITEM_H
#define _AFS_CODING_ITEM_H
#include<string.h>

#define MAX_num 10
#define O_MAX_num 11

struct __attribute__ ((__packed__)) CodingItem {

    // key pair
    int sid;
    long long id;

    // metadata
    bool isAdaptive;
    bool isCoded;
    int sequence;
    int progress;

    // attribute vector
    int raw_length;
    char* start;
    char raw[MAX_num * sizeof(double)];

    CodingItem() : sid(0), id(0), isAdaptive(false), isCoded(false), sequence(0), progress(1), raw_length(0), start(raw) {}

    //append a double variable
    void AppendItem(double value) {
        *((double*)start) = value;
        start += sizeof(double);
        raw_length += sizeof(double);
    }

    //append the whole double array
    void AppendItem(double* str, int length) {
        raw_length += length;
        memcpy(start, str, length);
        start += length;
    }

    //return the n-th item
    double GetRaw(int n) {
        char* start = raw + n * sizeof(double);
        double value = *(double*)start;
        return value;
    }

    //return the whole size
    uint32_t size() {
        return sizeof(long long) + sizeof(int) * 3 + sizeof(bool) * 2 + sizeof(char) + raw_length * sizeof(char);   
    }

};

struct __attribute__ ((__packed__)) O_CodingItem {

    // key pair
    int sid;
    long long id;

    // metadata
    bool isAdaptive;
    bool isCoded;
    int sequence;
    int progress;

    // attribute vector
    int raw_length;
    char* start;
    char raw[O_MAX_num * sizeof(double)];

    O_CodingItem() : sid(0), id(0), isAdaptive(false), isCoded(false), sequence(0), progress(1), raw_length(0), start(raw) {}

    //append a double variable
    void AppendItem(double value) {
        *((double*)start) = value;
        start += sizeof(double);
        raw_length += sizeof(double);
    }

    //append the whole double array
    void AppendItem(double* str, int length) {
        raw_length += length;
        memcpy(start, str, length);
        start += length;
    }

    //return the n-th item
    double GetRaw(int n) {
        char* start = raw + n * sizeof(double);
        double value = *(double*)start;
        return value;
    }

    //return the whole size
    uint32_t size() {
        return sizeof(long long) + sizeof(int) * 3 + sizeof(bool) * 2 + sizeof(char) + raw_length * sizeof(char);   
    }

};
#endif 
