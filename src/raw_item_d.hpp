#ifndef __AFS_RAW_ITEM_D_HPP_INCLUDED__
#define __AFS_RAW_ITEM_D_HPP_INCLUDED__

/// Original data from traces or external sources

#include <string.h>
#include "wrap_item.hpp"

namespace afs {

class DoubleItem {
public:
    double raw_data[MAX_ATTRIBUTE_NUM];

    DoubleItem& operator= (DoubleItem& i) {
        //fprintf(stderr, "size %lu\n", strlen(i.raw_data));
        memcpy(this->raw_data, i.raw_data, sizeof(double) * MAX_ATTRIBUTE_NUM);
        return *this;
    }
};

typedef WrapItem<DoubleItem> WDoubleItem;

}

#endif
