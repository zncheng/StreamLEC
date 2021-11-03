#include <streamlec.h>
#include "neural_util.hpp"
/**
* user-extensible interfaces for sink
* By default, we implement RS code for the Decode interface
**/

/*
    Declare global variables here
*/
int cnt = 0;
/*
* Implement interfaces below
*/

Result RSDecodeThread::Recompute(Item &decoded) {
    Result R;
    /**
    * re-compute the decoded item
    **/ 
    double target = decoded.pop_value(0);
    forward_nonlinear(&decoded);
    backward_propagate(target);
    weight_delta();
    load_gradient(&R);
    return R; 
}

void RSDecodeThread::Aggregate(Result &R) {

    accumulate_gradient(R, 0);
    cnt ++;
    if (cnt == batch_size) {
        model_update(batch_size);
        clear_accumulate_weight();
        load_model(feedback);
        cnt = 0;
    }
    
}

void RSDecodeThread::Decode(double *receive, double *decode) {
   
    uint32_t data_base = current_stripe_id * (RS_r + RS_k);
    uint32_t parity_base = data_base + RS_k;
    // find missed data item
    for (uint32_t i = data_base; i < data_base + RS_k; i++) {
        if (!coded_visited[i]) {
            while(!coded_visited[parity_base]) parity_base++;
            update_origin(i-data_base, parity_base - data_base - RS_k + 1, RS_k);
            // cout << "data location " << i << " parity location " << parity_base << endl;
            memcpy(&received[i][0], &received[parity_base][0], sizeof(double) * item_size);
            parity_base ++;
        }
    }
    
    // find decode matrix, stored in revers
    inverse(origin, revers, RS_k);

    for (uint32_t i = data_base; i < data_base + RS_k; i++) {
        if (!coded_visited[i]) { // decode missed data
            uint32_t offset = i - data_base;
            // non-SIMD version
            for(uint32_t k = 0; k < RS_k; k++) {
                for(uint32_t j= 0; j < item_size; j++) {
                    decoded[offset][j] += revers[offset][k] * received[data_base + k][j];
                }
            }
            // SIMD-optimized decoding
            //for(uint32_t k = 0; k < RS_k; k++) {
            //    for(uint32_t j= 0; j < item_size / 4; j++) {
            //        __m256d p = _mm256_set1_pd(revers[offset][k]);
            //        __m256d r = _mm256_load_pd(received[data_base + k] + 4 * j);
            //        __m256d d = _mm256_load_pd(decoded[offset] + 4 * j);
            //        __m256d pd = _mm256_fmadd_pd(r, p, d);
            //        _mm256_store_pd(decoded[offset] + 4 * j, pd);
            //    }
            //    for (uint32_t j = item_size - item_size %4; j < item_size; j++) {
            //        decoded[offset][j] += revers[offset][k] * received[data_base + k][j];
            //    }
            //}
        }
    }

    // clear for next decoding
    for(uint32_t i = 0; i < RS_k; i++)
        recover_origin(i, RS_k);

}

