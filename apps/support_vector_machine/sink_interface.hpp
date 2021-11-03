#include <streamlec.h>
/**
* user-extensible interfaces for sink
* By default, we implement RS code for the Decode interface
**/

/*
    Declare global variables here
*/

int attr_num = 12;
double gradient[MAX_DATA_LENGTH] = {0};
double model[MAX_DATA_LENGTH] = {0};
int cnt = 0;
double alpha = 0.01;;

/*
* Implement interfaces below
*/

Result RSDecodeThread::Recompute(Item &decoded) {
    Result R;
    /**
    * re-compute the decoded item
    **/ 
    double prediction = 0;
    for (int i = 1; i < attr_num; i ++) {
        prediction += model[i] * decoded.pop_value(i);
    }

    double label = decoded.pop_value(0);
    double hinge = (2 * label - 1) * prediction;

    for (int i = 1; i < attr_num; i++) {
        if (hinge < 1)
            R.push_value((label - prediction) * decoded.pop_value(i));
        else R.push_value (0);
    }
    return R; 
}

void RSDecodeThread::Aggregate(Result &R) {

    for(int i = 1; i < attr_num; i ++) {
        gradient[i] += R.pop_value(i-1);
    }    
    cnt ++;

    if (cnt== batch_size) {
        double l1norm = 0, l2norm = 0;
        for (int i = 1; i < attr_num; i++) {
            l2norm += model[i] * model[i];
        }
        for (int i = 1; i < attr_num; i++) {
            double new_val = (1 - sqrt(l2norm) * alpha ) * model[i] + alpha * gradient[i] / batch_size; 
            l1norm += abs(new_val - model[i]);
            model[i] = new_val;
            // for feedback
            feedback->push_value(new_val);
            gradient[i] = 0;
        }
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

