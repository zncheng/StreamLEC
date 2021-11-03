#include <streamlec.h>
/**
* user-extensible interfaces for sink
* By default, we implement RS code for the Decode interface
**/

/*
    Declare global variables here
*/

# define N_centoids 2
int attr_num = 12;
double model[N_centoids][MAX_MODEL_SIZE] = {0};
double tmp_model[N_centoids][MAX_MODEL_SIZE] = {0};
int tmp_centoids_counter[N_centoids] = {0};
int global_centoids_counter[N_centoids] = {0};
double alpha = 0.01;;
int cnt = 0;
/*
* Implement interfaces below
*/

Result RSDecodeThread::Recompute(Item &decoded) {
    Result R;
    /**
    * re-compute the decoded item
    **/ 
    double prediction[N_centoids] = {0};
    int index = -1;
    double min = 10e9;
    for (int i = 0; i < attr_num; i ++) {
        for (int j = 0; j < N_centoids; j ++) {
            prediction[j] += model[j][i] * decoded.pop_value(i);
        }
    }
    for (int i = 0; i < attr_num; i++) {
        if (min > prediction[i]) {
            min = prediction[i];
            index = i;
        }
    }

    for ( int i = 0 ;i < attr_num; i++)
        R.push_value(decoded.pop_value(0));
    R.push_value(index);

    return R; 
}

void RSDecodeThread::Aggregate(Result &R) {

    int index = R.pop_value(attr_num);
    for(int i = 0; i < attr_num; i ++) {
        tmp_model[index][i] += R.pop_value(i);
    }
    tmp_centoids_counter[index] ++;
    cnt ++;

    if (cnt== batch_size) {
        /*
           update rule:
           c_t+1 = [(c_t * n_t *a) + (x_t * m_t)] / [n_t*a + m_t]
           n_t+1 = n_t + m_t
           c_t is the previous centroid for the cluster
           x_t is the current estimated centroid
           m_t is the point of number of estimated centroid
           a is decay factor
         */
        for (int i = 0; i < N_centoids; i++) {
            for (int j = 0; j < attr_num; j++) {
                model[i][j] = model[i][j] * global_centoids_counter[i] * alpha + tmp_model[i][j] / (global_centoids_counter[i] * alpha + tmp_centoids_counter[i]);
                global_centoids_counter[i] += tmp_centoids_counter[i];
                tmp_centoids_counter[i] = 0;
            } 
        }
        for (int j = 0; j < N_centoids; j++) { 
            for (int i = 0; i < attr_num; i++) {
                feedback->push_value(model[j][i]);
                tmp_model[j][i] = 0;
            }
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

