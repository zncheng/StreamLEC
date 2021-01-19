#include <streamlec.h>

/**
* user-extensible interfaces for source
* By default, we implement RS code for real number
**/

void RSEncodeThread::Encode(int sIndex, Item &data, double* parity) {

    /**
    * udpate Parity using current data item with RS code
    * derive the encoding coefficient from Vandermonde matrix
    **/
    
    alignas(32) double temp[MAX_DATA_LENGTH];
    memcpy(&temp[0], data.value, sizeof(double) * item_size);
    // SIMD-optimized incremental encoding
    for(uint32_t i = 0; i < RS_r; i++) {
        for (uint32_t j = 0; j < item_size / 4; j++) {
        	__m256d r = _mm256_load_pd(temp + 4 * j);
        	__m256d p = _mm256_load_pd(Parity[i] + 4 * j);
        	__m256d a = _mm256_set1_pd(pow(i + 1, sIndex) + 0.0);
        	__m256d pd = _mm256_fmadd_pd(r, a, p);
        	_mm256_store_pd(Parity[i] + 4 * j, pd);
        }
        for (uint32_t j = item_size - item_size % 4; j < item_size; j++) {
        	Parity[i][j] += data.value[j];
        }
    }
}

void RSEncodeThread::ProcessACK(ACK &ack) {
    /**
    * Decide to run coded mode or uncoded mode for next micro-batch
    * re-transmit data item if necessary
    **/

    
    if (ack.check_retransmit()) { // need retransmit
        LOG_MSG("Need retransmit!\n");
        uint32_t base = ack.get_watermark() % batch_size; 
        for(uint32_t i = base; i < batch_size; i++) {
            Item* data = new Item;
            data->push_array(&cache[base][0], (int)item_size);
            data->key.sourceID = worker_id;
            data->key.sinkID = worker_id;
            data->key.stripeID = base / RS_k;
            data->key.sIndex = base % RS_k;
            data->key.adaptive = false;
            Emit(data->key.sIndex, *data);
            Encode(data->key.sIndex, *data, &Parity[0][0]);
            if ((uint32_t)data->key.sIndex == RS_k -1) {
                for (uint32_t i = 0; i < RS_r; i++) {
                    Item* parity = new Item;
                    parity->push_array(&Parity[i][0], item_size);
                    parity->key.sourceID = worker_id;
                    parity->key.sinkID = worker_id;
                    parity->key.stripeID = (counter % batch_size ) / RS_k;
                    parity->key.sIndex = RS_k + i;
                    parity->key.adaptive = false;
                    Emit(RS_k+i, *parity);
                    delete parity;
                    for (uint32_t j = 0; j < item_size; j++)
                        Parity[i][j] = 0;
                }
            }
            delete data; 
        }

        // wait for ACK again
        if (counter % batch_size == 0) {
            ACK ack = Recv(worker_id);
            ProcessACK(ack);
        }
    }
    else { // normal ack
        // LOG_MSG("Normal ACK !\n");
        if(enable_adaptive) {
            run_coded = ack.get_adaptive(); 
        }
    }

}
