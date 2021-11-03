#include<iostream>
#include<math.h>
#include<stdlib.h>
#include "data_type.hpp"

#define N_layer 2 + 1
#define S_layer 3
#define out_size 2
#define max_attribute_num 25

using namespace std;

// variables for store intermediate results during computation
double weight[N_layer][S_layer][max_attribute_num] = {0};
double bias[N_layer][S_layer] = {0};
double res[N_layer][S_layer][max_attribute_num] = {0};
double res_bias[N_layer][S_layer] = {0};

// the following array used to keep the linear resutls
double attached[S_layer] = {0};

double forward_value[N_layer][S_layer] = {0};
double backward_value[N_layer][S_layer] = {0};

double input_data[max_attribute_num] = {0};
int attribute_num = 1;
int target  = 0;
int lr = 0.001;

void init() {
    for (int i = 0; i < N_layer; i++)
        for (int j = 0 ; j < S_layer; j++){
            for (int k = 0; k < max_attribute_num; k++)                 
                weight[i][j][k] = 1 / 1 + (i + j + k);
            bias[i][j] = 1.0 / ( i + j);
        }
}

double sigmoid(double net) {
    return 1.0 / (1.0 + exp(-net));
}

void forward_linear(Message *linear) {
    for (int i = 0; i < S_layer; i ++){
        double net = 0;
        for (int k = 0; k < attribute_num; k ++) {
            net += weight[0][i][k] * input_data[k];
        }
        linear->push_value(net);
    }
}

void forward_nonlinear(Message *linear) {
    // first hidden layer
    for (int i = 0; i < S_layer; i ++){
        forward_value[0][i] = sigmoid(linear->pop_value(i) + bias[0][i]); 
    }
    // remaining hidden layers
    for (int i = 1; i < N_layer - 1; i++) {
        for (int j = 0 ; j < S_layer; j++){
            double net = 0;
            for (int k = 0; k < S_layer; k ++)
                net += forward_value[i-1][k] * weight[i-1][j][k];
            forward_value[i][j] = sigmoid(net + bias[i][j]);
        } 
    }
    // the output layer
    for (int j = 0 ; j < out_size; j++){
        double net = 0;
        for (int k = 0; k < S_layer; k ++)
            net += forward_value[N_layer - 2][k] * weight[N_layer-1][j][k];
        forward_value[N_layer-1][j] = sigmoid(net + bias[N_layer-1][j]);
    } 
}

void forward_propagate(bool coded) {
    // first hidden layer
    for (int i = 0; i < S_layer; i ++){
            double net = 0;
            for (int k = 0; k < attribute_num; k ++) {
                net += weight[0][i][k] * input_data[k];
            }
            if (coded)
               attached[i] = net; 
            forward_value[0][i] = sigmoid(net + bias[0][i]); 
    }
    // remaining hidden layers
    for (int i = 1; i < N_layer - 1; i++) {
        for (int j = 0 ; j < S_layer; j++){
            double net = 0;
            for (int k = 0; k < S_layer; k ++)
                net += forward_value[i-1][k] * weight[i-1][j][k];
            forward_value[i][j] = sigmoid(net + bias[i][j]);
        } 
    }
    // the output layer
    for (int j = 0 ; j < out_size; j++){
        double net = 0;
        for (int k = 0; k < S_layer; k ++)
            net += forward_value[N_layer - 2][k] * weight[N_layer-1][j][k];
        forward_value[N_layer-1][j] = sigmoid(net + bias[N_layer-1][j]);
    } 
}

double output_delta(double output, double target) {
    return output * (1 - output) * ( target - output);
}

void backward_propagate(double target) {
    // the output layer
    for (int j = 0 ; j < out_size; j++){
            backward_value[N_layer-1][j] = output_delta(forward_value[N_layer - 1][j], target);
    }
    // last hidden layer
    for(int i = 0; i < S_layer; i ++) {
        double back_net = 0;
        for(int k = 0 ; k < out_size; k ++) {
            back_net += backward_value[N_layer-1][k] * weight[N_layer-1][k][i];
        }
        backward_value[N_layer - 2][i] = forward_value[N_layer -2][i] * (1 - forward_value[N_layer - 2][i]) * back_net;   
    }
    // remaining hidden layers
    for (int j = N_layer - 3; j >= 0; j--) {
        for(int i = 0; i < S_layer; i ++) {
            double back_net = 0;
            for(int k = 0 ; k < S_layer; k ++) {
                back_net += backward_value[j+1][k] * weight[j+1][k][i];
            }
            backward_value[j][i] = forward_value[j][i] * (1 - forward_value[j][i]) * back_net;   
        }
    }
}

void weight_delta_accumulate() { //the gradient
    // first hidden layers, number of parameters per neural same as sample size
    for (int i = 0; i < S_layer; i ++) {
        for (int k = 0 ; k < attribute_num; k ++)
            res[0][i][k] += lr * forward_value[0][i] * backward_value[0][i];
        res_bias[0][i] += lr * backward_value[0][i];
    }
    // remaining hidden layers, number of parameters per neural is S_layer
    for (int j = 1; j < N_layer - 1; j ++) {
        for (int i = 0; i < S_layer; i ++) {
            for(int k = 0; k < S_layer; k++) {
                res[j][i][k] += lr * forward_value[j][i] * backward_value[j][i];
            }
            res_bias[j][i] += lr * backward_value[j][i];
        }   
    }
    // output layers, number of neural same as number of class, e.g., 2
    for (int i = 0; i < out_size; i ++) {
        for (int k = 0 ; k < S_layer; k ++)
            res[N_layer-1][i][k] += lr * forward_value[N_layer-1][i] * backward_value[N_layer-1][i];
        res_bias[N_layer-1][i] += lr * backward_value[N_layer-1][i];
    }
}

void weight_delta() { //the gradient
    // first hidden layers, number of parameters per neural same as sample size
    for (int i = 0; i < S_layer; i ++) {
        for (int k = 0 ; k < attribute_num; k ++)
            res[0][i][k] = lr * forward_value[0][i] * backward_value[0][i];
        res_bias[0][i] = lr * backward_value[0][i];
    }
    // remaining hidden layers, number of parameters per neural is S_layer
    for (int j = 1; j < N_layer - 1; j ++) {
        for (int i = 0; i < S_layer; i ++) {
            for(int k = 0; k < S_layer; k++) {
                res[j][i][k] = lr * forward_value[j][i] * backward_value[j][i];
            }
            res_bias[j][i] = lr * backward_value[j][i];
        }   
    }
    // output layers, number of neural same as number of class, e.g., 2
    for (int i = 0; i < out_size; i ++) {
        for (int k = 0 ; k < S_layer; k ++)
            res[N_layer-1][i][k] = lr * forward_value[N_layer-1][i] * backward_value[N_layer-1][i];
        res_bias[N_layer-1][i] = lr * backward_value[N_layer-1][i];
    }
}

void model_update(int mini_batch) { // gradient sum in res[][][]
    // first hidden layers, number of parameters per neural same as sample size
    for (int i = 0; i < S_layer; i ++) {
        for (int k = 0 ; k < attribute_num; k ++)
            weight[0][i][k] +=  res[0][i][k] / mini_batch;
        bias[0][i] += res_bias[0][i] / mini_batch;
    }
    // remaining hidden layers, number of parameters per neural is S_layer
    for (int j = 1; j < N_layer - 1; j ++) {
        for (int i = 0; i < S_layer; i ++) {
            for(int k = 0; k < S_layer; k++) {
                weight[j][i][k] += res[j][i][k] / mini_batch;
            }
            bias[0][i] += res_bias[0][i] / mini_batch;
        }   
    }
    // output layers, number of neural same as number of class, e.g., 2
    for (int i = 0; i < out_size; i ++) {
        for (int k = 0 ; k < S_layer; k ++)
            weight[N_layer-1][i][k] = res[N_layer-1][i][k] / mini_batch;
        bias[N_layer-1][i] += res_bias[N_layer-1][i] / mini_batch;
    }
}

void load_model(Message* tosend) {
    // frist hidden layer
    for (int i = 0 ; i < S_layer; i ++) 
        tosend->push_array(weight[0][i], attribute_num);
    tosend->push_array(bias[0], S_layer);
    
    // remaining hidden layer
    for (int i = 0 ; i < N_layer - 2; i ++)
        for (int j = 0 ; j < S_layer; j++)
            tosend->push_array(weight[j][i], S_layer);
        tosend->push_array(bias[0], S_layer);
    // output layer
    for (int i = 0; i < out_size; i ++)
        tosend->push_array(weight[N_layer - 1][i], S_layer);
    tosend->push_array(bias[N_layer-1], out_size);
}

void load_gradient(Message* tosend) {
    // frist hidden layer
    for (int i = 0 ; i < S_layer; i ++) 
        tosend->push_array(res[0][i], attribute_num);
    tosend->push_array(res_bias[0], S_layer);
    
    // remaining hidden layer
    for (int i = 0 ; i < N_layer - 2; i ++)
        for (int j = 0 ; j < S_layer; j++)
            tosend->push_array(res[j][i], S_layer);
        tosend->push_array(res_bias[0], S_layer);
    // output layer
    for (int i = 0; i < out_size; i ++)
        tosend->push_array(res[N_layer - 1][i], S_layer);
    tosend->push_array(res_bias[N_layer-1], out_size);
}

void accumulate_gradient(struct Message &update, int base) {
    // frist hidden layer
    for (int i = 0 ; i < S_layer; i ++)
        for (int j = 0; j < attribute_num; j++) {
            res[0][i][j] += update.pop_value(base);
            base ++;
        }
    for (int i = 0; i < S_layer; i++) {
        res_bias[0][i] += update.pop_value(base);
        base ++;
    }
    // remaining hidden layer
    for (int i = 0 ; i < N_layer - 2; i ++) {
        for (int j = 0 ; j < S_layer; j++) {
            for (int k = 0; k < S_layer; k ++) {
                res[i][j][k] += update.pop_value(base);
                base ++;
            }
        }
        for (int t = 0; t < S_layer; t++) {
            res_bias[i][t] = update.pop_value(base);
            base ++;
        }
    }
    // output layer
    for (int i = 0; i < out_size; i ++) {
        for (int k  = 0; k < S_layer; k ++) { 
            res[N_layer -1][i][k] = update.pop_value(base);
            base ++;
        }
        for (int k = 0; k < out_size; k++) {
            res_bias[N_layer -1][k] = update.pop_value(base);
            base++;
        }
    }
}

void store_model(struct Message &update, int base) {
    // frist hidden layer
    for (int i = 0 ; i < S_layer; i ++)
        for (int j = 0; j < attribute_num; j++) {
            weight[0][i][j] += update.pop_value(base);
            base ++;
        }
    for (int i = 0; i < S_layer; i++) {
        bias[0][i] += update.pop_value(base);
        base ++;
    }
    // remaining hidden layer
    for (int i = 0 ; i < N_layer - 2; i ++) {
        for (int j = 0 ; j < S_layer; j++) {
            for (int k = 0; k < S_layer; k ++) {
                weight[i][j][k] += update.pop_value(base);
                base ++;
            }
        }
        for (int t = 0; t < S_layer; t++) {
            bias[i][t] = update.pop_value(base);
            base ++;
        }
    }
    // output layer
    for (int i = 0; i < out_size; i ++) {
        for (int k  = 0; k < S_layer; k ++) { 
            weight[N_layer -1][i][k] = update.pop_value(base);
            base ++;
        }
        for (int k = 0; k < out_size; k++) {
            bias[N_layer -1][k] = update.pop_value(base);
            base++;
        }
    }
}

void clear_accumulate_weight() {
    for (int i = 0; i < N_layer; i ++)
        for (int j = 0; j < S_layer; j++)
            memset(res[i][j], 0, sizeof(double) * max_attribute_num);
    for (int i = 0 ; i < N_layer; i++)
        memset(res_bias[i], 0, sizeof(double) * S_layer);
}

