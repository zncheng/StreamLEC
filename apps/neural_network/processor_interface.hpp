#include <streamlec.h>
#include "neural_util.hpp"

/**
* extensible interfaces for processor
* Users should implement applications in corresponding interfaces
**/

Result ProcessorThread::ProcessLinear(Item &data) {
    Result Rl;
    double target = data.pop_value(0);
    Rl.push_value(target);
    for (int i = 1; i < attr_num; i ++) {
        input_data[i] = data.pop_value(i);
    }
    forward_linear(&Rl);
    return Rl;
}


Result ProcessorThread::ProcessNonlinear(Result &linear, Item &data){
    Result Rn;
    double target = linear.pop_value(0);
    forward_nonlinear(&linear);
    backward_propagate(target);
    weight_delta();
    load_gradient(&Rn);
    // non-linear operation
    return Rn;
}

Result ProcessorThread::ProcessData(Item &data) {
    Result R;
    double target = data.pop_value(0);
    for (int i = 1; i < attr_num; i ++) {
        input_data[i] = data.pop_value(i);
    }
    
    forward_propagate(false);
    backward_propagate(target);
    weight_delta();
    load_gradient(&R);
    
    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // handle feedback, update local model
    store_model(fed, 0);
}

