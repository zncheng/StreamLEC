#include <streamlec.h>

/**
* extensible interfaces for processor
* Users should implement applications in corresponding interfaces
**/

Result ProcessorThread::ProcessLinear(Item &data) {
    Result Rl;
    // linear operations
    return Rl;
}


Result ProcessorThread::ProcessNonlinear(Result &linear, Item &data){
    Result Rn;
    // non-linear operation
    return Rn;
}

Result ProcessorThread::ProcessData(Item &data) {
    Result R;
    double prediction = 0;
    for (int i = 1; i < attr_num; i ++) {
        prediction += model[i] * data.pop_value(i);
    }

    prediction += bias; 

    double label = data.pop_value(0);
    
    // update and push gradient results
    for (int i = 1; i < attr_num; i++) {
        R.push_value((label - prediction) * data.pop_value(i));
    }

    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // handle feedbackS
    for (int i = 1; i < attr_num; i++) {
        model[i] = fed.pop_value(i-1);
    }
    bias = fed.pop_value(attr_num);
}

