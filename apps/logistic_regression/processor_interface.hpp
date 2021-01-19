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

    static double overflow = 20.0;
    if (prediction < -overflow) prediction = -overflow;
    if (prediction > overflow) prediction = overflow;
    prediction = 1.0 / (1.0 + exp(-prediction));

    double label = data.pop_value(0);
    for (int i = 1; i < attr_num; i++) {
        R.push_value((label - prediction) * data.pop_value(i));
    }

    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // handle feedback
    for (int i = 1; i < attr_num; i++) {
        model[i] = fed.pop_value(i-1);
    }
}

