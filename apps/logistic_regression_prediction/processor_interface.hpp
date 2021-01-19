#include <streamlec.h>

/**
* extensible interfaces for processor
* Users should implement applications in corresponding interfaces
**/

Result ProcessorThread::ProcessLinear(Item &data) {
    Result Rl;
    double prediction = 0;
    for (int i = 1; i < attr_num; i ++) {
        prediction += model[i] * data.pop_value(i);
    }
    Rl.push_value(prediction);
    // linear operations
    return Rl;
}

Result ProcessorThread::ProcessNonlinear(Result &linear, Item &data){
    Result Rn;
    double prediction = linear.pop_value(0);
    static double overflow = 20.0;
    if (prediction < -overflow) prediction = -overflow;
    if (prediction > overflow) prediction = overflow;
    prediction = 1.0 / (1.0 + exp(-prediction));

    double label = data.pop_value(0);
    for (int i = 1; i < attr_num; i++) {
        Rn.push_value((label - prediction) * data.pop_value(i));
    }
    // non-linear operation
    return Rn;
}

Result ProcessorThread::ProcessData(Item &data) {
    Result R;
    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // blank feedback in prediction
}

