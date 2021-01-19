#include <streamlec.h>

/**
* extensible interfaces for processor
* Users should implement applications in corresponding interfaces
**/

Result ProcessorThread::ProcessLinear(Item &data) {
    Result Rl;
    // linear operation
    Rl.push_value(1.0);
    return Rl;
}


Result ProcessorThread::ProcessNonlinear(Result &linear, Item &data){
    Result Rn;
    Rn.push_value(1.0);
    // non-linear operation
    return Rn;
}

Result ProcessorThread::ProcessData(Item &data) {
    Result R;
    // operations;
    R.push_value(1.0);
    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // handle feedback
}

