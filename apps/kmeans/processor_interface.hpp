#include <streamlec.h>

/**
* extensible interfaces for processor
* Users should implement applications in corresponding interfaces
**/

/*
Declare variables here
*/
# define N_centoids 2
double model[N_centoids][MAX_MODEL_SIZE] = {0};

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
    double prediction[N_centoids] = {0};
    int index = -1;
    double min = 10e9;
    for (int i = 0; i < attr_num; i ++) {
        for (int j = 0; j < N_centoids; j ++) {
            prediction[j] += model[j][i] * data.pop_value(i);
        }
    }
    for (int i = 0; i < attr_num; i++) {
        if (min > prediction[i]) {
            min = prediction[i];
            index = i;
        }
    }

    R.push_value(index);

    return R;
}

void ProcessorThread::ProcessFeedback(Feedback &fed) {
    // LOG_MSG("Feedback done!");
    // handle feedback
    for (int c = 0; c < N_centoids; c++){
        for (int i = 1; i < attr_num; i++) {
            model[c][i] = fed.pop_value(i);
        }
    }
}

