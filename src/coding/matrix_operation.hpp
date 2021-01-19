#ifndef _AFS_MATRIX_INVERSE_H
#define _AFS_MATRIX_INVERSE_H

#include <vector>
#include <math.h>
#include <iostream>
//#include "parameters.hpp"

#define MAX_K_VALUE 20



/*
find the reverse matrix using the the row change method. that is, for A, do the row change to generate E, and for E, do the same thing so as to generate A^{-1}, origin arrary keeps the raw matrix, and the result will save to the array revers. 
*/

std::vector<double> origin[MAX_K_VALUE];
std::vector<double> revers[MAX_K_VALUE];

inline void matrix_debug(int len) {
    
        for(int i = 0; i < len; i++) {
            for(int j = 0; j < len; j++) {
                std::cout<<revers[i][j]<<" ";
            }
            std::cout<<std::endl;
        }
        cout << "debug output done!" << endl;
}

inline void init_origin(int len) {
    for (int i = 0; i < len; i++){
            origin[i] = std::vector<double>(len, 0);
            origin[i][i] = 1;
    }
}

inline void update_origin(int row, int base, int len) {
    origin[row].clear();
    double sum = 1;
    for (int i = 0; i < len; i++) {
        origin[row].push_back(sum);
        sum *= base;
    }
}

inline void recover_origin(int row, int len) {
    origin[row].clear();
    origin[row] = std::vector<double>(len, 0);
    origin[row][row] = 1;
}

inline std::vector<double> operator* (std::vector<double> a, double b) {
    int len = a.size();
    std::vector<double> result(len, 0);
    for (int i = 0; i < len; i++) {
        result[i] = a[i] * b;
    }
    return result;
}

inline std::vector<double> operator- (std::vector<double> a, std::vector<double> b) {
    int len = a.size();
    std::vector<double> result(len, 0);
    for (int i = 0; i < len; i++) {
        result[i] = a[i] - b[i];
    }
    return result;
}

inline void inverse(std::vector<double> A[], std::vector<double> B[], int len) {

    for (int i = 0; i < len; i++) {
        B[i] = std::vector<double>(len, 0);
        B[i][i] = 1;
    }
    for (int i = 0; i < len; i++) {
        for (int j = i; j < len; j++) {
            if (fabs(A[j][i]) > 0) {
                swap(A[i], A[j]);
                swap(B[i], B[j]);
                break;
            }
        }
        B[i] = B[i] * (1 / A[i][i]);
        A[i] = A[i] * (1 / A[i][i]);
        for (int j = 0; j < len; j++) {
            if( j!=i && fabs(A[j][i]) > 0) {
                B[j] = B[j] - B[i] * A[j][i];
                A[j] = A[j] - A[i] * A[j][i];
            }
        }
    }
    //matrix_debug(len);
}

#endif
