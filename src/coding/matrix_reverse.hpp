#ifndef _AFS_MATRIX_INVERSE_H
#define _AFS_MATRIX_INVERSE_H

#include <vector>
#include <math.h>
#include <iostream>
#define max_node_num 50
#define max_parallel 20

/*
   find the reverse matrix using the the row change method.
   for A, do the row change to generate E, and for E, do the same thing so as to generate A^{-1}
   the original array keeps the raw matrix, and the result will save to the array revers. 
 */

std::vector<double> origin[max_parallel][max_node_num];
std::vector<double> revers[max_parallel][max_node_num];

inline void matrix_debug(int index, int len) {

    for(int i = 0; i < len; i++) {
        for(int j = 0; j < len; j++) {
            std::cout<<revers[index][i][j]<<" ";
        }
        std::cout<<std::endl;
    }
}

// initialize the original matrix
inline void init_origin(int index, int len) {
    for (int i = 0; i < len; i++){
        origin[index][i] = std::vector<double>(len, 0);
        origin[index][i][i] = 1;
    }
}

// update the original matrxi
inline void update_origin(int index, int row, int base, int len) {
    origin[index][row].clear();
    double sum = 1;
    for (int i = 0; i < len; i++) {
        origin[index][row].push_back(sum);
        sum *= base;
    }
}

// recover the original matrix
inline void recover_origin(int index, int row, int len) {
    origin[index][row].clear();
    origin[index][row] = std::vector<double>(len, 0);
    origin[index][row][row] = 1;
}

// multiply two vectors
inline std::vector<double> operator* (std::vector<double> a, double b) {
    int len = a.size();
    std::vector<double> result(len, 0);
    for (int i = 0; i < len; i++) {
        result[i] = a[i] * b;
    }
    return result;
}

// vector a - vector b
inline std::vector<double> operator- (std::vector<double> a, std::vector<double> b) {
    int len = a.size();
    std::vector<double> result(len, 0);
    for (int i = 0; i < len; i++) {
        result[i] = a[i] - b[i];
    }
    return result;
}

// invrse the matrix A
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
}

#endif
