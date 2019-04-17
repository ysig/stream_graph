cimport cython
from libcpp.vector cimport vector
from libcpp.pair cimport pair

cdef extern from "include/functions.hpp":
    double closeness(vector[pair[int, pair[int, int]]] input, int x);

