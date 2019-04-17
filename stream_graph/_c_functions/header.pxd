cimport cython
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool

cdef extern from "include/functions.hpp":
    double closeness(vector[pair[int, pair[int, int]]] input, int x, bool both);

