"""A file concentrating all orderings."""
from __future__ import print_function
import inspect
import sys


LIST_BOUNDS = [(a, b, c) for a in range(1, 3) for b in [False, True] for c in [False, True]]
LIST = [(a, b) for a in range(1, 3) for b in [False, True]]


def events_sorted(a, key, reference=False):
    return sorted(([(b,) + u for b in [True, False] for u in a] if reference else a), key=key)


def print_order_bounds(iter_):
    def map_(k):
        t, c, s = k
        if s:
            if c:
                return '[' + str(t)
            else:
                return '(' + str(t)
        else:
            if c:
                return str(t) + ']'
            else:
                return str(t) + ')'

    return ' < '.join([map_(a) for a in iter_])


def key_order_bounds(key):
    return print_order_bounds(events_sorted(LIST_BOUNDS, key=key))


def key_order(key):
    return print_order(events_sorted(LIST, key=key))


def print_order(iter_):
    def map_(ev):
        t, s = ev
        if s:
            return '[' + str(t)
        else:
            return str(t) + ']'
    return ' < '.join([map_(a) for a in iter_])


def ref_map_(a):
    return ('a' if a else 'b')


def print_order_reference_bounds(iter_):
    def map_(ev):
        r, t, c, s = ev
        if s:
            if c:
                return '[' + str(t) + ref_map_(r)
            else:
                return '(' + str(t) + ref_map_(r)
        else:
            if c:
                return str(t) + ']' + ref_map_(r)
            else:
                return str(t) + ')' + ref_map_(r)

    return ' < '.join([map_(a) for a in iter_])


def key_order_reference_bounds(key):
    return print_order_reference_bounds(events_sorted(LIST_BOUNDS, key=key, reference=True))


def print_order_reference(iter_):
    def map_(ev):
        r, t, s = ev
        if s:
            return '[' + str(t) + ref_map_(r)
        else:
            return str(t) + ']' + ref_map_(r)
    return ' < '.join([map_(a) for a in iter_])


def key_order_reference(key):
    return print_order_reference(events_sorted(LIST, key=key, reference=True))


# NI - NR
def order_0_n1(k):
    return (k[0], not k[1])


def order_0_1(k):
    return (k[0], k[1])


# Bounds
def b_order_0_2(k):
    return (k[0], k[2])


def b_order_0_n2(k):
    return (k[0], not k[2])


def b_order_0_1n2_n2(k):
    # [ ) ( ]
    return (k[0], k[1] != k[2], not k[2])


def b_order_0_n2_1n2(k):
    # Extract information and possible keys
    # with key = (t, not s, s!=c)
    # We have the following order: '[3' < '(3' < '3)' < '3]'
    # If c is True for '[' and 's' is True for start
    return (k[0], not k[2], k[1] != k[2])


def b_order_0_1n2_2(k):
    return (k[0], k[1] != k[2], k[2])


# Reference
def r_order_1_n2(k):
    return (k[1], not k[2])


def r_order_1_n2_0n2(k):
    # t, start, start!=reference
    # [a < [b < b] < a]
    return (k[1], not k[2], k[0] != k[2])


def r_order_1_0n2_n2(k):
    # [a < ]b < [b < a]
    # ref | start |  A  |  B  |
    # ---------------------------
    #  T  |   T   |  F  |  F  |
    #  F  |   F   |  F  |  T  |
    #  F  |   T   |  T  |  F  |
    #  T  |   F   |  T  |  T  |
    # A = ref != start
    # B = not start
    return (k[1], k[0] != k[2], not k[2])


def r_order_1_n2_02(k):
    # [1b < [1a < 1]a < 1]b < [2b < ..
    return (k[1], not k[2], k[0] == k[2])


def r_order_1_n0_n2(k):
    return (k[1], not k[0], not k[2])


# Reference Bounds
def rb_order_1_2n3_2(k):
    return (k[1], k[2] != k[3], k[2])


def rb_order_1_n3_2n3_0n3(k):
    return (k[1], not k[3], k[2] != k[3], k[0] != k[3])


def rb_order_1_n3_2n3(k):
    # with key = (t, not s, s!=c)
    # We have the following order: '[3' < '(3' < '3)' < '3]'
    # If c is True for '[' and 's' is True for start
    return (k[1], not k[3], k[2] != k[3])


def rb_order_1_n3_0n3_2n3(k):
    # if ref=True for A and start= True when start
    # [a < (a < [b < (b < b) < b] < a) < a]
    return (k[1], not k[3], k[0] != k[3], k[2] != k[3])


def rb_order_1_n2_2e3_0e2(k):
    # with key = (t, not s, s==c)
    # We have the following order: [a < (a < [b < (b < b) < b] < a) < a]
    # If c is True for '[' and 's' is True for start
    return (k[1], not k[2], k[2] == k[3], k[0] == k[2])


# def rb_order_nonempty_intersection(k):
    # [a < ]b < (a < (b < b) < a) < [b < a]
    # closed | ref | start |  A  |  B  |  C  |
    # ----------------------------------------
    #    T   |  T  |   T   |  F  |  F  |  F  |
    #    T   |  F  |   F   |  F  |  F  |  T  |
    #    F   |  T  |   T   |  F  |  T  |  F  |
    #    F   |  F  |   T   |  F  |  T  |  T  |
    #    F   |  F  |   F   |  T  |  F  |  F  |
    #    F   |  T  |   F   |  T  |  F  |  T  |
    #    T   |  F  |   T   |  T  |  T  |  F  |
    #    T   |  T  |   F   |  T  |  T  |  T  |
    # Map for now.
    # [a < ]b < (a < (b < b) < a) < [b < a]
#    r, t, i, s = k[:4]
#    a = (s == (i and (not r)))
#    b = (i == a)
#    c = (r == a)
#    return (t, a, b, c)
    # crs_order_map = {t: i for t, i in enumerate(
    #    [(True, True, True), (True, False, False),
    #     (False, True, True), (False, False, True),
    #     (False, False, False), (False, True, False),
    #     (True, False, True), (True, True, False)])}
    # return (k[1], crs_order_map[(k[2], k[0], k[3])])

def rb_order_nonempty_intersection(k):
    # [a < ]b < (a < (b < b) < a) < [b < a]
    # closed | ref | start |  A  |  B  |  C  |
    # ----------------------------------------
    #    T   |  T  |   T   |  F  |  F  |  F  |
    #    T   |  F  |   F   |  F  |  F  |  T  |
    #    F   |  T  |   T   |  F  |  T  |  F  |
    #    F   |  F  |   T   |  F  |  T  |  T  |
    #    F   |  F  |   F   |  T  |  F  |  F  |
    #    F   |  T  |   F   |  T  |  F  |  T  |
    #    T   |  F  |   T   |  T  |  T  |  F  |
    #    T   |  T  |   F   |  T  |  T  |  T  |
    # Map for now.
    # 1)a < 1)b < [1a < 1]b < [1b < 1]a < (1a < (1b < 2)a < 2)b < [2a < 2]b < [2b < 2]a < (2a < (2b
    crs_order_map = {i: t for t, i in enumerate(
        [(True, False, False), (False, False, False),
         (True, True, True), (False, False, True),
         (False, True, True), (True, False, True),
         (True, True, False), (False, True, False)])}
    # [1b < 1)b < 1)a < 1]a < [1a < (1b < (1a < 1]b
    # crs_order_map = {i: t for t, i in enumerate(
    #     [(False, True, True), (False, False, False),
    #      (True, False, False), (True, False, True),
    #      (True, True, True), (False, True, False),
    #      (True, True, False), (False, False, True)])}
    # print(crs_order_map)
    return (k[1], crs_order_map[(k[0], k[3], k[2])])
#    r, t, i, s = k[:4]
#    a = (s == (i and (not r)))
#    b = (i == a)
#    c = (r == a)
#    return (t, a, b, c)


def rb_order_1_3_0n3(k):
    # }a < }b < a{ < b{
    return (k[1], k[3], k[0] != k[3])


def rb_order_1_n2_2e3_0e3(k):
    return (k[1], not k[2], k[2] == k[3], k[0] == k[3])


def rb_order_1_3_2n3(k):
    # with key = (t, not s, s==c)
    # We have the following order: '[3' < '(3' < '3)' < '3]'
    # If c is True for '[' and 's' is True for start
    return (k[1], k[3], k[2] != k[3])


def rb_order_1_n2_2e3(k):
    return (k[1], not k[2], k[2] == k[3])


def rb_order_1_3_2n3_0n3(k):
    return (k[1], k[3], k[2] != k[3], k[0] != k[3])


def r_order_1_2_0(k):
    return (k[1], k[2], k[0] == k[2])


if __name__ == '__main__':
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    kob, kor, korb, ko = [], [], [], []
    for key, value in all_functions:
        if len(key) > 5 and key[:5] == 'order':
            ko.append((key, value))
        elif len(key) > 7:
            if key[:7] == 'r_order':
                kor.append((key, value))
            elif key[:7] == 'b_order':
                kob.append((key, value))
            elif len(key) > 8 and key[:8] == 'rb_order':
                korb.append((key, value))

    print("Ordering Functions")
    print("------------------\n")
    print("NI-NR:")
    for key, value in ko:
        print(key, key_order(value), sep=': ', end='\n')
    print("\nBounds:")
    for key, value in kob:
        print(key, key_order_bounds(value), sep=': ', end='\n')
    print("\nReference:")
    for key, value in kor:
        print(key, key_order_reference(value), sep=': ', end='\n')
    print("\nReference Bounds:")
    for key, value in korb:
        print(key, key_order_reference_bounds(value), sep=': ', end='\n')
