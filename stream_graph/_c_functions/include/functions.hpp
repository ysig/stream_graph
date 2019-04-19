#ifndef FUNCTIONS_H_
#define FUNCTIONS_H_

#include <vector>
using namespace std;

vector<pair<int, double>> closeness(vector<pair<int, pair<int,int>>> input, int x, bool both);
double closeness_at(vector<pair<int, pair<int,int>>> input, int x, int t, bool both);

#endif
