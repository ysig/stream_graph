/* Arash Partov Function
 * Author: Ioannis Siglidis <y.siglidis@gmail.com>
 * License: BSD 3 clause"
 * Code taken from: http://www.partow.net/programming/hashfunctions/#APHashFunction
 */
#include "../include/functions.hpp"

/*
 * Initial Author: Marwan-Ghanem
 * Modifications by: Ioannis Siglidis
 */

#include <iostream>
#include <sstream>
#include <fstream>
#include <set>
#include <map>
#include <vector>
#include <string>	
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <queue>


using namespace std;

void print(vector<int> const &input)
{
	for (int i = 0; i < input.size(); i++) {
		cout << input.at(i) << ' ';
	}
	cout << "\n";
}

int binary_search(vector<int> list,int value){
    cout << "binary-search\nlist";
	int min,m = 0;
	int max =  list.size() - 1;
    print(list);
    cout << "m" << m << "min" << min <<"\n";
	while (true){
	    cout << "hierarchy\n";
		if(max < min){
		    cout << "1.value:";
		    cout << value << ":-1\n";
			return -1;
	    }
	    cout << "aaa\n";
		m = (min+max) / 2;
		if(list[m] < value){
    		cout << "bbb\n";
			min = m + 1;
		}
		else{
		    if(list[m] > value){
	    		cout << "ccc\n";
			    max = m - 1;
			}else{
		        cout << "2.value:";
		        cout << value << ":" << m << "\n";
			    return m;
			}
	    }
	}
}


pair<vector<int>, vector<double>> closeness_times_both(vector<pair<int, pair<int,int>>> input, int x){
	long cur_start_time = -1 ,prev_ts =-1 ,start_time = -1 ,t = -1;
	int u,v;

	map<int,vector<pair<long,long > > > reachable_from_at = map<int,std::vector<pair<long,long > > >();
	vector<double> cum_closeness;
	vector<int> time_closeness = vector<int>();
    auto it = input.begin();

	while (cur_start_time == -1 && it != input.end()){
        t = it->first;
        u = it->second.first;
        v = it->second.second;
		if(u == x || v == x){
			cur_start_time = t;
			vector<pair<long,long > > list = vector<pair <long,long> >();
			if (u == x){
				list.push_back(make_pair(t,t));
				reachable_from_at[v] = list;
			}else{
				list.push_back(make_pair(t,t));
				reachable_from_at[u] = list;
			}
		}
		if(time_closeness.empty() || t != time_closeness.back()){
			time_closeness.push_back(t);
			cum_closeness.push_back(0.0);
		}
		++it;
	}

	for(int i = 0 ; i < cum_closeness.size() ; i++){
		if(cur_start_time > time_closeness[i]){
            cum_closeness[i]+= 1.0 / (cur_start_time-time_closeness[i]);
		}
	}

	for(;it != input.end(); ++it){
        t = it->first;
        u = it->second.first;
        v = it->second.second;

		if(t != time_closeness.back()){
			time_closeness.push_back(t);
			cum_closeness.push_back(0.0);
		}
		if(u == x || v == x){
			cur_start_time = t;
			if (v == x){
				v = u;
				u = x;
			}	
			map<int,vector<pair<long,long > > >::iterator it;
			it = reachable_from_at.find(v);
			if(it != reachable_from_at.end()){
				vector<pair<long,long > > list = vector<pair <long,long> >();
				list = it->second;
				prev_ts = list.back().first;
				vector<pair<long,long > > tmp = vector<pair <long,long> >();
				for(int i = 0 ; i < list.size() ; i++){
					if(list[i].second < t){
						tmp.push_back(list[i]);
					}
				}
				tmp.push_back(make_pair(t,t));
				reachable_from_at[v] = tmp;
			}else{
				prev_ts = -1;
				vector<pair<long,long > > list = vector<pair <long,long> >();
				list.push_back(make_pair(t,t));
				reachable_from_at[v] = list;
			}
			/* update closeness */
			//cout << "time size : " << time_closeness.size() << endl;
			int i = time_closeness.size() - 2;
			start_time = time_closeness[i];
			if(time_closeness.size() > 3){
				while(start_time >= prev_ts){
				    //cout << "cum_closeness.at(" << i <<") before" << cum_closeness.at(i) << endl;
					cum_closeness.at(i)+= 1.0 / (t - start_time);
				    //cout << "cum_closeness.at(" << i <<") after" << cum_closeness.at(i) << endl;
					i = i - 1;
					if(i < 0){
						break;
					}
					start_time = time_closeness[i];
				}
			}
		 /* NEITHER NODES ARE X */
		 }else{
			//cout << "neither nodes " << endl;
			map<int,vector<pair<long,long > > >::iterator it,it2;
			it = reachable_from_at.find(u);
			it2 = reachable_from_at.find(v);
			if(it != reachable_from_at.end()){
				if(it2 != reachable_from_at.end()){
					vector<pair<long,long > > lu = vector<pair <long,long> >();
					vector<pair<long,long > > list = vector<pair <long,long> >();
					vector<pair<long,long > > list2 = vector<pair <long,long> >();

					list = it->second;
					for(int i = 0 ; i < list.size() ; i++){
						if(list[i].first > it2->second.back().first && list[i].second < t){
							lu.push_back(list[i]);
						}
					}

					if(lu.size() > 0){
						int start_time_index = binary_search(time_closeness,it2->second.back().first)+1;
						long start_time_value = time_closeness[start_time_index];
						
						while(start_time_value <= lu.back().first){
				            //cout << "1. cum_closeness[start_time_index=" << start_time_index << "] before" << cum_closeness[start_time_index] << endl;
							cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
				            //cout << "1. cum_closeness[start_time_index=" << start_time_index << "] after" << cum_closeness[start_time_index] << endl;
							start_time_index++;
							start_time_value = time_closeness[start_time_index];
						}

						vector<pair<long,long > > tmp = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].second  < t){
								tmp.push_back(list[i]);
							}
						}
						tmp.push_back(make_pair(lu.back().first,t));
						list = vector<pair<long,long >> (tmp.end() - min((int)tmp.size(),2),tmp.end());
						reachable_from_at[v] = list;
					
					}else{
						vector<pair<long,long > > lv = vector<pair <long,long> >();
						vector<pair<long,long > > list = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].first > it->second.back().first && list[i].second < t){
								lv.push_back(list[i]);
							}
						}
						if(lv.size() > 0){
							int start_time_index = binary_search(time_closeness,it->second.back().first)+1;
							long start_time_value = time_closeness[start_time_index];
							while(start_time_value <= lv.back().first){
	    			            //cout << "2. cum_closeness[start_time_index=" << start_time_index <<"] before" << cum_closeness[start_time_index] << endl;
								cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
	    			            //cout << "2. cum_closeness[start_time_index=" << start_time_index <<"] after" << cum_closeness[start_time_index] << endl;
								start_time_index++;
								start_time_value = time_closeness[start_time_index];
							}	
							vector<pair<long,long > > tmp = vector<pair <long,long> >();
							list = it->second;
							for(int i = 0 ; i < list.size() ; i++){
								if(list[i].second  < t){
									tmp.push_back(list[i]);
								}
							}
							tmp.push_back(make_pair(lv.back().first,t));
							list = vector<pair<long,long >> (tmp.end() - min((int)tmp.size(),2),tmp.end());
							reachable_from_at[u] = list;
						}else{
							continue;
						}
					}
				}else{
					vector<pair<long,long > > lu = vector<pair <long,long> >();
					vector<pair<long,long > > list = vector<pair <long,long> >();
					list = it->second;
					for(int i = 0 ; i < list.size() ; i++){
						if(list[i].second < t){
							lu.push_back(list[i]);
						}
					}
					if(lu.size() > 0){
						vector<pair<long,long > > tmp = vector<pair <long,long> >();
						tmp.push_back(make_pair(lu.back().first,t));
						reachable_from_at[v] = tmp;
						int start_time_index = 0;
						long start_time_value = time_closeness[start_time_index];
						while(start_time_value <= lu.back().first){
    			            //cout << "3. cum_closeness[start_time_index=" << start_time_index <<"] before" << cum_closeness[start_time_index] << endl;
    						cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
    			            //cout << "3. cum_closeness[start_time_index=" << start_time_index <<"] after" << cum_closeness[start_time_index] << endl;
							start_time_index++;
							start_time_value = time_closeness[start_time_index];
						}
					}
				}
			}else if (it2 != reachable_from_at.end()){
						vector<pair<long,long > > lv = vector<pair <long,long> >();
						vector<pair<long,long > > list = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].second < t){
								lv.push_back(list[i]);
							}
						}
						if(lv.size() > 0){
							vector<pair<long,long > > tmp = vector<pair <long,long> >();
							tmp.push_back(make_pair(lv.back().first,t));
							reachable_from_at[u] = tmp;
							int start_time_index = 0;
							long start_time_value = time_closeness[start_time_index];
							while(start_time_value <= lv.back().first){
        			            //cout << "4. cum_closeness[start_time_index=" << start_time_index <<"] before" << cum_closeness[start_time_index] << endl;
								cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
        			            //cout << "4. cum_closeness[start_time_index=" << start_time_index <<"] after" << cum_closeness[start_time_index] << endl;
								start_time_index++;
								start_time_value = time_closeness[start_time_index];
							}
						}
					}

				}
	}

    return make_pair(time_closeness, cum_closeness);
}


pair<vector<int>, vector<double>> closeness_times_out(vector<pair<int, pair<int,int>>> input, int x){
	long cur_start_time = -1 ,prev_ts =-1 ,start_time = -1 ,t = -1;
	int u,v;

	map<int,vector<pair<long,long > > > reachable_from_at = map<int,std::vector<pair<long,long > > >();
	vector<double> cum_closeness;
	vector<int> time_closeness = vector<int>();
    auto it = input.begin();

	while (cur_start_time == -1 && it != input.end()){
        t = it->first;
        u = it->second.first;
        v = it->second.second;
        
		if(u == x || v == x){
			cur_start_time = t;
			vector<pair<long,long > > list = vector<pair <long,long> >();
			if (u == x){
				list.push_back(make_pair(t,t));
				reachable_from_at[v] = list;
			}else{
				list.push_back(make_pair(t,t));
				reachable_from_at[u] = list;
			}
		}
		if(time_closeness.empty() || t != time_closeness.back()){
			time_closeness.push_back(t);
			cum_closeness.push_back(0.0);
		}
		++it;
	}

	for(int i = 0 ; i < cum_closeness.size() ; i++){
		if(cur_start_time > time_closeness[i]){
			cum_closeness[i]+= 1.0 / (cur_start_time-time_closeness[i]);
		}
	}

	for(;it != input.end(); ++it){
        t = it->first;
        u = it->second.first;
        v = it->second.second;
		if(t != time_closeness.back()){
			time_closeness.push_back(t);
			cum_closeness.push_back(0.0);
		}
		if(u == x || v == x){
			cur_start_time = t;
			if (v == x){
				v = u;
				u = x;
			}	
			map<int,vector<pair<long,long > > >::iterator it;
			it = reachable_from_at.find(v);
			if(it != reachable_from_at.end()){
				vector<pair<long,long > > list = vector<pair <long,long> >();
				list = it->second;
				prev_ts = list.back().first;
				vector<pair<long,long > > tmp = vector<pair <long,long> >();
				for(int i = 0 ; i < list.size() ; i++){
					if(list[i].second < t){
						tmp.push_back(list[i]);
					}
				}
				tmp.push_back(make_pair(t,t));
				reachable_from_at[v] = tmp;
			}else{
				prev_ts = -1;
				vector<pair<long,long > > list = vector<pair <long,long> >();
				list.push_back(make_pair(t,t));
				reachable_from_at[v] = list;
			}
			/*update closeness */
			// cout << "time size : " << time_closeness.size() << endl;
			int i = time_closeness.size() - 2;
			start_time = time_closeness[i];
			if(time_closeness.size() > 3){
				while(start_time >= prev_ts){
					cum_closeness.at(i)+= 1.0 / (t - start_time);
					i = i - 1;
					if(i < 0){
						break;
					}
					start_time = time_closeness[i];
				}
			}
		/*NEITHER NODES ARE X*/
		 }else{
			// cout << "neither nodes " << endl;
			map<int,vector<pair<long,long > > >::iterator it,it2;
			it = reachable_from_at.find(u);
			it2 = reachable_from_at.find(v);
			if(it != reachable_from_at.end()){
				if(it2 != reachable_from_at.end()){
					vector<pair<long,long > > lu = vector<pair <long,long> >();
					vector<pair<long,long > > list = vector<pair <long,long> >();
					vector<pair<long,long > > list2 = vector<pair <long,long> >();

					list = it->second;
					for(int i = 0 ; i < list.size() ; i++){
						if(list[i].first > it2->second.back().first && list[i].second < t){
							lu.push_back(list[i]);
						}
					}

					if(lu.size() > 0){
						int start_time_index = binary_search(time_closeness,it2->second.back().first)+1;
						long start_time_value = time_closeness[start_time_index];
						
						while(start_time_value <= lu.back().first){
							cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
							start_time_index++;
							start_time_value = time_closeness[start_time_index];
						}

						vector<pair<long,long > > tmp = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].second  < t){
								tmp.push_back(list[i]);
							}
						}
						tmp.push_back(make_pair(lu.back().first,t));
						list = vector<pair<long,long >> (tmp.end() - min((int)tmp.size(),2),tmp.end());
						reachable_from_at[v] = list;
					
					}else{
						vector<pair<long,long > > lv = vector<pair <long,long> >();
						vector<pair<long,long > > list = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].first > it->second.back().first && list[i].second < t){
								lv.push_back(list[i]);
							}
						}
						if(lv.size() > 0){
							int start_time_index = binary_search(time_closeness,it->second.back().first)+1;
							long start_time_value = time_closeness[start_time_index];
							while(start_time_value <= lv.back().first){
								cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
								start_time_index++;
								start_time_value = time_closeness[start_time_index];
							}	
							vector<pair<long,long > > tmp = vector<pair <long,long> >();
							list = it->second;
							for(int i = 0 ; i < list.size() ; i++){
								if(list[i].second  < t){
									tmp.push_back(list[i]);
								}
							}
							tmp.push_back(make_pair(lv.back().first,t));
							list = vector<pair<long,long >> (tmp.end() - min((int)tmp.size(),2),tmp.end());
							reachable_from_at[u] = list;
						}else{
							continue;
						}
					}
				}else{
					vector<pair<long,long > > lu = vector<pair <long,long> >();
					vector<pair<long,long > > list = vector<pair <long,long> >();
					list = it->second;
					for(int i = 0 ; i < list.size() ; i++){
						if(list[i].second < t){
							lu.push_back(list[i]);
						}
					}
					if(lu.size() > 0){
						vector<pair<long,long > > tmp = vector<pair <long,long> >();
						tmp.push_back(make_pair(lu.back().first,t));
						reachable_from_at[v] = tmp;
						int start_time_index = 0;
						long start_time_value = time_closeness[start_time_index];
						while(start_time_value <= lu.back().first){
							cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
							start_time_index++;
							start_time_value = time_closeness[start_time_index];
						}
					}
				}
			}else if (it2 != reachable_from_at.end()){
						vector<pair<long,long > > lv = vector<pair <long,long> >();
						vector<pair<long,long > > list = vector<pair <long,long> >();
						list = it2->second;
						for(int i = 0 ; i < list.size() ; i++){
							if(list[i].second < t){
								lv.push_back(list[i]);
							}
						}
						if(lv.size() > 0){
							vector<pair<long,long > > tmp = vector<pair <long,long> >();
							tmp.push_back(make_pair(lv.back().first,t));
							reachable_from_at[u] = tmp;
							int start_time_index = 0;
							long start_time_value = time_closeness[start_time_index];
							while(start_time_value <= lv.back().first){
								cum_closeness[start_time_index]+= 1.0/(t - start_time_value);
								start_time_index++;
								start_time_value = time_closeness[start_time_index];
							}
						}
					}

				}
	}

    return make_pair(time_closeness, cum_closeness);
}

pair<vector<int>, vector<double>> closeness_times(vector<pair<int, pair<int,int>>> input, int x, bool both){
    if(both){
        return closeness_times_both(input, x);
    }else{
        return closeness_times_out(input, x);
    }
}

double closeness_at(vector<pair<int, pair<int,int>>> input, int x, int t, bool both){
    pair<vector<int>, vector<double>> obj = closeness_times(input, x, both);
    vector<int> time_closeness = obj.first;
    vector<double> cum_closeness = obj.second;

    int idx = binary_search(time_closeness, t);
    return cum_closeness[idx];
}

vector<pair<int, double>> closeness(vector<pair<int, pair<int,int>>> input, int x, bool both){
    pair<vector<int>, vector<double>> obj = closeness_times(input, x, both);
    vector<int> time_closeness = obj.first;
    vector<double> cum_closeness = obj.second;
    vector<pair<int, double>> result = vector<pair<int, double>>();
    
  	for(int i = 0 ; i < cum_closeness.size() ; i++){
		result.push_back(make_pair(time_closeness[i], cum_closeness[i]));
	}
	return result;
}
