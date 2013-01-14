#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <deque>
#include <stack>
#include "Defs.h"
#include "MyPQ.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "File.h"
#include "Pipe.h"
#include "Comparison.h"
#include <time.h>
#include<stdlib.h>

//parameters for inter-thread communicate
typedef struct {
  Pipe * pIn;
  Pipe * pOut;
  OrderMaker * pSortOrder;
  int runlen;
  char fname[32];
}Param_bq;

class BigQ {
	//vector<int> lengthEachRun;
	Param_bq * p;
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
