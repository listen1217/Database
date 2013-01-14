#ifndef MYPRQ_H
#define MYPRQ_H

#include "File.h"
#include "Record.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class MyPQ{

	Record ** pRecords;
	int * pIndex;
	int qSize;
	int currentSize;
	ComparisonEngine ce;
	OrderMaker * ordermaker;
	
public:
	MyPQ();
	
	MyPQ(int size, Record ** pRecord, OrderMaker * ordermaker);
	
	~MyPQ();
	
	//push
	void push(int index);
	
	//POP
	int pop();
	
	
};

#endif