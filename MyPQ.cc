#include "MyPQ.h"
#include "Record.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

MyPQ :: MyPQ()
{
}

MyPQ :: MyPQ(int size, Record ** pRecord,OrderMaker * ordermaker)
{	
	this->qSize = size;
	this->pRecords = new Record * [size];
	this->pRecords = pRecord;
	this->currentSize = 0;
	this->pIndex = new int[size+1];
	this->ordermaker = ordermaker;
}

MyPQ :: ~MyPQ(){
	//for(int   i   =   0;   i   <   size;   i++) 
      //  delete   []pRecords[i];
	//delete [] pRecords;
	delete [] pIndex;
}

void MyPQ :: push(int index){
	pIndex[++currentSize] = index;
	int parent = currentSize;
	int temp;
	while( parent/2 >0 ){
		if(ce.Compare(pRecords[pIndex[parent]], pRecords[pIndex[parent/2]], this->ordermaker) < 0 ){
			temp=pIndex[parent];
			pIndex[parent]=pIndex[parent/2];
			pIndex[parent/2]=temp;
		}
		parent /=2;
	}
}

int MyPQ :: pop(){
	if(currentSize == 0){
		return -1;
	}
	int returnIndex = pIndex[1];
	pIndex[1] = pIndex[currentSize--];

	int child = 1;
	int temp,min;
	while( child*2 <= currentSize ){
		if(child*2+1 <= currentSize){
			min = (ce.Compare(pRecords[pIndex[child*2]], pRecords[pIndex[child*2+1]], this->ordermaker) < 0) ? child*2:(child*2+1);
		}else{
			min = child*2;
		}
		
		if(ce.Compare(pRecords[pIndex[min]], pRecords[pIndex[child]], this->ordermaker) < 0 ){
			temp=pIndex[min];
			pIndex[min]=pIndex[child];
			pIndex[child]=temp;
		}else{
			break;
		}
		child = min;
	}
	return returnIndex;
}
