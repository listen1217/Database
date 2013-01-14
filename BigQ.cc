#include "BigQ.h"
#include "Defs.h"
#include "MyPQ.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "File.h"
#include "Pipe.h"
#include "Comparison.h"
#include <iostream>
#include <pthread.h>
#include <time.h>
#include<stdlib.h>
#include <sstream>
#include <cstring>
#include <vector>
#include <deque>
#include <stack>
using namespace std;

#define random(x) (rand()%x + 1)


int  partition(Param_bq *params,Record ** recordVector,int low,int high){
	//T v=a[low];
	Record * v;
	v = recordVector[low];
 	ComparisonEngine ce;
	while(low<high){
 
		while(low<high && ce.Compare(recordVector[high], v, params->pSortOrder) >= 0) high--;
		recordVector[low]=recordVector[high];
		//a[low]=a[high];
		while(low<high && ce.Compare(recordVector[low], v, params->pSortOrder) <= 0) low++;
		recordVector[high]=recordVector[low];
		//a[high]=a[low];
	}
	
	recordVector[low] = v;
 	//a[low]=v;
 	return low;
}

void  inMemoryQuickSort(Param_bq *params, Record ** recordVector,int p, int q){
	stack<int> st;
	do{
		while(p<q){
			int j=partition(params,recordVector,p,q); 
			
			if( (j-p)<(q-j) ){
				st.push(j+1);
				st.push(q);
				q=j-1;
         	}else{
				st.push(p);
				st.push(j-1);
           		p=j+1;
         	}
		}
		if(st.empty()) return;
		q=st.top();
		st.pop();
		p=st.top();
		st.pop();
	}while(1);
	return;
}


void writeSortedIntoFile(Record ** recordVector, int vector_size,File &allRuns, int &pageCounter){
	Page bufferPage;
	int pageNumber = 0;
	for(int i=0;i< vector_size;i++){
		if(!bufferPage.Append(recordVector[i])){
		//------bufferPage is full---------
		//--------write to File------------
			allRuns.AddPage(&bufferPage,pageCounter++);
			bufferPage.EmptyItOut();
			bufferPage.Append(recordVector[i]);
		}
	}
	if(vector_size > 0)
		allRuns.AddPage(&bufferPage,pageCounter++);
	return;
}

int ReadRecord(Param_bq *params,int runIndex, int &pageCounter, Record *recordBuffer, Page *pageBuffer, File &fromThisFile,vector<int> &lengthEachRun){
//runIndex [0,k-1]; pageCounter [0,];
 	
	while(!pageBuffer->GetFirst(recordBuffer)){
			if(!fromThisFile.GetLength()){
			//file contains no data
				return 0;
			}
			int totalPage = 0;
			if(runIndex>0)
				totalPage = lengthEachRun.at((runIndex-1)*2+1);
			
			if(pageCounter == lengthEachRun.at(runIndex*2)){
				//end of run
				return 0;
			}
			if( totalPage+pageCounter >= fromThisFile.GetLength()){
			
				return 0;
			}
			fromThisFile.GetPage(pageBuffer, totalPage+pageCounter);
			pageCounter++;
	}

	return 1;
}

int recordToPrint(File &fromThisFile,int &pageCounter,Page &pageBuffer){
//runIndex [0,k-1]; pageCounter [0,];
 
 	Record recordBuffer;
	if(!pageBuffer.GetFirst(&recordBuffer)){
			if( pageCounter+1 >= fromThisFile.GetLength()){
				
				return 0;
			}
			fromThisFile.GetPage(&pageBuffer, pageCounter++);
			pageBuffer.GetFirst(&recordBuffer);
	}
	
	return 1;
}

int  TPMMS_PhaseOne(Param_bq *params,vector<int>& lengthEachRun,vector<int>::size_type &ix){
	
	Record tempRecord;
	int rSize = 0, pageNumber = 0, pageCounter=0, runCounter = 0;
	File tempFile;
	
	tempFile.Open(0, params->fname);
	printf("Phase 1 - tempFile name: %s\t tempFile addr: %p\n", params->fname,&tempFile);
	//test
	
	Record ** recordVector = new Record * [100000*params->runlen];
	int vector_size = 0;

	//firstly, read one page records into a page buffer, then, sort those records
	Page onePageBuffer;
	
	Record  temp;
	int total_size = 0;
	//int cc = 0;
	Schema sc("catalog","supplier");
	while(params->pIn->Remove(&tempRecord)){
		if(!onePageBuffer.Append(&tempRecord)){
		//one page reached
			pageNumber++;

			//store one page records into vector
			while(onePageBuffer.GetFirst(&temp)){
			  
				recordVector[vector_size] = new Record;
				recordVector[vector_size++]->Consume(&temp);
			}
			
			onePageBuffer.EmptyItOut();
			onePageBuffer.Append(&tempRecord);
			if(pageNumber == params->runlen){
				//run len pages have been read
				total_size += vector_size;
				
				inMemoryQuickSort(params,recordVector,0,vector_size-1);
				
				writeSortedIntoFile(recordVector,vector_size,tempFile,pageCounter);
			
				if(runCounter ==0){
					lengthEachRun[ix++]=pageCounter;
					lengthEachRun[ix++]=pageCounter;
				}else{
					int insertValue = pageCounter;
					for(int it = 1;it<=runCounter;it++)
						insertValue-=lengthEachRun.at((runCounter-it)*2);
					
					lengthEachRun[ix++]=insertValue;
					lengthEachRun[ix++]=pageCounter;
				}
				
				vector_size = 0;
				pageNumber = 0;
				runCounter++;
			}
			//cout << "if end" <<endl;
		}
	}

	while(onePageBuffer.GetFirst(&temp)){

		recordVector[vector_size] = new Record;
		recordVector[vector_size++]->Consume(&temp);

	}

	inMemoryQuickSort(params,recordVector,0,vector_size-1);

	writeSortedIntoFile(recordVector,vector_size,tempFile,pageCounter);
				if(runCounter ==0){
					lengthEachRun[ix++]=pageCounter;
					lengthEachRun[ix++]=pageCounter;
				}else{
					int insertValue = pageCounter;
					for(int it = 1;it<=runCounter;it++)
						insertValue-=lengthEachRun.at((runCounter-it)*2);
					
					lengthEachRun[ix++]=insertValue;
					lengthEachRun[ix++]=pageCounter;
				}

	pageNumber = 0;
	vector_size = 0;
	runCounter++;

	tempFile.Close();
	return runCounter;
}

void TPMMS_PhaseTwo(Param_bq *params,int runNumber, int k,vector<int> &lengthEachRun){ //k-way merge
	File tempFile;
	Page pageBuffer;
	tempFile.Open(1, params->fname);
	printf("Phase 2 - tempFile name: %s\t tempFile addr: %p\n", params->fname,&tempFile);
	//int * internalNode = new int[k-1];
	int * pageCounter = new int[k];
	int totalRun = runNumber;
	
	Record ** pRecordBuffer = new Record * [k];
	Page ** pPageBuffer = new Page * [k];
	for(int i = 0; i<k; i++){//allocate memory, create buffers
		pPageBuffer[i] = new Page;
		pRecordBuffer[i] = new Record;
		pageCounter[i] = 0;
	}

	MyPQ prQ(runNumber,pRecordBuffer,params->pSortOrder);

	for(int i =0; i<k;i++){
		if(ReadRecord(params,i,pageCounter[i],pRecordBuffer[i],pPageBuffer[i],tempFile,lengthEachRun))
			prQ.push(i);
	}
	File sortedFile;
	Page writePageBuffer;

	int writePageCounter = 0;
	int cc = 0;

	while(1){
	  
		int Ind = prQ.pop();
		if(Ind==-1){
			//end of priority queue
			break;
		}
		
		params->pOut->Insert(pRecordBuffer[Ind]);
	
		if(ReadRecord(params,Ind,pageCounter[Ind],pRecordBuffer[Ind],pPageBuffer[Ind],tempFile,lengthEachRun))
			prQ.push(Ind);
		
	}

	params->pOut->ShutDown();
	tempFile.Close();

	remove(params->fname);
	
}

void * consumer_thread(void * arg){
        Param_bq *pParams = (Param_bq *) arg;

	Param_bq params;
	params.pIn  = pParams->pIn;
	params.pOut = pParams->pOut;
	params.pSortOrder = pParams->pSortOrder;
	params.runlen = pParams->runlen;
	
	
		
		//srand((unsigned)time(NULL));
	for(int t = 0; t< strlen(pParams->fname);t++){
	  params.fname[t] =pParams->fname[t];
	}
	params.fname[strlen(pParams->fname)] = '\0';
	
	vector<int> lengthEachRun(2000);
	vector<int>::size_type ix = 0;

	int runNumber = TPMMS_PhaseOne(&params,lengthEachRun,ix);
	TPMMS_PhaseTwo(&params,runNumber, runNumber,lengthEachRun);
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  
      p = new Param_bq();
      p->pIn = &in;
      p->pOut = &out;
      p->pSortOrder = &sortorder;
      p->runlen = runlen;
      stringstream sstr;
      sstr << p;
      //	cout << this <<endl;
      string str_temp = sstr.str();
      for(int t = 0; t< str_temp.length();t++){
	p->fname[t]=str_temp.c_str()[t];
      }
      p->fname[str_temp.length()] = '\0';
      pthread_t ntid;
      int err = pthread_create(&ntid,NULL,consumer_thread,(void*)p);

}

BigQ::~BigQ () {
  //delete p;
}
