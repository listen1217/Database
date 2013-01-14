#include "RelOp.h"
#include "BigQ.h"
#include "Defs.h"
#include "DBFile.h"
#include "MyPQ.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "File.h"
#include "Pipe.h"
#include "Comparison.h"
#include "Function.h"
#include <iostream>
#include <pthread.h>
#include <sstream>
using namespace std;



void * SelectFile_worker_thread(void * arg){

        Param_sf * param = (Param_sf *) arg;
	DBFile * inFile=param->inFile;
	Pipe *pOut = param->pOut;
	CNF * pCNF = param->pCNF;
	Record * pLiteral = param->pLiteral;

	Record toPipe;

        while(inFile->GetNext(toPipe,*pCNF,*pLiteral)){
		pOut->Insert(&toPipe);
		//cout << debugCount++ <<endl;

	}
	pOut->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
  p = new Param_sf();
  p->inFile = &inFile;
  p->pOut = &outPipe;
  p->pCNF = &selOp;
  p->pLiteral = &literal;

  int err = pthread_create(&thread,NULL,SelectFile_worker_thread,(void*)p);
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
	delete p;
}


//==================================SelectPipe================================================


void * SelectPipe_worker_thread(void * arg){
	
	Param_sp * param = (Param_sp *) arg;
	Pipe * in = param->in;
	Pipe * out = param->out;
	CNF * selOp = param->pCNF;
	Record * literal = param->pLiteral;
	Record fromPipe;
	ComparisonEngine ce;
	while(in->Remove(&fromPipe)){
		if(ce.Compare(&fromPipe,literal,selOp))
			out->Insert(&fromPipe);
	}
	out->ShutDown();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
         p = new Param_sp();
	 p->in = &inPipe;
	 p->out = &outPipe;
	 p->pCNF = &selOp;
	 p->pLiteral = &literal;
	int err = pthread_create(&thread,NULL,SelectPipe_worker_thread,(void*)p);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
	delete p;
}



//==================================Project==========Y======================================


void * Project_worker_thread(void * arg){

  Param_pr * param = (Param_pr *) arg;
  Pipe * in = param->in;
  Pipe * out = param->out;
	int * keepMe = param->keepMe;
	int numAttsInput = param->numAttsInput;
	int numAttsOutput = param->numAttsOutput;

	Record fromPipe;
	ComparisonEngine ce;
	while(in->Remove(&fromPipe)){
		fromPipe.Project( keepMe, numAttsOutput, numAttsInput);
		//printf("after Project\n");
		//	printf("\t#bytes: %d\n", ((int*)(fromPipe.bits))[0]);
		out->Insert(&fromPipe);
	}
	 out->ShutDown();
}
void Project :: Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
  p = new Param_pr();
  p->in = &inPipe;
  p->out = &outPipe;
  p->keepMe = keepMe;
  p->numAttsInput = numAttsInput;
  p->numAttsOutput=numAttsOutput;
  
  int err = pthread_create(&thread, NULL, Project_worker_thread,(void*)p);
}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
	delete p;
}



//===========================Join======= Y===============================================




void *  Join_worker_thread(void * arg){
        Param_j * params = (Param_j *) arg;
	  Pipe * inL = params->inL;
	  Pipe * inR = params->inR;
	  Pipe * out = params->out;
	  CNF * pCNF = params->pCNF;
	  Record * pLiteral = params->pLiteral;
	Record fromPipe;
	OrderMaker leftOrder,rightOrder;
	
	if(!pCNF->GetSortOrders(leftOrder,rightOrder)){
	  //block-nested join
	}

	Pipe tempPipeR(50);
	BigQ rightBigQ(*inR,tempPipeR,rightOrder,10);
	Record recordVectorLeft[1000], recordVectorRight[1000];
	int vectorCountLeft = 0, vectorCountRight = 0;
	ComparisonEngine ce;
	Record preL,preR,lastL,lastR;
	//sleep(1);
	Pipe tempPipeL(50);
	BigQ leftBigQ(*inL,tempPipeL,leftOrder,10);

	if(!(tempPipeL.Remove(&preL) && tempPipeR.Remove(&preR))){
	  out->ShutDown();
	  return NULL;
	}
	

	while(1){
	  // cout << "lets compare" <<endl;
	  int compare_result = ce.Compare(&preL,&leftOrder, &preR, &rightOrder);

	  if(compare_result > 0){
	    if(tempPipeR.Remove(&preR))
	      continue;
	    else
	      break;
	  }
	  if(compare_result < 0){
	    if(tempPipeL.Remove(&preL))
	      continue;
	    else
	      break;
	  }

	  // cout << "records are same: 1 time" <<endl;
	  int dif_flagL = 0, dif_flagR = 0;
	  //dif_flag = 0 : pipe empty, dif_flag!=0 : no records of same value


	  int test_r = 0, test_l = 0;
	  while(tempPipeL.Remove(&lastL)){
	    if(ce.Compare(&preL, &lastL, &leftOrder)){
	      dif_flagL = 1;
	      recordVectorLeft[vectorCountLeft++].Consume(&preL);
	      preL.Consume(&lastL);
	      test_l++;
	      break;
	    }
	    recordVectorLeft[vectorCountLeft++].Consume(&preL);
	    preL.Consume(&lastL);
	    test_l++;
	  }//WHILE LEFT pipe
	  if(dif_flagL == 0){
	    recordVectorLeft[vectorCountLeft++].Consume(&preL);
	  }

	  //deal with right
		     
	  //dif_flag = 0 : pipe empty, dif_flag!=0 : no records of same value
	  while(tempPipeR.Remove(&lastR)){
	    if(ce.Compare(&preR, &lastR, &rightOrder)){
	      dif_flagR = 1;
	      recordVectorRight[vectorCountRight++].Consume(&preR);
	      preR.Consume(&lastR);
	      test_r++;
	      break;
	    }
	    recordVectorRight[vectorCountRight++].Consume(&preR);
	    preR.Consume(&lastR);
	    test_r++;
	  }//WHILE LEFT pipe
	  if(dif_flagR == 0){
	    recordVectorRight[vectorCountRight++].Consume(&preR);
	  }

	  // cout << "left record #: " << test_l << "  right record #: " <<test_r <<endl;
	  //  cout << "left count #: " <<vectorCountLeft << "  right count #: " <<vectorCountRight <<endl;
	  //--start join all records

	  //--number of atts in left&right
	  int numAttsLeft = ((int*)(recordVectorLeft[0].bits))[1]/sizeof(int)-1;
	  int numAttsRight = ((int*)(recordVectorRight[0].bits))[1]/sizeof(int)-1;
	  // int attsSub = rightOrder.numAtts;
		    
	  int totAttsToKeep = numAttsLeft+numAttsRight;
	  int * attsToKeep = new int [totAttsToKeep];
	  int startOfRight = numAttsLeft;
	  //----get atts array-----
	  for(int i = 0;i< startOfRight;i++){
	    attsToKeep[i] = i;
	  }

	  for(int i = 0;i+startOfRight<totAttsToKeep ;i++){
	      attsToKeep[i+startOfRight] = i;
	  }
	  //----array---got------

	  //start merge------
	  Record toMerge;
	  for( int indexLeft = 0; indexLeft<vectorCountLeft;indexLeft++){
	    for(int indexRight = 0;indexRight < vectorCountRight;indexRight++){
	      toMerge.MergeRecords(&recordVectorLeft[indexLeft], &recordVectorRight[indexRight],numAttsLeft,numAttsRight, attsToKeep,totAttsToKeep, startOfRight);
	      out->Insert(&toMerge);
	    }
	  }
	  // cout << "merged" <<endl;
	  //end of merge-----
	  delete [] attsToKeep;
	  vectorCountLeft = 0;
	  vectorCountRight = 0;
	  if( test_l == 0 ||  test_r == 0 )
	    break;
	}

	

	//clear pipe
	while(tempPipeL.Remove(&fromPipe)){
	}
	while(tempPipeR.Remove(&fromPipe)){
	}
	out->ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
  p = new Param_j();
  p->inL = &inPipeL;
  p->inR = &inPipeR;
  p->out = &outPipe;
  p->pCNF = &selOp;
  p->pLiteral = &literal;
	int err = pthread_create(&thread,NULL,Join_worker_thread,(void*)p);

}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
	delete p;
	
}



//=================================Duplicate Removal==========Y=======================================


void *  DuplicateRemoval_worker_thread(void * arg){
	Param_dr * param = (Param_dr*) arg;
	
	Pipe * in = param->in, *out = param->out;
	
	Schema * mySchema = param->mySchema;
	Pipe tempPipe(100);
	//cout <<  mySchema->GetNumAtts() <<endl;
	Record tempRecord, fromPipe;
	Record tempR;
	ComparisonEngine ce;
	//cout <<  mySchema->GetNumAtts() <<endl;
	OrderMaker myOrder(mySchema);
	
	BigQ myBigQ(*in,tempPipe,myOrder,5);

	if(tempPipe.Remove(&fromPipe)){
		tempRecord.Copy(&fromPipe);
		
		out->Insert(&fromPipe);
	}
	//int cc = 0;
	while(tempPipe.Remove(&fromPipe)){
		if(ce.Compare(&fromPipe,&tempRecord,&myOrder)){
		       
			tempRecord.Copy(&fromPipe);
			//cout << "fromPipe is consumed by tempRecord. Insert tempR to outPipe." <<endl;
			//printf("Duplicate removal, outPipe.addr = %p\n",out);
			//printf("\ttoPipe.#bytes = %d\n",((int*)fromPipe.bits)[0]);
			out->Insert(&fromPipe);
			//cout << "output count = " <<cc++ <<endl;
		}
		
	}
	//cout << "RelOp.outPipe shutdown" <<endl;
	out->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){

	p = new Param_dr();
	p->in = &inPipe;
	p->mySchema = &mySchema;
	p->out = &outPipe;
	
	int err = pthread_create(&thread,NULL, DuplicateRemoval_worker_thread,(void*)p);
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
	//delete p;
}



//================================SUM============Y======================================


void * Sum_worker_thread(void * arg){
        Param_sum * param = (Param_sum*) arg;
	Pipe * in = param->in, *out = param->out;
	Function * computeMe = param->computeMe;

	Record fromPipe;
	int sumInt = 0, tempInt = 0;
	double sumDouble = 0,tempDouble = 0;
	stringstream sstr;
	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};

	while(in->Remove(&fromPipe)){
	  computeMe->Apply(fromPipe,tempInt,tempDouble);
	  sumInt+=tempInt;
	  sumDouble+=tempDouble;
	}
	if(sumInt){
		sstr << sumInt <<'|';
		Schema temp_sch ("temp", 1, &IA);
		string str_temp = sstr.str();
		fromPipe.ComposeRecord(&temp_sch, str_temp.c_str());
	}else{
		sstr << sumDouble<<'|';
		Schema temp_sch ("temp", 1, &DA);
		string str_temp = sstr.str();
		fromPipe.ComposeRecord(&temp_sch, str_temp.c_str());
	}
	out->Insert(&fromPipe);
	out->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){

  p = new Param_sum();
  p->in = &inPipe;
  p->out = &outPipe;
  p->computeMe = &computeMe;
	
	int err = pthread_create(&thread,NULL,Sum_worker_thread,(void*)p);
}

void Sum::WaitUntilDone () {
	pthread_join (thread, NULL);
	delete p;
}

//===================================GroupBy===============================================


void * GroupBy_worker_thread(void * arg){
        Param_gr * param = (Param_gr*) arg;
	Pipe *in = param->in,*out = param->out;
	OrderMaker * groupAtts = param->groupAtts;
	Function * computeMe = param->computeMe;

        Record fromPipe,tempRecord, sumRecord, recordGroup;
	
	int sumInt = 0, tempInt = 0;
	double sumDouble = 0, tempDouble = 0;

	stringstream sstr;
	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};
	ComparisonEngine ce;
	//sleep(1);
	Pipe tempPipe(50);
	BigQ mybq(*in,tempPipe,*groupAtts,5);

	int numAttsOutput = groupAtts->numAtts;
	int * keepMe = new int[numAttsOutput];
	int * keepInMerge = new int[numAttsOutput+1];
	keepInMerge[0] = 0;
	for(int i = 0;i < numAttsOutput; i++){
	  keepMe[i] = groupAtts->whichAtts[i];
	  keepInMerge[i+1] = i;
	}
	

	if(tempPipe.Remove(&tempRecord)){
		computeMe->Apply(tempRecord,tempInt,tempDouble);
		sumInt += tempInt;
		sumDouble+=tempDouble;
		
	}
	int numAttsInput = ((int*)(tempRecord.bits))[1] / sizeof(int) - 1;
	Record toMerge;
	while(tempPipe.Remove(&fromPipe)){
		if(ce.Compare(&fromPipe,&tempRecord,groupAtts)){
			if(sumInt){
				  sstr << sumInt <<'|';
				  Schema temp_sch ("temp", 1, &IA);
				  string str_temp = sstr.str();
				  sumRecord.ComposeRecord(&temp_sch, str_temp.c_str());
				  
				  tempRecord.Project( keepMe, numAttsOutput,numAttsInput);
				  toMerge.MergeRecords(&sumRecord, &tempRecord,1,numAttsOutput,keepInMerge,numAttsOutput+1,1);
				  tempRecord.Copy(&fromPipe);
				  sumInt = 0;
			}else{
				  sstr << sumDouble<<'|';
				  Schema temp_sch ("temp", 1, &DA);
				  string str_temp = sstr.str();
				  sumRecord.ComposeRecord(&temp_sch, str_temp.c_str());
				  
				  tempRecord.Project( keepMe, numAttsOutput,numAttsInput);
				  toMerge.MergeRecords(&sumRecord, &tempRecord,1,numAttsOutput,keepInMerge,numAttsOutput+1,1);
				  tempRecord.Copy(&fromPipe);
				  sumDouble = 0;
			}
			out->Insert(&toMerge);
			
		}
		computeMe->Apply(fromPipe,tempInt,tempDouble);
		sumInt += tempInt;
		sumDouble+=tempDouble;
		
	}
	if(sumInt){
	  sstr << sumInt <<'|';
	  Schema temp_sch ("temp", 1, &IA);
	  string str_temp = sstr.str();
	  sumRecord.ComposeRecord(&temp_sch, str_temp.c_str());
	  tempRecord.Project( keepMe, numAttsOutput,numAttsInput);
	  toMerge.MergeRecords(&sumRecord, &tempRecord,1,numAttsOutput,keepInMerge,numAttsOutput+1,1);
	  sumInt = 0;
	}else{
	  sstr << sumDouble<<'|';
	  Schema temp_sch ("temp", 1, &DA);
	  string str_temp = sstr.str();
	  sumRecord.ComposeRecord(&temp_sch, str_temp.c_str());
	  tempRecord.Project( keepMe, numAttsOutput,numAttsInput);
	  toMerge.MergeRecords(&sumRecord, &tempRecord,1,numAttsOutput,keepInMerge,numAttsOutput+1,1);
	  sumDouble = 0;
	}
	out->Insert(&toMerge);
	out->ShutDown();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){

        p = {&inPipe, &outPipe, &groupAtts,  &computeMe};
	
	int err = pthread_create(&thread,NULL,GroupBy_worker_thread,(void*)&p);
}

void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
}


//===================================WriteOut===============================================
void * WriteOut_worker_thread(void * arg){
  Param_wr * param = (Param_wr*) arg;
  Pipe * in = param->in;
  
  FILE * outFile = param->outFile;
  Schema * mySchma = param->mySchema;
  //cout << "---------start output----------" <<endl;
  Record fromPipe;
  int cc = 0;
  while(in->Remove(&fromPipe)){
    //printf("outside Print, in.addr = %p\n",in);
    //printf("outside Print, fromPipe.addr = %p\n",&fromPipe);
    fromPipe.fPrint(mySchma,outFile);
    
    cc++;  
  }
  cout << cc << " records were written to file." <<endl;
}
void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema){
      p = new Param_wr();
      p->in = &inPipe;
      p->outFile = outFile;
      p->mySchema = &mySchema;
      
      int err = pthread_create(&thread,NULL,WriteOut_worker_thread,(void*)p);
}
void WriteOut::WaitUntilDone () {
  pthread_join (thread, NULL);
  delete p;

}
