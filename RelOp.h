#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Schema.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "File.h"
#include "Comparison.h"
#include "BigQ.h"
#include "Defs.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

typedef struct {
  DBFile * inFile;
  Pipe * pOut;
  CNF * pCNF;
  Record * pLiteral;
}Param_sf;

class SelectFile : public RelationalOp { 

	private:

	pthread_t thread;
	int runLen;
	Param_sf * p;
	//	static void * SelectFile_worker_thread(void * arg);
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) {runLen = n; }

};

typedef struct {
  Pipe * in;
  Pipe * out;
  CNF * pCNF;
  Record * pLiteral;
}Param_sp;
class SelectPipe : public RelationalOp {
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_sp * p;
	//	static void * SelectPipe_worker_thread(void * arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};

typedef struct {
  	Pipe * in;
	Pipe * out;
	int * keepMe;
	int numAttsInput;
	int numAttsOutput;
}Param_pr;

class Project : public RelationalOp { 
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_pr * p;
	//	static void * Project_worker_thread(void * arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};

typedef struct {
  Pipe * inL;
  Pipe * inR;
  Pipe * out;
  CNF * pCNF;
  Record * pLiteral;
}Param_j;

class Join : public RelationalOp { 
	void * worker_thread(void * arg);
	pthread_t thread;
	int size;
	Param_j * p;
	//	static void * Join_worker_thread(void * arg);
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { size = n; }
};


typedef struct {
  Pipe * in;
  Pipe * out;
  Schema * mySchema;
}Param_dr;

class DuplicateRemoval : public RelationalOp {
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_dr* p;
	//	static void * DuplicateRemoval_worker_thread(void * arg);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};

typedef struct {
  Pipe * in;
  Pipe * out;
  Function * computeMe;
}Param_sum;

class Sum : public RelationalOp {
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_sum * p;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};

typedef struct {
  Pipe * in;
  Pipe * out;
  OrderMaker * groupAtts;
  Function * computeMe;
}Param_gr;

class GroupBy : public RelationalOp {
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_gr p;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};

typedef struct {
  Pipe * in;
  FILE * outFile;
  Schema * mySchema;
}Param_wr;

class WriteOut : public RelationalOp {
	void * worker_thread(void * arg);
	pthread_t thread;
	int runLen;
	Param_wr * p;
	
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { runLen = n; }
};
#endif
