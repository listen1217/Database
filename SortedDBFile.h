#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"
#include "BigQ.h"

typedef struct{
  OrderMaker *myOrder;
  int runLength;
}SortInfo;

class SortedDBFile : virtual public GenericDBFile {
private:
 //-------------buffers--------------
	File * dataFile;
	Page * dataPage;
	Record * dataRecord;
	BigQ * bq;
	Pipe * pIn;
	Pipe * pOut;
	OrderMaker * myOrder;
	OrderMaker * queryOrder;
	CompOperator * myCompOperator;//remember each operator of the queryOrder
	CNF * queryCNF;
  //-------------flags--------------   
	bFlagType bufferFlag;
	rwState state;
	int dataFlag;
  //----------info--------------
	off_t currentPage; 	//start from 0, 1st page's index is 0
	int pageCounter;	//count the number of Pages in File, when empty, pageCounter = 0
	int runLen;
	string dbfile_name;
	off_t currentDFPage;
  //-------------private functions--------------  
	void InternalMerge();
	int ReadFromFile(File *file, Page *pageBuffer, Record *record, off_t &currentPage);
	void WriteToFile(File *file, Page *pageBuffer, Record *record, off_t &currentPage);

	int searchNext(int &return_index, Record &literal, CNF *cnf);
	int searchRemaining(Record *record, ComparisonEngine &ce, Record &literal, CNF &cnf);
 public:
	SortedDBFile();
	int Create (char *fpath, void *startup);
	int Open (char *fpath,void *startup);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif
