#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <string>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "GenericDBFile.h"
#include "SortedDBFile.h"
#include "HeapDBFile.h"

using namespace std;

DBFile::DBFile () {
  myInternalVar = NULL;
}
DBFile::~DBFile () {
  myOrder = NULL;
  myInternalVar = NULL;
}
/*
typedef struct{
  OrderMaker *myOrder;
  int runLength;
}SortInfo;
*/
int DBFile::Create (char *f_path, fType f_type, void *startup) {
  SortInfo * params = (SortInfo*) startup;
	metaFileName = f_path;
	metaFileName += "meta";
	ofstream fout(metaFileName.c_str());
	if( f_type == heap){
	  dbfile_type = f_type;
	  //fout << f_path <<endl;
	  fout << "heap" <<endl;
	  myInternalVar = new HeapDBFile();
	}else if( f_type == sorted){
	  dbfile_type = f_type;
	  //fout << f_path <<endl;
	  fout << "sorted" <<endl;
	  params->myOrder->serializer(fout);
	  fout << params->runLength << endl;
	  fout.flush();
	  myInternalVar = new SortedDBFile();
	}else{
	  cerr << "Not supported for other DBFile types!" <<endl;
	  exit(1);
	}
	fout << flush;
	fout.close();

	
	return myInternalVar -> Create(f_path, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	myInternalVar ->Load (f_schema, loadpath);
	return;
}

int DBFile::Open (char *f_path) {
  //1. open meta-file, decide whether heap, sorted or B+-Tree
	metaFileName = f_path;
	metaFileName += "meta";
	ifstream fin(metaFileName.c_str());
	string str_dbfile_type;
	int find_flag = 0;
	getline(fin,str_dbfile_type);

  //-----------------code here----------------------
  //2. open file according to f_type
	if( str_dbfile_type.compare("heap") == 0){
	  dbfile_type = heap;
	  myInternalVar = new HeapDBFile();
	}else if(str_dbfile_type.compare("sorted") == 0 ){
	  dbfile_type = sorted;
	  myOrder = new OrderMaker();
	  myOrder->deserializer(fin);
	  fin >> runLen;
	  //cout << "runlen=" << runLen <<endl;
	  myInternalVar = new SortedDBFile();
	}else{
	  cerr << "Not supported for other DBFile types!" <<endl;
	  exit(1);
	}
	SortInfo si = {myOrder,runLen};
	int returnValue =  myInternalVar -> Open (f_path,(void*)&si);
	
	return returnValue;
	
}

void DBFile::MoveFirst () {
  myInternalVar ->MoveFirst();
	return;
}

int DBFile::Close () {
  //1. write to meta-file
  //2. do close()
  
	int return_value  = myInternalVar -> Close();
	if(myInternalVar!=NULL)
	  delete myInternalVar;
	myInternalVar = NULL;
	return return_value;

}

void DBFile::Add (Record &rec) {
  myInternalVar -> Add (rec) ;
	return;
}

int DBFile::GetNext (Record &fetchme) {
 
	return  myInternalVar -> GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
 
	return   myInternalVar -> GetNext(fetchme,cnf,literal);
}
