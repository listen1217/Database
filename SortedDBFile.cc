#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include "BigQ.h"

SortedDBFile :: SortedDBFile () {
  
	currentPage = 0;
	pageCounter = 0;
	bufferFlag = bufferEmpty;
	state = reading;
	currentDFPage = 0;
	dataFile = NULL;
	dataRecord = NULL;
	dataPage = NULL;
	dataPage = new Page;
	dataRecord = new Record;
}

int SortedDBFile::Create (char *f_path, void *startup) {
	SortInfo * params = (SortInfo *)startup;
	myOrder = new OrderMaker(params->myOrder);
	runLen = params->runLength;
	dbfile_name = f_path;//save the dbfile name for further use
	dataFile = new File;
	dataFile->Open(0,f_path);
	currentPage = 0;	
	pageCounter = 0;
	currentDFPage = 0;
	state = reading;

	bufferFlag = bufferEmpty;
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {

	FILE * filetoload = fopen(loadpath,"r");
	if(!filetoload){
		cerr << "Unable to open file: " << loadpath <<endl;
		exit(1);
	}
	if(state == reading){
		pIn = new Pipe(50);
		pOut = new Pipe(50);
		bq = new BigQ(*pOut,*pIn,*myOrder,runLen);
		state = writing;
	}
	
	//int count_records = 0;
	while(dataRecord->SuckNextRecord(&f_schema, filetoload)){
		pOut->Insert(dataRecord);
		//count_records++;
	}	
	//cout << "total pages in File(one is still in page buffer): " << dataFile.GetLength() << endl;
	
	fclose(filetoload);
	return;
	
}

int SortedDBFile::Open (char *f_path,void *startup) {
	SortInfo * params = (SortInfo *)startup;
	myOrder = new OrderMaker(params->myOrder);
	runLen = params->runLength;
	dbfile_name = f_path;//save the dbfile name for further use
	if(dataFile!=NULL){
	  delete dataFile;
	}
	dataFile = new File;
	dataFile->Open(1,f_path);
	cout << f_path <<endl;
	currentPage = 0;
	pageCounter = dataFile->GetLength();
	//cout <<"total " <<pageCounter<<" pages"<<endl;
	currentDFPage = 0;
	state = reading;
	bufferFlag = bufferEmpty;
	return 1;
	
}
int  SortedDBFile:: ReadFromFile(File *file, Page *pageBuffer, Record *record, off_t &currentPage){
	if(!pageBuffer->GetFirst(record)){
	       	if(!file->GetLength()){
			//file contains no data
	       		return 0;
	       	}
	       	if( currentPage + 1 >= file->GetLength()){
	       	//reach end of the file
	       		return 0;
	       	}
	       	file->GetPage(pageBuffer, currentPage++);
	       	if(pageBuffer->GetFirst(record)){
	       		return 0;
	       	}
       	}
	return 1;
}
void  SortedDBFile:: WriteToFile(File *file, Page *pageBuffer, Record *record, off_t &pageCounter){
	if(!pageBuffer->Append(record)){
		file->AddPage(pageBuffer,pageCounter++);
		pageBuffer->EmptyItOut();
		pageBuffer->Append(record);
		
     	}
	return;
}
void  SortedDBFile::InternalMerge(){
	state = reading;
	pOut->ShutDown();
	Page pageBufferFromFile, pageBufferToFile;
	Record recordFromPipe, recordFromFile;
	File tempFile;
	string temp_name = dbfile_name + "temp";
	//open a new temporary file to output
	tempFile.Open(0,const_cast<char*>(temp_name.c_str()));
	currentPage = 0;
	currentDFPage = 0;
	off_t pageCounterDF = 0;
	
	//Schema sc("catalog","lineitem"); 

	if(ReadFromFile(dataFile,&pageBufferFromFile,&recordFromFile,currentPage)){//file contains data
		ComparisonEngine ce;
		//merge file and pipe, store records into new temporary file
		while(pIn->Remove(&recordFromPipe)){
			if(ce.Compare(&recordFromPipe,&recordFromFile,myOrder) < 0){
				WriteToFile(&tempFile,&pageBufferToFile,&recordFromPipe,pageCounterDF);
				continue;
			}else{
				WriteToFile(&tempFile,&pageBufferToFile,&recordFromFile,pageCounterDF);
				if(ReadFromFile(dataFile,&pageBufferFromFile,&recordFromFile,currentPage)){
					continue;
				}else{
					break;
				}
			}
		}//while

		//continue reading from pipe
		while(pIn->Remove(&recordFromPipe)){
			WriteToFile(&tempFile,&pageBufferToFile,&recordFromPipe,pageCounterDF);
		}
		//continue reading from file
		while(ReadFromFile(dataFile,&pageBufferFromFile,&recordFromFile,currentPage)){
			WriteToFile(&tempFile,&pageBufferToFile,&recordFromFile,pageCounterDF);
		}
	}else{  //if file contains no data
	        //direct store records in dq to file
		while(pIn->Remove(&recordFromPipe)){
		  //recordFromPipe.Print(&sc);
			WriteToFile(&tempFile,&pageBufferToFile,&recordFromPipe,pageCounterDF);
		}
	}
	tempFile.AddPage(&pageBufferToFile,pageCounterDF++);
	pageCounter = pageCounterDF;
	remove(dbfile_name.c_str());
	rename (temp_name.c_str(), dbfile_name.c_str());
	//cout << "tempFile length = " << tempFile.GetLength() <<endl;
	tempFile.Close();
	if(dataFile == NULL)
	  dataFile = new File;
	dataFile->Open(1,const_cast<char*>(dbfile_name.c_str()));
	//pIn->ShutDown();
	delete pIn;
	delete pOut;
	delete bq;
  
}

void SortedDBFile::MoveFirst () {
	//get the first page in file
	
	//deal with the buffer
	if(state == writing){
		InternalMerge();
	}
	currentPage = 0;

}

int SortedDBFile::Close () {
	if(state == writing){
		InternalMerge();
	}
	//cout << "total pages in File: " << dataFile->GetLength() << ";" <<endl;
	currentPage = 0;
	int returnValue = dataFile->Close();
	delete dataPage;
	delete dataRecord;
	delete dataFile;
	delete myOrder;
	return returnValue;

}

void SortedDBFile::Add (Record &rec) {

	if(state == reading){
		pIn = new Pipe(20);
		pOut = new Pipe(20);
		bq = new BigQ(*pOut,*pIn,*myOrder,runLen);
		state = writing;
	}
	pOut->Insert(&rec);
	//可能加到几十个就加不动了
	return;
}

int SortedDBFile::GetNext (Record &fetchme) {
	
	if(state == writing){
		InternalMerge();
	}
	if (bufferFlag == dataRead){
		if(!dataPage->GetFirst(&fetchme)){
			bufferFlag = bufferEmpty;
			if(!dataFile->GetLength()){
			//file contains no data
				return 0;
			}
			if( currentPage + 1 >= dataFile->GetLength()){
			//reach end of the file
				//cout << "currentPage = " << currentPage <<endl;
				return 0;
			}
			dataFile->GetPage(dataPage, currentPage++);
			bufferFlag = dataRead;
			if(!dataPage->GetFirst(&fetchme)){
				bufferFlag = bufferEmpty;
				return 0;
			}
		}
	}else{
	//buffer empty
		if(!dataFile->GetLength()){
		//file contains no data
			return 0;
		}
		if( currentPage + 1 >= dataFile->GetLength()){
		//reach end of the file
			//cout << "currentPage = " << currentPage <<endl;
			return 0;
		}
		dataFile->GetPage(dataPage, currentPage++);
		bufferFlag = dataRead;
		if(!dataPage->GetFirst(&fetchme)){
			bufferFlag = bufferEmpty;
			return 0;
		}
	}

	return 1;
}

int SortedDBFile::searchNext(int &return_index, Record &literal, CNF *cnf)
{
      int startIndex = currentPage;
      Page * tempPage = new Page;
      Record * record = new Record;
      ComparisonEngine ce;
      
      Schema sc("catalog","lineitem");
      //literal.Print(&sc);
	
      while(1){
		if(startIndex >= pageCounter-1){
			//not found
			return_index = pageCounter-1;
			return 0;
        }
	    //cout << startIndex << " " << dataFile->GetLength() <<endl;
        dataFile->GetPage(tempPage,startIndex++);
	    tempPage->GetFirst(record);
	    
	    //record->Print(&sc);
	   // cout << "---------------------------" <<endl;

	    int compare_result = ce.Compare(record, &literal, cnf);
	    if(compare_result){
          return_index = startIndex-2;
		  return 1;
	    }
      }
      
}//end_searchNext

int SortedDBFile::searchRemaining(Record *record, ComparisonEngine &ce, Record &literal, CNF &cnf)
{
	while(GetNext(*record)){
		if (ce.Compare (record, &literal, &cnf)){
			return 1;
		}
	}//end_while
	return 0;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(state == writing){
		InternalMerge();
	}
	//construct "query" OrderMaker
	//if queryOrder not constructed
	ComparisonEngine ce;
	CompOperator myCompOperator;
	queryCNF = new CNF;
	int find_flag = 0;
	if( myOrder->numAtts >0){
	  for(int j = 0; j< cnf.numAnds;j++){

	    	if (cnf.orLens[0] != 1) {
				continue;
			}
			//Target operand1;
			//int whichAtt1;
			//Target operand2;
			//int whichAtt2;
			//Type attType;
			//CompOperator op;
			if(cnf.orList[j][0].whichAtt1 == myOrder->whichAtts[0]){
				find_flag = 1;
				//cout << "which atts = " <<myOrder->whichAtts[i] <<endl;
				queryCNF->numAnds = 1;
				queryCNF->orList[0][0].whichAtt1 = cnf.orList[j][0].whichAtt1;
				queryCNF->orList[0][0].whichAtt2 = cnf.orList[j][0].whichAtt2;
				queryCNF->orList[0][0].operand1 = cnf.orList[j][0].operand1;
				queryCNF->orList[0][0].operand2 = cnf.orList[j][0].operand2;
				queryCNF->orList[0][0].attType = cnf.orList[j][0].attType;
				queryCNF->orList[0][0].op = cnf.orList[j][0].op;
				myCompOperator = cnf.orList[j][0].op;
				queryCNF->orLens[0] = 1;
				
				break;
			}
			if(cnf.orList[j][0].whichAtt2 == myOrder->whichAtts[0]){
				find_flag = 1;
				queryCNF->numAnds = 1;
				queryCNF->orList[0][0].whichAtt1 = cnf.orList[j][0].whichAtt1;
				queryCNF->orList[0][0].whichAtt2 = cnf.orList[j][0].whichAtt2;
				queryCNF->orList[0][0].operand1 = cnf.orList[j][0].operand1;
				queryCNF->orList[0][0].operand2 = cnf.orList[j][0].operand2;
				queryCNF->orList[0][0].attType = cnf.orList[j][0].attType;
				queryCNF->orList[0][0].op = cnf.orList[j][0].op;
				queryCNF->orLens[0] = 1;
				
				if(cnf.orList[j][0].op == LessThan)
					myCompOperator = GreaterThan;
				else if(cnf.orList[j][0].op ==  GreaterThan)
					myCompOperator =  LessThan;
				else
					myCompOperator=  Equals;
				break;
			}
	    
	  }
	}//if
	
	//construction complete
	
	//start find the record
	if(dataRecord == NULL)
		dataRecord = new Record;
		
	if(!GetNext(*dataRecord)){ //no records
	  return 0;
	}
	
	if (ce.Compare (dataRecord, &literal, &cnf)){
		fetchme.Consume(dataRecord);
		return 1;
	}
	
	
	if(find_flag==1 && myCompOperator != Equals){//cnf match myOrder

		int accepted = ce.Compare(dataRecord, &literal, queryCNF);
		if(accepted ==0 && myCompOperator == LessThan){	
			return 0;
		}
		//search the first page that meet requirement
		int return_index = 0;
		searchNext(return_index, literal, queryCNF);
		if(return_index > currentPage ){
			currentPage = return_index;
		}
		//then start from the previous page, scan
		
	}
	//do search one by one
	int search_flag = searchRemaining(dataRecord, ce, literal,cnf);
	if(search_flag){
		fetchme.Consume(dataRecord);
		return 1;
	}
	return 0;
}
