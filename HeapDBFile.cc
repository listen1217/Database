#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapDBFile.h"
#include "Defs.h"

HeapDBFile::HeapDBFile () {
	currentPage = 0;
	pageCounter = 0;
	bufferFlag = bufferEmpty;
}

int HeapDBFile::Create (char *f_path, void *startup) {

	//deal with the buffer
	if(bufferFlag == needWrite){
		dataFile.AddPage(&dataPage,pageCounter++);
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}else if (bufferFlag == dataRead){
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}
	dataFile.Open(0,f_path);
	currentPage = 0;
	
	pageCounter = 0;
	bufferFlag = bufferEmpty;
	return 1;
}

void HeapDBFile::Load (Schema &f_schema, char *loadpath) {
	FILE * filetoload = fopen(loadpath,"r");
	if(!filetoload){
		cerr << "Unable to open file: " << loadpath <<endl;
		exit(1);
	}
	
	//deal with the buffer
	if(bufferFlag == needWrite){
		dataFile.AddPage(&dataPage,pageCounter++);
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}else if (bufferFlag == dataRead){
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}
	
	int count_records = 0;
	if(dataRecord.SuckNextRecord(&f_schema, filetoload)){
		//dataRecord.Print(&testschema);
		if(!dataPage.Append(&dataRecord)){
			dataFile.AddPage(&dataPage,pageCounter++);
			dataPage.EmptyItOut();
			dataPage.Append(&dataRecord);
			bufferFlag = needWrite;
			
		}
		bufferFlag = needWrite;
		//count_records++;
	}
	while(dataRecord.SuckNextRecord(&f_schema, filetoload)){
		//count_records++;
		if(!dataPage.Append(&dataRecord)){
			dataFile.AddPage(&dataPage,pageCounter++);
			dataPage.EmptyItOut();
			dataPage.Append(&dataRecord);
			bufferFlag = needWrite;
		}
	}
	fclose(filetoload);
	return;
	
}

int HeapDBFile::Open (char *f_path,void *startup) {

	//deal with the buffer
	if(bufferFlag == needWrite){
		cerr << "Error: You previously didn't Close()." <<endl;
		exit(1);
	}else if (bufferFlag == dataRead){
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}
	
	dataFile.Open(1,f_path);
	currentPage = 0;
	pageCounter = dataFile.GetLength()-1;
	//	cout << "total pages in File: " << dataFile.GetLength() << ";" <<endl;
	return 1;
	
}

void HeapDBFile::MoveFirst () {
	//get the first page in file
	
	//deal with the buffer
	if(bufferFlag == needWrite){
		dataFile.AddPage(&dataPage,pageCounter++);
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}else if (bufferFlag == dataRead){
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}
	currentPage = 0;
	//dataFile.GetPage(&dataPage, currentPage++);
}

int HeapDBFile::Close () {

	//deal with the buffer
	if(bufferFlag == bufferEmpty){
		//cout << "total pages in File: " << dataFile.GetLength() << ";" <<endl;
		currentPage = 0;
		return dataFile.Close();
	}
	
	if(bufferFlag == needWrite){
		dataFile.AddPage(&dataPage,pageCounter++);
		dataPage.EmptyItOut();
		
	}
	dataPage.EmptyItOut();
	bufferFlag = bufferEmpty;
	//cout << "total pages in File: " << dataFile.GetLength() << ";" <<endl;
	currentPage = 0;
	return dataFile.Close();

}

void HeapDBFile::Add (Record &rec) {

	//deal with the buffer
	if(bufferFlag == dataRead){
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}

	if(!dataPage.Append(&rec)){
		if(bufferFlag == needWrite){
			dataFile.AddPage(&dataPage,pageCounter++);
			dataPage.EmptyItOut();
		}
		dataPage.Append(&rec);
		bufferFlag = needWrite;
	}
	
	return;
}

int HeapDBFile::GetNext (Record &fetchme) {
	//cout << "current page: " << currentPage << ";" <<endl;
	
	//deal with the buffer
	if(bufferFlag == needWrite){
		dataFile.AddPage(&dataPage,pageCounter++);
		dataPage.EmptyItOut();
		bufferFlag = bufferEmpty;
	}
	if (bufferFlag == dataRead){
		if(!dataPage.GetFirst(&fetchme)){
			bufferFlag = bufferEmpty;
			if(!dataFile.GetLength()){
			//file contains no data
				return 0;
			}
			if( currentPage + 1 >= dataFile.GetLength()){
			//reach end of the file
				//cout << "currentPage = " << currentPage <<endl;
				return 0;
			}
			dataFile.GetPage(&dataPage, currentPage++);
			bufferFlag = dataRead;
			if(!dataPage.GetFirst(&fetchme)){
				bufferFlag = bufferEmpty;
				return 0;
			}
		}
	}else{
	//buffer empty
		if(!dataFile.GetLength()){
		//file contains no data
			return 0;
		}
		if( currentPage + 1 >= dataFile.GetLength()){
		//reach end of the file
			//cout << "currentPage = " << currentPage <<endl;
			return 0;
		}
		dataFile.GetPage(&dataPage, currentPage++);
		bufferFlag = dataRead;
		if(!dataPage.GetFirst(&fetchme)){
			bufferFlag = bufferEmpty;
			return 0;
		}
	}
	return 1;
}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	int find_flag = 0;
	//Schema mySchema ("catalog", "partsupp");
	ComparisonEngine comp;
	//int cnt = 0;
	while(GetNext(fetchme)){
	  //fetchme.Print(&mySchema);
	    //cout << "get next cnt = " <<cnt++<<endl;
	    
	    if (comp.Compare (&fetchme, &literal, &cnf)){
		//fetchme.Print(&mySchema);
        	find_flag = 1;
        	break;
	    }
	}
	if(find_flag == 0){
	    //no records found
	    return 0;
	}
	return 1;
}
