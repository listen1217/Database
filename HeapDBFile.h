#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"

class HeapDBFile : virtual public GenericDBFile {
private:
  //-------------buffers--------------
	File dataFile;
	Page dataPage;
	Record dataRecord;

  //-------------flags--------------   
	off_t currentPage; 		//start from 0, 1st page's index is 0
	int pageCounter;		//count the number of Pages in File, when empty, pageCounter = 0
	bFlagType bufferFlag;

 public:
	HeapDBFile();
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
