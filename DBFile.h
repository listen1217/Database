#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Defs.h"
#include "Comparison.h"


class DBFile {

private:
  fType dbfile_type;
  GenericDBFile * myInternalVar;   //stores pointer to either Heap/Sorted
  OrderMaker * myOrder;
  int runLen;
  string metaFileName;
public:
	DBFile ();
	~DBFile ();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
