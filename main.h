#ifndef MAIN_H
#define MAIN_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "RelOp.h"
#include "Pipe.h"
#include <pthread.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"

using namespace std;

// make sure that the information below is correct


char *dbfile_dir = "/cise/tmp/xin_10M/"; // dir where binary heap files should be stored
char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/10M/";
char *catalog_path = "catalog"; // full path of the catalog file
//char *statistic_path = "Statistics.txt";

struct SortInfo {
	OrderMaker *myOrder;
	int runLength;
};
extern  struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern	struct TableList *tables; // the list of tables and aliases in the query
extern	struct AndList *boolean; // the predicate in the WHERE clause
extern	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern	struct NameList *attsToSortOn;
extern	struct NewAttr *attsList;
extern	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern	int tableType;
extern	char *myfile;
extern	qMode query_mode;

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
	
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
}


class relation {

private:
	char *rname;
	char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (char *_name, Schema *_schema, char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) {
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	char* name () { return rname; }
	char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info () {
		cout << " relation info\n";
		cout << "\t name: " << name () << endl;
		cout << "\t path: " << path () << endl;
	}

	void get_cnf (CNF &cnf_pred, Record &literal) {
		cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
  		if (yyparse() != 0) {
			std::cout << "Can't parse your CNF.\n";
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
	}
	void get_sort_order (OrderMaker &sortorder) {
		cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
  		if (yyparse() != 0) {
			std::cout << "Can't parse your sort CNF.\n";
			exit (1);
		}
		cout << " \n";
		Record literal;
		CNF sort_pred;
		sort_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
		OrderMaker dummy;
		sort_pred.GetSortOrders (sortorder, dummy);
	}
};


int UpdateCatalog(string tName, NewAttr *attsList);
int DeleteTable(string tName);

relation *rel;


char *supplier = "supplier"; 
char *partsupp = "partsupp"; 
char *part = "part"; 
char *nation = "nation"; 
char *customer = "customer"; 
char *orders = "orders"; 
char *region = "region"; 
char *lineitem = "lineitem"; 

relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup () {
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";

	s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
	li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
}

void cleanup () {
	 delete s;
	 delete p;
	 delete ps;
	 delete n;
	 delete li;
	 delete r;
	 delete o;
	 delete c;
}

#endif
