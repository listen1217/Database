#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>
#include <cstring>
#include <pthread.h>
#include "RelOp.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "ParseTree.h"
#include "BigQ.h"
#include "QueryPlan.h"
#include "main.h"

//#include "stdafx.h"



using namespace std;


SelectFile * sf[10];
SelectPipe * sp;
Join * jn[10];
Project * pj;
GroupBy * gb;
Sum * sm;
DuplicateRemoval * dist;

static int joinCount = 0;
static int dbfileCount = 0;
DBFile * dbfile[10];
Pipe * pipes[20];


void select(struct TreeNode * p)
{
    if(p == NULL)
	return;
    if(p->type == TYPE_SF){
	char path[100]="\0";
	strcpy(path,dbfile_dir);
	strcat(path,p->relName);
	strcat(path,".bin");
	dbfile[dbfileCount]->Open(path);
	sf[dbfileCount]->Run(*(dbfile[dbfileCount]), *(pipes[p->out_pipe]), *(p->cnf), *(p->literal));
	dbfileCount++;
	return;
    }
    if(p->leftchild)
	select(p->leftchild);
    if(p->rightchild)
	select(p->rightchild);
    switch(p->type){
    case TYPE_SP:
	printf("Select Pipe Operation.\n");
	sp->Run(*(pipes[p->left_pipe]),*(pipes[p->out_pipe]),*(p->cnf), *(p->literal));
	break;
    case TYPE_JN:
	printf("Join Operation.\n");
	jn[joinCount++]->Run(*(pipes[p->left_pipe]),*(pipes[p->right_pipe]),*(pipes[p->out_pipe]),*(p->cnf), *(p->literal));
	break;
    case TYPE_PJ:
	printf("Project Operation.\n");
	pj->Run(*(pipes[p->left_pipe]),*(pipes[p->out_pipe]),p->attsToKeep,p->numAttsInput,p->numAttsOutput);
	break;
    case TYPE_SM:
	printf("Sum Operation.\n");
	sm->Run(*(pipes[p->left_pipe]),*(pipes[p->out_pipe]),*(p->function));
	break;
    case TYPE_GB:
	printf("GroupBy Operation.\n");
	gb->Run(*(pipes[p->left_pipe]),*(pipes[p->out_pipe]),*(p->ordermaker),*(p->function));
	break;
    case TYPE_DIST:
	printf("Distinct Operation.\n");
	dist->Run(*(pipes[p->left_pipe]),*(pipes[p->out_pipe]),*(p->s));
	break;
    }
    return;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int main () {
	
	setup();
	cout<<"DBMS has been set up."<<endl;
	
	while(true) {
		cout<<"Please enter SQL and press Ctrl-D to execute: "<<endl;
		
		if(yyparse() == 0) {
			//cout<<"query type: "<<query_mode<<endl;
			//SELECT
			if(query_mode == 0) {
			    QueryPlan qp;
 
			    qp.Print();
			    for(int i = 0; i< 20; i++)
				pipes[i] = new Pipe(100);
			    for(int i = 0;i<10;i++){
				dbfile[i] = new DBFile;
				sf[i] = new SelectFile;
				jn[i] = new Join;
			    }
			    pj = new Project;
			    gb = new GroupBy;
			    sm = new Sum;
			    dist = new DuplicateRemoval;
			    cout << "start selection" <<endl;
			    select(qp.p_QueryPlanTreeRoot);
			    int cnt = 0;
			    if(myfile == NULL){
				cnt = clear_pipe(*(pipes[qp.p_QueryPlanTreeRoot->out_pipe]),qp.p_QueryPlanTreeRoot->s ,true);
				cout << "returned "<<cnt<< " records." <<endl;
				continue;
			    }
			    if(strcmp(myfile,"NONE") == 0){
				cnt = clear_pipe(*(pipes[qp.p_QueryPlanTreeRoot->out_pipe]),qp.p_QueryPlanTreeRoot->s ,false);
				cout << "returned "<<cnt<< " records." <<endl;
				
			    }else if (strcmp(myfile,"STDOUT") == 0){
				cnt = clear_pipe(*(pipes[qp.p_QueryPlanTreeRoot->out_pipe]),qp.p_QueryPlanTreeRoot->s ,true);
				cout << "returned "<<cnt<< " records." <<endl;
			    }else{
				WriteOut wrOut;
				FILE *writefile = fopen (myfile, "w");
				wrOut.Run(*(pipes[qp.p_QueryPlanTreeRoot->out_pipe]), writefile,*(qp.p_QueryPlanTreeRoot->s) );
				wrOut.WaitUntilDone ();
				cout << "write to file: "<<myfile<<endl;
			    }
			    
			}
			//CREATE
			if(query_mode == 1) {
				string tablename (tables->tableName);
				char tbl_path[100];
				sprintf (tbl_path, "%s%s.bin", dbfile_dir, tables -> tableName ); 

				NewAttr *atts = attsList;				
				UpdateCatalog(tablename, atts);
				
				OrderMaker o;
				struct SortInfo si;
				si.runLength = 8;
				si.myOrder = &o;
				
				
				DBFile dbfile;
				if(tableType == HEAP) {
					dbfile.Create(tbl_path, heap, &si);
				}
				dbfile.Close();
				cout<< "Table "<<tablename<<" created." << endl;

			}
			//INSERT
			if(query_mode == 2) {
				cout<<"insert "<<myfile<<" into "<<tables->tableName<<endl;
				string totable ( tables -> tableName );
				Schema myschema (catalog_path , tables -> tableName) ;
				
				char toDB[100];

				sprintf( toDB,"%s%s.bin", dbfile_dir, tables -> tableName);
				
				char fromFile[100];
				sprintf (fromFile, "%s%s", tpch_dir, myfile);
				
				DBFile dbfile;
				dbfile.Open( toDB );
				dbfile.Load(myschema, fromFile);

			}
			//DROP
			if(query_mode == 3) {
				string tName ( tables -> tableName);
				char tbl_path[100];
				sprintf (tbl_path, "%s%s.bin", dbfile_dir, tables -> tableName ); 
				if ( remove ( tbl_path ) != 0)
				{
					cout<< " Cannnot delete file :" << tbl_path <<endl;
				}
				DeleteTable(tName);
				char meta[100];
				sprintf (meta, "%s%s.binmeta", dbfile_dir, tables->tableName);
				remove(meta);
				cout<<"Table "<<tables->tableName<<" dropped"<<endl;

			}
			//SETOUT
			if(query_mode == 4) {
				cout<<"output set to "<<myfile<<endl;
			}
		}
	}
}

int UpdateCatalog(string tName, NewAttr *attsList) {
	ofstream out(catalog_path, ios::app);
	out<<"BEGIN"<<endl;
	out<<tName<<endl;
	out<<tName<<".tbl"<<endl;
	while(attsList) {
		out<<attsList->name<<" ";
		if(attsList->code == INT) {
			out<<"Int"<<endl;
		}
		else if(attsList->code == STRING) {
			out<<"String"<<endl;
		}
		else if(attsList->code == DOUBLE) {
			out<<"Double"<<endl;
		}
		attsList = attsList->next;
	}
	out<<"END"<<endl;
	out<<endl;
	out.close();
}

int DeleteTable(string tName) {
	string outFile ("catalog");
	string tmp= outFile  + "tmp";
	ifstream oldFile("catalog");
	ofstream newFile(tmp.c_str());

	if  (!( oldFile.is_open() && newFile.is_open())) {
		cout<< "Error for updating the catalogfile!!!!!!"<< endl;
		return -1;
	}

	string temp;
	vector<string> content;
	bool flag = false;
	string begin("BEGIN");
	string end("END");

	while( ! oldFile.eof()){
		getline (oldFile,temp);
		if  (!( temp.size() > 0))
			continue;
		content.push_back(temp);
		if ( temp.compare( end) == 0){
			if( content[1].compare(tName) != 0){
				for (int i =0; i < content.size(); ++i)
					newFile<< content[i]<< endl;
				newFile<<endl;
			}
			content.clear();
		}
	}
	newFile.close();
	oldFile.close();

	rename( tmp.c_str() , outFile.c_str() );

	return 0;

}
