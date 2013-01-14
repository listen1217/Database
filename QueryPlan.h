#ifndef QUERY_PLAN_
#define QUERY_PLAN_
#include "Statistics.h"
#include "ParseTree.h"
#include "Record.h"
#include "Function.h"
#include "Schema.h"
#include "ComparisonEngine.h"
#include "Comparison.h"
#include "Defs.h"
#include <iostream>
#include <stdio.h>

using namespace std;


#define TYPE_SF 1
#define TYPE_SP 2
#define TYPE_JN 3
#define TYPE_PJ 4
#define TYPE_SM 5
#define TYPE_GB 6
#define TYPE_DIST 7

typedef struct RelNameList {

	// this is the original table name
	char *tableName;

	// this is the value it is aliased to
	char *aliasAs;

}RelNameList;

typedef char strName[32];

typedef struct RelNameValue{
    char name[32];
    int numDistinct;
    int numAtts;
}RelNameValue;

typedef struct AttNameValue{
    char name[32];
    int numDistinct;
}AttNameValue;

typedef struct TreeNode{
    int type;
    int left_pipe;
    int right_pipe;
    int out_pipe;
    char * relName;
    char * aliasAs;
    CNF * cnf;
    int * attsToKeep;
    int numAttsInput;
    int numAttsOutput;
    Function * function;
    OrderMaker * ordermaker;
    Schema * s;
    Record * literal;
    TreeNode * leftchild;
    TreeNode * rightchild;
}TreeNode;

class QueryPlan
{
  private:
    //-------data----------
    int relNum;
    RelNameList * p_relNames;
    int ** p_WhoJoinWho;
    double * allNumDistincts;
    int ** OptSplit;
    Statistics state;
    int pipeID;
    int sp_need;

    //-------internal functions-------------
    void GetJoinExpression(int i, int j);
    void Initialization();
    int GetRelNameIndex(const char * name);
    void OrderNames();
    void Selection();
    void RemoveDot(char * to, const char * from);
    void FindOptJoin();
    void  ExchangeOrder(int a, int b);
    void RecursivelyBuildJoinTree(int a, int b,TreeNode * p);
    void GetJoinExpressions();
    void BuildJoinAndList(AndList * al, int a, int b);
    void BuildSelectAndList(AndList * al, int a, Schema * s);
    void BuildDummySelectDisjunction(AndList * al, int a);
    int GetAliasName(char * to, const char * from);
    int RecursiveReturnIndex(char * aliasAs, char * attName, TreeNode * p_Node, Type &t);
    void AddSumNode();
    void AddProjectNode();
    void  AddGroupNode();
    void BuildFunctionList(FuncOperator * f, FuncOperator * from);
    void AddDistNode();
    int SetPipeID(struct TreeNode * p);
    void InOrderPrint(struct TreeNode * p);
    void BuildSelectPipeAndList(AndList * al, Schema * s);
    void AddSPNode();
  public:
    //constructor builds the query plan, a QueryPlan object needs to be create after yyparse() is called
    //it will use the info extracted by yyparse()
    TreeNode * p_QueryPlanTreeRoot;
    QueryPlan();
    void Print();
    ~QueryPlan();

};

#endif
