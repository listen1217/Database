#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#define DATA_SET_MAX 10000

//----------internal data structures------------
typedef struct Names{
  char name[32];
  Names * next;
}Names, *p_Names;

typedef struct Node{
  int flag;
  int partition;
  char name[32];
  int num;
  p_Names contain_belong;
  Node * next;
}Node, *p_Node;

typedef struct RelNode{
  int flag;
  char l_rname[32];
  char r_rname[32];
  char l_attname[32];
  char r_attname[32];
  int opType;
  int relNext;
  RelNode * next;
}RelNode, *p_RelNode;

//----------internal data structures------------

class Statistics
{

 private:


	void AddRelWhenRead(char *relName, int numTuples, int partition);
	int  CheckParseTree (struct AndList *parseTree, char * relNames[], int numToJoin);
	int CheckRelNames(char *relNames[], int numToJoin);
	int CheckAtt(char * attName, char * relNames[], int numToJoin);
        int MergePartitions(int to, int from);
	void ParseTreeToRels(struct AndList *parseTree, int to_index,char * relNames[], int numToJoin);
	double AnalyzParseTree(struct AndList *parseTree,char * relNames[], int numToJoin);
	int getNumByName(char * relName, int flag);
 public:
	p_Node * dataset;
	p_Names * p_partition;
	unsigned int partition_count;
	p_RelNode * p_rels;
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
