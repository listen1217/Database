#include "ParseTree.h"
#include "QueryPlan.h"
#include <stdio.h>
#include <cstring>
#include "Statistics.h"

using namespace std;

#define REL_SIZE 8

#define ATT_SIZE 61

#define FILE_NAME "stat.txt"

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

void  QueryPlan::ExchangeOrder(int a, int b)
{

    int temp;
    char * p_temp;
    for(int j = 1; j<= relNum; j++){
	temp = p_WhoJoinWho[a][j];
	p_WhoJoinWho[a][j] = p_WhoJoinWho[b][j];
	p_WhoJoinWho[b][j] = temp;
    }
    //exchange all columns in p_WhoJoinWho[x][j] with p_WhoJoinWho[x][1]
    for(int j = 1; j<= relNum; j++){
	temp = p_WhoJoinWho[j][a];
	p_WhoJoinWho[j][a] = p_WhoJoinWho[j][b];
	p_WhoJoinWho[j][b] = temp; 
    }
    //exchange alias
    p_temp = p_relNames[b].aliasAs;
    p_relNames[b].aliasAs = p_relNames[a].aliasAs;
    p_relNames[a].aliasAs = p_temp;
    //exchange name
    p_temp = p_relNames[b].tableName;
    p_relNames[b].tableName = p_relNames[a].tableName;
    p_relNames[a].tableName = p_temp;
}

void QueryPlan::OrderNames()
{
    int firstOrLast = 0;
    char * p_temp;
    for(int i = 1; i <= relNum ; i++){
	int count = 0;
	for(int j = 1; j<= relNum; j++){
	    if(p_WhoJoinWho[i][j] == 1)
		count++;
	}
	if(count == 1){
	    if(firstOrLast == 0){
		firstOrLast = 1;
		//exchange all columns in p_WhoJoinWho[i][x] with p_WhoJoinWho[1][x]
		ExchangeOrder(1,i);
	    }else{
		ExchangeOrder(relNum,i);
	    }
	}
    }

    for(int k = 1; k < relNum;k++){
	int loc = 0;
	for(int i = k+1; i<relNum ;i++){
	    if(p_WhoJoinWho[k][i] == 1){
		loc = i;
		break;
	    }
	}
	if(loc!=k+1&&loc!=0)
	    ExchangeOrder(loc,k+1);
    }
}

int QueryPlan:: GetRelNameIndex(const char * name)
{
    int dotLoc = 0;
    const char * p_char = name;
    char tempName[32] = "\0";
    for(int si = 0; si < strlen(p_char); si++){
	if(p_char[si] == '.'){
	    dotLoc = si;
	    break;
	}
    }
    if(dotLoc){
	  
	int si = 0;
	for(si = 0; si< dotLoc ;si++){
	    tempName[si] = p_char[si];
	}
	tempName[si] = '\0';

	//find aliase in p_relNames
	for(int i = 1; i<=relNum ; i++){
	    if(strcmp(tempName,p_relNames[i].aliasAs) == 0){
		return i;
	    }
	}

	//did not find match
	return -1;
    }

    return -1;

}
void  QueryPlan:: GetJoinExpressions()
{
    //p_WhoJoinWho[1:n][1:n] (char*) stores all expressions
    //relNum
    //AndList * boolean
    //all names and aliases are stored in p_relNames[1:relNum]
    AndList *parseTree = boolean;
    for (int whichAnd = 0 ; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
	struct OrList *myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL)
		break;
	    if (myOr->left->left->code == NAME && myOr->left->right->code == NAME) {
		int index_left = GetRelNameIndex(myOr->left->left->value);
		int index_right = GetRelNameIndex(myOr->left->right->value);
		if(index_left > 0 && index_right > 0){
		    p_WhoJoinWho[index_left][index_right] = 1;
		    p_WhoJoinWho[index_right][index_left] = 1;
		}else{
		    printf("error in query plan: getjoinExpressions()\n");
		}
	    }else{
		break;
	    }

	}
    }

}

void QueryPlan:: Initialization()
{
     //----get # of relations from TableList * tables
    relNum = 0;
    TableList *p_TableList = tables;
    while(p_TableList!=NULL){
	p_TableList = p_TableList->next;
	relNum++;
    }
    //---# of relations got

    //----initialization of relation names
    p_relNames = new RelNameList[relNum+1];
    p_TableList = tables;
    for(int i = 1; i < relNum+1; i++, p_TableList = p_TableList->next){

	p_relNames[i].tableName = new char[32];
	memset(p_relNames[i].tableName,0,32);
	strcpy(p_relNames[i].tableName, p_TableList->tableName);

	p_relNames[i].aliasAs = new char[32];
	memset(p_relNames[i].aliasAs,0,32);
	strcpy(p_relNames[i].aliasAs,p_TableList->aliasAs);

    }//for
    //-----relation names got

    //--initialize join expression
    p_WhoJoinWho = new int * [relNum+1];
    for(int i = 1; i <= relNum ; i++){
	p_WhoJoinWho[i] = new int[relNum];
	for(int j = 1; j<= relNum; j++){
	    p_WhoJoinWho[i][j] = 0;
	}
    }
    //---store all boolean expression flags in p_WhoJoinWho[1:n][1:n] <- (0/1)
    GetJoinExpressions();
    OrderNames();
    allNumDistincts = new double[relNum+1];
}
int QueryPlan:: GetAliasName(char * to, const char * from)
{
    int dotLoc = 0;
    const char * p_char = from;
    char tempName[32] = "\0";
    int si = 0;
    for(si = 0; si < strlen(p_char); si++){
	if(p_char[si] == '.'){
	    dotLoc = si;
	    break;
	}
	tempName[si] = p_char[si];
    }
    tempName[dotLoc] = '\0';
    strcpy(to,tempName);
    if(dotLoc == 0)
	return 0;
    else
	return 1;
}

void QueryPlan::RemoveDot(char * to, const char * from)
{
    int dotLoc = 0;
    const char * p_char = from;
    char tempName[32] = "\0";
    for(int si = 0; si < strlen(p_char); si++){
	if(p_char[si] == '.'){
	    dotLoc = si;
	    break;
	}
    }
    if(dotLoc){
	  
	int si = dotLoc;
	for(si = dotLoc+1; si< strlen(p_char) ;si++){
	    tempName[si-dotLoc-1] = p_char[si];
	}
	tempName[si-dotLoc-1] = '\0';
	strcpy(to,tempName);
    }else{
	strcpy(to,from);
    }
   
}

void QueryPlan:: Selection()
{
    char ** p = new char*[1];
    //for each relation, do a selection updating allNumDistincs
    //find by p_relNames[i].aliasAs
    for(int i = 1; i<=relNum ;i++){
	p[0] = p_relNames[i].aliasAs;
	allNumDistincts[i] = state.Estimate(boolean,p,1);
    }
    delete[] p;
}
void QueryPlan:: FindOptJoin()
{
    double ** OptCost = new double* [relNum+1];
    OptSplit = new int* [relNum+1];
    char  ** allNames = new char* [relNum+1];
    for(int i = 0; i<=relNum; i++){
	OptCost[i] = new double[relNum+1];
	OptSplit[i] = new int[relNum+1];
	allNames[i] = new char[32];
	memset(allNames[i],0,32);
    }
    


    for(int i = 1; i<=relNum; i++){
	OptCost[i][i] = 0;
	OptSplit[i][i] = 0;
	strcpy(allNames[i],p_relNames[i].aliasAs);
    }
    for(int i = 2; i <= relNum; i++){
	for(int j = 1; j+i-1<=relNum;j++){
	    //OptCost[j][j+i-1]
	    int min_index;
	    double min_cost;
	    int count = 0;
	    double result;
	    double temp_mul = 0;
	    for(int k = j; k < j+i-1;k++){
		result = OptCost[j][k]+OptCost[k+1][j+i-1];
		temp_mul = state.Estimate(boolean,&(allNames[j]),k-j+1);
		temp_mul *= state.Estimate(boolean,&(allNames[k+1]),j+i-1-k);
		if(count == 0){
		    min_index = k;
		    min_cost = result+temp_mul;
		}else{
		    if(result+temp_mul<min_cost){
			min_cost = result+temp_mul;
			min_index = k;
		    }
		}
		count++;
	    }
	    OptCost[j][j+i-1] = min_cost;
	    OptSplit[j][j+i-1] = min_index;
	}
    }
    printf("OptCost = %f\n", OptCost[1][relNum]);

    

    
    for(int i = 0; i<=relNum; i++){
	delete [] OptCost[i];
	delete[] allNames[i];
    }
    delete[] OptCost;
    delete[] allNames;

}
void QueryPlan:: BuildSelectAndList(AndList * al, int a, Schema * s)
{

    
    struct AndList *parseTree = boolean;
    struct AndList * p = al;
    struct OrList * p_or = NULL;
    int andCount = 0;
    for (int whichAnd = 0 ; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
	int orCount = 0;
	struct OrList *myOr = parseTree->left;

	//check whether a select pipe needed
	//for example: (l.l_tax < 100 OR o.o_key > 1)

	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    if (myOr == NULL){
		
		break;
	    }
	    if (myOr->left->left->code == NAME && myOr->left->right->code != NAME) {
		//example: l.l_tax > 0.1
		int index_left = GetRelNameIndex(myOr->left->left->value);
		
		if(index_left == a){
		  OrList* tempOr = parseTree->left;
		  int allSame = 1;
		  while(tempOr){
		    if( a != GetRelNameIndex(myOr->left->left->value))
		      allSame = 0;
		    tempOr = tempOr->rightOr;
		  }
		  if(allSame == 0){
		    sp_need = 1;
		    continue;
		  }
		    if(orCount == 0){
			if(andCount>0){
			    p->rightAnd = new AndList;
			    p = p->rightAnd;
			    p->rightAnd = NULL;
			}
			andCount++;
			p->left = new OrList;
			p->left->left = new ComparisonOp;
			p->left->rightOr = NULL;
			p->left->left->code = myOr->left->code;
			p->left->left->left = new Operand;
			p->left->left->right = new Operand;
			p->left->left->left->code = myOr->left->left->code;
			
			
			p->left->left->left->value = new char[32];
			memset( p->left->left->left->value, 0, 32);
			RemoveDot(p->left->left->left->value,myOr->left->left->value);

			Type temp_t = s->FindType(p->left->left->left->value);
			if(temp_t == Int){
			    p->left->left->right->code = INT;
			}else if(temp_t == String){
			    p->left->left->right->code = STRING;
			}else if(temp_t == Double){
			    p->left->left->right->code = DOUBLE;
			}

			p->left->left->right->value = new char[strlen(myOr->left->right->value)+1];
			strcpy(p->left->left->right->value,myOr->left->right->value);
			p_or = p->left;
		    }else{
			p_or->rightOr = new OrList;
			p_or = p_or->rightOr;
			p_or->left = new ComparisonOp;
			p_or->rightOr = NULL;
			p_or->left->code = myOr->left->code;
			p_or->left->left = new Operand;
			p_or->left->right = new Operand;
			p_or->left->left->code = myOr->left->left->code;
			p_or->left->left->value = new char[32];
			memset( p_or->left->left->value, 0, 32);
			RemoveDot(p_or->left->left->value,myOr->left->left->value);
			Type temp_t = s->FindType(p_or->left->left->value);
			if(temp_t == Int){
			    p_or->left->right->code = INT;
			}else if(temp_t == String){
			    p_or->left->right->code = STRING;
			}else if(temp_t == Double){
			    p_or->left->right->code = DOUBLE;
			}
			p_or->left->right->value = new char[strlen(myOr->left->right->value)+1];
			strcpy(p_or->left->right->value,myOr->left->right->value);
		    }
		    orCount++;
		}
		
	    }
	    if (myOr->left->left->code != NAME && myOr->left->right->code == NAME) {
		//example: 0.1 > l.l_tax
		int index_right = GetRelNameIndex(myOr->left->right->value);
		if(index_right == a){
		  OrList * tempOr = parseTree->left;
		  int allSame = 1;
		  while(tempOr){
		    if( a != GetRelNameIndex(myOr->left->right->value))
		      allSame = 0;
		    tempOr = tempOr->rightOr;
		  }
		  if(allSame == 0){
		    sp_need = 1;
		    continue;
		  }
		    if(orCount == 0){
			if(andCount>0){
			    p->rightAnd = new AndList;
			    p = p->rightAnd;
			    p->rightAnd = NULL;
			}
			andCount++;
			p->left = new OrList;
			p->left->left = new ComparisonOp;
			p->left->rightOr = NULL;
			p->left->left->code = myOr->left->code;
			p->left->left->left = new Operand;
			p->left->left->right = new Operand;
			
			p->left->left->right->code = myOr->left->right->code;
			p->left->left->right->value = new char[32];
			memset( p->left->left->right->value, 0, 32);
			RemoveDot(p->left->left->right->value,myOr->left->right->value);

			Type temp_t = s->FindType(p->left->left->right->value);
			if(temp_t == Int){
			   p->left->left->left->code = INT;
			}else if(temp_t == String){
			    p->left->left->left->code = STRING;
			}else if(temp_t == Double){
			    p->left->left->left->code = DOUBLE;
			}
			p->left->left->left->value = new char[strlen(myOr->left->left->value)+1];
			strcpy(p->left->left->left->value,myOr->left->left->value);
			p_or = p->left;
			
		    }else{
			p_or->rightOr = new OrList;
			p_or = p_or->rightOr;
			p_or->left = new ComparisonOp;
			p_or->rightOr = NULL;
			p_or->left->code = myOr->left->code;
			p_or->left->left = new Operand;
			p_or->left->right = new Operand;
		
			p_or->left->right->code = myOr->left->right->code;
			p_or->left->right->value = new char[32];
			memset( p_or->left->right->value, 0, 32);
			RemoveDot(p_or->left->right->value,myOr->left->right->value);
			Type temp_t = s->FindType(p_or->left->right->value);
			if(temp_t == Int){
			   p_or->left->left->code = INT;
			}else if(temp_t == String){
			    p_or->left->left->code = STRING;
			}else if(temp_t == Double){
			    p_or->left->left->code = DOUBLE;
			}
			p_or->left->left->value = new char[strlen(myOr->left->left->value)+1];
			strcpy(p_or->left->left->value,myOr->left->left->value);
		    }
		    orCount++;
		}
	    }
	    
	}//for or

    }//for
}
void QueryPlan:: BuildJoinAndList(AndList * al, int a, int b)
{
    //relation name[a].att = name[b].att
    //al = new AndList;
    al->left = new OrList;
    al->left->rightOr = NULL;
    al->rightAnd = NULL;

    AndList *parseTree = boolean;
    for (int whichAnd = 0 ; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
	struct OrList *myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL)
		break;
	    if (myOr->left->left->code == NAME && myOr->left->right->code == NAME) {
		int index_left = GetRelNameIndex(myOr->left->left->value);
		int index_right = GetRelNameIndex(myOr->left->right->value);
		if(index_left == a && index_right == b || index_left == b && index_right == a){
		    //join disjunction founded
		    al->left->left = new ComparisonOp;
		    // code should be '='
		    al->left->left->code = myOr->left->code;
		    //left operand
		    al->left->left->left = new Operand;
		    al->left->left->left->code = myOr->left->left->code;
		    al->left->left->left->value = new char[32];
		    memset( al->left->left->left->value, 0, 32);
		    RemoveDot(al->left->left->left->value,myOr->left->left->value);
		    //right operand
		    al->left->left->right = new Operand;
		    al->left->left->right->code = myOr->left->right->code;
		    al->left->left->right->value = new char[32];
		    memset( al->left->left->right->value, 0, 32);
		    RemoveDot(al->left->left->right->value,myOr->left->right->value);
		    break;
		    
		}
	    }else{
		break;
	    }

	}
    }
    
}

void QueryPlan::BuildDummySelectDisjunction(AndList * al, int a)
{
    AndList *parseTree = boolean;
    al->left = new OrList;
    al->left->rightOr = NULL;
    al->rightAnd = NULL;
    for (int whichAnd = 0 ; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
	struct OrList *myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL)
		break;
	    if (myOr->left->left->code == NAME && myOr->left->right->code == NAME) {
		int index_left = GetRelNameIndex(myOr->left->left->value);
		int index_right = GetRelNameIndex(myOr->left->right->value);
		if(index_left == a){
		    //join disjunction founded
		    al->left->left = new ComparisonOp;
		    // code should be '='
		    al->left->left->code = myOr->left->code;
		    //left operand
		    al->left->left->left = new Operand;
		    al->left->left->left->code = myOr->left->left->code;
		    al->left->left->left->value = new char[32];
		    memset( al->left->left->left->value, 0, 32);
		    RemoveDot(al->left->left->left->value,myOr->left->left->value);
		    //right operand
		    al->left->left->right = new Operand;
		    al->left->left->right->code = myOr->left->left->code;
		    al->left->left->right->value = new char[32];
		    memset( al->left->left->right->value, 0, 32);
		    strcpy(al->left->left->right->value,al->left->left->left->value);
		    return;
		    
		}
		if(index_right == a){
		    //join disjunction founded
		    al->left->left = new ComparisonOp;
		    // code should be '='
		    al->left->left->code = myOr->left->code;
		    //left operand
		    al->left->left->right = new Operand;
		    al->left->left->right->code = myOr->left->right->code;
		    al->left->left->right->value = new char[32];
		    memset( al->left->left->right->value, 0, 32);
		    RemoveDot(al->left->left->right->value,myOr->left->right->value);
		    //right operand
		    al->left->left->left = new Operand;
		    al->left->left->left->code = myOr->left->right->code;
		    al->left->left->left->value = new char[32];
		    memset( al->left->left->left->value, 0, 32);
		    strcpy(al->left->left->left->value,al->left->left->right->value);
		    return;
		    
		}
	    }else{
		break;
	    }

	}
    }
}
void QueryPlan:: RecursivelyBuildJoinTree(int a, int b, TreeNode * p_Node)
{
    
    int split = OptSplit[a][b];
    if(split == 0){
	//leaf node reached

	//write all info into tree node
	/*typedef struct TreeNode{
	  int type;
	  CNF * cnf;
	  int * attsToKeep;
	  int numAttsInput;
	  int numAttsOutput;
	  Function * function;
	  Record * literal;
	  OrderMaker * ordermaker;
	  Schema * s;
	  TreeNode * leftchild;
	  TreeNode * rightchild;
	  }TreeNode;
	*/
	p_Node->type = TYPE_SF;
	p_Node->leftchild = NULL;
	p_Node->rightchild = NULL;
	p_Node->attsToKeep = NULL;
	p_Node->function = NULL;
	p_Node->relName = new char[32];
	memset(p_Node->relName,0,32);
	strcpy(p_Node->relName,p_relNames[a].tableName);
	p_Node->aliasAs = new char[32];
	memset(p_Node->aliasAs,0,32);
	strcpy(p_Node->aliasAs,p_relNames[a].aliasAs);
	p_Node->numAttsInput = 0;
	p_Node->numAttsOutput = 0;
	p_Node->ordermaker = NULL;

	p_Node->s = new Schema("catalog",p_relNames[a].tableName);
	p_Node->literal = new Record;
	p_Node->cnf = new CNF;
	AndList * al = new AndList;
	al->rightAnd = NULL;
	al->left = NULL;
	BuildSelectAndList(al,a,p_Node->s);
	if(al->rightAnd == NULL && al->left == NULL){
	    BuildDummySelectDisjunction(al,a);
	}
	p_Node->cnf->GrowFromParseTree(al, p_Node->s, *(p_Node->literal));

	return;
    }
    p_Node->leftchild = new TreeNode;
    p_Node->rightchild = new TreeNode;
    RecursivelyBuildJoinTree(a,split,p_Node->leftchild);
    RecursivelyBuildJoinTree(split+1,b,p_Node->rightchild);
    
    //suck all info into a Join Node, type: TYPE_JN
    p_Node->type = TYPE_JN;
    p_Node->attsToKeep = NULL;
    p_Node->function = NULL;
    p_Node->numAttsInput = 0;
    p_Node->numAttsOutput = 0;
    p_Node->ordermaker = NULL;
    p_Node->relName = NULL;
    p_Node->aliasAs = NULL;

    //join the two schema
    p_Node->s = new Schema(p_Node->leftchild->s, p_Node->rightchild->s);
    p_Node->cnf = new CNF;
    p_Node->literal = new Record;
    AndList * al = new AndList;
    BuildJoinAndList(al,split, split+1);
    p_Node->cnf-> GrowFromParseTree (al, p_Node->leftchild->s, p_Node->rightchild->s, *(p_Node->literal));
    
    
}

int QueryPlan::RecursiveReturnIndex(char * aliasAs, char * attName, TreeNode * p_Node,Type &t)
{
    
    if(p_Node->type == TYPE_SF || p_Node->type ==TYPE_SP){
	if(strcmp(p_Node->aliasAs,aliasAs) == 0){
	    t = p_Node->s->FindType(attName);
	    return p_Node->s->Find(attName);
	}
	else
	    return -1;
    }
    if(p_Node->s->Find(attName) == -1)
	return -1;
    int returnValue = RecursiveReturnIndex(aliasAs,attName,p_Node->leftchild,t);
    if( returnValue == -1){
	return RecursiveReturnIndex(aliasAs,attName,p_Node->rightchild,t)+p_Node->leftchild->s->GetNumAtts();
    }else{
	return returnValue;
    }
}
void QueryPlan:: AddProjectNode()
{
    
    TreeNode * p_newNode = new TreeNode;
    p_newNode->aliasAs = NULL;
    p_newNode->cnf = NULL;
    p_newNode->function = NULL;
    p_newNode->leftchild = p_QueryPlanTreeRoot;
    p_newNode->rightchild = NULL;
    p_newNode->type = TYPE_PJ;
    p_QueryPlanTreeRoot = p_newNode;

    struct NameList * p_atts = attsToSelect;
    int numToProject = 0;
    while(p_atts != NULL){
	numToProject++;
	p_atts = p_atts->next;
    }
    p_newNode->attsToKeep = new int[numToProject];
    p_newNode->numAttsOutput = numToProject;
    p_newNode->numAttsInput = p_newNode->leftchild->s->GetNumAtts();
    Attribute * atts = new Attribute[numToProject];

    char * attName = new char[32];
    char * aliasAs = new char[32];
    p_atts = attsToSelect;
    for(int i = 0; p_atts!=NULL; p_atts = p_atts->next, i++){
	Type temp_t;
	memset(attName,0,32);
	RemoveDot(attName,p_atts->name);
	memset(aliasAs,0,32);
	int haveAlias = GetAliasName(aliasAs,p_atts->name);
	
	if(haveAlias == 0){
	    //lookup in root schema to decide indices
	    p_newNode->attsToKeep[i] = p_newNode->leftchild->s->Find(p_atts->name);
	    temp_t = p_newNode->leftchild->s->FindType(p_atts->name);
	}else{
	    p_newNode->attsToKeep[i] = RecursiveReturnIndex(aliasAs,attName,p_newNode->leftchild,temp_t);
	}
	atts[i].name = new char[32];
	memset(atts[i].name,0,32);
	strcpy(atts[i].name,attName);
	atts[i].myType = temp_t;
    }
    p_newNode->s = new Schema("", numToProject,atts);
    delete[] attName;
    delete[] aliasAs;
}
void QueryPlan::BuildFunctionList(FuncOperator * f, FuncOperator * from)
{
    f->code = from->code;
    if(from->leftOperand){
	f->leftOperand = new FuncOperand;
	f->leftOperand->code = from->leftOperand->code;
	f->leftOperand->value = new char[32];
	memset(f->leftOperand->value,0,32);
	RemoveDot(f->leftOperand->value,from->leftOperand->value);
    }else{
	f->leftOperand = NULL;
    }
    if(from->leftOperator!=NULL){
	f->leftOperator = new FuncOperator;
	BuildFunctionList(f->leftOperator,from->leftOperator);
    }else{
	f->leftOperator = NULL;
    }
    if(from->right != NULL){
	f->right = new FuncOperator;
	BuildFunctionList(f->right,from->right);
    }else{
	f->right = NULL;
    }
    
}
void QueryPlan:: AddSumNode()
{
    struct FuncOperator * f = new FuncOperator;
    BuildFunctionList(f,finalFunction);
    struct TreeNode * p_new = new TreeNode;
    p_new->type = TYPE_SM;
    p_new->function = new Function;
    p_new->function->GrowFromParseTree(f,*(p_QueryPlanTreeRoot->s));
    Attribute DA = {"sum", Double};
    p_new->s = new Schema("sum_sch", 1, &DA);
    p_new->leftchild = p_QueryPlanTreeRoot;
    p_new->rightchild = NULL;
    p_QueryPlanTreeRoot = p_new;
    
}
/*void QueryPlan::SetOrderMaker(OrderMaker * o)
{
    o = new OrderMaker;
    struct NameList * p_name = groupingAtts;
    o->numAtts = 0;
    char tempName[32];

    while(p_name){
	
	memset(tempName,0,32);
	RemoveDot(tempName,p_name->name);
	o->whichAtts[o->numAtts] = p_QueryPlanTreeRoot->s->Find(tempName);
	o->whichTypes[o->numAtts] = p_QueryPlanTreeRoot->s->FindType(tempName);
	o->numAtts++;
	p_name = p_name->next;
    }
    
    }*/

void QueryPlan:: AddGroupNode()
{
    struct FuncOperator * f = new FuncOperator;
    BuildFunctionList(f,finalFunction);
    struct TreeNode * p_new = new TreeNode;
    p_new->type = TYPE_GB;
    p_new->function = new Function;
    p_new->function->GrowFromParseTree(f,*(p_QueryPlanTreeRoot->s));
    p_new->function->Print();
    // SetOrderMaker(p_new->ordermaker);
    p_new->ordermaker = new OrderMaker;
    
    struct NameList * p_name = groupingAtts;
    p_new->ordermaker->numAtts = 0;
    char tempName[32];
    int countAtt = 0;
    while(p_name){
	p_name = p_name->next;
	countAtt++;
    }
    struct Attribute * atts = new Attribute[countAtt+1];
    atts[0].myType = Double;
    atts[0].name = new char[32];
    memset(atts[0].name,0,32);
    strcpy(atts[0].name,"sumGroupBy");
    p_name = groupingAtts;
    while(p_name){
	
	memset(tempName,0,32);
	RemoveDot(tempName,p_name->name);
	p_new->ordermaker->whichAtts[p_new->ordermaker->numAtts] = p_QueryPlanTreeRoot->s->Find(tempName);
	p_new->ordermaker->whichTypes[p_new->ordermaker->numAtts] = p_QueryPlanTreeRoot->s->FindType(tempName);
	atts[p_new->ordermaker->numAtts+1].name = new char[32];
	memset(atts[p_new->ordermaker->numAtts+1].name,0,32);
	strcpy(atts[p_new->ordermaker->numAtts+1].name,tempName);
	atts[p_new->ordermaker->numAtts+1].myType = p_new->ordermaker->whichTypes[p_new->ordermaker->numAtts];
	p_new->ordermaker->numAtts++;
	p_name = p_name->next;
    }
    p_new->s = new Schema("groupby_sch",countAtt+1,atts);
    p_new->leftchild = p_QueryPlanTreeRoot;
    p_new->rightchild = NULL;
    p_QueryPlanTreeRoot = p_new;
    
}
void QueryPlan :: AddDistNode()
{
    struct TreeNode * p_new = new TreeNode;
    p_new->s = p_QueryPlanTreeRoot->s;
    p_new->type = TYPE_DIST;
    p_new->leftchild = p_QueryPlanTreeRoot;
    p_new->rightchild = NULL;
    p_QueryPlanTreeRoot = p_new;
}
int QueryPlan:: SetPipeID(struct TreeNode * p)
{
    p->out_pipe = pipeID++;
    if(p->leftchild == NULL && p->rightchild == NULL){
	if(p->type == TYPE_SF){
	    p->left_pipe = 0;
	    p->right_pipe = 0;
	    return  p->out_pipe;
	}else if(p->type == TYPE_DIST  || p->type == TYPE_SP || p->type == TYPE_SM || p->type == TYPE_GB || p->type == TYPE_PJ){
	    p->left_pipe = pipeID++;
	    p->right_pipe = 0;
	}else{
	    p->left_pipe = pipeID++;
	    p->right_pipe = pipeID++;    
	}

    }
    if(p->leftchild != NULL)
	p->left_pipe = SetPipeID(p->leftchild);
    if(p->rightchild != NULL)
	p->right_pipe = SetPipeID(p->rightchild);
    
    return p->out_pipe;
}
void QueryPlan :: InOrderPrint(struct TreeNode * p)
{
    if(p == NULL)
	return;
    if(p->leftchild != NULL)
	InOrderPrint(p->leftchild);
    
    //print
    switch(p->type){
    case TYPE_SP:
	printf("Select Pipe Operation.\n");
	printf("Input pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->out_pipe);
	p->s->Print();
	printf("Select CNF:\n");
	p->cnf->Print();
	printf("-----------------------------------\n");
	break;
    case TYPE_SF:
	printf("Select File Operation.\n");
	printf("Output pipe ID %d\nOutput Schema:\n",p->out_pipe);
	p->s->Print();
	printf("Select CNF:\n");
	p->cnf->Print();
	printf("-----------------------------------\n");
	break;
    case TYPE_JN:
	printf("Join Operation.\n");
	printf("Input pipe ID %d\nInput pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->right_pipe,p->out_pipe);
	p->s->Print();
	printf("Join CNF:\n");
	p->cnf->Print();
	printf("-----------------------------------\n");
	break;
    case TYPE_PJ:
	printf("Project Operation.\n");
	printf("Input pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->out_pipe);
	p->s->Print();
	printf("Attributes to keep:\n\t");
	for(int i = 0;i<p->numAttsOutput;i++)
	    printf("#%d ",p->attsToKeep[i]);
	printf("\n");
	printf("-----------------------------------\n");
	break;
    case TYPE_SM:
	printf("Sum Operation.\n");
	printf("Input pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->out_pipe);
	p->s->Print();
	printf("Aggregate Function:\n");
	p->function->Print();
	printf("-----------------------------------\n");
	break;
    case TYPE_GB:
	printf("GroupBy Operation.\n");
	printf("Input pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->out_pipe);
	p->s->Print();
	printf("Aggregate Function:\n");
	p->function->Print();
	printf("OrderMaker:\n");
	p->ordermaker->Print();
	printf("-----------------------------------\n");
	break;
    case TYPE_DIST:
	printf("Distinct Operation.\n");
	printf("Input pipe ID %d\nOutput pipe ID %d\nOutput Schema:\n",p->left_pipe,p->out_pipe);
	p->s->Print();
	printf("-----------------------------------\n");
	break;
    }
    
    if(p->rightchild != NULL)
	InOrderPrint(p->rightchild);
    
}
void QueryPlan:: Print()
{
    printf("--------Optimized Query Plan--------\n");
    InOrderPrint(p_QueryPlanTreeRoot);
}

void QueryPlan:: BuildSelectPipeAndList(AndList * al, Schema * s)
{

    
    struct AndList *parseTree = boolean;
    struct AndList * p = al;
    struct OrList * p_or = NULL;
    int andCount = 0;
    int relFlag[20];
    for(int i = 0;i<10;i++)
	relFlag[i] = 0;
    for (int whichAnd = 0 ; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
	int orCount = 0;
	struct OrList *myOr = parseTree->left;

	//check whether a select pipe needed
	//for example: (l.l_tax < 100 OR o.o_key > 1)
	int allSame = 1;
	int countRel = 0;
	for(int i = 0;i<20;i++)
	    relFlag[i] = 0;
	while(myOr){
	    int index =  GetRelNameIndex(myOr->left->left->value);
	    if(relFlag[index] == 0){
		countRel++;
		relFlag[index] = 1;
	    }
	    
	    myOr = myOr->rightOr;
	}
	
	if(countRel <= 1){
	    continue;
	}
	myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    if (myOr == NULL){
		
		break;
	    }
	    if (myOr->left->left->code == NAME && myOr->left->right->code != NAME) {
		//example: l.l_tax > 0.1
		    if(orCount == 0){
			if(andCount>0){
			    p->rightAnd = new AndList;
			    p = p->rightAnd;
			    p->rightAnd = NULL;
			}
			andCount++;
			p->left = new OrList;
			p->left->left = new ComparisonOp;
			p->left->rightOr = NULL;
			p->left->left->code = myOr->left->code;
			p->left->left->left = new Operand;
			p->left->left->right = new Operand;
			p->left->left->left->code = myOr->left->left->code;
					
			p->left->left->left->value = new char[32];
			memset( p->left->left->left->value, 0, 32);
			RemoveDot(p->left->left->left->value,myOr->left->left->value);

			Type temp_t = s->FindType(p->left->left->left->value);
			if(temp_t == Int){
			    p->left->left->right->code = INT;
			}else if(temp_t == String){
			    p->left->left->right->code = STRING;
			}else if(temp_t == Double){
			    p->left->left->right->code = DOUBLE;
			}

			p->left->left->right->value = new char[strlen(myOr->left->right->value)+1];
			strcpy(p->left->left->right->value,myOr->left->right->value);
			p_or = p->left;
		    }else{
			p_or->rightOr = new OrList;
			p_or = p_or->rightOr;
			p_or->left = new ComparisonOp;
			p_or->rightOr = NULL;
			p_or->left->code = myOr->left->code;
			p_or->left->left = new Operand;
			p_or->left->right = new Operand;
			p_or->left->left->code = myOr->left->left->code;
			p_or->left->left->value = new char[32];
			memset( p_or->left->left->value, 0, 32);
			RemoveDot(p_or->left->left->value,myOr->left->left->value);
			Type temp_t = s->FindType(p_or->left->left->value);
			if(temp_t == Int){
			    p_or->left->right->code = INT;
			}else if(temp_t == String){
			    p_or->left->right->code = STRING;
			}else if(temp_t == Double){
			    p_or->left->right->code = DOUBLE;
			}
			p_or->left->right->value = new char[strlen(myOr->left->right->value)+1];
			strcpy(p_or->left->right->value,myOr->left->right->value);
		    }
		    orCount++;	
		
	    }
	    if (myOr->left->left->code != NAME && myOr->left->right->code == NAME) {
		//example: 0.1 > l.l_tax
		    if(orCount == 0){
			if(andCount>0){
			    p->rightAnd = new AndList;
			    p = p->rightAnd;
			    p->rightAnd = NULL;
			}
			andCount++;
			p->left = new OrList;
			p->left->left = new ComparisonOp;
			p->left->rightOr = NULL;
			p->left->left->code = myOr->left->code;
			p->left->left->left = new Operand;
			p->left->left->right = new Operand;
			
			p->left->left->right->code = myOr->left->right->code;
			p->left->left->right->value = new char[32];
			memset( p->left->left->right->value, 0, 32);
			RemoveDot(p->left->left->right->value,myOr->left->right->value);

			Type temp_t = s->FindType(p->left->left->right->value);
			if(temp_t == Int){
			   p->left->left->left->code = INT;
			}else if(temp_t == String){
			    p->left->left->left->code = STRING;
			}else if(temp_t == Double){
			    p->left->left->left->code = DOUBLE;
			}
			p->left->left->left->value = new char[strlen(myOr->left->left->value)+1];
			strcpy(p->left->left->left->value,myOr->left->left->value);
			p_or = p->left;
			
		    }else{
			p_or->rightOr = new OrList;
			p_or = p_or->rightOr;
			p_or->left = new ComparisonOp;
			p_or->rightOr = NULL;
			p_or->left->code = myOr->left->code;
			p_or->left->left = new Operand;
			p_or->left->right = new Operand;
		
			p_or->left->right->code = myOr->left->right->code;
			p_or->left->right->value = new char[32];
			memset( p_or->left->right->value, 0, 32);
			RemoveDot(p_or->left->right->value,myOr->left->right->value);
			Type temp_t = s->FindType(p_or->left->right->value);
			if(temp_t == Int){
			   p_or->left->left->code = INT;
			}else if(temp_t == String){
			    p_or->left->left->code = STRING;
			}else if(temp_t == Double){
			    p_or->left->left->code = DOUBLE;
			}
			p_or->left->left->value = new char[strlen(myOr->left->left->value)+1];
			strcpy(p_or->left->left->value,myOr->left->left->value);
		    }
		    orCount++;
	    }
	    
	}//for or

    }//for
}
void QueryPlan:: AddSPNode()
{
    struct TreeNode * p_new = new TreeNode;
    p_new->type = TYPE_SP;
    p_new->s = p_QueryPlanTreeRoot->s;
    p_new->rightchild = NULL;
    
    struct AndList * al = new AndList;
    al->left = NULL;
    al->rightAnd = NULL;
    BuildSelectPipeAndList(al, p_new->s);
    p_new->cnf = new CNF;
    p_new->literal = new Record;
    p_new->cnf->GrowFromParseTree (al, p_new->s, *(p_new->literal));
    p_new->leftchild = p_QueryPlanTreeRoot;
    p_QueryPlanTreeRoot = p_new;
}
QueryPlan:: QueryPlan()
{
    state.Read(FILE_NAME);
    sp_need = 0;
    Initialization();

    for(int i = 1; i<=relNum;i++){
	state.CopyRel(p_relNames[i].tableName,p_relNames[i].aliasAs);
    }
    Selection();
    FindOptJoin();
    p_QueryPlanTreeRoot = new TreeNode;
    RecursivelyBuildJoinTree(1,relNum,p_QueryPlanTreeRoot);

    if(sp_need == 1)
	AddSPNode();
    if(finalFunction == NULL)
	AddProjectNode();
    else{
	//aggregate
	if(groupingAtts == NULL){
	    //sum
	    AddSumNode();
	}else{
	    //group by
	    AddGroupNode();
	}
    }
    if(distinctAtts == 1){
	//distinct , non-aggregate
	AddDistNode();
    }
    if(distinctFunc == 1){
	//distinct , aggregate
    }
    pipeID = 1;
    int tempID = SetPipeID(p_QueryPlanTreeRoot);
    p_QueryPlanTreeRoot->out_pipe = tempID;
}


QueryPlan:: ~QueryPlan()
{
}
