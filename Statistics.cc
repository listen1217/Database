#include "Statistics.h"
#include <cstring>
#include <string>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
using namespace std;

unsigned int BKDRHash(char *str)
{
    unsigned int seed = 131; // 31 131 1313 13131 131313 etc..
    unsigned int hash = 0;
 
    while (*str)
	{
	    hash = hash * seed + (*str++);
	}
 
    return (hash & 0x7FFFFFFF);
}

Statistics::Statistics()
{
    dataset = new p_Node[DATA_SET_MAX];
    for(int i = 0; i < DATA_SET_MAX;i++)
	dataset[i] = NULL;
    p_partition = new p_Names[DATA_SET_MAX];
    for(int i = 0; i < DATA_SET_MAX;i++)
	p_partition[i] = NULL;
    partition_count = 0;

    p_rels = new p_RelNode[DATA_SET_MAX];
    for(int i = 0; i < DATA_SET_MAX;i++)
	p_rels[i] = NULL;
    //printf("p_rels: #%p\n",p_rels);
}
Statistics::Statistics(Statistics &copyMe)
{	
    /*	//delete all data in this
	//1. clear dataset
	//	for(int i = 0; i< DATA_SET_MAX; i++){
	//	    p_Node p_current = dataset[i];
	//	    p_Node toDelete = p_current;
	//	    while(toDelete!=NULL){
	//	        p_current = toDelete->next;
	//delete all contain_belong area
	//		p_Names p_current_name = toDelete->contain_belong;
	//		p_Names p_toDelete_name = toDelete->contain_belong;
	//		while(p_toDelete_name!=NULL){
	//		    p_current_name = p_toDelete_name->next;
	//		    delete p_toDelete_name;
	//		    p_toDelete_name = p_current_name;
	//		}
	delete toDelete;
	toDelete = p_current;
	}//while
	}//END for

	//2. clear partition info
	for(int i = 0; i< DATA_SET_MAX; i++){
	p_Names p_current = p_partition[i];
	p_Names toDelete = p_current;
	while(toDelete!=NULL){
	p_current = toDelete->next;
	delete toDelete;
	toDelete = p_current;
	}//while
	}//END for
	delete p_partition;
	delete dataset;
    */
    //do the copy
    dataset = new p_Node[DATA_SET_MAX];
    partition_count = copyMe.partition_count;
    p_Node * copyDataset = copyMe.dataset;
    for(int i = 0; i < DATA_SET_MAX; i++){
	    
	if( copyDataset[i] == NULL){
	    dataset[i] = NULL;
	    continue;
	}
	dataset[i] = new Node;
	p_Node currentFrom = copyDataset[i];
	p_Node current = dataset[i];
	while(currentFrom!=NULL){
		
	    current->flag = currentFrom->flag;
	    current->num = currentFrom->num;
	    current->partition = currentFrom->partition;
	    strcpy(current->name,currentFrom->name);
	    p_Names cbFrom = currentFrom->contain_belong;
	    p_Names pTempName;
	    if(cbFrom!=NULL){
		current->contain_belong = new Names;
		pTempName = current->contain_belong;
	    }
	    while(cbFrom!=NULL){
		strcpy(pTempName->name,cbFrom->name);
		if(cbFrom->next!=NULL){
		    pTempName->next = new Names;
		    pTempName = pTempName->next;
		    cbFrom = cbFrom->next;
		}else{
		    pTempName->next = NULL;
		    break;
		}
	    }
	    if(currentFrom->next!=NULL){
		current->next = new Node;
		current = current->next;
		currentFrom = currentFrom->next;
		    
	    }else{
		current->next = NULL;
		break;
	    }
		
	}//end while
    }//end for
    p_partition = new p_Names[DATA_SET_MAX];
    p_Names * copyFrom = copyMe.p_partition;
    for(int i = 0; i < DATA_SET_MAX;i++){
	p_partition[i] = NULL;
	if(copyFrom[i] == NULL)
	    continue;
	p_partition[i] = new Names;
	p_Names currentFrom = copyFrom[i];
	p_Names current = p_partition[i];
	while(currentFrom){
	    strcpy(current->name,currentFrom->name);
	    if(currentFrom->next){
		current->next = new Names;
		current = current->next;
		currentFrom = currentFrom->next;
	    }else{
		current->next = NULL;
		break;
	    }
	}//end while
    }//end for
    //copy p_rels
    p_rels = new p_RelNode[DATA_SET_MAX];
    p_RelNode * copyFromRel = copyMe.p_rels;
    for(int i = 0; i < DATA_SET_MAX;i++){
	p_rels[i] = NULL;
	if(copyFromRel[i] == NULL)
	    continue;
	p_rels[i] = new RelNode;
	p_RelNode currentFrom = copyFromRel[i];
	p_RelNode current = p_rels[i];
	while(currentFrom){
	    current->relNext = currentFrom->relNext;
	    current->flag = currentFrom->flag;
	    current->opType = currentFrom->opType;
	    strcpy(current->l_rname,currentFrom->l_rname);
	    strcpy(current->l_attname,currentFrom->l_attname);
	    strcpy(current->r_rname,currentFrom->r_rname);
	    strcpy(current->r_attname,currentFrom->r_attname);
	       
	    if(currentFrom->next){
		current->next = new RelNode;
		current = current->next;
		currentFrom = currentFrom->next;
	    }else{
		current->next = NULL;
		break;
	    }
	}//end while
    }//end for

}

Statistics::~Statistics()
{
    //1. clear dataset
    for(int i = 0; i< DATA_SET_MAX; i++){
	p_Node p_current = dataset[i];
	p_Node toDelete = p_current;
	while(toDelete!=NULL){
	    p_current = toDelete->next;
	    //delete all contain_belong area
	    p_Names p_current_name = toDelete->contain_belong;
	    p_Names p_toDelete_name = toDelete->contain_belong;
	    while(p_toDelete_name!=NULL){
		p_current_name = p_toDelete_name->next;
		delete p_toDelete_name;
		p_toDelete_name = p_current_name;
	    }
	    delete toDelete;
	    toDelete = p_current;
	}//while
    }//END for

    //2. clear partition info
    for(int i = 0; i< DATA_SET_MAX; i++){
	p_Names p_current = p_partition[i];
	p_Names toDelete = p_current;
	while(toDelete!=NULL){
	    p_current = toDelete->next;
	    delete toDelete;
	    toDelete = p_current;
	}//while
    }//END for
	
    //3. clear relation info
    for(int i = 0; i< DATA_SET_MAX; i++){
	p_RelNode p = p_rels[i];
	p_RelNode toDelete = p;
	while(toDelete!=NULL){
	    p = toDelete->next;
	    delete toDelete;
	    toDelete = p;
	}//while
    }//END for
 
    delete[] p_partition;
    delete[] dataset;
    delete[] p_rels;
}
void Statistics::AddRelWhenRead(char *relName, int numTuples, int partition)
{
    unsigned int hashValue = BKDRHash(relName) % DATA_SET_MAX;
    p_Node p = dataset[hashValue];
    p_Node temp  = new Node;
    temp->flag = 0;
    strcpy(temp->name,relName);
    temp->num = numTuples;
    temp->contain_belong = NULL;
    //remember partition data
    temp->partition = partition;
    temp->next = dataset[hashValue];
    dataset[hashValue] = temp;

    p_Names temp_node = new Names;
    strcpy(temp_node->name,relName);
    temp_node->next = p_partition[partition];
    p_partition[partition] = temp_node;

}
void Statistics::AddRel(char *relName, int numTuples)
{
    // printf("add: before: p_rels: #%p\n",p_rels);
    unsigned int hashValue = BKDRHash(relName) % DATA_SET_MAX;
    p_Node p = dataset[hashValue];
    int find_flag = 0;
    if(dataset[hashValue] == NULL){
	dataset[hashValue] = new Node;
	p = dataset[hashValue];
	p->flag = 0;
	strcpy(p->name,relName);
	p->num = numTuples;
	p->contain_belong = NULL;
	p->next = NULL;
	//remember partition data
	p->partition = partition_count;
	p_Names temp = new Names;
	strcpy(temp->name,relName);
	temp->next = p_partition[partition_count];
	p_partition[partition_count] = temp;
	partition_count++;
	return;
    }
    while(1){
	if(p->flag == 0){
	    if(strcmp(relName,p->name) == 0){
		p->num = numTuples;
		find_flag = 1;
		break;
	    }
	}
	if(p->next == NULL){
	    //make node and copy data
	    p->next = new Node;
	    p->next->flag = 0;
	    strcpy(p->next->name,relName);
	    p->next->num = numTuples;
	    p->next->contain_belong = NULL;
	    p->next->next = NULL;
	    //remember partition data
	    p->partition = partition_count;
	    p_Names temp = new Names;
	    strcpy(temp->name,relName);
	    temp->next = p_partition[partition_count];
	    p_partition[partition_count] = temp;
	    partition_count++;
	    break;
	}
	p = p->next;
    }//end while
    //printf("add: after: p_rels: #%p\n",p_rels);
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //printf("addatt, before: p_rels: #%p\n",p_rels);
    //first, add att info into dataset
    int numDistincts_toAdd = numDistincts;

    unsigned int index_rel = BKDRHash(relName) % DATA_SET_MAX;
    unsigned int index_att = BKDRHash(attName) % DATA_SET_MAX;
    p_Node p = dataset[index_rel];
    int find_att_flag = 0;
    while(1){

	if(p == NULL){
	    printf("error, not relation: %s\n", relName);
	    return;
	}
	if(p->flag == 0){
	    if(strcmp(relName,p->name) == 0){
		//relation found

	        if(numDistincts == -1)
		    numDistincts_toAdd =  p->num;
	        else
		    numDistincts_toAdd = numDistincts;

		//find whether this relation has the att
		p_Names p_name_node = p->contain_belong;
		find_att_flag = 0;
		while(p_name_node != NULL){
		    if(strcmp(attName,p_name_node->name) == 0){
			//same att found, update the numDistincts
			p_Node temp_dataset = dataset[index_att];
			while(temp_dataset!=NULL){
			    if(temp_dataset->flag == 1 && (strcmp(temp_dataset->name, attName)==0)){
			        temp_dataset->num = numDistincts_toAdd;
			        break;
			    }
			    temp_dataset = temp_dataset->next;
			}//updated
			find_att_flag = 1;
			break;
		    }// same found
		    p_name_node = p_name_node->next;
		}//while end
		if(find_att_flag == 1){
		    //same att in rel found, break
		    break;
		}

		//no att found in rel, continue
		p_Names tempName = new Names;
		strcpy(tempName->name,attName);
		tempName->next =NULL;
		p_Names p_temp = p->contain_belong;
		if(p->contain_belong == NULL){
		    p->contain_belong = tempName;
		    break;
		}
		//add this att into the end of relation
		while(p_temp!=NULL){
		    if(p_temp->next == NULL){
			p_temp->next = tempName;
			break;
		    }
		    p_temp = p_temp->next;
		}
		break;
	    }//found
	}
	p = p->next;
    }

    //if not att in rel, then insert it info into dataset
    if(find_att_flag == 1){
	return;
    }

    p_Node temp = new Node;
    temp->flag = 1;
    temp->contain_belong = new Names;
    strcpy(temp->contain_belong->name,relName);
    temp->contain_belong->next = NULL;
    strcpy(temp->name,attName);
    temp->num = numDistincts_toAdd;
    temp->next = dataset[index_att];
    dataset[index_att] = temp;

    //printf("addatt: after: p_rels: #%p\n",p_rels);
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    unsigned int index_old = BKDRHash(oldName) % DATA_SET_MAX;
    unsigned int index_new = BKDRHash(newName) % DATA_SET_MAX;
    p_Node p_old = dataset[index_old];
    p_Node p_temp_new = new Node;
    p_temp_new->flag = 0;
    strcpy(p_temp_new->name,newName);
    while(p_old!=NULL){
	if(strcmp(p_old->name,oldName)==0 && p_old->flag == 0){
	    //old rel found
	    p_temp_new->num = p_old->num;
	    p_temp_new->partition = partition_count;
	    p_Names temp = new Names;
	    strcpy(temp->name,newName);
	    temp->next = p_partition[partition_count];
	    p_partition[partition_count] = temp;
	    partition_count++;

	    //update contain_belong linkedlist
	    p_Names p_relNameNode = p_old->contain_belong;
	    p_temp_new->contain_belong = NULL;
	    p_Names p_last = p_temp_new->contain_belong;
	    unsigned int attHash;
	    p_Node p_attNode;
	    p_Names p_attNodeName;
	    while(p_relNameNode !=NULL){
		   
		if(p_temp_new->contain_belong == NULL){
		    p_temp_new->contain_belong = new Names;
		    strcpy(p_temp_new->contain_belong->name,p_relNameNode->name);

		    //update att's belong
		    attHash = BKDRHash(p_relNameNode->name) % DATA_SET_MAX;
		    p_attNode = dataset[attHash];
		    while(p_attNode!=NULL){
			if(strcmp(p_attNode->name,p_relNameNode->name)==0 && p_attNode->flag == 1){
			    p_attNodeName = new Names;
			    strcpy(p_attNodeName->name,newName);
			    p_attNodeName->next = p_attNode->contain_belong;
			    p_attNode->contain_belong = p_attNodeName;
			    break;
			}
			p_attNode = p_attNode->next;
		    }
		    p_temp_new->contain_belong->next = NULL;
		    p_last = p_temp_new->contain_belong;
		    p_relNameNode = p_relNameNode->next;
		    continue;
		}
		p_last->next = new Names;
		strcpy(p_last->next->name,p_relNameNode->name);
		//update att's belong
		attHash = BKDRHash(p_relNameNode->name) % DATA_SET_MAX;
		p_attNode = dataset[attHash];
		while(p_attNode!=NULL){
		    if(strcmp(p_attNode->name,p_relNameNode->name)==0){
			p_attNodeName = new Names;
			strcpy(p_attNodeName->name,newName);
			p_attNodeName->next = p_attNode->contain_belong;
			p_attNode->contain_belong = p_attNodeName;
			break;
		    }
		    p_attNode = p_attNode->next;
		}
		p_last->next->next = NULL;
		p_last = p_last->next;
		p_relNameNode = p_relNameNode->next;
		   
		   
	    }//while end
	    //contain_belong copied
	      
	    break;
	}
	p_old = p_old->next;
    }
    //insert new node into dataset
    p_temp_new->next = dataset[index_new];
    dataset[index_new] = p_temp_new;
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream infile(fromWhere);
    int nFlag; unsigned int nPartition; int nNum;
    string name;
    char * cName, *cRelName;
    cName = new char[32];
    cRelName = new char[32];
    unsigned int max_partition_count = 0;
    while(1){
	infile >> nFlag >> name >>nPartition >> nNum;
	strcpy(cName,name.c_str());
	if(nFlag == 22){
	    //eof
	    break;
	}
	if(nFlag == 0){
	    strcpy(cRelName,name.c_str());//remember this relation's name, in order to add attributs easily
	    if(nPartition > max_partition_count)
		max_partition_count = nPartition;
	    AddRelWhenRead(cName, nNum, nPartition);
	    continue;
	}
	if(nFlag == 1){
	    AddAtt(cRelName, cName, nNum);
	}
    }
    delete[] cName;
    delete[] cRelName;
    partition_count = max_partition_count+1;
     
    int nIndex, op, relnext;
    string n1,n2,n3,n4,s;
    char l_rname[32], l_attname[32], r_rname[32], r_attname[32];
    getline(infile,s);
    while(1){
	getline(infile,s);
	nIndex = atoi(s.c_str());
	if(nIndex == 20000){
	    break;
	}
	  
	p_RelNode p = NULL;
	while(1){
	    getline(infile,s);
	    nFlag = atoi(s.c_str());
	    getline(infile,n1);
	    getline(infile,n2);
	    getline(infile,n3);
	    getline(infile,n4);

	    getline(infile,s);
	    op = atoi(s.c_str());

	    getline(infile,s);
	    relnext = atoi(s.c_str());

	    if(nFlag == 11){
		p->next = NULL;
		break;
	    }
	    if(p==NULL){
		p_rels[nIndex] = new RelNode;
		p = p_rels[nIndex];
	    }else{
		p->next = new RelNode;
		p = p->next;
	    }
	    strcpy(p->l_rname,n1.c_str());
	    strcpy(p->r_rname,n2.c_str());
	    strcpy(p->l_attname,n3.c_str());
	    strcpy(p->r_attname,n4.c_str());
	    p->opType = op;
	    p->relNext = relnext;
	    p->flag = nFlag;
	    
	}
    }
      
}
void Statistics::Write(char *fromWhere)
{
    ofstream outfile(fromWhere);
    //1. clear dataset
    for(int i = 0; i< DATA_SET_MAX; i++){
	p_Node p_current = dataset[i];
	while(p_current!=NULL){
	    //output
	    if(p_current->flag == 1){
		p_current=p_current->next;
		continue;
	    }
	    //relation output format:    flag name partition numAtts
	    outfile << "0 " << p_current->name << " " << p_current->partition <<" "<< p_current->num << endl;

	    p_Names p_current_name = p_current->contain_belong;
	    while(p_current_name!=NULL){
		//find the att in dataset and output it
		int index_att = BKDRHash(p_current_name->name) % DATA_SET_MAX;
		p_Node p_att = dataset[index_att];
		while(p_att!=NULL){
		    if(p_att->flag == 1 && (strcmp(p_att->name,p_current_name->name) == 0)){
			//att output format:    flag name 1 numDistincts
			outfile << "1 " << p_att->name <<" 1 "<<p_att->num <<endl;
			break;
		    }
		    p_att = p_att->next;
		}
		p_current_name = p_current_name->next;
	    }
	    p_current=p_current->next;
	}//while
    }//END for
    //write end identifier
    outfile << "22 22 22 22"<<endl;
    //start output join, selection, project info
    for(int i = 0; i < DATA_SET_MAX;i++){
	if(p_rels[i] == NULL)
	    continue;
	p_RelNode p = p_rels[i];
	    
	outfile << i <<endl;
	while(p){
	    outfile << p->flag << endl; 
	    outfile <<  p->l_rname << endl;
	    outfile << p->r_rname<<endl;
	    outfile << p->l_attname <<endl;
	    outfile << p->r_attname <<endl;
	    outfile << p->opType <<endl;
	    outfile << p->relNext <<endl;
	    p = p->next;
	}
	outfile << "11"<<endl<<"1"<<endl<<"1"<<endl<<"1"<<endl<<"1"<<endl<<"1"<<endl<<"1"<<endl;
    }

    outfile << "20000"<<endl;
}
int Statistics::MergePartitions(int to, int from)
{
    //printf("merge: before: p_rels: #%p\n",p_rels);
    p_Names p_to = p_partition[to];
    p_Names p_from = p_partition[from];
    int count_to = 0;
    while(p_from){
	int index = BKDRHash(p_from->name) % DATA_SET_MAX;
	p_Node p = dataset[index];
	while(p){
	    if(p->flag == 0){
		if(strcmp(p->name,p_from->name) == 0){
		    p->partition = to;
		    break;
		}
	    }
	    p = p->next;
	}
	p_from = p_from->next;
    }
    while(p_to!=NULL){
	if(p_to->next == NULL){
	    count_to++;
	    p_to->next = p_partition[from];
	    p_partition[from] = NULL;
	    break;
	}
	count_to++;
	p_to = p_to->next;
    }
    //merge p_rels
    p_RelNode rel_to = p_rels[to];
    while(rel_to!=NULL){
	if(rel_to->next == NULL){
	    rel_to->relNext = 0;
	    rel_to->next = p_rels[from];
	    p_rels[from] = NULL;
	    break;
	}
	rel_to = rel_to->next;
    }
    return count_to;
}

void Statistics::ParseTreeToRels(struct AndList *parseTree, int to_index, char * relNames[], int numToJoin)
{
  
    p_RelNode p, last;
    if(p_rels[to_index]){
	p = p_rels[to_index];
	while(p->next){
	    p = p->next;
	}
    }
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
		
	struct OrList *myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL){
			  
		if(p!=NULL){
		    p->relNext = 0;
		}
		break;
	    }

	    if(p_rels[to_index] == NULL){
		p_rels[to_index] = new RelNode;
		p_rels[to_index]->next = NULL;
		p = p_rels[to_index];
				
	    }else{
		p->next = new RelNode;
		p = p->next;
		p->next = NULL;
	    }
	    p->relNext = 1;//or
	    strcpy(p->l_attname,myOr->left->left->value);
	    strcpy(p->r_attname,myOr->left->right->value);

	    if (myOr->left->code == LESS_THAN) {
		p->opType = -1;
	    } else if (myOr->left->code == GREATER_THAN) {
		p->opType = 1;	
	    } else if (myOr->left->code == EQUALS) {
		p->opType = 0;
	    } 
	    p->flag = 0;//not cross product
	    if(myOr->left->left->code == NAME && myOr->left->right->code == NAME)
		p->flag = 1;
	    if (myOr->left->left->code == NAME) {
		//need to check whether the term n.value exits here
		int docLoc = 0;
		char * p_char = myOr->left->left->value;
		for(int si = 0; si < strlen(p_char); si++){
		    if(p_char[si] == '.'){
			docLoc = si;
			break;
		    }
		}
		if(docLoc){
		    char tempRelName[32]="\0", tempAttName[32]="\0";
		    int si = 0;
		    for(si = 0; si<docLoc; si++){
			tempRelName[si] = p_char[si];
		    }
		    tempRelName[docLoc] = '\0';
		    for(si = docLoc+1; si< strlen(p_char) ;si++){
			tempAttName[si-docLoc-1] = p_char[si];
		    }
		    tempAttName[si] = '\0';
		    strcpy(p->l_rname,tempRelName);
		    strcpy(p->l_attname, tempAttName);
		}else{
		    unsigned int index = BKDRHash(myOr->left->left->value) % DATA_SET_MAX;
		    p_Node p_att = dataset[index];
		    int break_flag = 0;
		    while(p_att!=NULL){
			if(p_att->flag == 1){
			    if(strcmp(p_att->name,myOr->left->left->value)==0){
				p_Names p_name = p_att->contain_belong;
				      
				while(p_name!=NULL){
				    for(int i = 0;i<numToJoin;i++){
					if(strcmp(relNames[i],p_name->name)==0){
					    strcpy(p->l_rname,relNames[i]);
					    break_flag = 1;
					    break;
					}
				    }
				    if(break_flag == 1)
					break;
				    p_name=p_name->next;
				}
			    }
			}
			if(break_flag == 1)
			    break;
			p_att = p_att->next;
		    }
		    if(break_flag == 0)
			strcpy(p->l_rname,"att_not_found");
		}//if docLoc
	    }else{
		strcpy(p->l_rname,"null_constant");
	    }
	    if(myOr->left->right->code == NAME){
		int docLoc = 0;
		char * p_char = myOr->left->right->value;
		for(int si = 0; si < strlen(p_char); si++){
		    if(p_char[si] == '.'){
			docLoc = si;
			break;
		    }
		}
		if(docLoc){
		    char tempRelName[32], tempAttName[32];
		    int si = 0;
		    for(si = 0; si<docLoc; si++){
			tempRelName[si] = p_char[si];
		    }
		    tempRelName[docLoc] = '\0';
		    for(si = docLoc+1; si< strlen(p_char) ;si++){
			tempAttName[si-docLoc-1] = p_char[si];
		    }
		    tempAttName[si] = '\0';
		    strcpy(p->r_rname,tempRelName);
		    strcpy(p->r_attname, tempAttName);
		}else{
		    unsigned int index = BKDRHash(myOr->left->right->value) % DATA_SET_MAX;
		    p_Node p_att = dataset[index];
		    int break_flag = 0;
		    while(p_att!=NULL){
			if(p_att->flag == 1){
			    if(strcmp(p_att->name,myOr->left->right->value)==0){
				p_Names p_name = p_att->contain_belong;
				      
				while(p_name!=NULL){
				    for(int i = 0;i<numToJoin;i++){
					if(strcmp(relNames[i],p_name->name)==0){
					    strcpy(p->r_rname,relNames[i]);
					    break_flag = 1;
					    break;
					}
				    }
				    if(break_flag == 1)
					break;
				    p_name=p_name->next;
				}
			    }
			}
			if(break_flag == 1)
			    break;
			p_att = p_att->next;
		    }
		    if(break_flag == 0){
			strcpy(p->r_rname,"att_not_found");
		    }
		}//if docloc
	    }else{
		strcpy(p->r_rname,"null_constant");
	    }

	}
    }
    return;

}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  
    int checkAttValue = CheckRelNames (relNames, numToJoin);
    int checkPTValue = CheckParseTree(parseTree, relNames, numToJoin);
    int count_to = 0;
    if(checkAttValue == 0 || checkPTValue == 0)
	return;
    int allPartitions[100];
    for(int i = 0;i<numToJoin; i++){
	int v = BKDRHash(relNames[i]) % DATA_SET_MAX;
	p_Node p = dataset[v];
	while(p!=NULL){
	    if(p->flag == 0){
		if(strcmp(p->name, relNames[i]) == 0){
		    allPartitions[i] = p->partition;
		    break;
		}
	    }
	    p = p->next;
	}
    }

    int min_index = 0;
    for(int i = 1;i<numToJoin; i++){
	if(allPartitions[i] < allPartitions[min_index]){
	    min_index = i;
	}
    }
    if(checkAttValue == 2){

	for(int i = 0; i< numToJoin;i++){
	    if(allPartitions[i] == allPartitions[min_index]){
		continue;
	    }
	    count_to = MergePartitions(allPartitions[min_index],allPartitions[i]);
	    for(int j = 0;j<numToJoin;j++){
		if(allPartitions[j] == allPartitions[i]){
		    allPartitions[j] = allPartitions[min_index];
		}
	    }
	}
	if(parseTree==NULL){
	    //cross product of two partitions
	      
	}else{
	    ParseTreeToRels(parseTree,  allPartitions[min_index],relNames,numToJoin);
	}

    }

    if(checkAttValue == 1){
	if(parseTree!=NULL){
	    ParseTreeToRels(parseTree,allPartitions[min_index],relNames,numToJoin);
	}
    }
    //delete[] allPartitions;
    return;
}

double Statistics::AnalyzParseTree(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double result = 1, temp = 0;
    double allValue[50];
    unsigned int nameHash[50];
    int ignore_flag[50] = {0};
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
		
	struct OrList *myOr = parseTree->left;
	int num_count = 0;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL){

		for(int i = 0; i<num_count;i++)
		    ignore_flag[i] = 0;
			  
		for(int i = 0;i<num_count;i++){
		    if(ignore_flag[i] == 1)
			continue;
		    for(int j = i+1;j<num_count;j++){
			if(nameHash[i] == nameHash[j]){
			    ignore_flag[j] = 1;
			    allValue[i] += allValue[j];
			}
		    }
		}
		temp = 1;
		for(int i = 0;i<num_count;i++){
		    if(ignore_flag[i] == 1)
			continue;
		    temp *= (1-allValue[i]);
		}
		if(temp < 1){   
		    temp = 1-temp;
		    result *= temp;
		}
			  
		break;
	    }
			
	    if (myOr->left->left->code == NAME && myOr->left->right->code == NAME) {
		if(numToJoin >1)
		    if(CheckAtt(myOr->left->left->value,  relNames,  numToJoin) == 1 && CheckAtt(myOr->left->right->value,  relNames,  numToJoin) == 1) {
			int lv = getNumByName(myOr->left->left->value,1);
			int rv = getNumByName(myOr->left->right->value,1);
			int maxV = lv>rv ? lv:rv;
			allValue[num_count] = 1.0 / (double)maxV;
			nameHash[num_count] = 0;
			num_count++;
		    }
	    }else{
		int value = 1;
		if(myOr->left->left->code == NAME){
		    if(CheckAtt(myOr->left->left->value,  relNames,  numToJoin)==0)
			continue;
		    nameHash[num_count] = BKDRHash(myOr->left->left->value);
		    
		}else{
		    if(CheckAtt(myOr->left->right->value,  relNames,  numToJoin)==0)
			continue;
		    nameHash[num_count] = BKDRHash(myOr->left->right->value);
		    
		}
		if (myOr->left->code == EQUALS) {
		    if(myOr->left->left->code == NAME){
			value = getNumByName(myOr->left->left->value,1);
				      
		    }else{
			value = getNumByName(myOr->left->right->value,1);
				      
		    }
		    allValue[num_count] = 1.0 / (double)value;
				    
		}else{
		    allValue[num_count] = 1.0 / 3.0;
				     
		}
		num_count++;
	    }

	}
    }
    return result;
}
int Statistics::getNumByName(char * relName, int flag)
{
    int docLoc = 0;
    char * p_char = relName;
    char tempAttName[32] = "\0";
    for(int si = 0; si < strlen(p_char); si++){
	if(p_char[si] == '.'){
	    docLoc = si;
	    break;
	}
    }
    if(docLoc){
	  
	int si = 0;
	for(si = docLoc+1; si< strlen(p_char) ;si++){
	    tempAttName[si-docLoc-1] = p_char[si];
	}
	tempAttName[si-docLoc-1] = '\0';
    }else{
	strcpy(tempAttName,relName);
    }
    int index = BKDRHash(tempAttName) % DATA_SET_MAX;
    p_Node p = dataset[index];
    while(p){
	if(p->flag == flag && strcmp(tempAttName,p->name)==0){
	    return p->num;
	}
	p = p->next;
    }
    return -1;
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    int checkAttValue = CheckRelNames (relNames, numToJoin);
    int checkPTValue = CheckParseTree(parseTree, relNames, numToJoin);
    int count_to = 0;
    if(checkAttValue == 0 || checkPTValue == 0){
	cout << "relation not exit, error in estimate." <<endl;
	return -1;
    }
	
    //know which partitions are we going to deal with
    int allPartitions[100];
    for(int i = 0;i<numToJoin; i++){
	int v = BKDRHash(relNames[i]) % DATA_SET_MAX;
	p_Node p = dataset[v];
	while(p!=NULL){
	    if(p->flag == 0){
		if(strcmp(p->name, relNames[i]) == 0){
		    allPartitions[i] = p->partition;
		    break;
		}
	    }
	    p = p->next;
	}
    }
    double temp = 0, result = 1;
    int hashIndex = 0;
    //after knowing the partitions, we firstly scan these partitions, get their history
    //	if(checkAttValue == 2){

    for(int i = 0; i< numToJoin;i++){
	if(allPartitions[i] == -1){
	    continue;
	}

	//get all join & select info
	p_RelNode p_rel_data = p_rels[allPartitions[i]];
	while(p_rel_data){
	    if(p_rel_data->flag == 0){
		//constant comparison

		if(p_rel_data->opType == -1 || p_rel_data->opType == 1)
		    temp+= 1.0/3.0;
		else{
		    if(strcmp(p_rel_data->l_rname,"null_constant")==0){
			//see right att name
			hashIndex = BKDRHash(p_rel_data->r_attname) % DATA_SET_MAX;
			p_Node p_dataset = dataset[hashIndex];
			while(p_dataset){
			    if(strcmp(p_dataset->name,p_rel_data->r_attname)==0 && p_dataset->flag == 1){
				temp += 1.0/(double)p_dataset->num;
				break;
			    }
			    p_dataset = p_dataset->next;
			}
		    }else{
			//see left att name
			hashIndex = BKDRHash(p_rel_data->l_attname) % DATA_SET_MAX;
			p_Node p_dataset = dataset[hashIndex];
			while(p_dataset){
			    if(strcmp(p_dataset->name,p_rel_data->l_attname)==0 && p_dataset->flag == 1){
				temp += 1.0/(double)p_dataset->num;
				break;
			    }
			    p_dataset = p_dataset->next;
			}
		    }
		}//end if
			
		if(p_rel_data->relNext == 0){
		    //AND
		    result *= temp;
		    temp = 0;
		}
	    }//END IF
	    if(p_rel_data->flag == 1){
		//att comparison, join
		//see left att name
		int left_value, right_value;
		double max_value;
		hashIndex = BKDRHash(p_rel_data->l_attname) % DATA_SET_MAX;
		p_Node p_dataset = dataset[hashIndex];
		while(p_dataset){
		    if(strcmp(p_dataset->name,p_rel_data->l_attname)==0 && p_dataset->flag == 1){
			left_value = p_dataset->num;
			break;
		    }
		    p_dataset = p_dataset->next;
		}
		hashIndex = BKDRHash(p_rel_data->r_attname) % DATA_SET_MAX;
		p_dataset = dataset[hashIndex];
		while(p_dataset){
		    if(strcmp(p_dataset->name,p_rel_data->r_attname)==0 && p_dataset->flag == 1){
			right_value = p_dataset->num;
			break;
		    }
		    p_dataset = p_dataset->next;
		}
		max_value = (double)(left_value > right_value ? left_value:right_value);
		result *= (1.0/max_value);
			
	    }//END IF
	    p_rel_data = p_rel_data->next;
	}//END WHILE
	int thisP = allPartitions[i];
	for(int j = 0; j< numToJoin;j++)
	    if(thisP == allPartitions[j])
		allPartitions[j] = -1;

    }//end for

    for(int i = 0;i <numToJoin; i++){
	int relValue = getNumByName(relNames[i],0);
	result *= (double)relValue;
    }

    if(parseTree==NULL){
	//cross product of two partitions
	      
    }else{
	result *= AnalyzParseTree(parseTree,relNames, numToJoin);
    }

    //	}

    return result;
}

int  Statistics::CheckAtt(char * attName, char * relNames[], int numToJoin)
{
    int docLoc = 0;
    char * p_char = attName;
    char tempAttName[32] = "\0";
    char tempRelName[32] = "\0";
    for(int si = 0; si < strlen(p_char); si++){
	if(p_char[si] == '.'){
	    docLoc = si;
	    break;
	}
    }
    if(docLoc){
	
	int si = 0;
	for(si = 0; si < docLoc; si++){
	    tempRelName[si] = p_char[si];
	}
	tempRelName[si] = '\0';
	for(si = docLoc+1; si< strlen(p_char) ;si++){
	    tempAttName[si-docLoc-1] = p_char[si];
	}
	tempAttName[si-docLoc-1] = '\0';
    }else{
	strcpy(tempAttName,attName);
    }
    int attInRel = 0;
    for(int ri = 0;ri<numToJoin;ri++){
	if(strcmp(tempRelName,relNames[ri]) == 0){
	    attInRel = 1;
	    break;
	}
    }
    if(attInRel == 0)
	return 0;
    int attIndex = BKDRHash(tempAttName) % DATA_SET_MAX;
    p_Node p_att = dataset[attIndex];
    int relIndex = BKDRHash(tempRelName) % DATA_SET_MAX;
    p_Node p_rel = dataset[relIndex];
    int find_flag = 0;
    while(p_att!=NULL){
	if(p_att->flag == 1){
	    if(strcmp(p_att->name,tempAttName)==0){
		p_Names p_relname = p_att->contain_belong;
		while(p_relname!=NULL){
		    for(int i = 0;i<numToJoin;i++){
			if(strcmp(relNames[i], p_relname->name)==0){
			    find_flag = 1;
			    return 1;
			}
		    }
		    p_relname = p_relname->next;
		}
		if(find_flag == 0)
		    return 0;
		else
		    return 1;
	    }

	}
	p_att = p_att->next;
    }
    return 0;
	  
}

//return: 0: error in check, 1: all relnames belongs to one partition, 2: belongs to several partitions
int  Statistics::CheckRelNames (char * relNames[], int numToJoin)
{
    int checkFlag[100];
    for(int i = 0;i<numToJoin;i++){

	int index = BKDRHash(relNames[i]) % DATA_SET_MAX;
	p_Node p = dataset[index];
	int find_flag = 0;
	while(p!=NULL){
	    if(p->flag == 0){
		if(strcmp(relNames[i],p->name) == 0){
		    checkFlag[i] = p->partition;
		    find_flag = 1;
		    break;
		}
	    }
	    p = p->next;
	}
	if(find_flag == 0){
	    cout << "no relation: "<<relNames[i]<<endl;
		  
	    return 0;
	}
    }
    int count_partitions = 0;
    for(int i = 0;i<numToJoin;i++){

	int index = BKDRHash(relNames[i]) % DATA_SET_MAX;
	int count = 0;
	if(checkFlag[i]!= -1){
	    count_partitions++;
	    int thispartition = checkFlag[i];
	    int allFlag = 0;
	    for(int j = 0; j<numToJoin;j++){
		if(checkFlag[j] == thispartition){
		    count++;
		    if( j!=i)
			checkFlag[j] = -1;
		}
	    }
	    if(count == numToJoin){
		allFlag = 1;
	    }
	    p_Names p = p_partition[checkFlag[i]];
	    while(p!=NULL){
		count--;
		p = p->next;
	    }
	    if(count < 0){
		cout << "lack relation in this partition. error." <<endl;
		    
		return 0;
	    }
	    if(allFlag == 1){
		    
		return 1;
	    }
		  
	}
    }
	
	
    return 2;
}
int  Statistics::CheckParseTree (struct AndList *parseTree, char * relNames[], int numToJoin) {
  /*
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
	if (parseTree == NULL)
	    break;
		
	struct OrList *myOr = parseTree->left;
	for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {
	    // see if we have run off of the end of the ORs
	    if (myOr == NULL)
		break;
			
	    if (myOr->left->left->code == NAME && myOr->left->right->code == NAME) {
		if(CheckAtt(myOr->left->left->value,  relNames,  numToJoin) == 0) {
		    cout << "ERROR: Could not find attribute " <<
			myOr->left->left->value << "\n";
		    return 0;	
		}
		if (CheckAtt(myOr->left->right->value,  relNames,  numToJoin) == 0) {
		    cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
		    return 0;	
		}
	    }

	}
    }
  */
    return 1;
}

