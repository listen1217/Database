#ifndef DEFS_H
#define DEFS_H

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define METAFILE "dbfile.meta"

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

typedef enum {reading, writing} rwState;
typedef enum {heap, sorted, tree} fType;
typedef enum {needWrite,dataRead,bufferEmpty} bFlagType;

unsigned int Random_Generate();


#endif

