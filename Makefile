CC = g++ -O0 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

test.out:  MyPQ.o Record.o HeapDBFile.o SortedDBFile.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o y.tab.o lex.yy.o main.o
	$(CC) -o test.out MyPQ.o Record.o HeapDBFile.o SortedDBFile.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o QueryPlan.o Statistics.o y.tab.o lex.yy.o main.o -lfl -lpthread

#a1test.out: Record.o HeapDBFile.o SortedDBFile.o Comparison.o BigQ.o MyPQ.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
#	$(CC) -o a1test.out Record.o HeapDBFile.o SortedDBFile.o BigQ.o MyPQ.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl -lpthread

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

main.o: main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

MyPQ.o: MyPQ.cc
	$(CC) -g -c  MyPQ.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c  HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c  SortedDBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc


Record.o: Record.cc
	$(CC) -g -c Record.cc

y.tab.o: Parser.y
	yacc -d Parser.y
#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
