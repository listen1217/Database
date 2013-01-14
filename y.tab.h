
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     Name = 258,
     Float = 259,
     Int = 260,
     String = 261,
     SELECT = 262,
     CREATE = 263,
     INSERT = 264,
     DROP = 265,
     TABLE = 266,
     GROUP = 267,
     DISTINCT = 268,
     BY = 269,
     FROM = 270,
     WHERE = 271,
     SUM = 272,
     Heap = 273,
     Sorted = 274,
     INTG = 275,
     DBL = 276,
     STR = 277,
     AS = 278,
     AND = 279,
     OR = 280,
     ON = 281,
     INTO = 282,
     SET = 283,
     OUTPUT = 284
   };
#endif
/* Tokens.  */
#define Name 258
#define Float 259
#define Int 260
#define String 261
#define SELECT 262
#define CREATE 263
#define INSERT 264
#define DROP 265
#define TABLE 266
#define GROUP 267
#define DISTINCT 268
#define BY 269
#define FROM 270
#define WHERE 271
#define SUM 272
#define Heap 273
#define Sorted 274
#define INTG 275
#define DBL 276
#define STR 277
#define AS 278
#define AND 279
#define OR 280
#define ON 281
#define INTO 282
#define SET 283
#define OUTPUT 284




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 1676 of yacc.c  */
#line 31 "Parser.y"

 	struct FuncOperand *myOperand;
	struct FuncOperator *myOperator; 
	struct TableList *myTables;
	struct ComparisonOp *myComparison;
	struct Operand *myBoolOperand;
	struct OrList *myOrList;
	struct AndList *myAndList;
	struct NameList *myNames;
	struct NewAttr *myAtts;
	char *actualChars;
	char whichOne;



/* Line 1676 of yacc.c  */
#line 126 "y.tab.h"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;


