Database System
by Xin Li
================
To compile the code in GNU/Linux or Mac, use command "make", the makefile is contained in the codes.

The database contents in my local use TPC-H benchmark 10Mb version, the code can also run on 1Gb data.
To use the data of TPC-H 10Mb benchmark, please refer to:
http://www.tpc.org/tpch/

Since the size is large, the contents of my database is not included in the codes.

In main.h, the following 3 strings need to be changed according to environment settings.

char *dbfile_dir // dir where binary heap files should be stored
char *tpch_dir;//TPC-H benchmark's location
char *catalog_path;// the catalog of database, the catalog file is included for the TPC-h benchmark, this can be set to default


the database use some file to store the internal stuffs, In Defs.h,  "dbfile.meta" is the name for these files
