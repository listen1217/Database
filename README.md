Database System ReadMe file:
================
by Xin Li

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


Several test case can be as following:
======================================
(q1)
============================================================
SELECT SUM (ps.ps_supplycost)
FROM part AS p, supplier AS s, partsupp AS ps
WHERE (p.p_partkey = ps.ps_partkey) AND
    (s.s_suppkey = ps.ps_suppkey) AND
	  (s.s_acctbal > 2500)


(q2)
============================================================
SELECT SUM (c.c_acctbal)
FROM customer AS c, orders AS o
WHERE (c.c_custkey = o.o_custkey) AND
	  (o.o_totalprice < 10000)


ANSWER: 1.33123e+08 (17.31 sec) 



(q3)
============================================================
caution!!!!!!!!tooooooo slow!!!!!!!! in 1Gb data

SELECT l.l_orderkey, l.l_partkey, l.l_suppkey
FROM lineitem AS l
WHERE (l.l_returnflag = 'R') AND
	(l.l_discount < 0.04 OR l.l_shipmode = 'MAIL') AND 
	(l.l_orderkey > 5000) AND (l.l_orderkey < 6000)


ANSWER: 109 rows in set (17.47 sec)


==========================================================

(q5)


SELECT SUM (l.l_discount)
FROM customer AS c, orders AS o, lineitem AS l
WHERE (c.c_custkey = o.o_custkey) AND
	  (o.o_orderkey = l.l_orderkey) AND
	  (c.c_name = 'Customer#000070919') AND
	  (l.l_quantity > 30) AND (l.l_discount < 0.03)

ANSWER: 0.0075 (55.4 sec)


(q6)
===========================================================
SELECT DISTINCT s.s_name
FROM supplier AS s, part AS p, partsupp AS ps
WHERE (s.s_suppkey = ps.ps_suppkey) AND
	  (p.p_partkey = ps.ps_partkey) AND
	  (p.p_mfgr = 'Manufacturer#4') AND
	  (ps.ps_supplycost < 350)


ANSWER: 9964 rows (1.51 sec)


(q7)
===========================================================
SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority
FROM customer AS c, orders AS o, lineitem AS l 
WHERE (c.c_mktsegment = 'BUILDING') AND 
      (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND
	  (l.l_orderkey < 100 OR o.o_orderkey < 100)
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority

ANSWER: 7 rows (41.2 sec)

===========================================================

