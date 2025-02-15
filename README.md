# Introduction
This post common core [42](https://42.fr)'s piscine is an intensive program which allows to learn basical database and SQL skills.
Each module introduces a new concept, more or less one by one. They must be validated in the order they are proposed.

## Note
As each of these days are supposed to be defended once at a time,
the Makefile+docker setup files are in each Day* directory.

---

# The Modules (also called Days)
## Python - 0 - Data Engineer
### ex00: Create postgres DB
Docker, PostGres, Adminer
### ex01: Show me your DB
Briefly create and populate a postgres table, and then visualize it with adminer.
### ex02: First table
Create and populate a table from a CSV with .sql file. Discover the postgreSQL data types.
### ex03: automatic table
Create and populate a table from several CSV with a python script,
using psycopg to connect to the postgres database.
### ex04: items table
Nothing new here, just practicing previous skills.

## Python - 1 - Data Warehouse
### ex00: Show me your DB
Same exercise as ex01 from the previous module Python - 0 - Data Engineer
### ex01: customers table
Same exercise as ex03 from the previous module Python - 0 - Data Engineer. ANALYZE and VACUUM sql commands.
### ex02: remove duplicates
Remove duplicates using SQL tricks such as autojoin.
Comparisons order also matter for execution time!
Monitoring with EXPLAIN ANALYZE.
### ex03: fusion
Update table's rows with another CSV which contains a common column with the original table.

## Python - 2 - Data Analyst : (still in progress)
### ex00: American apple Pie
### ex01: initial data exploration
### ex02: My beautiful mustache
### ex03: Highest Building
### ex04: Elbow
### ex05: Clustering

## Python - 3 - Datascientist Part 1
### ex00: Histogram
### ex01: Correlation
### ex02: it’s raining cats no points!
### ex03: standardization
### ex04: Normalization
### ex05: Split

## Python - 4 - Datascientist Part 2
### ex00: Confusion Matrix
### ex01: It is warm
### ex02: Variances
### ex03: Feature Selection
### ex04: Forest
### ex05: KNN
### ex06: democracy !

---

# The piscine concept
Each module must be validated separately in a dedicated repo.
As soon as the repo is locked, we have two days to defend it in front of two students also working on the piscine.
We are encouraged to ask eachother in case of we would be stuck,
and to compare our solutions.
