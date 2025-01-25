# Introduction
This post common core [42](https://42.fr)'s piscine is an intensive program which allows to learn basical database and SQL skills.
Each module introduces a new concept, more or less one by one. They must be validated in the order they are proposed.

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

## Python - 2 - DataTable :
### ex00: Load my Dataset
Load a CSV into a pandas's dataframe.
### ex01: draw my country
Plot data from a specific line in a CSV with Matplotlib.
### ex02: compare my country
Plot two lines of a CSV on the same figure, with a specific data crop.
### ex03: draw my year
Merge two CSV to draw a scatter plot on specific common column.
Modify the scale (logarithmic).

This exercise lead to a mini self initiative "project":
[log_vs_lin_scale_on_scatter](github.com/Nociception/log_vs_lin_scale_on_scatter).

## Python - 3 - OOP : for "Object Oriented Programmation"
As this piscine is available only after the 42's common core, I already have studied OOP with common core CPP modules.
### ex00: GOT S1E9
Abstract classes, abstract methods, inheritance.
### ex01: GOT S1E7
.super() method, magic methods (\_\_repr\_\_, \_\_str\_\_), classmethod.
### ex02: Now it’s weird!
Properties, Diamond inheritance in python.
### ex03: Calculate my vector
Operators override.
### ex04: Calculate my dot product
staticmethod

## Python - 4 - DOD : for "Data Oriented Design"
### ex00: Calculate my statistics
Manage an unknown number of parameters with \*\*kwargs.
### ex01: Outer_inner
nonlocal python keyword usage, with a function defined inside a function.
### ex02: my first decorating
Building a decorator from scratch, with the nonlocal keyword, and a function, defined in a function, defined in a function.
### ex03: data class
Discover dataclass decorator.

---

# Tests
The subject provides minimal tests, and encourages us to write better ones.
To do so, I tried to create a tester machine to avoid redundant code:
- [general_function_tester.py](general_function_tester.py) for functions
- [general_tester.py](general_tester.py) for programs

Eventually, I prefered the pytest way.

---

# The piscine concept
Each module must be validated separately in a dedicated repo.
As soon as the repo is locked, we have two days to defend it in front of two students also working on the piscine.
We are encouraged to ask eachother in case of we would be stuck,
and to compare our solutions.
