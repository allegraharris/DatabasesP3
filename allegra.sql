create table test (a string, b string, c string, primary key(a));
insert into test (a,b,c) values ('a','b','c');
insert into test (a,b,c) values ('b','c','d');
insert into test (a,b,c) values ('c','d','e');
insert into test (a,b,c) values ('d','e','f');

create table test2 (d int, e int, f int, primary key(d));
insert into test2 (d,e,f) values (1,2,3);
insert into test2 (d,e,f) values (4,5,6);
insert into test2 (d,e,f) values (7,8,9);
insert into test2 (d,e,f) values (10,11,12);

create table test3 (a int, b int, c int, primary key(a));
insert into test3 (a,b,c) values (1,23,34);
insert into test3 (a,b,c) values (4,53,64);
insert into test3 (a,b,c) values (7,83,94);
insert into test3 (a,b,c) values (10,131,124);
insert into test3 (a,b,c) values (12,532,624);
insert into test3 (a,b,c) values (13,823,924);
insert into test3 (a,b,c) values (14,1321,1224);
insert into test3 (a,b,c) values (15,13241,14224);

select * from test2;

select * from test3;

select test2.d, test3.a, test3.b from test2 join test3 on test2.d = test3.a;

CREATE TABLE departments (dep_id INT, dep_name STRING, primary key (dep_id));

CREATE TABLE employees (emp_id INT, emp_name STRING, dep_id INT, primary key (emp_id), foreign key (dep_id) references departments(dep_id));

INSERT INTO departments VALUES (1,'chem'), (2,'math'), (3,'cs'), (4,'physics'), (5,'biology');

INSERT INTO employees VALUES (8227,'charlie mei',3), (2353,'allegra harris',2), (2370,'sarah green',4), (3746,'charlie mei',5), (1746, 'bebe green',5), (1499, 'bandit green',1), (9746, 'mark twain', 5), (1776, 'bebe snow', 4), (3333, 'john jackson', 2), (4321, 'munchkin miles', 5), (9834, 'jj mccarthy', 1), (6509, 'logan smores', 3), (1111, 'max johnson', 2), (2121, 'caroline smith', 5), (5555, 'jack smith', 4);

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id;
