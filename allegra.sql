CREATE TABLE departments (dep_id INT, dep_name STRING, primary key (dep_id));

CREATE TABLE employees (emp_id INT, emp_name STRING, dep_id INT, primary key (emp_id), foreign key (dep_id) references departments(dep_id));

INSERT INTO departments () VALUES (1,'chem'), (2,'math'), (3,'cs'), (4,'physics'), (5,'biology');

INSERT INTO employees () VALUES (8227,'charlie mei',3), (2353,'allegra harris',2), (2370,'sarah green',4), (3746,'charlie mei',5), (1746, 'bebe green',5), (1499, 'bandit green',1), (9746, 'mark twain', 5), (1776, 'bebe snow', 4), (3333, 'john jackson', 2), (4321, 'munchkin miles', 5), (9834, 'jj mccarthy', 1), (6509, 'logan smores', 3), (1111, 'max johnson', 2), (2121, 'caroline smith', 5), (5555, 'jack smith', 4);
