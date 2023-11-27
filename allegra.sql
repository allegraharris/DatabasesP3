CREATE TABLE departments (dep_id INT, dep_name STRING, primary key (dep_id));

CREATE TABLE employees (emp_id INT, emp_name STRING, dep_id INT, primary key (emp_id), foreign key (dep_id) references departments(dep_id));

INSERT INTO departments () VALUES (1,'chem'), (2,'math'), (3,'cs'), (4,'physics'), (5,'biology');

INSERT INTO employees () VALUES (8227,'charlie mei',3), (2353,'allegra harris',2), (2370,'sarah green',4), (3746,'charlie mei',5);
SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE departments.dep_id < 5 and departments.dep_name != 'cs';