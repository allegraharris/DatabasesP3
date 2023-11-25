create table departments (
    dep_id INT, 
    dep_name STRING, 
    primary key (dep_id)
);

create table employees (
    emp_id INT, 
    emp_name STRING, 
    dep_id INT, 
    primary key (emp_id), 
    foreign key (dep_id) references departments(dep_id)
);

INSERT INTO departments () VALUES (1,'chem'), (2,'math'), (3,'cs'), (4,'physics'), (5,'biology');

INSERT INTO employees () VALUES (8227,'charlie mei',3), (2353,'allegra harris',2), (2370,'sarah green',4), (3746,'charlie mei',5);

SELECT * FROM departments;

SELECT departments.dep_id FROM departments;

SELECT dep_id, dep_name FROM departments;

SELECT * FROM employees;

SELECT emp_id, emp_name, dep_id FROM employees;

SELECT emp_id, emp_name FROM employees WHERE emp_id >= 2370;

SELECT dep_id, dep_name FROM departments WHERE dep_name = 'physics';

SELECT emp_id, emp_name, dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id;

SELECT emp_id, emp_name, dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3;

SELECT emp_id, emp_name, dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3 AND employees.emp_name='charlie mei';

SELECT emp_id, emp_name, dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE departments.dep_id=3 OR employees.emp_name='charlie mei';

SELECT emp_id, emp_name, dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.emp_name='sarah green' OR departments.dep_name= 'cs';

