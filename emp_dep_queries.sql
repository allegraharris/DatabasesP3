SELECT * FROM departments;

SELECT * FROM employees;

SELECT max(emp_id) FROM employees;

SELECT * FROM employees WHERE emp_id >= 2370;

SELECT * FROM departments WHERE dep_name = 'css' OR dep_name = 'physics';

SELECT employees.emp_id, employees.dep_id, employees.emp_name, departments.dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id;

SELECT employees.emp_id, employees.dep_id, employees.emp_name, departments.dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3;

SELECT employees.emp_id, employees.dep_id, employees.emp_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3 AND employees.emp_name='charlie mei';

SELECT employees.emp_id, departments.dep_id, employees.emp_name, departments.dep_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.emp_name='sarah green' OR departments.dep_name= 'cs';

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE departments.dep_name != 'biology' AND employees.emp_id >= 1499;

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id != departments.dep_id;

CREATE TABLE course (course_num INT, yr INT, emp_id INT, primary key (course_num, yr), foreign key (emp_id) references employees(emp_id));

INSERT INTO course VALUES (1020, 1990, 8227), (1020, 2000, 2370), (2730, 2000, 2353);

SELECT * FROM course;

SHOW TABLES;

DESCRIBE course;
