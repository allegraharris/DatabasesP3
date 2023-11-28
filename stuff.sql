SELECT * FROM departments;

SELECT * FROM employees;

SELECT max(emp_id) FROM employees;

SELECT * FROM employees WHERE emp_id >= 2370;

SELECT * FROM departments WHERE dep_name = 'css' OR dep_name = 'physics';

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id;

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3;

SELECT employees.emp_id, employees.dep_id, employees.emp_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id=3 AND employees.emp_name='charlie mei';

SELECT employees.emp_id, employees.dep_id, employees.emp_name FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.emp_name='sarah green' OR departments.dep_name= 'cs';

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE departments.dep_name != 'biology' AND employees.emp_id >= 1499;

SELECT * FROM employees JOIN departments ON employees.dep_id = departments.dep_id WHERE employees.dep_id != departments.dep_id;
