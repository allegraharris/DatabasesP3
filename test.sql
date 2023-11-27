create table departments (
    dep_id INT, 
    dep_name STRING, 
    primary key(dep_id)
);
create table employees (
    emp_id INT, 
    emp_name STRING, 
    dep_id INT, 
    primary key(emp_id), 
    foreign key(dep_id) references departments(dep_id)
);

INSERT INTO departments() 
    VALUES (1,'chem'), (2,'math'), (3,'cs'), (4,'physics'), (5,'biology');

INSERT INTO employees() 
    VALUES (822278827,'charlie mei',3), (235123,'allegra harris',2), (2358437820,'sarah green',4);

insert into departments(dep_id, dep_name) values (6,'econ');
select * from departments;
select max(dep_id) from departments;
select min(dep_id) from departments;
select * from departments where dep_id = 1;
select * from departments where dep_name = 'chem';
select * from departments where dep_id < 3;
select dep_id from departments where dep_name != 'math';
select * from departments where dep_id <= 5 and dep_name = 'math';
select * from departments where dep_id = 1 or dep_id = 2;
select max(dep_id) from 
departments where dep_id = 1 or dep_id = 2;
select * from departments join employees on departments.dep_id = employees.dep_id;
select * from departments join employees on departments.dep_id = employees.dep_id where departments.dep_id = 2;
select * from departments join employees on departments.dep_id = employees.dep_id where departments.dep_id = 2 and employees.dep_id = 2;
select * from departments join employees on departments.dep_id = employees.dep_id where departments.dep_id = 2 or employees.dep_id = 3;
select departments.dep_name,departments.dep_id from departments join employees on departments.dep_id = employees.dep_id where departments.dep_id = 2 or employees.dep_id = 3;
