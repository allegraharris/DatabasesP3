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
