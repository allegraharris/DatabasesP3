create table students (id int, first_name string, last_name string, primary key (id,first_name) );
show tables;
desribe students;
insert into students (id,first_name,last_name) values (1,'charlie','mei'), (2,'allegra','harris'), (3,'sarah','green');
select a.name b.info from TABLE1 a LEFT JOIN table2 b ON a.name = b.name;
-- {id}
