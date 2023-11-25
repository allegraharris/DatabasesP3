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


select * from test2 where d != 1 or e != 5;
select * from test2 where d > 2 and e > 3;
select * from test2;
select * from test2 where d = 10;
