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


select * from test2 where e = 11 and f = 9;
select d from test2 where d = 1 or e = 5;
select a,b,c from test where a = 'd';
