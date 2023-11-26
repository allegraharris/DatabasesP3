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

create table test3 (a int, b int, c int, primary key(a));
insert into test3 (a,b,c) values (1,23,34);
insert into test3 (a,b,c) values (4,53,64);
insert into test3 (a,b,c) values (7,83,94);
insert into test3 (a,b,c) values (10,131,124);
insert into test3 (a,b,c) values (12,532,624);
insert into test3 (a,b,c) values (13,823,924);
insert into test3 (a,b,c) values (14,1321,1224);
insert into test3 (a,b,c) values (15,13241,14224);

select * from test2;

select * from test3;

select test2.d, test3.a, test3.b from test2 join test3 on test2.d = test3.a;


