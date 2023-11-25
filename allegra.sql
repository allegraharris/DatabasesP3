create table test (a int, b int, c int, primary key(a));
insert into test (a,b,c) values (1,2,3);
insert into test (a,b,c) values (2,3,4);
insert into test (a,b,c) values (3,4,5);
insert into test (a,b,c) values (4,5,6);

select a,b,c from test where a = 2;