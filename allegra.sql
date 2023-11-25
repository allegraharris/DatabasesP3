create table test (a string, b string, c string, primary key(a));
insert into test (a,b,c) values ('a','b','c');
insert into test (a,b,c) values ('b','c','d');
insert into test (a,b,c) values ('c','d','e');
insert into test (a,b,c) values ('d','e','f');

select a,b,c from test where a = 'd';