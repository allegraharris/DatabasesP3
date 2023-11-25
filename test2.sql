create table test (
    a int not null,
    b int not null,
    c varchar(20) not null,
    primary key (a)
);

create table test2 (
    a int not null,
    b int not null,
    c varchar(20) not null,
    primary key (b)
);

insert into test values (1,1,'charlie');
select * from test;

describe test;
describe test2;

drop table test,test2;