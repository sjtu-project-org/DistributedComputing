CREATE TABLE commodity (
    id int PRIMARY KEY,
    name varchar(20) NOT NULL,
    price double NOT NULL,
    currency varchar(5) NOT NULL,
    inventory int NOT NULL
);

CREATE TABLE result (
    id varchar(15) PRIMARY KEY,
    user_id varchar(15) NOT NULL,
    initiator varchar(5) NOT NULL,
    success varchar(10) NOT NULL,
    paid double NOT NULL

);

insert into commodity values (1, "A", 66.0, "USD", 123);
insert into commodity values (2, "B", 88.0, "RMB", 789);
insert into commodity values (3, "C", 99.0, "JPY", 666);
insert into commodity values (4, "D", 45.5, "EUR", 333);