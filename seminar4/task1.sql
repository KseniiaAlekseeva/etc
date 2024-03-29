create TABLE if not EXISTS news(
iid bigint not null,
category_id int not null,
author varchar(200) not null,
title varchar(200),
rate int not null
);

create table If not exists news_1 (check (category_id=1)) inherits (news);
create table If not exists news_2 (check (category_id=2)) inherits (news);
create table If not exists news_3 (check (category_id=3)) inherits (news);

create rule insert_to_news_1 AS ON INSERT To news where (category_id=1) Do instead INSERT INTO news_1 VALUES (NEW.*);
create rule insert_to_news_2 AS ON INSERT To news where (category_id=2) Do instead INSERT INTO news_2 VALUES (NEW.*);
create rule insert_to_news_3 AS ON INSERT To news where (category_id=3) Do instead INSERT INTO news_3 VALUES (NEW.*);

INSERT INTO news (iid,category_id,author,title,rate) VALUES (11,1,'Text','Text',1);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (21,2,'Text','Text',1);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (31,3,'Text','Text',1);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (12,1,'Text','Text',1);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (32,3,'Text','Text',1);


select * from news;
select * from news_1;

