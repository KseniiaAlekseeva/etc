create table news_rate_100 (CHECK (rate<=100)) INHERITS (news);
create table news_rate_100_200 (CHECK (rate>100 and rate<200)) INHERITS (news);
create table news_rate_200 (CHECK (rate>=200)) INHERITS (news);

create rule insert_to_rate_100 AS on Insert to news where (rate<=100) do INSTEAD insert into news_rate_100 values (new.*);
create rule insert_to_rate_100_200 AS on Insert to news where (rate>100 and rate<200) do INSTEAD insert into news_rate_100_200 values (new.*);
create rule insert_to_rate_200 AS on Insert to news where (rate>=200) do INSTEAD insert into news_rate_200 values (new.*);

INSERT INTO news (iid,category_id,author,title,rate) VALUES (111,1,'Text','Text',100);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (121,2,'Text','Text',100);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (131,3,'Text','Text',200);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (112,1,'Text','Text',290);
INSERT INTO news (iid,category_id,author,title,rate) VALUES (132,3,'Text','Text',390);

