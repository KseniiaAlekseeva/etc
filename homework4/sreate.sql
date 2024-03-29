create table if not exists movies (
  id bigint not null primary key,
  movies_type varchar(200),
  director varchar(200),
  year_of_issue int,
  length_in_minute int,
  rate int
);

create table if not exists year_1990 (Check (year_of_issue<1990)) INHERITS (movies);
create table if not exists year_1990_2000 (Check (year_of_issue>=1990 and year_of_issue<2000)) INHERITS (movies);
create table if not exists year_2000_2010 (Check (year_of_issue>=2000 and year_of_issue<2010)) INHERITS (movies);
create table if not exists year_2010_2020 (Check (year_of_issue>=2010 and year_of_issue<2020)) INHERITS (movies);
create table if not exists year_2020 (Check (year_of_issue>=2020)) INHERITS (movies);

create or replace rule check_year_1 as on insert to movies where (year_of_issue<1990)
do instead insert into year_1990 values (new.*);
create or replace rule check_year_2 as on insert to movies where (year_of_issue>=1990 and year_of_issue<2000)
do instead insert into year_1990_2000 values (new.*);
create or replace rule check_year_3 as on insert to movies where (year_of_issue>=2000 and year_of_issue<2010)
do instead insert into year_2000_2010 values (new.*);
create or replace rule check_year_4 as on insert to movies where (year_of_issue>=2010 and year_of_issue<2020)
do instead insert into year_2010_2020 values (new.*);
create or replace rule check_year_5 as on insert to movies where (year_of_issue>=2020)
do instead insert into year_2020 values (new.*);

create table if not exists length_40 (Check (length_in_minute<40)) INHERITS (movies);
create table if not exists length_40_90 (Check (length_in_minute>=40 and length_in_minute<90)) INHERITS (movies);
create table if not exists length_90_130 (Check (length_in_minute>=90 and length_in_minute<130)) INHERITS (movies);
create table if not exists length_130 (Check (length_in_minute>=130)) INHERITS (movies);

create or replace rule check_length_1 as on insert to movies where (length_in_minute<40)
do instead insert into length_40 values (new.*);
create or replace rule check_length_2 as on insert to movies where (length_in_minute>=40 and length_in_minute<90)
do instead insert into length_40_90 values (new.*);
create or replace rule check_length_3 as on insert to movies where (length_in_minute>=90 and length_in_minute<130)
do instead insert into length_90_130 values (new.*);
create or replace rule check_length_4 as on insert to movies where (length_in_minute>=130)
do instead insert into length_130 values (new.*);

create table if not exists rate_5 (Check (rate<5)) INHERITS (movies);
create table if not exists rate_5_8 (Check (rate>=5 and rate<8)) INHERITS (movies);
create table if not exists rate_8_10 (Check (rate>=8 and rate<10)) INHERITS (movies);

create or replace rule check_rate_1 as on insert to movies where (rate<5)
do instead insert into rate_5 values (new.*);
create or replace rule check_rate_2 as on insert to movies where (rate>=5 and rate<8)
do instead insert into rate_5_8 values (new.*);
create or replace rule check_rate_3 as on insert to movies where (rate>=8 and rate<10)
do instead insert into rate_8_10 values (new.*);


