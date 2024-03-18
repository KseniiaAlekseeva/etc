CREATE TABLE cities (
    city_code INTEGER PRIMARY KEY NOT NULL, 
    home_city TEXT NOT NULL
);

INSERT INTO cities VALUES (26, 'Moscow');
INSERT INTO cities VALUES (56, 'Perm');

CREATE TABLE people (
    employee_id VARCHAR (20) PRIMARY KEY NOT NULL, 
    name TEXT NOT NULL, 
    city_code INTEGER NOT NULL,
    FOREIGN KEY (city_code) REFERENCES cities (city_code)
);

INSERT INTO people VALUES ('E001','Alice', 26);
INSERT INTO people VALUES ('E002', 'Bob', 56);
INSERT INTO people VALUES ('E003','Alice', 56);

CREATE TABLE jobs (
    job_code VARCHAR(20) PRIMARY KEY NOT NULL, 
    job TEXT NOT NULL
);

INSERT INTO jobs VALUES ('J01','Chef');
INSERT INTO jobs VALUES ('J02','Waiter');
INSERT INTO jobs VALUES ('J03','Bartender');

CREATE TABLE employees (
    id INTEGER PRIMARY KEY AUTO_INCREMENT, 
    employee_id VARCHAR(20) NOT NULL, 
    job_code VARCHAR(20) NOT NULL, 
    FOREIGN KEY (employee_id) REFERENCES people (employee_id),
    FOREIGN KEY (job_code) REFERENCES jobs (job_code)
);

INSERT INTO employees VALUES (default, 'E001','J01');
INSERT INTO employees VALUES (default, 'E001','J02');
INSERT INTO employees VALUES (default, 'E002','J02');
INSERT INTO employees VALUES (default, 'E002','J03');
INSERT INTO employees VALUES (default, 'E003','J01');

SELECT * FROM cities;
SELECT * FROM people;
SELECT * FROM jobs;
SELECT * FROM employees;
