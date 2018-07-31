CREATE TABLE users (
	id STRING,
	age INT,
	sex STRING,
	region STRING,
	income FLOAT,
	married STRING,
	children INT,
	car STRING,
	save_act STRING,
	current_act STRING,
	mortgage STRING,
	pep STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

# load data from bank-data.csv
# The first line of bank-data.csv should be removed before load data
# hive> LOAD DATA LOCAL INPATH '/blablabla/bank-data.csv' OVERWRITE INTO TABLE users;

# 查询各个区域年龄大于30岁男性的平均收入
select users.region, avg(users.income) from users where users.age > 30 group by users.region;
