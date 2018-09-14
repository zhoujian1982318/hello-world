drop table if exists contents;
create table if not exists contents (json string);
load data inpath '${hiveconf:inputFile}' into table contents;

