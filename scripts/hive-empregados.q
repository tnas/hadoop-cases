drop table empregados;
drop table empregados_estado;

create table empregados(
id int, 
nome string, 
cargo string, 
estado string, 
computador string) 
row format delimited 
fields terminated by ',';

load data local inpath 'hive-cases/empregados/empregados.txt' 
into table empregados;

create table empregados_estado(id int, nome string, cargo string, computador string) 
partitioned by (estado string);

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table empregados_estado partition(estado) 
select id, nome, cargo, computador, estado from empregados;

select * from empregados_estado;
show partitions empregados_estado;
