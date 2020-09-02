#schematool -initSchema -dbType derby
#schematool -dbType derby -info
create table frequencia(palavra STRING, ocorrencia INT)
row format delimited
fields terminated by ',';
