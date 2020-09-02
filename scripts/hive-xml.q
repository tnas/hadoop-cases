drop table boletimxml;

create table boletimxml(xmldata string);

load data local inpath 'hive-cases/xml/boletim.xml' overwrite into table boletimxml;

select xpath(xmldata, 'boletim/aluno/nome/text()') from boletimxml;
