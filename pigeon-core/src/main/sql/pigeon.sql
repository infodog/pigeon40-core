create table t_listband(
        id numeric(12) primary key,
        listName varbinary(512),
        isHead numeric(1),
        isMeta numeric(1),
        value mediumblob,
        hash numeric(12),
        nextMetaBandId numeric(12),
        prevMetaBandId numeric(12)
);

create index idx_listband on t_listband(listName,isHead);
create index idx_listband_id on t_listband(id);

create table t_simpleatom(
  name varchar(128) primary key,
  value numeric(12)
);
create unique index idx_atom_name on t_simpleatom(name);

create table t_pigeontransaction(
  name varchar(128) primary key,
  version numeric(16),
  LastTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
create unique index idx_transaction_name on t_pigeontransaction(name);

create table t_flexobject(
  name varbinary(512) primary key,
  content mediumblob,
  hash int(16),
  isCompressed bool,
  isString bool
);
create unique index idx_flexobject_name on t_flexobject(name);

create table t_ids(
  TableName varchar(128) primary key,
  NextValue numeric(12)
);
create unique index idx_id_tablename on t_ids(TableName);

create table t_testwhileidle (
  id varchar(128) primary key
);

insert into t_testwhileidle values ('test');
