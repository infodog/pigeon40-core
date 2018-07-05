alter table t_flexobject
  add column txid numeric(16),
  add column lastModified DATETIME not null default current_timestamp on update current_timestamp

alter table t_ids
  add column txid numeric(16),
  add column lastModified DATETIME not null default current_timestamp on update current_timestamp

alter table t_listband
  add column txid numeric(16),
  add column lastModified DATETIME not null default current_timestamp on update current_timestamp


alter table t_pigeontransaction
  add column txid numeric(16),
  add column lastModified DATETIME not null default current_timestamp on update current_timestamp


alter table t_simpleatom
  add column txid numeric(16),
  add column lastModified DATETIME not null default current_timestamp on update current_timestamp

