-- Read-only role crumb extracts with, mirroring a prod read replica login.
create role crumb_reader login password 'crumb_reader';
grant connect on database navi to crumb_reader;
grant usage on schema public to crumb_reader;
alter default privileges for role navi in schema public grant select on tables to crumb_reader;
