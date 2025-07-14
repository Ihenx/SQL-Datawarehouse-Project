use master;
go

if exists (select 1 from sys.databases where name = 'SalesDataWarehouse')
begin
	alter database SalesDataWarehouse
	set single_user with rollback immediate;
	drop database SalesDataWarehouse
end;
go

--create database: SalesWarehouse
create Database SalesDataWareHouse;
go

use SalesDataWareHouse;
go

--create schemas : bronze, silver and Gold
create schema  bronze;
go

create schema silver;
go

create schema gold;
go




