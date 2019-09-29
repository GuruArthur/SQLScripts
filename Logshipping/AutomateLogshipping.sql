create database DBA
go
use DBA
go
/*
https://docs.microsoft.com/en-us/sql/database-engine/log-shipping/configure-log-shipping-sql-server?view=sql-server-2017
*/
create schema [LS]
go

create table [LS].[Config]
( [ConfigID] int not null
, [Name] varchar(50) not null
, [CharValue] varchar(100) null
, [DateTimeValue] datetime2 null
, [IntValue] int null)
go

alter table [LS].[Config]
add constraint [PKConfig] primary key ([ConfigID])
go

create table [LS].[Application]
( [ApplicationID] int identity(1,1) not null
, [Name] sysname 
, [DBPrefix] sysname)
go

alter table [LS].[Application]
add constraint [PKApplication] primary key ([ApplicationID])
go

create table [LS].[Database]
( [DatabaseID] int identity(1,1) not null
, [ApplicationID] int not null
, [Name] sysname
, CreateDate datetime2 not null
, DropDate datetime2 null
, StatusID int not null
)
go

alter table [LS].[Database]
add constraint [PKDatabase] primary key ([DatabaseID])
go

alter table [LS].[Database]
add constraint [FKDatabaseApplication] foreign key ([ApplicationID])
	references [LS].[Application] ([ApplicationID])
go

create table [LS].[ActionType]
( [ActionTypeID] int identity(1,1) not null
, [Name] varchar(25) not null
)
go

alter table [LS].[ActionType]
add constraint [PKActionType] primary key ([ActionTypeID])
go

create table [LS].[Action]
( [ActionID] int identity(1,1) not null
, [DatabaseID] int not null
, [ActionTypeID] int not null
, [CreateDate] datetime not null
, [ExecuteDate] datetime null
)
go

alter table [LS].[Action]
add constraint [PKAction] primary key ([ActionID])
go

create procedure [LS].[DropDatabase]
( @DatabaseName sysname)
as
begin
	set nocount on

	declare @Role varchar(100)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
	
	/*This procedure is only executed on server with the secondary role*/
	if @Role <> 'Secondary' or @Role is null
	begin
		return
	end
	
	declare @Command nvarchar(max)
	--Check if database exists
	if exists(select 1 from sys.databases where [name] = @DatabaseName)
	begin
		--check if db is part of logshipping
		if exists(select 1 from sys.databases where [name] = @DatabaseName)
		begin
			print' remove logshipping'
		end
		
		set @Command = 'Drop database [' + @DatabaseName +']'
		exec(@command)
	end
end
go

create procedure [LS].[RecoverDatabase]
( @DatabaseName sysname)
as
begin
	set nocount on
	declare @Role varchar(100)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
	
	/*This procedure is only executed on server with the secondary role*/
	if @Role <> 'Secondary' or @Role is null
	begin
		return
	end

	declare @Command nvarchar(max)
	--Check if database exists
	if exists(select 1 from sys.databases where [name] = @DatabaseName)
	begin
		--check if db is part of logshipping
		if exists(select 1 from sys.databases where [name] = @DatabaseName)
		begin
			print' remove logshipping'
		end
		
		set @Command = 'restore database [' + @DatabaseName +'] with recovery'
		exec(@command)
	end
end
go


create procedure [LS].[SyncDatabases]
as
begin
	set nocount on

	declare @Secondary sysname
	declare @Command nvarchar(max)
	declare @Role varchar(100)
	declare @LastSyncRun datetime

	select @Secondary = [CharValue]
	from [LS].[Config]
	where [Name] = 'Secondary'


	select  @LastSyncRun = [DateValue]
	from [LS].[Config]
	where [Name] = 'LastSyncRun'

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'

	/*This procedure is only executed on server with the primary role*/
	if @Role <> 'Primary' or @Role is null
	begin
		return
	end

	create table #PrimaryDatabases
	( [Name] sysname
	, [CreateDate] datetime
	, [Sequence] int
	)

	create table #SecondaryDatabases
	( [Name] sysname
	, [CreateDate] datetime
	, [Sequence] int
	)

	insert into #PrimaryDatabases
	( [Name]
	, [CreateDate]
	, [Sequence]
	)
	select
	  d.[name]
	, d.[create_date]
	, substring(d.[name], len(a.[DBPrefix])+1, len(d.[name]) - len(a.[DBPrefix]))
	from sys.databases d
	cross join [LS].[Application] a 
	where d.[name] like a.[DBPrefix] + '%'

	set @Command  = N'select ,d.name, d.create_date, substring(d.[name], len(a.[DBPrefix])+1, len(d.[name]) - len(a.[DBPrefix]))
		from ['+ @Secondary +'].master.sys.databases d
		cross join LSApplication a 
		where d.[name] like a.DBPrefix + ''%'''

	insert into #SecondaryDatabases
	exec sp_executesql @stmt = @command

	/*are there new databases?*/
	select * from 
	#PrimaryDatabases p
	where not exists (select 1 from #SecondaryDatabases s
	where p.[Name] = s.[Name])

	insert into [LS].[Database]
	( [ApplicationID]
	, [Name]
	, [CreateDate]
	, [StatusID])
	select
	  a.[ApplicationID]
	, p.[Name]
	, p.[create_date]
	, 0 
	from 
	#PrimaryDatabases p
	cross join [LS].[application] a
	where p.[name] like a.[DBPrefix] + '%'
	and not exists (select 1 from #SecondaryDatabases s
					where p.[Name] = s.[Name])


	/*are there databases dropped?*/
/*	select * from 
	#SecondaryDatabases s
	where not exists (select 1 from #PrimaryDatabases p
	where s.[Name] = p.[Name])
*/
	update d
	set [DropDate] = getdate()
	, [Status] = 4
	from [LS].[Database] d
	left join #PrimaryDatabases p on d.[DatabaseName] = p.[Name]
	where p.[name] is null

	

	/*check for databases with logshipping
	only 2 database per application need to be logshipped*/


	/*Start performing actions*/
	while (select count(1) from [LS].[Action] where [ExecuteDate] is null)
	begin
		select top 1 
		from [LS].[Action] 
	


		update a
		set [ExecuteDate] = getdate()
		from [LS].[Action]
		where [ActionID] = @ActionID
	end

end
go

insert into LSConfig
(LSConfigID, [Name],[CharValue])
values
(1,'Role','Primary'),
(2,'Secondary','Server2')

insert into LSConfig
(LSConfigID, [Name],[DateValue])
values
(3,'LastSyncRun','2019, sep 1')

insert into LSApplication(Name,DBPrefix)
values
('AppA','AppA_'),
('AppB','AppB_')

insert into [LS].[ActionType]
([Name])
values
('Add'),
('Revover'),
('Drop')
-- get databases primary
-- get databases secondary
-- detect new databases
-- detect removed databases
-- determine databases with obsolete logshipping (max 2 + standard app)
-- prepare actions
	-- add logshipping
	-- remove logshipping + db recovery
	-- remove db
-- execute actions primary
-- execute actions secondary



/*
logshipping parameters
serverrole primary/secondary
lastsyncruntime
*/
/*>> SourceSqlInstance = 'sql1'
>> DestinationSqlInstance = 'sql2'
>> Database = 'db1'
>> SharedPath= '\\sql1\logshipping'
>> LocalPath= 'D:\Data\logshipping'
>> BackupScheduleFrequencyType = 'daily'
>> BackupScheduleFrequencyInterval = 1
>> CompressBackup = $true
>> CopyScheduleFrequencyType = 'daily'
>> CopyScheduleFrequencyInterval = 1
>> GenerateFullBackup = $true
>> RestoreScheduleFrequencyType = 'daily'
>> RestoreScheduleFrequencyInterval = 1
>> SecondaryDatabaseSuffix = 'DR'
>> CopyDestinationFolder = */
