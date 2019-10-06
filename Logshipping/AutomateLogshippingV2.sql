use [master]
go
alter database [DBA] set  single_user with rollback immediate

drop database [DBA]

go

create database [DBA]
go

use [DBA]
go

create schema [LS]

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

create procedure [LS].[BackupDatabase]
(@DatabaseName sysname)
as
begin
	set nocount on

	declare @Command nvarchar(max)
	declare @BackupFolder varchar(100)
	declare @Role varchar(100)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
 
	/*This procedure is only executed on server with the primary role*/
	if @Role = 'Primary'
	begin
		select @BackupFolder = c.[CharValue]
		from [LS].[Config] c
		where c.Name = 'BackupFolder'

		set @Command = 'backup database [' + @DatabaseName + '] to disk = N''' + @BackupFolder + '\' +@DatabaseName + '.bak'''
		exec (@Command)
	end
end
go

create procedure [LS].[DropDatabase]
( @DatabaseName sysname)
as
begin
	set nocount on

	declare @Role varchar(100)
	declare @Command nvarchar(max)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
	
	/*This procedure is only executed on server with the secondary role*/
	if @Role = 'Secondary'
	begin
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
end
go

create procedure [LS].[RecoverDatabase]
( @DatabaseName sysname)
as
begin
	set nocount on
	declare @Role varchar(100)
	declare @Command nvarchar(max)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
	
	/*This procedure is only executed on server with the secondary role*/
	if @Role = 'Secondary'
	begin
	
		--Check if database exists and is in restoring state
		if exists(select 1 from sys.databases where [name] = @DatabaseName and state_desc = 'RESTORING')
		begin
			--check if db is part of logshipping
			if exists(select 1 from msdb.dbo.log_shipping_secondary_databases where [secondary_database] = @DatabaseName)
			begin
				exec master.dbo.sp_delete_log_shipping_secondary_database @secondary_database = @DatabaseName
			end
		
			set @Command = 'restore database [' + @DatabaseName +'] with recovery'
			exec(@command)
		end
	end
end
go

create procedure [LS].[RestoreDatabase]
(@DatabaseName sysname)
as
begin
	set nocount on

	declare @Command nvarchar(max)
	declare @RestoreFolder varchar(100)
	declare @Role varchar(100)

	select @Role = [CharValue]
	from [LS].[Config]
	where [Name] = 'Role'
 
	/*This procedure is only executed on server with the secondary role*/
	if @Role = 'Secondary' 
	begin

		select @RestoreFolder = c.[CharValue]
		from [LS].[Config] c
		where c.Name = 'RestoreFolder'

		set @Command = 'restore database [' + @DatabaseName + '] from disk = N''' + @RestoreFolder + '\' +@DatabaseName + '.bak'' with norecovery'
		exec (@Command)

	end
end
go

create procedure [LS].[AddDatabase]
(@DatabaseName sysname)
as
begin
 set nocount on
 declare @Role varchar(100)
 declare @Secondary varchar(100)
 declare @BackupFolder varchar(100)
 declare @Command nvarchar(max)
 declare @RecoveryModel varchar(100)
 declare @BackupJobName nvarchar(255)
 declare @JobScheduleName nvarchar(255)
 declare @LS_BackupJobId uniqueidentifier 
 declare @LS_PrimaryId uniqueidentifier 
 declare @LS_BackUpScheduleUID uniqueidentifier 
 declare @LS_BackUpScheduleID int 
 

 select @Role = [CharValue]
 from [LS].[Config]
 where [Name] = 'Role'
 
 if @Role is null
 begin
  return
 end

 /*This procedure is only executed on server with the primary role*/
 if @Role = 'Primary'
 begin
  select @Secondary = [CharValue]
  from [LS].[Config]
  where [Name] = 'Secondary'
  
  select @BackupFolder = [CharValue]
  from [LS].[Config]
  where [Name] = 'BackupFolder'
 

  select @RecoveryModel = recovery_model_desc 
  from sys.databases where [name] = @DatabaseName

  if @RecoveryModel <> 'FULL'
  begin
   set @Command = 'alter database ['+@DatabaseName +'] set recovery full'
   exec(@Command)
  end
  
  /*backup database on primary*/
  exec [LS].[BackupDatabase] @DatabaseName = @DatabaseName

  /*execute restore database on secondary*/
  set @Command = '['+@Secondary +'].[DBA].[LS].[RestoreDatabase] ''' + @DatabaseName + ''''
  exec(@Command)

  /*Add logshipping info*/
  set @BackupJobName = N'LSBackup_' + @DatabaseName
  set @JobScheduleName = N'LSBackupSchedule_' + @DatabaseName

  exec [master].[dbo].sp_add_log_shipping_primary_database
    @database = @DatabaseName
  , @backup_directory = @BackupFolder 
  , @backup_share = @BackupFolder 
  , @backup_job_name = @BackupJobName
  , @backup_retention_period = 4320
  , @backup_compression = 1
  , @backup_threshold = 120 
  , @threshold_alert_enabled = 1
  , @history_retention_period = 5760 
  , @backup_job_id = @LS_BackupJobId output 
  , @primary_id = @LS_PrimaryId output 
  , @overwrite = 1 

  exec msdb.dbo.sp_add_schedule 
   @schedule_name = @JobScheduleName 
  ,@enabled = 1 
  ,@freq_type = 4 
  ,@freq_interval = 1 
  ,@freq_subday_type = 4 
  ,@freq_subday_interval = 15 
  ,@freq_recurrence_factor = 0 
  ,@active_start_date = 20190930 
  ,@active_end_date = 99991231 
  ,@active_start_time = 0 
  ,@active_end_time = 235900 
  ,@schedule_uid = @LS_BackUpScheduleUID output 
  ,@schedule_id = @LS_BackUpScheduleID output 

  exec msdb.dbo.sp_attach_schedule 
    @job_id = @LS_BackupJobId 
  , @schedule_id = @LS_BackUpScheduleID  

  exec msdb.dbo.sp_update_job 
    @job_id = @LS_BackupJobId 
  , @enabled = 1 


  /*Setup logshipping on secondary*/
  set @Command = '['+@Secondary +'].[DBA].[LS].[SetupLSSecondary] ''' + @DatabaseName + ''''
  exec(@Command)

  exec master.dbo.sp_add_log_shipping_primary_secondary  
    @primary_database = @DatabaseName
  , @secondary_server = @Secondary
  , @secondary_database = @DatabaseName

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

 select  @LastSyncRun = [DateTimeValue]
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
 , p.[CreateDate]
 , 0 
 from 
 #PrimaryDatabases p
 cross join [LS].[application] a
 where p.[name] like a.[DBPrefix] + '%'
 and not exists (select 1 from #SecondaryDatabases s
     where p.[Name] = s.[Name])


 /*are there databases dropped?*/
/* select * from 
 #SecondaryDatabases s
 where not exists (select 1 from #PrimaryDatabases p
 where s.[Name] = p.[Name])

 update d
 set [DropDate] = getdate()
 , [StatusID] = 4
 from [LS].[Database] d
 left join #PrimaryDatabases p on d.[Name] = p.[Name]
 where p.[name] is null
*/
 

 /*check for databases with logshipping
 only 2 database per application need to be logshipped*/


 /*Start performing actions*/


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


;with x as (
select
  d.[name]
, d.[create_date]
, a.name [Application]
, ROW_NUMBER() OVER(partition by a.name order by substring(d.[name], len(a.[DBPrefix])+1, len(d.[name]) - len(a.[DBPrefix])) desc ) [Sequence]
from sys.databases d
cross join [LS].[Application] a 
where d.[name] like a.[DBPrefix] + '%'
and d.name not like '%amt%'
)
select 'backup database ['+x.[Name]+'] to disk = N''\\172.30.51.136\SQLLogShipping$\'+ x.[name] + '.bak'' with init, stats = 100 '
from x
where [sequence] > 2


;with x as (
select
  d.[name]
, d.[create_date]
, a.name [Application]
, ROW_NUMBER() OVER(partition by a.name order by substring(d.[name], len(a.[DBPrefix])+1, len(d.[name]) - len(a.[DBPrefix])) desc ) [Sequence]
from sys.databases d
cross join [LS].[Application] a 
where d.[name] like a.[DBPrefix] + '%'
and d.name not like '%amt%'
)
select 'backup database ['+x.[Name]+'] to disk = N''\\172.30.51.136\SQLLogShipping$\'+ x.[name] + '.bak'' with init, stats = 100 
go
insert into [172.30.51.136].[DBA].[dbo].[RestoreAction]
([DatabaseName],[QueueID])
select '''+x.[Name]+''',@QueueID
go'
from x
where [sequence] > 2
end
go

insert into [LS].[Config]
([ConfigID],[Name],[CharValue],[DateTimeValue],[IntValue])
values
(1,'Role','Primary',null,null),
(2,'Primary','192.168.152.161',null,null),
(3,'Secondary','192.168.152.162',null,null),
(4,'BackupFolder','\\192.168.152.162\SQLLogshipping',null,null),
(6,'RestoreFolder','C:\SQLLogShipping',null,null)
go

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

