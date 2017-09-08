/******************************
** FileName: SetupDeadlockMonitoring.sql 
** Omschrijving: Script for creating an XEvent session storing deadlock info.
** Auteur: Arthur Baan
** Datum: 02-05-2016
**************************
** Change History
**************************
** Datum		Auteur		Omschrijving	
** --------		-----------	------------------------------------
*******************************/

create event session [DeadlockMonitor] 
on server 
add event sqlserver.database_xml_deadlock_report
add target package0.event_file
	(set 
	  filename=N'DeadlockMonitor'
	, max_file_size=(50)
	, max_rollover_files= 5)
with 
( max_memory=4096 kb
, event_retention_mode=allow_single_event_loss
, max_dispatch_latency=30 seconds
, max_event_size=0 kb
, memory_partition_mode=none
, track_causality=off
, startup_state=on)
go

use [DBA]
go

set ansi_nulls on
go

set quoted_identifier on
go

if (select object_id('[dbo].[DeadlockInfo]') )is not null
begin 
	drop view [dbo].[DeadlockInfo]
end
go

create view [dbo].[DeadlockInfo] 
( [DeadlockMoment]
, [Database]
, [ProcessID1]
, [Login1]
, [Application1]
, [HostName1]
, [InputBuffer1]
, [ProcessID2]
, [Login2]
, [Application2]
, [HostName2]
, [InputBuffer2]
, [VictimProcess]
, [Object1]
, [Object2]
, [XMLdata]
, [NumberOfProcesses])
as
with x as (select
  data.value ('(/event[@name=''database_xml_deadlock_report'']/@timestamp)[1]', 'datetime2(0)') AS [Time]
, data.value ('(/event/data[@name=''database_name'']/value)[1]', 'varchar(128)') AS [Database]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@id)[1]','varchar(128)') AS [ProcessID1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@loginname)[1]','varchar(128)') AS [Login1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@spid)[1]','varchar(128)') AS [SPID1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@clientapp)[1]','varchar(128)') AS [Application1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@hostname)[1]','varchar(128)') AS [HostName1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/inputbuf)[1]','varchar(max)') AS [InputBuffer1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@id)[2]','varchar(128)') AS [ProcessID2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@loginname)[2]','varchar(128)') AS [Login2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@spid)[2]','varchar(128)') AS [SPID2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@clientapp)[2]','varchar(128)') AS [Application2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/@hostname)[2]','varchar(128)') AS [HostName2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/process-list/process/inputbuf)[2]','varchar(max)') AS [InputBuffer2]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/victim-list/victimProcess/@id)[1]','varchar(128)') AS [VictimProcess]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/resource-list/ridlock/@objectname)[1]','varchar(128)') AS [Object1]
, data.value ('(/event/data[@name=''xml_report'']/value/deadlock/resource-list/ridlock/@objectname)[2]','varchar(128)') AS [Object2]
, data.value('count(/event/data[@name=''xml_report'']/value/deadlock/process-list/*)','int') [NumberOfProcesses]
, data [XMLdata]
from 
(select convert (xml, event_data) as data 
from sys.fn_xe_file_target_read_file('Deadlock*.xel',null,null,null)
 ) DeadlockData
 )
 select dateadd(mi, datediff(mi, getutcdate(), getdate()), [Time])  [DeadlockMoment]
, [Database]
, [SPID1]
, [Login1]
, [Application1]
, [HostName1]
, [InputBuffer1]
, [SPID2]
, [Login2]
, [Application2]
, [HostName2]
, [InputBuffer2]
, case 
  when [VictimProcess] = [ProcessID1] then [SPID1]
  when [VictimProcess] = [ProcessID2] then [SPID2]
  else 'Unable to determine victim. Examine XMLData.'end [VictimProcess]
, [Object1]
, [Object2]
, [XMLdata]
, [NumberOfProcesses]
from x
go

use [msdb]
go

exec msdb.dbo.sp_delete_alert 
  @name=N'Error 1205 - Deadlock'
go

exec msdb.dbo.sp_add_alert 
  @name=N'Error 1205 - Deadlock'
, @message_id=1205
, @enabled=1
, @delay_between_responses=60
, @include_event_description_in=1
, @notification_message=N'Execute the following query to get the info of deadlocks in the last 2 hours:
select * from [DBA].[dbo].[DeadlockInfo] where [DeadlockMoment] > dateadd(hour,-2,getdate())'
, @category_name=N'[Uncategorized]'
go


exec msdb.dbo.sp_add_notification 
  @alert_name=N'Error 1205 - Deadlock'
, @operator_name=N'Database Operators'
, @notification_method = 1
go

