declare @blocker int
	, @spid int
	, @handle binary(20)
	, @start int
	, @end int

set nocount on
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
--drop table #RunningSPIDs
-- use a permanent table so multiple users can watch at the same time
if object_id('tempdb..#RunningSPIDs','U') is null
	begin
		create table #RunningSPIDs
			(spid smallint
				, host varchar(15)
				, cpu int
				, io bigint
				, lastcpu int
				, lastio bigint
				, start varchar(20)
				, app varchar(400)
				, recdt datetime
				, lastrecdt datetime
				, op varchar(20)
				, open_tran smallint)
	end

delete r
from #RunningSPIDs r
left join master.dbo.sysprocesses p
on p.spid = r.spid
where cast(p.last_batch as varchar(20)) <> r.start
or p.spid is null
or p.status <> 'runnable' 

update r
	set cpu = p.cpu
		, io = cast(p.physical_io as int)
		, lastcpu = r.cpu
		, lastio = r.io
		, recdt = getdate()
		, lastrecdt = r.recdt
		, open_tran = p.open_tran
from master.dbo.sysprocesses p
left join msdb.dbo.sysjobs j
on substring(p.program_name,charindex('0x', p.program_name) + 18, 16)
   = substring(replace(j.job_id, '-',''),17,16) 
join #RunningSPIDs r 
on p.spid = r.spid
where (p.status='runnable' 
	or p.spid in (Select blocked from master.dbo.sysprocesses where blocked <> 0))
and p.spid<>@@spid
and r.app = case when p.program_name like 'SQLAgent - TSQL JobStep%'
		then 'Job: ' + substring(j.name,1,400)
		else substring(p.program_name,1,400)
		end 

insert #RunningSPIDs 
select p.spid
	, substring(p.hostname,1,15)
	, p.cpu
	, cast(p.physical_io as int)
	, null
	, null
	, cast(p.last_batch as varchar(20))
	, case when p.program_name like 'SQLAgent - TSQL JobStep%'
		then 'Job: ' + substring(j.name,1,200)
		else substring(p.program_name,1,200)
		end
	, getdate()
	, null
	, substring(replace(replace(replace(p.cmd,char(13),char(32)),char(10)
	                                    ,char(32)),char(9), char(32)),1,20)
	, p.open_tran
from master.dbo.sysprocesses p
left join msdb.dbo.sysjobs j
on substring(p.program_name,charindex('0x', p.program_name) + 18, 16)
   = substring(replace(j.job_id, '-',''),17,16) 
where (p.status='runnable' 
	or p.spid in (Select blocked from master.dbo.sysprocesses where blocked <> 0))
and p.spid<>@@spid
and not exists (select 1 from #RunningSPIDs where spid = p.spid)


print '"Runnable" SPIDs ordered by CPU usage' 
print '		[cur cpu] and [cur io] in last [cur dur] (current duration) seconds - rerun proc to refresh'
print '		Null [cur dur] means [cur cpu] and [cur io] values are total values since batch started.'
print '		If [cur dur] and batch started not changing check for open tran -->'
print ''

select spid
	, isnull(cpu-lastcpu,cpu) [cur cpu]
	, isnull(io-lastio,io) [cur io]
	, datediff(second,lastrecdt,recdt) as [cur dur]
	, start as [batch started]
	, host
	, op
	, app 
	, open_tran
	, cpu [tot cpu]
	, io [tot io]
from #RunningSPIDs
order by cpu desc


if exists(select 1 from master.dbo.sysprocesses where blocked <> 0)
	begin
		print 'Blocking and Blocked SPIDs'
		print ''
		select p.spid,
			p.blocked [Blocker],
			p.waittime,
			cast(p.lastwaittype as varchar(20)) [lastwaittype],
			cast(rtrim(ltrim(p.waitresource)) as varchar(20)) [waitresource],
			cast(p.last_batch as varchar(20)) as [last batch],
			substring(p.hostname,1,15) as [Host Name],
			substring(replace(replace(replace(p.cmd,char(13),char(32)),char(10)
			                                 ,char(32)),char(9), char(32)),1,20) as [op],
			case when p.program_name like 'SQLAgent - TSQL JobStep%'
				then 'Job: ' + substring(j.name,1,200)
				else substring(p.program_name,1,250)
				end as [Application Name]
			from master.dbo.sysprocesses p
			left join msdb.dbo.sysjobs j
			on substring(p.program_name,charindex('0x', p.program_name) + 18, 16)
			   = substring(replace(j.job_id, '-',''),17,16) 
			where p.spid<>@@spid
			and (p.blocked <> 0
				or p.spid in (select blocked 
						from master.dbo.sysprocesses
						where blocked <> 0))
			order by blocked, p.last_batch
                 
                select @blocker = min(blocked) 
		from master.dbo.sysprocesses
		where blocked > 0
                
                while @blocker is not null
                        begin
				select @handle = sql_handle 
					, @start = stmt_start
					, @end = stmt_end
				from master.dbo.sysprocesses
				where spid = @blocker

				 print 'Info on Blocking SPID ' + cast(@blocker as varchar(10))
				IF NOT EXISTS (SELECT * FROM ::fn_get_sql(@Handle))  
					select 'Unknown, handle not found in cache' [Currently Executing Statement]

	                        else  
				select replace(replace(replace(substring(text, 
						(@start + 2)/2,CASE @end
								WHEN -1 THEN (datalength(text))
								ELSE (@end - @start + 2)/2
								END)
							,char(13),char(32))
						,char(10),char(32))
					,char(9),char(32)) [Currently Executing Statement]
				from ::fn_get_sql(@handle)
	
                                print 'Input Buffer:'
                                dbcc inputbuffer(@blocker) 

                                print 'Output Buffer:'
                                dbcc outputbuffer(@blocker) 

                                print 'Output Buffer:'
				exec sp_lock @blocker

                                select @blocker = min(blocked) 
		                from master.dbo.sysprocesses 
		                where blocked > @blocker
                        end
 	end
