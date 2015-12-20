begin tran
exec p_vComponents_Rotate @table = 'HSubscribers_Points', @demo = 1

exec p_vComponents_Rotate @table = 'CallBase_Points', @demo = 1

exec p_vComponents_Rotate @table = 'ServiceUseBase_Points', @demo = 1

--rollback tran


begin tran 

exec p_bis_DataStorage
-- каталог для файлов БД  
    @Dir = 'H:\ASTRA\DataStorage\' 
-- каталог для копии файла данных БД 
  , @Dir2 = 'E:\temp\'
-- каталог для копии файла данных БД 
-- (этот каталог проверяется архивным сервером) 
  , @Dir3 = 'E:\temp\'  

rollback tran



          exec p_vHSubscribers_Points_Rebuild  
    select @ViewName   = replace( @ViewNameMask, '<viewname>', @Prefix + 'HSubscribers_Points_Last6Month' )  
ServiceUseBaseDetails

hHsubscriber_points

ServiceUseBaseDetailsThreeMonth

    select * 
      from vCallBase_Components

    select * 
      from vServiceUseBase_Components

    select * 
      from vServiceUseBase_Points_Components

    select * 
      from vCallBase_Points_Components

    select * 
      from vHSubscribers_Points_Components


    select *--top 1  [Key] 
      from CyclingDataStorage --where [DT] = '2009-12-01 00:00:00.000'



select distinct object_name( id )
from syscomments
where text like '%_RecordIDD%'



Callbase112
dbo.vCallsDetailsA
vCallsD
vCallsA

vCallsA --[ProcessDateTime] >= '20100401' and [ProcessDateTime] < '20100501'
vCallsB --[ProcessDateTime] >= '20100501' and [ProcessDateTime] < '20100601'
vCallsC --[ProcessDateTime] >= '20100601' and [ProcessDateTime] < '20100701'
vCallsD --[ProcessDateTime] >= '20091201' and [ProcessDateTime] < '20100101'
vCallsE --[ProcessDateTime] >= '20100101' and [ProcessDateTime] < '20100201'
vCallsF --[ProcessDateTime] >= '20100201' and [ProcessDateTime] < '20100301'
vCallsG --[ProcessDateTime] >= '20100301' and [ProcessDateTime] < '20100401'

callbase

select top 20 * from CyclingDataStorage_Log order by 1 desc

p_bis_DataStorage

p_bis_DataStorage


select distinct object_name( id )
from syscomments
where text like '%update%vCallbase_Components%'



    select *
      from vCallBase_Components 
      where YYYYMM = @next1st_
        and PartNo = 0

select * from VCallsD

select * from VCallsDetailsD

ServiceUseBaseDetails


update vServiceFeeDetailsD
  set ProcessDateTime = dateadd( month, 14, ProcessDateTime )

update VCallsDetailsD
  set ProcessDateTime = dateadd( month, 7, ProcessDateTime )

select * from vCallbase_Components

select * from vCallbase_Versions
select * from vServiceUsebase_Versions

select top 20 * from CyclingDataStorage_Log order by 1 desc

----------------------------------------------------------
 select *
      from CyclingDataStorage 

update CyclingDataStorage
set DT = '2009-12-01 00:00:00.000' , IsBottom  = 1, IsTop  =  0
where [Key] = 'D'

update CyclingDataStorage
set IsBottom  = 0
where [Key] = 'E'

update CyclingDataStorage
set IsTop  = 1
where [Key] = 'C'

--
select * from vCallbase_Components

delete from vCallbase_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vCallbase_Components
  set SourceName = 'dbo.vCallsD', flgFake = 0
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0

update vCallbase_Components
  set SourceName = 'dbo.vCallsDetailsD', flgFake = 0
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 1

--
select * from vServiceUsebase_Components

delete from vServiceUsebase_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vServiceUsebase_Components
  set SourceName = 'dbo.vServiceFeeD', flgFake = 0
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0

update vServiceUsebase_Components
  set SourceName = 'dbo.vServiceFeeDetailsD', flgFake = 0
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 1

update vServiceUsebase_Components
  set SourceName = 'dbo.vServiceFeeDetails2D', flgFake = 0
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 2

-- drop database Datastorage200912

--
select * from vHSubscribers_points_Components

delete from vHSubscribers_points_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vHSubscribers_points_Components
  set SourceName = 'dbo.vPointsD', flgFake = 0, ViewName = 'HSubscribers_Points112'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0

--

select * from vServiceUseBase_points_Components


delete from vServiceUseBase_points_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vServiceUseBase_points_Components
  set SourceName = 'dbo.vService_PointsD', flgFake = 0, ViewName = 'ServiceUseBase_Points112'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0
--

select * from vCallBase_points_Components


delete from vCallBase_points_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vCallBase_points_Components
  set SourceName = 'dbo.vCallBase_PointsD', flgFake = 0, ViewName = 'CallBase_Points112'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0

--

select * from vDataEvent_Components

delete from vDataEvent_Components where  YYYYMM = '2010-07-01 00:00:00.000'

update vDataEvent_Components
  set SourceName = 'dbo.DataEventA'--, ViewName = 'DataEventA'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 0

update vDataEvent_Components
  set SourceName = 'dbo.DataEventPriceA'--, ViewName = 'CallBaDataEventPriceAse_Points112'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 1

update vDataEvent_Components
  set SourceName = 'dbo.DataEventPropertiesA'--, ViewName = 'DataEventPropertiesA'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 2

update vDataEvent_Components
  set SourceName = 'dbo.DataEventBaseDiscountA'--, ViewName = 'DataEventBaseDiscountA'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 3

update vDataEvent_Components
  set SourceName = 'dbo.DataEventAccDiscountA'--, ViewName = 'DataEventAccDiscountA'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 4

update vDataEvent_Components
  set SourceName = 'dbo.DataChargeBaseA'--, ViewName = 'DataChargeBaseA'
  where YYYYMM = '2009-12-01 00:00:00.000' and Partno = 5

------------------------------------------------------------------------------------------

CallBaseDetailsD

    select *
      from vServiceUseBase_Components 
      where YYYYMM = @next1st_
        and PartNo = 0

    select *
      from vHSubscribers_Points_Components 

select * from vPointsD
select * from vCallBase_PointsD
select * from vService_PointsD

CallBase_PointsCur

   select *--@SourceName = SourceName 
      from vCallBase_Points_Components 
      where YYYYMM = @next1st_

    select *--@SourceName = SourceName 
      from vServiceUseBase_Points_Components 
      where YYYYMM = @next1st_
        and PartNo = 0


select * from contract_number cn (nolock)
  inner join numbers n (nolock)
  on n.numberid = cn.numberid 
    inner join contract cnt 
    on cnt.contractid = cn.contractid 
      inner join client cl
      on cl.clientid = cnt.clientid
  where CategoryID = 1 and credit-debet > 0


-- ServiceUseBaseDetails Label
  if exists( 
                                 select 1 
                                  from dbo.sysobjects 
                                  where [id] = object_id( '[dbo].[ServiceUseBaseDetails]' )  
                                    and objectproperty( id, 'IsView' ) = 1 
                                 )                     
                        drop view [dbo].[ServiceUseBaseDetails]
go
/*
    HANDS OFF! 
    View was generated by special procedure. 
*/
create view [dbo].[ServiceUseBaseDetails] as 
select * from [dbo].[ServiceUseBase007] union all
select * from [dbo].[ServiceUseBaseDetails006] union all
select * from [dbo].[ServiceUseBaseDetails005] union all
select * from [dbo].[ServiceUseBaseDetails004] union all
select * from [dbo].[ServiceUseBaseDetails003] union all
select * from [dbo].[ServiceUseBaseDetails002] union all
select * from [dbo].[ServiceUseBaseDetails001] union all
select * from [dbo].[ServiceUseBaseDetails112]
go
grant select on [dbo].[ServiceUseBaseDetails] to public
go