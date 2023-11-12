create table patient_drug_dispensations as
select 
p.patient_id ,
p.date_of_birth ,
p.gender,
dd.drug_name,
dd.dispense_date ,
dd.quantity_dispensed 
from patients p 
inner join drug_dispensations dd on p.patient_id  = dd.patient_id;
create table patient_appointments as
select
	a.patient_id,
	dd.dispense_date,
	a.appointment_date,
	dd.drug_name ,
	sum(dd.quantity_dispensed)
from
	appointments a
inner join drug_dispensations dd on
	a.patient_id = dd.patient_id
group by
	a.patient_id,
	dd.dispense_date,
	a.appointment_date,
	dd.drug_name;