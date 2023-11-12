insert into summaries(table_name,total_records) (
            select 'patient_drug_dispensations' table_name ,count(*) Total_Records from patient_drug_dispensations
            where extract(year from dispense_date) =  {{params.year}}
            union 
            select 'patient_appointments' table_name ,count(*) Total_Records from patient_appointments
            );