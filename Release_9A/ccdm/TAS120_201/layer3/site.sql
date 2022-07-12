/*
CCDM Site mapping
Client:taiho
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
    site_data AS (
  
    SELECT  distinct 'TAS120_201'::text AS studyid,
			NULL::text AS studyname,
                        "oid"::text AS siteid,
                        case when tss.sitename_std is null then split_part(s."name",'_',2)
						else  tss.sitename_std end::text AS sitename,
                        'UBC'::text AS croid,
                        'UBC'::text AS sitecro,
                        case 
							when length(trim(SUBSTRING( "name",1, POSITION('_' in "name")-1)))=3
								THEN CASE when left(SUBSTRING( "name",1, POSITION('_' in "name")-1),1)='0' then 'United States'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '100' and '149' then 'United Kingdom'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '150' and '199' then 'Spain'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '200' and '249' then 'Portugal'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '250' and '299' then 'France'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '300' and '349' then 'Italy'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '350' and '399' then 'Canada'
									 end
						else 'United States' end::text AS sitecountry,
                        case 
							when length(trim(SUBSTRING( "name",1, POSITION('_' in "name")-1)))=3
								 THEN CASE when left(SUBSTRING( "name",1, POSITION('_' in "name")-1),1)='0' then 'North America'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '100' and '149' then 'Europe'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '150' and '199' then 'Europe'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '200' and '249' then 'Europe'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '250' and '299' then 'Europe'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '300' and '349' then 'Europe'
										   when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '350' and '399' then 'North America'
									 end
						else 'North America' end::text AS siteregion,
                        "effectivedate"::date AS sitecreationdate,
						--null:: date as siteplannedactivationdate,
                        "effectivedate"::date AS siteactivationdate,
						case when lower(sc.closeout_status) = 'site closeout' then sc.cov_visit_end_date
                        else null end::date AS sitedeactivationdate,
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        case 
                       	when "active"='Yes' then 'Initiating'
                       	else 'Inactive' end::text AS sitestatus,
                       case 
                       	when "active"='Yes' then "effectivedate" else null end ::date AS sitestatusdate,
                       	true::boolean AS statusapplicable
						
			from tas120_201."__sites" s left join internal_config.taiho_sitename_standards tss
						on 
						 s."oid" = tss.siteid
						--and split_part(s."name",'_',2) = tss.sitename
						left join tas120_201_ctms.site_closeout sc on split_part(s."name",'_',1) = sc.site_id
						--where tss.studyid = 'TAS120_201'
						/*LIMIT LIMIT 100 LIMIT*/
						),
						sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry)
						
	SELECT 
        /*KEY (s.studyid || '~' || s.siteid)::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.studyname::text AS studyname,
        s.siteid::text AS siteid,
        s.sitename::text AS sitename,
        s.croid::text AS croid,
        s.sitecro::text AS sitecro,
       	case when s.sitecountry='United States' then 'United States of America'
        else s.sitecountry
        end::text AS sitecountry,
        cc.countrycode3_iso::text AS sitecountrycode,
        s.siteregion::text AS siteregion,
        s.sitecreationdate::date AS sitecreationdate,
        s.siteactivationdate::date AS siteactivationdate,
        s.sitedeactivationdate::date AS sitedeactivationdate,
        s.siteinvestigatorname::text AS siteinvestigatorname,
        s.sitecraname::text AS sitecraname,
        s.siteaddress1::text AS siteaddress1,
        s.siteaddress2::text AS siteaddress2,
        s.sitecity::text AS sitecity,
        s.sitestate::text AS sitestate,
        s.sitepostal::text AS sitepostal,
        s.sitestatus::text AS sitestatus,
        s.sitestatusdate::date AS sitestatusdate,
        s.statusapplicable::boolean AS statusapplicable
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid)
LEFT JOIN sitecountrycode_data cc ON (s.studyid = cc.studyid AND LOWER(s.sitecountry)=LOWER(cc.countryname_iso));



