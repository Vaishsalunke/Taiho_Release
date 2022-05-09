/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    site_data AS (
                SELECT  'TAS117_201'::text AS studyid,
                        null::text AS studyname,
                        concat('TAS117_201','_',substring("name",1,3)) ::text AS siteid,
                        substring("name",position('_' in "name")+1,length(trim("name"))-8) ::text AS sitename,
                        'Syneos'::text AS croid,
                        'Syneos'::text AS sitecro,
                        case when length(split_part("name",'_',1))=3 then
                       		 case when split_part("name",'_',1) between '0' and '199' then 'United States'
                			 	  when split_part("name",'_',1) between '200' and '300' then 'France'
                			      when split_part("name",'_',1) between '301' and '499' then 'United Kingdom'
                			      when split_part("name",'_',1) between '500' and '599' then 'Austria'
                			      when split_part("name",'_',1) between '600' and '649' then 'Germany'
                			      when split_part("name",'_',1) between '650' and '699' then 'Italy'
                			      when split_part("name",'_',1) between '700' and '799' then 'Japan'
                			 	  when split_part("name",'_',1) between '800' and '849' then 'South Korea'
                			 	  when split_part("name",'_',1) between '850' and '899' then 'Spain'
                			      when split_part("name",'_',1) between '900' and '999' then 'Singapore'
                			 else 'UNKNOWN' end 
                		else 'UNKNOWN'	 
                		end::text AS sitecountry,
                        null::text AS sitecountrycode,
                        case when length(split_part("name",'_',1))=3 then
                        	 case when split_part("name",'_',1) between '0' and '199' then 'North America'
                			 	  when split_part("name",'_',1) between '200' and '300' then 'Europe'
                			      when split_part("name",'_',1) between '301' and '499' then 'Europe'
                			      when split_part("name",'_',1) between '500' and '599' then 'Europe'
                			      when split_part("name",'_',1) between '600' and '649' then 'Europe'
                			      when split_part("name",'_',1) between '650' and '699' then 'Europe'
                			      when split_part("name",'_',1) between '700' and '799' then 'Asia'
                			      when split_part("name",'_',1) between '800' and '849' then 'Asia'
                			      when split_part("name",'_',1) between '850' and '899' then 'Asia'
                			      when split_part("name",'_',1) between '900' and '999' then 'Asia'
                			 else 'UNKNOWN' end
                		else 'UNKNOWN'	 
                		end::text AS siteregion,
						True::BOOLEAN AS statusapplicable,
                        null::date AS sitecreationdate,
                        null::date AS siteactivationdate,
                        null::date AS sitedeactivationdate,
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        null::text AS sitestatus,
                        null::date AS sitestatusdate
                		from tas117_201."__sites" s ),

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
        --s.sitecountry::text AS sitecountry,
        case when s.sitecountry='United States' then 'United States of America'
        	 when s.sitecountry='South Korea' then 'Korea'
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
		s.statusapplicable::BOOLEAN AS statusapplicable
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid)
LEFT JOIN sitecountrycode_data cc ON (s.studyid = cc.studyid AND LOWER(s.sitecountry)=LOWER(cc.countryname_iso));

