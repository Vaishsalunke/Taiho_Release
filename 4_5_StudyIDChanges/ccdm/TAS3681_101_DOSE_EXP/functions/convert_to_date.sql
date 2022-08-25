/*
CDM convert_to_date function
Client: Regeneron
Notes: 
    Cast text to a date and return null otherwise
*/

CREATE OR REPLACE FUNCTION convert_to_date(pDate text) 
RETURNS date AS
$$
DECLARE
    lDate date := null;
BEGIN
    lDate := pDate::date;
    RETURN lDate;
EXCEPTION
    WHEN OTHERS THEN
        RETURN null;
END
$$ LANGUAGE plpgsql;

