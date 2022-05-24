/*
CCDM convert_to_numeric function
Notes: 
    Cast string to a numeric and return null otherwise
*/

CREATE OR REPLACE FUNCTION convert_to_numeric(pNum text) 
RETURNS numeric AS
$$
DECLARE
    lNum numeric := null;
BEGIN
    lNum := pNum::numeric;
    RETURN lNum;
EXCEPTION
    WHEN OTHERS THEN
        RETURN null;
END
$$ LANGUAGE plpgsql;



