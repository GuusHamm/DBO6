CREATE LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION calculate_total_price()
  RETURNS TRIGGER
AS $$
import plpy
from datetime import datetime

extra_statement = plpy.prepare("SELECT * FROM extra e JOIN huurobject h on e.\"HuurobjectID\" = h.\"ID\" WHERE e.\"Factuurnummer\" = $1", ["integer"])
voertuig_statement = plpy.prepare("SELECT * FROM voertuig v JOIN voertuigtype ON v.\"VoertuigtypeID\" = voertuigtype.\"ID\" WHERE v.\"ID\" = $1", ["integer"])

total_price = 0
huurovereenkomst = TD['new']

van_datum = datetime.strptime(huurovereenkomst['VanDatum'], '%Y-%m-%d')
tot_datum = datetime.strptime(huurovereenkomst['TotDatum'], '%Y-%m-%d')
number_of_days = (tot_datum - van_datum).days

voertuig = plpy.execute(voertuig_statement, [huurovereenkomst['VoertuigID']])[0]
total_price += voertuig['PrijsPerDag'] * number_of_days

extras = plpy.execute(extra_statement, [huurovereenkomst['Factuurnummer']])
if extras is not None:
    for extra in extras:
        plpy.log(extra)
        total_price += extra['PrijsPerDag'] * number_of_days

TD['new']['TotaalPrijs'] = total_price
return "MODIFY"
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION car_is_available()
  RETURNS TRIGGER
AS $$
import plpy

huurovereenkomst = TD['new']
overlap_statement = plpy.prepare("SELECT * FROM huurovereenkomst WHERE $1 <= \"TotDatum\" AND $2 >= \"VanDatum\" AND \"VoertuigID\" = $3", ["date", "date", "integer"])

overlap = plpy.execute(overlap_statement, [
    huurovereenkomst['VanDatum'],
    huurovereenkomst['TotDatum'],
    huurovereenkomst['VoertuigID']
])

if not overlap:
    return "OK"
else:
    return "SKIP"
$$ LANGUAGE plpythonu;

CREATE TRIGGER check_insert_car_available
BEFORE INSERT ON huurovereenkomst
FOR EACH ROW
EXECUTE PROCEDURE car_is_available();

CREATE TRIGGER calculate_total_price_insert_trigger
BEFORE INSERT ON huurovereenkomst
FOR EACH ROW
EXECUTE PROCEDURE calculate_total_price();

CREATE TRIGGER calculate_total_price_update_trigger
BEFORE UPDATE ON huurovereenkomst
FOR EACH ROW
EXECUTE PROCEDURE calculate_total_price();

INSERT INTO huurovereenkomst ("Factuurnummer", "HuurderRelatienummer", "VoertuigID", "VanDatum", "TotDatum")
VALUES (100209, 654897321, 1, make_date(2017, 03, 25), make_date(2017, 03, 27));

INSERT INTO huurovereenkomst ("Factuurnummer", "HuurderRelatienummer", "VoertuigID", "VanDatum", "TotDatum")
VALUES (100209, 654897321, 1, make_date(2017, 04, 25), make_date(2017, 04, 27));

UPDATE huurovereenkomst
SET "TotaalPrijs" = 0
WHERE "Factuurnummer" = 976184;