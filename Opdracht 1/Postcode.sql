DROP TABLE STREET, CITY, MUNICIPALITY, PROVINCE;


CREATE TABLE PROVINCE (
  id   BIGSERIAL NOT NULL PRIMARY KEY,
  name VARCHAR(255),
  code VARCHAR(2)
);

CREATE TABLE MUNICIPALITY (
  id       BIGSERIAL NOT NULL PRIMARY KEY,
  name     VARCHAR(255),
  province BIGSERIAL REFERENCES PROVINCE (id)
);

CREATE TABLE CITY (
  id           BIGSERIAL NOT NULL PRIMARY KEY,
  name         VARCHAR(255),
  municipality BIGSERIAL REFERENCES MUNICIPALITY (id)
);

CREATE TABLE STREET (
  id              BIGSERIAL NOT NULL PRIMARY KEY,
  name            VARCHAR(255),
  pnum            VARCHAR(4),
  pchar           VARCHAR(4),
  minimum         INTEGER DEFAULT 0,
  maximum         INTEGER DEFAULT 99999,
  numbertype      VARCHAR(16),
  lat             NUMERIC(15, 13),
  lon             NUMERIC(15, 13),
  rd_x            NUMERIC(31, 20),
  rd_y            NUMERIC(31, 20),
  location_detail VARCHAR(16),
  changed_date    TIMESTAMP,
  city            BIGSERIAL REFERENCES CITY (id)
);

ALTER SEQUENCE municipality_id_seq RESTART WITH 2000;

CREATE OR REPLACE LANGUAGE "plpythonu";

DO $$
from plpy import spiexceptions
import plpy

cursor = plpy.cursor("SELECT * FROM postcode")

provinces = {}
municipalities = {}
cities = {}

municipality_exists_statement = plpy.prepare("SELECT id, name FROM municipality WHERE name = $1", [
    "varchar"
])
city_exists_statement = plpy.prepare("SELECT id, name FROM city WHERE name = $1 AND municipality = $2", [
    "varchar",
    "int8"
])
city_exists_error_statement = plpy.prepare("SELECT id, name FROM city WHERE id = $1", [
    "int8"
])

province_insert_statement = plpy.prepare("INSERT INTO province(name,code) VALUES ($1,$2) RETURNING *", [
    "varchar",
    "varchar"
])
municipality_insert_statement = plpy.prepare(
    "INSERT INTO municipality(id,name,province) VALUES ($1,$2,$3) RETURNING *", [
        "int8",
        "varchar",
        "int8"
    ])
municipality_insert_error_statement = plpy.prepare(
    "INSERT INTO municipality(name,province) VALUES ($1,$2) RETURNING *", [
        "varchar",
        "int8"
    ])

city_insert_statement = plpy.prepare("INSERT INTO city(id,name,municipality) VALUES ($1,$2,$3) RETURNING *", [
    "int8",
    "varchar",
    "int8"
])
city_insert_error_statement = plpy.prepare("INSERT INTO city(name,municipality) VALUES ($1,$2) RETURNING *", [
    "varchar",
    "int8"
])
street_insert_statement = plpy.prepare(
    "INSERT INTO street(id,name,pnum,pchar,minimum,maximum,numbertype,lat,lon,rd_x,rd_y,location_detail,changed_date,city) " +
    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id", [
        "int8",
        "varchar",
        "int8",
        "varchar",
        "int8",
        "int8",
        "varchar",
        "numeric",
        "numeric",
        "numeric",
        "numeric",
        "varchar",
        "timestamp",
        "int8"
    ])

while True:
    postcodes = cursor.fetch(10)
    if not postcodes:
        # At this point all postcodes have been processed
        break
    for postcode in postcodes:
        # Add province to provinces if it's not yet in there
        if postcode['PROVINCE'] not in provinces.keys():
            province = plpy.execute(province_insert_statement, [
                postcode['PROVINCE'],
                postcode['PROVINCE_CODE']
            ])
            provinces[postcode['PROVINCE']] = province[0]['id']

        # Add municipality to municipalities if it's not yet there
        if postcode['MUNICIPALITY'] not in municipalities.keys():
            # Check if the municpality exists in the database
            municipality_exists = plpy.execute(municipality_exists_statement, [
                postcode['MUNICIPALITY']
            ])
            if not municipality_exists:
                try:
                    # Create a new municipality and add it to municipalities
                    municipality = plpy.execute(municipality_insert_statement, [
                        postcode['MUNICIPALITY_ID'],
                        postcode['MUNICIPALITY'],
                        provinces[postcode['PROVINCE']]
                    ])[0]
                    municipality_id = municipality['id']
                    municipalities[postcode['MUNICIPALITY']] = municipality
                except spiexceptions.UniqueViolation:
                    # The data set contains some conflicting municipality id's these are handled by this insert
                    municipality = plpy.execute(municipality_insert_error_statement, [
                        postcode['MUNICIPALITY'],
                        provinces[postcode['PROVINCE']]
                    ])[0]
                    municipalities[postcode['MUNICIPALITY']] = municipality
                    municipality_id = municipality['id']
            else:
                # Use the municipality from the database
                municipality_id = municipality_exists[0]['id']
        else:
            # Use the municipality from the municipality array
            municipality = municipalities[postcode['MUNICIPALITY']]
            municipality_id = municipality['id']

        if postcode['CITY'] not in cities.keys():
            # Check if city exists in the database
            city_exists = plpy.execute(city_exists_statement, [
                postcode['CITY'], municipality_id
            ])
            if not city_exists:
                try:
                    # Create a new city and add it to cities
                    city = plpy.execute(city_insert_statement, [
                        postcode["CITY_ID"],
                        postcode["CITY"],
                        municipality_id
                    ])[0]
                    city_id = city['id']
                    cities[postcode['CITY']]  = city
                except spiexceptions.UniqueViolation:
                    # Some cities have different names but a matching id.
                    other_city = plpy.execute(city_exists_error_statement, [
                        postcode['CITY_ID']
                    ])
                    if other_city and postcode['CITY'] not in other_city[0]['name']:
                        # If the name is not in the other cities name create a new city with a new id.
                        city = plpy.execute(city_insert_error_statement, [
                            other_city[0]['name'],
                            municipality_id
                        ])[0]
                        city_id = city['id']
                        cities[postcode['CITY']] = city
            else:
                # The use the city from the database
                city_id = city_exists[0]['id']
        else:
            # Use the city from the cities array
            city = cities[postcode['CITY']]
            city_id = city['id']

        # Insert our new street into the database
        plpy.execute(street_insert_statement, [
            postcode['ID'],
            postcode['STREET'],
            postcode['PNUM'],
            postcode['PCHAR'],
            postcode['MINNUMBER'],
            postcode['MAXNUMBER'],
            postcode['NUMBERTYPE'],
            postcode['LAT'],
            postcode['LON'],
            postcode['RD_X'],
            postcode['RD_Y'],
            postcode['LOCATION_DETAIL'],
            postcode['CHANGED_DATE'],
            city_id
        ])
$$ LANGUAGE plpythonu;