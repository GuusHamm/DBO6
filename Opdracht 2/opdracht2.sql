DROP TABLE IF EXISTS GerelateerdNieuws;
DROP TABLE IF EXISTS CategorieAbonnement;
DROP TABLE IF EXISTS Reactie;
DROP TABLE IF EXISTS Nieuwsbericht;
DROP TABLE IF EXISTS Auteur;
DROP TABLE IF EXISTS Categorie;
DROP TABLE IF EXISTS MailAbonnee;

-- CREATE LANGUAGE plpythonu;

CREATE TABLE MailAbonnee
(
  id                  SERIAL PRIMARY KEY,
  voornaam            VARCHAR(64),
  achternaam          VARCHAR(255),
  email               VARCHAR(128),
  abonnementType      VARCHAR(128),
  laatstVerzondenMail TIMESTAMP
);

CREATE TABLE Categorie
(
  naam         VARCHAR(64) PRIMARY KEY,
  omschrijving VARCHAR(2048)
);

CREATE TABLE Auteur
(
  id         SERIAL PRIMARY KEY,
  voornaam   VARCHAR(64),/mnt/ssd/Fontys/S6/ESD6/goverment-angular
  achternaam VARCHAR(255),
  persbureau VARCHAR(255),
  details    VARCHAR(2048)
);

CREATE TABLE Nieuwsbericht
(
  id            SERIAL PRIMARY KEY,
  geplaatstOp   TIMESTAMP,
  berichtKop    VARCHAR(255),
  bericht       VARCHAR(2048),
  bronLink      VARCHAR(255),
  categorieNaam VARCHAR(64) REFERENCES Categorie (naam),
  auteurId      INT REFERENCES Auteur (id)
);

CREATE TABLE Reactie
(
  id              SERIAL PRIMARY KEY,
  naam            VARCHAR(64),
  geplaatstOp     TIMESTAMP,
  ipAdres         VARCHAR(39),
  reactieTekst    VARCHAR(1024),
  zichtbaar       NUMERIC(1),
  nieuwsberichtId INT REFERENCES Nieuwsbericht (id)
);

CREATE TABLE CategorieAbonnement
(
  mailAbonneeId INT REFERENCES MailAbonnee (id),
  categorieNaam VARCHAR(64) REFERENCES Categorie (naam),
  PRIMARY KEY (mailAbonneeId, categorieNaam)
);

CREATE TABLE GerelateerdNieuws
(
  nieuwsberichtId            INT REFERENCES Nieuwsbericht (id),
  gerelateerdNieuwsberichtId INT REFERENCES Nieuwsbericht (id),
  PRIMARY KEY (nieuwsberichtId, gerelateerdNieuwsberichtId)
);

CREATE OR REPLACE FUNCTION create_test_data(tablename VARCHAR(255), number INTEGER)
  RETURNS INTEGER
AS $$
from faker import Faker
from collections import OrderedDict
import plpy

faker = Faker()

column_prepare = plpy.prepare('SELECT * FROM information_schema.columns WHERE table_name=$1', ['varchar'])

column_names = plpy.execute(column_prepare, [tablename])

foreign_key_prepare = plpy.prepare('SELECT tc.constraint_name, tc.constraint_type, tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE tc.table_name = $1',
                                   ['varchar'])

foreign_key_value_prepare = 'SELECT %s FROM %s ORDER BY random()'

columns = plpy.execute(foreign_key_prepare, [tablename])
foreign_keys = {column['column_name'].lower(): column for column in columns}

plpy.log("Generating data for %d columns" % len(column_names))

columns = []

generate_count = number * 3

query_base = 'INSERT INTO %s (%s) VALUES (%s)'
query_columns = OrderedDict()  # ','.join({}.keys())
query_values = [[] for i in range(generate_count)]  # ','.join(['$' + i for i in range(2,10)])

for column in column_names:
    column_name = column['column_name'].lower()

    if column_name in foreign_keys.keys():
        foreign_key = foreign_keys[column_name]
        if foreign_key['constraint_type'] == 'PRIMARY KEY':
            continue
        foreign_key_values = plpy.execute(foreign_key_value_prepare % (foreign_key['foreign_column_name'], foreign_key['foreign_table_name']), number)
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            random_value = foreign_key_values[faker.random.randint(0, number - 1)]
            query_values[i].append(random_value[foreign_key['foreign_column_name']])

        continue

    # if column_name in ['id'] and column['column_default']:
    #     # do nothing with this column, assume autogenerate
    #     continue

    if column_name in ['firstname', 'voornaam']:
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.first_name())
        continue

    if column_name in ['lastname', 'achternaam']:
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.last_name())
        continue

    if column_name in ['name', 'naam']:
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.name())
        continue

    if column_name in ['email']:
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.email())
        continue

    if any(part in column_name for part in ['link', 'url']):
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.url())
        continue

    if column['data_type'] == 'character varying':
        query_columns[column_name] = column['data_type']
        max_value = column['character_maximum_length']
        for i in range(generate_count):
            query_values[i].append(faker.text(max_nb_chars=faker.random.randint(5, max_value)))
        continue

    if column['data_type'].startswith('timestamp '):
        query_columns[column_name] = column['data_type']
        withtz = 'with timezone' in column['data_type']
        for i in range(generate_count):
            if any(part in column_name for part in ['laatst', 'recent']):
                date = faker.date_time_this_decade(tzinfo=faker.timezone() if withtz else None)
            else:
                date = faker.date_time(tzinfo=faker.timezone() if withtz else None)
            query_values[i].append(date)
        continue

    if column['data_type'] in ['inet']:
        query_columns[column_name] = column['data_type']
        for i in range(generate_count):
            query_values[i].append(faker.ipv4(network=False))
        continue

query = query_base % (tablename,
                      ','.join(query_columns.keys()),
                      ','.join(['$' + str(i) for i in range(1, len(query_columns.keys()) + 1)])
                      )
query = plpy.prepare(query, query_columns.values())

inserted = 0

for value_line in query_values:
    try:
        plpy.execute(query, value_line)
        inserted += 1
        if inserted >= number:
            break  # We have inserted enough
    except Exception as e:
        import traceback

        plpy.info(traceback.format_exc())

return inserted
$$ LANGUAGE plpythonu;

SELECT create_test_data('nieuwsbericht', 10);
SELECT create_test_data('mailabonnee', 10);
SELECT * from Auteur;
SELECT * from Categorie;
SELECT * from MailAbonnee;
SELECT * from Nieuwsbericht;

SELECT *
FROM MailAbonnee;

SELECT *
FROM information_schema.columns
WHERE table_name = 'mailabonnee';

SELECT *
FROM information_schema.columns
WHERE table_schema = 'public';

SELECT
  tc.constraint_name, tc.table_name, kcu.column_name,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name
FROM
  information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
  JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.table_name = 'nieuwsbericht';
