# 1
SELECT
  ga.sql_vragen_nr,
  status,
  COUNT(status)               AS number,
  COUNT(status) * 100 / count AS percentage
FROM gebruiker_activiteit ga
  JOIN (SELECT
          sql_vragen_nr,
          COUNT(nr) AS count
        FROM gebruiker_activiteit
        GROUP BY sql_vragen_nr) AS ga2
    ON ga.sql_vragen_nr = ga2.sql_vragen_nr
GROUP BY sql_vragen_nr, STATUS
ORDER BY percentage DESC;

# 2
SELECT
  pcn,
  COUNT(DISTINCT sql_vragen_nr) AS number
FROM gebruiker g
  JOIN gebruiker_activiteit ga ON g.nr = ga.gebruiker_nr
WHERE ga.status = 'correct'
GROUP BY pcn
ORDER BY NUMBER DESC;

# 3
SELECT
  gebruiker_nr,
  AVG(ga.count) AS average
FROM (SELECT
        gebruiker_nr,
        sql_vragen_nr,
        count(nr) AS count
      FROM gebruiker_activiteit
      GROUP BY gebruiker_nr, sql_vragen_nr) AS ga
GROUP BY gebruiker_nr
ORDER BY AVG(count);

# 4
SELECT
  ga.datetime,
  COUNT(ga2.nr) AS correcte_antwoorden
FROM gebruiker_activiteit ga
  JOIN gebruiker g ON ga.gebruiker_nr = g.nr
  JOIN gebruiker_activiteit ga2 ON ga.datetime >= ga2.datetime
                                   AND ga.gebruiker_nr = ga2.gebruiker_nr
                                   AND ga.status = ga2.status
WHERE g.pcn = 'i266921'
      AND ga.status = 'correct'
GROUP BY ga.datetime
ORDER BY ga.datetime;

# 5
SELECT
  dbtype,
  count(dbtype)               AS number,
  count(dbtype) * 100 / count AS percentage
FROM gebruiker_activiteit
  JOIN (SELECT COUNT(ga.nr) AS count
              FROM gebruiker_activiteit AS ga) AS total
GROUP BY dbtype
ORDER BY percentage DESC;

# 7
SELECT
  DATE(aangemaakt),
  COUNT(nr)
FROM gebruiker
GROUP BY DATE(aangemaakt);

# 8
SELECT
  DATE(datetime),
  COUNT(DISTINCT gebruiker_nr)
FROM gebruiker_activiteit
GROUP BY DATE(datetime);

# 12
SELECT DISTINCT
  ga.sql_vragen_nr,
  status,
  g.pcn
FROM gebruiker g
  JOIN gebruiker_activiteit ga ON g.nr = ga.gebruiker_nr
GROUP BY g.pcn, ga.sql_vragen_nr
ORDER BY DATETIME DESC;