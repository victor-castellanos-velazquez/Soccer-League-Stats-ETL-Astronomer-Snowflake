COPY INTO "PUBLIC"."FOOTBALL_LEAGUES" 
(equipo, Jugados, ganados, empatados, perdidos, gf, gc, diff, puntos, liga, created_at)
FROM @"PUBLIC"."DEMO_STAGE"
FILES = ('football_positions.csv.gz') 
FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1) 
ON_ERROR = 'CONTINUE';