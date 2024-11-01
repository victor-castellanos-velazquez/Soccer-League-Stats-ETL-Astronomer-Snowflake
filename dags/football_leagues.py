import pandas as pd
import uuid
import time
import random
from datetime import datetime

# Función para obtener los datos de una liga
def get_data(url, liga):
    tiempo = [1, 3, 2]
    time.sleep(random.choice(tiempo))  # Simula un retardo aleatorio en la descarga
    df = pd.read_html(url)  # Lee las tablas de la URL proporcionada
    df = pd.concat([df[0], df[1]], ignore_index=True, axis=1)  # Combina tablas
    df = df.rename(columns={0: 'EQUIPO', 1: 'J', 2: 'G', 3: 'E', 4: 'P', 5: 'GF', 6: 'GC', 7: 'DIF', 8: 'PTS'})
    df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric() else x[4:])  # Limpia nombres de equipo
    df['LIGA'] = liga  # Agrega columna de liga

    # Fecha de creación
    run_date = datetime.now().strftime("%Y-%m-%d")
    df['CREATED_AT'] = run_date

    return df

# Función para procesar datos de todas las ligas
def data_processing(df_ligas):
    dfs = []
    for i in range(len(df_ligas)):
        df = get_data(df_ligas['URL'][i], df_ligas['LIGA'][i])
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=False)
    return df_final

# URLs de las ligas y sus nombres
url = [
    'https://www.espn.com.co/futbol/posiciones/_/liga/esp.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/eng.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/ita.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/ger.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/fra.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/por.1',
    'https://www.espn.com.co/futbol/posiciones/_/liga/ned.1'
]
ligas = ['ESPAÑA', 'INGLATERRA', 'ITALIA', 'GERMANY', 'FRANCIA', 'PORTUGAL', 'HOLANDA']

# DataFrame de ligas y URLs
df_ligas = pd.DataFrame({
    'LIGA': ligas,
    'URL': url
})

# Procesar datos de todas las ligas
df_final = data_processing(df_ligas)

# Función para generar un ID único por equipo
def id_generator(team):
    return str(uuid.uuid1())[:8]

# Generar IDs únicos para los equipos
equipos = df_final['EQUIPO'].unique()
lista_resultado = list(map(id_generator, equipos))

# Crear DataFrame con equipos e IDs
df_team = pd.DataFrame({
    'EQUIPO': equipos,
    'ID_TEAM': lista_resultado
})

# Guardar la tabla de equipos
df_team.to_csv('team_table.csv', index=False)

# Hacer el merge con el DataFrame de equipos para añadir los IDs
df_final = pd.merge(df_final, df_team, how='inner', on='EQUIPO')

# Reordenar columnas
df_final = df_final[['ID_TEAM', 'EQUIPO', 'J', 'G', 'E', 'P', 'GF', 'GC', 'DIF', 'PTS', 'LIGA', 'CREATED_AT']]
