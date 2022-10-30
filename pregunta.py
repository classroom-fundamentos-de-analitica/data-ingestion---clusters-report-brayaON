"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo
espacio entre palabra y palabra.


"""
import pandas as pd
import re


def ingest_data():

    #
    # Inserte su código aquí
    #

    filename = './clusters_report.txt'
    cols = ['Cluster',  'Cantidad de\npalabras clave','Porcentaje de\npalabras clave','Principales palabras clave']
    df = pd.read_csv(
            filename,
            decimal=',',
            sep='\t',
            skiprows=[0,1,2,3],
            names=cols,
            skipinitialspace=True,
    )

    ccol, ccpal, cpor, cpal = [], [], [], []
    l = []

    for row in df['Cluster']:
        aux = re.split(r'\s+', row)
        if '%' in aux:
            aux = re.split(r',\s+', row)

        for elem in aux:
            if '%' in elem:
                aux2 = re.split(r'\s+', row)
                first = re.split(r'\s{4,}', elem)
                ccol.append(int(first[0]))
                ccpal.append(int(first[1]))
                cpor.append(first[2])
                if len(l) > 0:
                    cpal.append(' '.join(l))
                    row_words = []
                    l = []
                l.append(first[3])

            else:
                l.append(elem)

    cpal.append(' '.join(l))

    # Reemplazos manuales :'(
    cpal[0] = cpal[0].replace('tracking', 'tracking,').replace('control', 'control,').replace('pv)', 'pv),')
    cpal[1] = cpal[1].replace('machine', 'machine,').replace('memory', 'memory,')
    cpal[2] = cpal[2].replace('grid', 'grid,').replace('power', 'power,').replace('reinforcement learning', 'reinforcement learning,').replace('management', 'management,')
    cpal[3] = cpal[3].replace('turbine', 'turbine,').replace('diagnosis', 'diagnosis,').replace('biodiesel', 'biodiesel,').replace('detection', 'detection,')
    cpal[4] = cpal[4].replace('vehicle', 'vehicle,').replace('batteries', 'batteries,').replace('charge', 'charge,')
    cpal[5] = cpal[5].replace('particle  swarm  optimization', 'particle  swarm  optimization,').replace('ration', 'ration,').replace('charge', 'charge,')
    cpal[6] = cpal[6].replace('zation', 'zation,').replace('age', 'age,')
    cpal[7] = cpal[7].replace('thm', 'thm,').replace('ment', 'ment,').replace('saving', 'saving,')
    cpal[8] = cpal[8].replace('fis', 'fis,').replace('global   solar   irradiance', 'global   solar   irradiance,').replace('cast', 'cast,')
    cpal[9] = cpal[9].replace('grid', 'grid,').replace('tems', 'tems,').replace('urce', 'urce,')
    cpal[10] = cpal[10].replace('gen', 'gen,').replace('char', 'char,').replace('mass', 'mass,').replace('biogas', 'biogas,').replace('cell', 'cell,').replace('gasification', 'gasification,')
    cpal[11] = cpal[11].replace('estimation', 'estimation,').replace('function', 'function,')
    cpal[12] = cpal[12].replace('cell', 'cell,').replace('networks', 'networks,')


    # Procesamiento previos de filas
    cpor = [ float(s.replace(',', '.').replace(' %', '')) for s in cpor ]

    aux = cpal
    cpal = []
    for line in aux:
        s = re.sub('\s+', ' ', line)
        s = re.sub('\.', '', s)
        s = re.sub('\,,', ',', s)
        if s:
            cpal.append(s)


    # Prueba del resultado final
    # print(ccol)
    # print(ccpal)
    # print(cpor)
    # print(cpal)

    # Nuevo index
    nw_cols = list(df.columns)
    nw_cols = [ s.replace('\n', ' ').replace(' ', '_').lower() for s in nw_cols ]

    # print(nw_cols)
    # Construcción del dataframe
    new_df = pd.DataFrame(list(zip(ccol, ccpal, cpor, cpal)), columns = nw_cols)

    # print(new_df.cluster.to_list())
    # print(new_df.cantidad_de_palabras_clave.to_list())
    return new_df

ingest_data()
