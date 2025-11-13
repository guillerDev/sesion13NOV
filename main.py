import csv

## 1. Leer el archivo Csv. 

def generar_script_sql(nombre_tabla="sensor_data"):
    
    with open('lecturas_bis.csv', mode = 'r', encoding = 'utf-8') as archivo_entrada:
        lector = csv.reader(archivo_entrada)
        ## Leer el encabezado para usarlo en SQL
        encabezado_sql = next(lector)
        ## Leer todos los datos para el procesamiento posterior
        datos = list(lector) 
        
        tipos_inferidos = {}
        for i, nombre_columna in enumerate(encabezado_sql):
            # Inferir el tipo de dato para la columna
            tipos_posibles = set()
            for file in datos[:10]:
                tipos_posibles.add(inferir_tipo_sql(file[i]))

            if "INTEGER" in tipos_posibles:
                tipos_inferidos[nombre_columna] = "INTEGER"
            elif "DECIMAL(10, 2)" in tipos_posibles:
                tipos_inferidos[nombre_columna] = "DECIMAL(10, 2)"
            else:
                tipos_inferidos[nombre_columna] = "VARCHAR(255)"

        # Generar el script SQL
        script_sql = f"CREATE TABLE {nombre_tabla} (\n"
        for columna, tipo in tipos_inferidos.items():
            script_sql += f"  {columna} {tipo},\n"  

        print(script_sql)


def inferir_tipo_sql(valor):
    try:
        int(valor)
        return "INTEGER"
    except ValueError:
        pass
    
    try:
        float(valor)
        return "DECIMAL(10, 2)"
    except ValueError:
        pass 

    return "VARCHAR(255)"


## 2. Inferir el tipo para cada columna.

## 3. Dar formato en SQL (CSV en SQL) <


generar_script_sql()