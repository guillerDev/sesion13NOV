import csv



def generar_script_sql(nombre_tabla="sensor_data",
                        nombre_archivo="lecturas_bis.csv",
                        nombre_script="script.sql"):
    ## 1. Leer el archivo Csv. 
    with open(nombre_archivo, mode = 'r', encoding = 'utf-8') as archivo_entrada:
        lector = csv.reader(archivo_entrada)
        ## Leer el encabezado para usarlo en SQL
        encabezado_sql = next(lector)
        ## Leer todos los datos para el procesamiento posterior
        datos = list(lector) 
        ## 2. Inferir el tipo para cada columna.
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
    ## 3. Dar formato en SQL (CSV en SQL) <
    with open(nombre_script, mode='w', encoding='utf-8') as archivo_salida:
        # Generar el schema en SQL para crear la tabla
        script_sql = f"CREATE TABLE {nombre_tabla} (\n"
        for columna, tipo in tipos_inferidos.items():
            script_sql += f"  {columna} {tipo},\n"  
        script_sql = script_sql.rstrip(",\n") + "\n);\n\n"


        archivo_salida.write(script_sql)

        # Insertar los datos en la tabla
        base_insert = f"INSERT INTO {nombre_tabla} ({', '.join(encabezado_sql)}) VALUES\n"

        for fila in datos:
            valores_formateados = []
            for i, valor in enumerate(fila):
                tipo = tipos_inferidos[encabezado_sql[i]]
                if tipo == "INTEGER" or tipo.startswith("DECIMAL"):
                    valores_formateados.append(valor)
                else:
                    valores_formateados.append(f"'{valor}'")
            base_insert += f"({', '.join(valores_formateados)}),\n"

        archivo_salida.write(base_insert)



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



generar_script_sql()