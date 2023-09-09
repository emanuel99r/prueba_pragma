# Documentación del Pipeline de Procesamiento de Datos

Este documento proporciona información sobre el Pipeline de Procesamiento de Datos, que se utiliza para leer archivos CSV, cargar datos en una base de datos SQLite y calcular estadísticas acumulativas tanto de los datos mientras se almacenan como cuando ya se encuentran almacenados.

## Configuración

La configuración para la ejecución del pipeline se hace a través del archivo parameters.py, el cual contiene un diccionario con las variables requeridas. Antes de ejecutar el pipeline, debe configurar los parámetros en el diccionario:

1. **db_path**: Configure la ubicación de la base de datos SQLite..

2. **table_name**: Configure el nombre de la tabla en la base de datos.

3. **csv_file**: Agregue un arreglo con las rutas de los archivos CSV que desea procesar.

4. **micro_batche_size**: Tamaño de los fragmentos en los que se divide el conjunto de datos para su procesamiento. Por ejemplo, si su valor es 1 significa que se va a ir leyendo una fila a la vez.

## Uso

Para ejecutar el pipeline, simplemente ejecute el script Python:

```bash
python pipeline.py
```

## FUNCIONES

### Función `process_csv_file`

La función `process_csv_file` lee un archivo CSV y carga sus datos en una base de datos SQLite.

#### Parámetros

`file_path` (`str`): Ruta al archivo CSV que se va a procesar.
`connection` (`sqlite3.Connection`): Objeto con la conexión a la base de datos SQLite.
`table_name` (`str`): Nombre de la tabla en la base de datos donde se cargarán los datos.
`micro_batch_size` (`int`): Tamaño de los fragmentos en los que leerá el conjunto de datos.

#### Devuelve

None

### Función `print_cumulative_statistics`

La función `print_cumulative_statistics` imprime estadísticas acumulativas de las filas que se van cargando en la base de datos. Esta función trabaja netamente con los datos de los archivos CSV.

#### Parámetros

- `total_rows` (`int`): Número total de filas cargadas en la base de datos.
- `total_valid_rows` (`int`): Número total de filas NO VACÍAS.
- `min_price` (`float or None`): Valor mínimo del campo "price" acumulado o None si no se ha registrado ninguno.
- `max_price` (`float or None`): Valor máxmimo del campo "price" acumulado o None si no se ha registrado ninguno.
- `sum_price` (`float`): Suma acumulada del campo "price".

#### Devuelve

None

### Función `get_price_statistics`

La función `get_price_statistics` consulta estadísticas del campo "price" en una tabla de la base de datos SQLite.

#### Parámetros

- `connection` (`sqlite3.Connection`): Objeto con la conexión a la base de datos SQLite.
- `table_name` (`str`): Nombre de la tabla en la base de datos.

#### Devuelve

La función `get_price_statistics` devuelve un diccionario que contiene las estadísticas consultadas:

- `Total de filas`: El número total de filas en la tabla.
- `Promedio de "price"`: El valor promedio de la columna 'price'.
- `Mínimo de "price"`: El valor mínimo de la columna 'price'.
- `Máximo de "price"`: El valor máximo de la columna 'price'.
