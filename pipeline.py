import pandas as pd
import sqlite3
from parameters import configuration


def process_csv_file(file_path, connection, table_name, micro_batch_size):
    """
    Lee un archivo CSV y carga sus datos en una base de datos SQLite fila por fila.

    Args:
        file_path (str): Ruta al archivo CSV que se va a procesar.
        connection (sqlite3.Connection): Objeto con la conexión a la base de datos SQLite.
        table_name (str): Nombre de la tabla en la base de datos donde se cargarán los datos.
        micro_batch_size (int): Tamaño de los fragmentos en los que se leerá el conjunto de datos.

    Returns:
        None

    Raises:
        ValueError: Si se produce un error al procesar o cargar los datos.

    """
    global total_rows, total_valid_rows, min_price, max_price, sum_price  # Declara como variables globales
    cursor = connection.cursor()
    try:
        for rows in pd.read_csv(file_path, chunksize=micro_batch_size):
            for index, row in rows.iterrows():
                #print(row)
                #row = rows.iloc[0]

                # Convierte los valores de 'price' y 'user_id' a enteros, si son válidos
                price = int(row['price']) if pd.notna(row['price']) else None
                user_id = int(row['user_id']) if pd.notna(row['user_id']) else None

                cursor.execute(f"INSERT INTO {table_name} (timestamp, price, user_id) VALUES (?, ?, ?)",
                            (row['timestamp'], price, user_id))
                connection.commit()

                # Actualiza estadísticas acumulativas
                total_rows += 1
                if price is not None:
                    total_valid_rows += 1 # Acumula las filas sin datos vacíos
                    current_price = price
                    if min_price is None or current_price < min_price:
                        min_price = current_price
                    if max_price is None or current_price > max_price:
                        max_price = current_price
                    sum_price += current_price

                # Imprime estadísticas acumulativas
            print_cumulative_statistics(total_rows, total_valid_rows, min_price, max_price, sum_price)
    except Exception as e:
        raise ValueError(f"Error al procesar el archivo CSV '{file_path}': {str(e)}")


def print_cumulative_statistics(total_rows, total_valid_rows, min_price, max_price, sum_price):
    """
    Imprime estadísticas acumulativas de las filas que se van cargando en la base de datos. Esta función trabaja netamente con los datos de los archivos CSV.

    Args:
        total_rows (int): Número total de filas cargadas en la base de datos.
        total_valid_rows (int): Número total de filas NO VACÍAS.
        min_price (float or None): Valor mínimo del campo "price" acumulado o None si no se ha registrado ninguno.
        max_price (float or None): Valor máxmimo del campo "price" acumulado o None si no se ha registrado ninguno.
        sum_price (float): Suma acumulada del campo "price".

    Returns:
        None

    """
    try:
        if total_valid_rows > 0:
            mean_price = sum_price / total_valid_rows
            print(f'\nTotal de filas cargadas: {total_rows}')
            print(f'Promedio de "price": {mean_price:.2f}')
            print(f'Mínimo de "price": {min_price:.2f}')
            print(f'Máximo de "price": {max_price:.2f}')
    except Exception as e:
        print(f"Error al imprimir estadísticas: {str(e)}")     


def get_price_statistics(connection, table_name):
    """
    Consulta estadísticas del campo "price" en una tabla de la base de datos SQLite.

    Args:
        connection (sqlite3.Connection): Objeto con la conexión a la base de datos SQLite.
        table_name (str): Nombre de la tabla en la base de datos.

    Returns:
        dict: Un diccionario que contiene las estadísticas consultadas:
            - 'Total de filas': El número total de filas en la tabla.
            - 'Promedio de "price': El valor promedio de la columna 'price'.
            - 'Mínimo de "price': El valor mínimo de la columna 'price'.
            - 'Máximo de "price': El valor máximo de la columna 'price'.

    Raises:
        ValueError: Si se produce un error al consultar las estadísticas.

    """
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0] # Se implementa fethone para obtener la fila que se desea del resultado de la consulta

        cursor.execute(f"SELECT AVG(price), MIN(price), MAX(price) FROM {table_name}")
        result = cursor.fetchone()
        mean_price = result[0] if result[0] is not None else 0.0
        min_price = result[1] if result[1] is not None else 0.0
        max_price = result[2] if result[2] is not None else 0.0

        return {
            'Total de filas': total_rows,
            'Promedio de "price"': mean_price,
            'Mínimo de "price"': min_price,
            'Máximo de "price"': max_price
        }
    except Exception as e:
        raise ValueError(f"Error al consultar estadísticas en la tabla '{table_name}': {str(e)}")   
    
if __name__ == "__main__":
    # Estas variables se utilizan para llevar un seguimiento de estadísticas acumulativas
    total_rows = 0  # Número total de filas cargadas
    total_valid_rows = 0 # Total de filas no vacías
    min_price = None  # Precio mínimo acumulado
    max_price = None  # Precio máximo acumulado
    sum_price = 0  # Suma acumulada de precios válidos

    # Se leen los parámetros del archivo de configuración    
    db_path = configuration['db_path']
    table_name = configuration['table_name']
    csv_file = configuration['csv_files']
    micro_batch_size = configuration["micro_batch_size"]

    # Procesa cada archivo CSV fila por fila y almacena los datos en una sola tabla
    with sqlite3.connect(db_path) as conn:
        # Crea la tabla si no existe
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp DATETIME,
                price INTEGER,
                user_id INTEGER
            )
        ''')
        for csv_file in csv_file:
            process_csv_file(csv_file, conn, table_name, micro_batch_size)
        
        # Se solicita confirmación de usuario para continuar el flujo
        input('\n***presiona enter para realizar la consulta en la base de datos del: recuento total de filas, valor promedio, valor mínimo y valor máximo para el campo “price”*** ')

        # Llama a la función de consulta de estadísticas y muestra los resultados
        statistics = get_price_statistics(conn, table_name)
        print("\nESTADISTICAS REALIZANDO LA CONSULTA EN LA BASE DE DATOS:")
        for key, value in statistics.items():
            print(f"{key}: {value}")