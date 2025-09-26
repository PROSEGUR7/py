#DIMENSIONALES

import mysql.connector

# Configuración de conexión a MySQL
DB_CONFIG = {
    "host": "roundhouse.proxy.rlwy.net",
    "port": 38517,
    "user": "root",
    "password": "wZGotyxIDqpAtQjPxNuxxezSbbroztiw",
    "database": "railway"
}

# Función para ejecutar un procedimiento almacenado
def ejecutar_sp(sp_name):
    """
    Ejecuta un procedimiento almacenado en MySQL.
    :param sp_name: Nombre del Stored Procedure a ejecutar.
    """
    conn = None
    try:
        # Conectar a la base de datos
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print(f"Ejecutando {sp_name}...")
        cursor.callproc(sp_name)  # Ejecutar el procedimiento almacenado
        conn.commit()  # Confirmar cambios en la base de datos

        print(f"{sp_name} ejecutado con éxito.")

    except mysql.connector.Error as err:
        print(f"Error al ejecutar {sp_name}: {err}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Ejecutar el SP de dimensiones
ejecutar_sp('sp_upsert_dimensionales')

print("Proceso de carga de dimensiones finalizado.")


# Función para ejecutar un procedimiento almacenado
def ejecutar_sp(sp_name):
    """
    Ejecuta un procedimiento almacenado en MySQL.
    :param sp_name: Nombre del Stored Procedure a ejecutar.
    """
    conn = None
    try:
        # Conectar a la base de datos
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print(f"Ejecutando {sp_name}...")
        cursor.callproc(sp_name)  # Ejecutar el procedimiento almacenado
        conn.commit()  # Confirmar cambios en la base de datos

        print(f"{sp_name} ejecutado con éxito.")

    except mysql.connector.Error as err:
        print(f"Error al ejecutar {sp_name}: {err}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Ejecutar el SP de hechos
ejecutar_sp('sp_upsert_hechos')

print("Proceso de carga de hechos finalizado.")
