import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import sys

def initialize_database():
    """
    Se conecta a la base de datos y ejecuta el script schema.sql
    para crear todas las tablas necesarias.
    """
    load_dotenv()
    
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    if not all([db_host, db_port, db_user, db_password, db_name]):
        print("Error: Faltan variables de entorno para la base de datos en el archivo .env", file=sys.stderr)
        sys.exit(1)

    conn = None
    cursor = None
    try:
        print(f"Conectando a la base de datos '{db_name}' en {db_host}...")
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor()
        print("Conexión exitosa.")

        schema_file = 'schema.sql'
        print(f"Leyendo el archivo de esquema: {schema_file}")
        with open(schema_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        print("Ejecutando el script SQL comando por comando...")
        # Parser manual para manejar delimitadores y ejecutar comando por comando
        sql_commands = []
        current_command = ""
        delimiter = ";"
        for line in sql_script.splitlines():
            line = line.strip()
            if not line or line.startswith('--'):
                continue

            if line.lower().startswith('delimiter'):
                delimiter = line.split()[1]
                continue

            current_command += line + " "
            if line.endswith(delimiter):
                # Quitamos el delimitador del final del comando
                sql_commands.append(current_command.strip()[:-len(delimiter)].strip())
                current_command = ""
        
        # Añadir el último comando si no termina con delimitador (poco probable pero seguro)
        if current_command.strip():
            sql_commands.append(current_command.strip())

        for command in sql_commands:
            if command:
                try:
                    print(f"  -> Ejecutando: {command[:80].replace('\n', ' ')}...")
                    cursor.execute(command)
                except Error as e:
                    print(f"    ERROR al ejecutar comando: {e}")
                    # Opcional: decidir si parar o continuar en caso de error
                    # raise e # Descomentar para parar en el primer error

        conn.commit()
        print("\n¡Base de datos inicializada con éxito!")
        print("Todas las tablas han sido creadas.")

    except Error as e:
        print(f"\nError al inicializar la base de datos: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Conexión cerrada.")

if __name__ == "__main__":
    initialize_database()
