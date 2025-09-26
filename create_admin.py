import os
import hashlib
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import sys

def create_admin_user():
    """
    Asegura que el usuario 'admin' exista en la base de datos.
    Si no existe, lo crea con la contraseña 'admin' hasheada con SHA-256.
    """
    print("--- INICIANDO SCRIPT CREATE_ADMIN ---")
    load_dotenv()
    print("Archivo .env cargado.")

    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    print(f"DB_HOST: {db_host}")
    print(f"DB_PORT: {db_port}")
    print(f"DB_USER: {db_user}")
    print(f"DB_PASSWORD: {'*' * len(db_password) if db_password else None}") # No mostrar la contraseña real
    print(f"DB_NAME: {db_name}")

    if not all([db_host, db_port, db_user, db_password, db_name]):
        print("Error: Faltan variables de entorno para la base de datos. Saliendo.", file=sys.stderr)
        sys.exit(1)
    
    print("Todas las variables de entorno están presentes.")

    admin_user = 'admin'
    admin_pass = 'admin'
    # Hash de la contraseña usando SHA-256
    password_hash = hashlib.sha256(admin_pass.encode('utf-8')).hexdigest()

    conn = None
    try:
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor()

        # Verificar si el usuario ya existe
        cursor.execute("SELECT id FROM users WHERE username = %s", (admin_user,))
        if cursor.fetchone():
            print(f"El usuario '{admin_user}' ya existe. No se necesita ninguna acción.")
        else:
            # Si no existe, crearlo
            print(f"Creando al usuario '{admin_user}'...")
            cursor.execute(
                "INSERT INTO users (username, password, is_admin) VALUES (%s, %s, %s)",
                (admin_user, password_hash, 1) # is_admin = 1 (True)
            )
            conn.commit()
            print(f"¡Usuario '{admin_user}' creado con éxito!")

    except Error as e:
        print(f"Error al conectar o crear el usuario admin: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_admin_user()
