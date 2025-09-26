import mysql.connector
from mysql.connector import Error
import os
import hashlib
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de la base de datos
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'mantenimientos')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_PORT = os.getenv('DB_PORT', 3306)

def create_connection():
    """Crear conexión a MySQL"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("Conexión a MySQL exitosa")
        return connection
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

def create_database(cursor):
    """Crear base de datos si no existe"""
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"Base de datos {DB_NAME} creada o ya existente")
    except Error as e:
        print(f"Error al crear base de datos: {e}")

def create_tables(connection, cursor):
    """Crear tablas necesarias"""
    try:
        cursor.execute(f"USE {DB_NAME}")
        
        # Tabla de usuarios
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password_hash VARCHAR(64) NOT NULL,
            is_admin TINYINT(1) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Tabla de datos históricos
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS datos_historicos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            equipo VARCHAR(100) NOT NULL,
            fecha DATE NOT NULL,
            valor FLOAT NOT NULL,
            tipo VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Tabla de predicciones
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS predicciones (
            id INT AUTO_INCREMENT PRIMARY KEY,
            equipo VARCHAR(100) NOT NULL,
            fecha_prediccion DATE NOT NULL,
            valor_predicho FLOAT NOT NULL,
            confianza FLOAT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        print("Tablas creadas correctamente")
        
        # Crear usuario administrador por defecto si no existe
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        
        if count == 0:
            admin_password = "admin"  # Contraseña por defecto
            password_hash = hashlib.sha256(admin_password.encode()).hexdigest()
            
            cursor.execute("""
            INSERT INTO users (username, password_hash, is_admin)
            VALUES (%s, %s, %s)
            """, ("admin", password_hash, 1))
            
            connection.commit()
            print("Usuario administrador creado: usuario=admin, contraseña=admin")
        
        connection.close()
        
    except Error as e:
        print(f"Error al crear tablas: {e}")

def main():
    """Función principal para configurar la base de datos"""
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        create_database(cursor)
        create_tables(connection, cursor)
        
        cursor.close()
        connection.close()
        print("Configuración de la base de datos completada")

if __name__ == "__main__":
    main()
