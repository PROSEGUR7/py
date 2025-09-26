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

def migrate_users():
    """Migra los usuarios de la tabla antigua a la nueva estructura"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = connection.cursor()
        
        # Verificar si existe la tabla antigua de usuarios
        cursor.execute("SHOW TABLES LIKE 'usuarios'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("Migrando usuarios...")
            
            # Obtener usuarios de la tabla antigua
            cursor.execute("SELECT username, password, is_admin FROM usuarios")
            users = cursor.fetchall()
            
            for user in users:
                username, password, is_admin = user
                
                # Verificar si el usuario ya existe en la nueva tabla
                cursor.execute("SELECT COUNT(*) FROM users WHERE username = %s", (username,))
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # Insertar usuario en la nueva tabla
                    cursor.execute("""
                    INSERT INTO users (username, password_hash, is_admin)
                    VALUES (%s, %s, %s)
                    """, (username, password, is_admin))
            
            connection.commit()
            print(f"Se migraron {len(users)} usuarios")
        else:
            print("No se encontró la tabla antigua de usuarios")
        
        connection.close()
        
    except Error as e:
        print(f"Error al migrar usuarios: {e}")

def migrate_historical_data():
    """Migra los datos históricos de la tabla antigua a la nueva estructura"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = connection.cursor()
        
        # Verificar si existe la tabla antigua de datos históricos
        cursor.execute("SHOW TABLES LIKE 'datos_historicos_old'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("Migrando datos históricos...")
            
            # Obtener datos de la tabla antigua
            cursor.execute("SELECT equipo, fecha, valor, tipo FROM datos_historicos_old")
            data = cursor.fetchall()
            
            for item in data:
                equipo, fecha, valor, tipo = item
                
                # Insertar datos en la nueva tabla
                cursor.execute("""
                INSERT INTO datos_historicos (equipo, fecha, valor, tipo)
                VALUES (%s, %s, %s, %s)
                """, (equipo, fecha, valor, tipo))
            
            connection.commit()
            print(f"Se migraron {len(data)} registros históricos")
        else:
            print("No se encontró la tabla antigua de datos históricos")
        
        connection.close()
        
    except Error as e:
        print(f"Error al migrar datos históricos: {e}")

def migrate_predictions():
    """Migra las predicciones de la tabla antigua a la nueva estructura"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = connection.cursor()
        
        # Verificar si existe la tabla antigua de predicciones
        cursor.execute("SHOW TABLES LIKE 'predicciones_old'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("Migrando predicciones...")
            
            # Obtener datos de la tabla antigua
            cursor.execute("SELECT equipo, fecha_prediccion, valor_predicho, confianza FROM predicciones_old")
            data = cursor.fetchall()
            
            for item in data:
                equipo, fecha_prediccion, valor_predicho, confianza = item
                
                # Insertar datos en la nueva tabla
                cursor.execute("""
                INSERT INTO predicciones (equipo, fecha_prediccion, valor_predicho, confianza)
                VALUES (%s, %s, %s, %s)
                """, (equipo, fecha_prediccion, valor_predicho, confianza))
            
            connection.commit()
            print(f"Se migraron {len(data)} predicciones")
        else:
            print("No se encontró la tabla antigua de predicciones")
        
        connection.close()
        
    except Error as e:
        print(f"Error al migrar predicciones: {e}")

def main():
    """Función principal para migrar los datos"""
    print("Iniciando migración de datos...")
    migrate_users()
    migrate_historical_data()
    migrate_predictions()
    print("Migración completada")

if __name__ == "__main__":
    main()
