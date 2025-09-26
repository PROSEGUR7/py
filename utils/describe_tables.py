import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Añadir el directorio raíz al path para poder importar desde otros módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def describe_tables():
    """Se conecta a la base de datos y describe las tablas de predicciones."""
    load_dotenv()

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    if not all([db_user, db_password, db_host, db_port, db_name]):
        print("Error: Faltan variables de entorno para la conexión a la base de datos.")
        return

    try:
        db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)

        tables_to_describe = [
            'Predicciones_Vehiculo_Tipo',
            'Predicciones_Tipo_Mantenimiento'
        ]

        with engine.connect() as connection:
            for table_name in tables_to_describe:
                print(f"\n--- Describiendo tabla: {table_name} ---")
                try:
                    result = connection.execute(text(f"DESCRIBE {table_name}"))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(df.to_string())
                except Exception as e:
                    print(f"No se pudo describir la tabla '{table_name}': {e}")

    except Exception as e:
        print(f"Error al conectar o describir las tablas: {e}")

if __name__ == "__main__":
    describe_tables()
