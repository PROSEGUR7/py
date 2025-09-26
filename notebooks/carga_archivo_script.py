import pandas as pd
import mysql.connector
import os
import concurrent.futures
from pathlib import Path

# Configuración de la conexión a la base de datos MySQL
DB_CONFIG = {
    "host": "roundhouse.proxy.rlwy.net",
    "port": 38517,
    "user": "root",
    "password": "wZGotyxIDqpAtQjPxNuxxezSbbroztiw",
    "database": "railway"
}

def resolve_input_dir():
    """Resolve the location of the input_files folder for different deployments."""
    env_dir = os.getenv("INPUT_FILES_DIR")
    if env_dir:
        candidate = Path(env_dir).expanduser()
        if candidate.exists():
            return candidate.resolve()

    script_path = Path(__file__).resolve()
    parents = list(script_path.parents)
    candidate_dirs = []

    if len(parents) > 1:
        candidate_dirs.append(parents[1] / "input_files")
        candidate_dirs.append(parents[1] / "py" / "input_files")
    else:
        candidate_dirs.append(parents[0] / "input_files")

    for parent in parents[2:]:
        candidate_dirs.append(parent / "input_files")

    seen = set()
    final_candidates = []
    for candidate in candidate_dirs:
        resolved = candidate.resolve()
        key = str(resolved)
        if key not in seen:
            seen.add(key)
            final_candidates.append(resolved)

    for candidate in final_candidates:
        if candidate.exists():
            return candidate

    return final_candidates[0]


def process_file(file_path):
    """Procesa un único archivo Excel y lo carga en la base de datos."""
    archivo = os.path.basename(file_path)
    resumen = {"archivo": archivo, "cumple": False, "registros": 0, "mensaje": ""}
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print(f"Procesando archivo en hilo: {archivo}")
        df = pd.read_excel(file_path, header=7)

        if df.shape[1] < 15:
            resumen["mensaje"] = "No cumple con la cantidad requerida de columnas."
            return resumen

        df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 14]]
        df.columns = [
            "Codigo_contable", "NombreVehiculo", "Comprobante", "Secuencia", "FechaElaboracion",
            "IdentificacionTercero", "NombreTercero", "Descripcion", "Debito", "TipoMantenimiento",
            "Categoria", "Matricula", "TipoMatricula"
        ]

        resumen["cumple"] = True
        registros_insertados = 0

        for _, row in df.iterrows():
            try:
                fecha_elaboracion = pd.to_datetime(row['FechaElaboracion'], dayfirst=True).strftime('%Y-%m-%d') if pd.notnull(row['FechaElaboracion']) else None
                
                sql = """
                INSERT INTO STG_Mantenimientos 
                (Codigo_contable, NombreVehiculo, Comprobante, Secuencia, FechaElaboracion, 
                 IdentificacionTercero, NombreTercero, Descripcion, Debito, TipoMantenimiento, Categoria, Matricula, TipoMatricula) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                val = (
                    row['Codigo_contable'], row['NombreVehiculo'], row['Comprobante'], row['Secuencia'],
                    fecha_elaboracion, row['IdentificacionTercero'], row['NombreTercero'], row['Descripcion'],
                    row['Debito'], row['TipoMantenimiento'], row['Categoria'], row['Matricula'], row['TipoMatricula']
                )
                cursor.execute(sql, val)
                conn.commit()
                registros_insertados += 1
            except mysql.connector.IntegrityError as e:
                if "Duplicate entry" not in str(e):
                    conn.rollback()
            except Exception:
                conn.rollback()

        resumen["registros"] = registros_insertados
        return resumen

    except Exception as e:
        resumen["mensaje"] = f"Error crítico: {e}"
        return resumen
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def main():
    # Limpiar la tabla STG_Mantenimientos una sola vez al inicio
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM STG_Mantenimientos")
                conn.commit()
                print("Datos anteriores eliminados de la tabla STG_Mantenimientos.")
    except mysql.connector.Error as err:
        print(f"Error de MySQL al limpiar la tabla: {err}")
        return

    input_dir = resolve_input_dir()

    if not input_dir.exists():
        print(f"No se encontró el directorio de entrada: {input_dir}")
        return

    file_paths = [str(path) for path in sorted(input_dir.rglob('*.xlsx'))]

    if not file_paths:
        print(f"No se encontraron archivos Excel en la ruta: {input_dir}")
        return

    # Procesar archivos en paralelo
    print(f"Iniciando carga paralela de {len(file_paths)} archivos...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 1) as executor:
        future_to_file = {executor.submit(process_file, path): path for path in file_paths}
        for future in concurrent.futures.as_completed(future_to_file):
            results.append(future.result())

    # Mostrar resumen final
    print("\n--- Resumen de Carga ---")
    total_registros = 0
    for res in sorted(results, key=lambda x: x['archivo']):
        estado = "[OK] Cumple" if res["cumple"] else "[ERROR] No cumple"
        print(f"{res['archivo']}: {estado}. Registros cargados: {res['registros']}. {res['mensaje']}")
        total_registros += res['registros']
    
    print(f"\nTotal de registros insertados: {total_registros}")
    print("Proceso de carga finalizado.")

if __name__ == "__main__":
    main()

