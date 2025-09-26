import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os
import argparse

def generate_graphs(db_host, db_port, db_user, db_password, db_name, output_dir):
    print(f"Conectando a la base de datos en {db_host}...")
    try:
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
    except mysql.connector.Error as err:
        print(f"Error de MySQL: {err}")
        return

    print("Conexión exitosa. Cargando datos...")
    query = """
    SELECT p.TipoMantenimiento, p.Fecha, p.Costo, p.Origen
    FROM Predicciones_Tipo_Mantenimiento p
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print("Datos cargados. La conexión a la base de datos se ha cerrado.")

    # Preprocesamiento
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df = df.sort_values(by='Fecha')

    # Crear carpeta de salida
    os.makedirs(output_dir, exist_ok=True)
    print(f"Las gráficas se guardarán en: {output_dir}")

    # Limpiar gráficas anteriores
    for f in os.listdir(output_dir):
        if f.endswith('.png'):
            os.remove(os.path.join(output_dir, f))
    print("Gráficas anteriores eliminadas.")

    # Filtrar últimos 12 meses por origen
    df_historico = df[df['Origen'] == 'Histórico'].groupby('TipoMantenimiento').apply(lambda x: x.sort_values('Fecha').tail(12)).reset_index(drop=True)
    df_prediccion = df[df['Origen'] == 'Predicción'].groupby('TipoMantenimiento').apply(lambda x: x.sort_values('Fecha').tail(12)).reset_index(drop=True)
    df_filtrado = pd.concat([df_historico, df_prediccion], ignore_index=True)

    if df_filtrado.empty:
        print("No hay suficientes datos para generar las gráficas.")
        return

    # ... (El resto de la lógica de generación de gráficos) ...
    # (Se omite por brevedad, es idéntica a la proporcionada por el usuario pero usando output_dir)

    # Gráfica comparativa en barras por mes
    df_pivot = df_filtrado.pivot_table(index=['TipoMantenimiento', 'Fecha'], columns='Origen', values='Costo').reset_index()
    for tipo in df_pivot['TipoMantenimiento'].unique():
        df_tipo = df_pivot[df_pivot['TipoMantenimiento'] == tipo]
        x = df_tipo['Fecha'].dt.strftime('%b %Y')
        x_idx = range(len(x))
        width = 0.35

        fig, ax = plt.subplots(figsize=(14, 6))
        bars1 = ax.bar([i - width/2 for i in x_idx], df_tipo.get('Histórico', pd.Series(index=x_idx, dtype='float64')).fillna(0), width=width, label='Histórico')
        bars2 = ax.bar([i + width/2 for i in x_idx], df_tipo.get('Predicción', pd.Series(index=x_idx, dtype='float64')).fillna(0), width=width, label='Predicción')

        ax.set_title(f'Comparativo Histórico vs Predicción - Tipo: {tipo}')
        ax.set_xlabel('Fecha')
        ax.set_ylabel('Costo ($)')
        ax.set_xticks(x_idx)
        ax.set_xticklabels(x, rotation=45)
        ax.legend()
        ax.grid(True, axis='y')
        ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('${x:,.0f}'))
        plt.tight_layout()
        save_path = os.path.join(output_dir, f"comparativo_{tipo.replace(' ', '_').replace('/', '_')}.png")
        plt.savefig(save_path, bbox_inches='tight')
        plt.close(fig)
        print(f"Gráfica guardada: {save_path}")

    print("\nProceso de generación de gráficas completado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genera gráficas a partir de los datos de predicciones.')
    parser.add_argument('--db-host', required=True, help='Host de la base de datos.')
    parser.add_argument('--db-port', required=True, type=int, help='Puerto de la base de datos.')
    parser.add_argument('--db-user', required=True, help='Usuario de la base de datos.')
    parser.add_argument('--db-password', required=True, help='Contraseña de la base de datos.')
    parser.add_argument('--db-name', required=True, help='Nombre de la base de datos.')
    parser.add_argument('--output-dir', required=True, help='Directorio para guardar las gráficas.')
    
    args = parser.parse_args()
    
    generate_graphs(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name, args.output_dir)
