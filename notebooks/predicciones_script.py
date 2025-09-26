# generación de predicción por tipo de vehículo y tipo de mantenimiento
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text
from pyecharts import options as opts
from pyecharts.charts import Bar, Line
import os
from dotenv import load_dotenv

# --- Conexión a la Base de Datos ---
def get_db_engine(db_config):
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        return engine
    except Exception as e:
        print(f"Error al crear el motor de base de datos: {e}")
        return None

# --- Creación de Gráficos Pyecharts ---
def create_pyechart(df, title, r2_score_val):
    try:
        print(f"Creando gráfico para: {title}")
        print(f"Columnas en DataFrame: {list(df.columns)}")
        print(f"Muestra de datos:\n{df.head()}")
        
        if 'Fecha' not in df.columns:
            print("ERROR: Columna 'Fecha' no encontrada en el DataFrame")
            return None
            
        if 'Origen' not in df.columns:
            print("ERROR: Columna 'Origen' no encontrada en el DataFrame")
            return None
            
        if 'Costo' not in df.columns:
            print("ERROR: Columna 'Costo' no encontrada en el DataFrame")
            return None
        
        # Verificar datos para debugging
        print(f"Tipos de datos en el DataFrame:\n{df.dtypes}")
        print(f"Valores únicos de 'Origen': {df['Origen'].unique()}")
        print(f"Rango de fechas en datos: {df['Fecha'].min()} a {df['Fecha'].max()}")
        
        # Convertir fechas a datetime si no lo son ya
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Separar datos históricos y predicciones
        df_hist = df[df['Origen'] == 'Histórico'].sort_values('Fecha')
        df_pred = df[df['Origen'] == 'Predicción'].sort_values('Fecha')
        
        print(f"  - Histórico: {len(df_hist)} registros, rango: {df_hist['Fecha'].min() if not df_hist.empty else 'N/A'} a {df_hist['Fecha'].max() if not df_hist.empty else 'N/A'}")
        print(f"  - Predicción: {len(df_pred)} registros, rango: {df_pred['Fecha'].min() if not df_pred.empty else 'N/A'} a {df_pred['Fecha'].max() if not df_pred.empty else 'N/A'}")

        # Si no hay datos ni históricos ni de predicción, devuelve un gráfico vacío con un mensaje.
        if df_hist.empty and df_pred.empty:
            print("  - No hay datos para graficar")
            return (
                Line(init_opts=opts.InitOpts(height="500px", theme="white"))
                .set_global_opts(
                    title_opts=opts.TitleOpts(title=title, subtitle="No hay datos disponibles para graficar"),
                    xaxis_opts=opts.AxisOpts(type_="category"),
                )
            )

        # Combinar todos los datos en un único DataFrame para el eje X
        all_dates = pd.concat([df_hist['Fecha'], df_pred['Fecha']]).drop_duplicates().sort_values()
        all_dates_formatted = all_dates.dt.strftime('%Y-%m').tolist()
        print(f"  - Todas las fechas para el eje X: {all_dates_formatted}")
        
        # Preparar datos para el gráfico con manejo de excepciones
        df_hist_values = []
        df_pred_values = []
        
        for date in all_dates:
            try:
                # Para histórico
                hist_value = df_hist[df_hist['Fecha'] == date]['Costo'].sum() if date in df_hist['Fecha'].values else None
                df_hist_values.append(round(hist_value, 2) if hist_value is not None else None)
            except Exception as e:
                print(f"Error procesando dato histórico para fecha {date}: {e}")
                df_hist_values.append(None)
            
            try:
                # Para predicción
                pred_value = df_pred[df_pred['Fecha'] == date]['Costo'].sum() if date in df_pred['Fecha'].values else None
                df_pred_values.append(round(pred_value, 2) if pred_value is not None else None)
            except Exception as e:
                print(f"Error procesando dato de predicción para fecha {date}: {e}")
                df_pred_values.append(None)
        
        subtitle = f"R² = {r2_score_val:.4f}"
        print(f"Datos procesados para gráfico:\n - Históricos: {df_hist_values}\n - Predicción: {df_pred_values}")

        # Crear el objeto Line chart con manejo de errores
        try:
            line_chart = (
                Line(init_opts=opts.InitOpts(height="500px", theme="white"))
                .add_xaxis(xaxis_data=all_dates_formatted)
                .add_yaxis(
                    series_name="Histórico",
                    y_axis=df_hist_values,
                    symbol="circle",
                    is_symbol_show=True,
                    label_opts=opts.LabelOpts(is_show=False),
                    is_connect_nones=False  # No conectar puntos nulos
                )
                .add_yaxis(
                    series_name="Predicción",
                    y_axis=df_pred_values,
                    symbol="circle",
                    is_symbol_show=True,
                    label_opts=opts.LabelOpts(is_show=False),
                    linestyle_opts=opts.LineStyleOpts(type_="dashed"),
                    is_connect_nones=False  # No conectar puntos nulos
                )
                .set_global_opts(
                    title_opts=opts.TitleOpts(title=title, subtitle=subtitle),
                    legend_opts=opts.LegendOpts(pos_top="8%", pos_left="center"),
                    xaxis_opts=opts.AxisOpts(name="Fecha", type_="category"),
                    yaxis_opts=opts.AxisOpts(name="Costo"),
                    tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
                    datazoom_opts=[opts.DataZoomOpts(type_="slider"), opts.DataZoomOpts(type_="inside")]
                )
            )
            return line_chart
        except Exception as e:
            print(f"Error al crear el objeto PyEcharts: {e}")
            import traceback
            traceback.print_exc()
            return None
    except Exception as e:
        print(f"Error general en create_pyechart: {e}")
        import traceback
        traceback.print_exc()
        return None

# --- Función Principal de Predicción y Actualización ---
def get_prediction_charts_and_update_db(db_config):
    engine = get_db_engine(db_config)
    if not engine:
        return {}

    try:
        df = pd.read_sql("SELECT * FROM Hechos_Mantenimiento", engine)
        df['FechaElaboracion'] = pd.to_datetime(df['FechaElaboracion'])
        df['Debito'] = pd.to_numeric(df['Debito'], errors='coerce')
        df.dropna(subset=['FechaElaboracion', 'Debito'], inplace=True)
        df['Año'] = df['FechaElaboracion'].dt.year
    except Exception as e:
        print(f"Error al cargar datos: {e}")
        return {}

    # Inicializar diccionario de gráficos
    charts = {'by_vehicle': {}, 'by_type': {}}
    print(f"\n=== Iniciando generación de gráficos y predicciones ===\nDiccionario charts inicializado: {charts.keys()}")
    all_preds_vehiculo = []

    # --- 1. Predicciones y Modelos por Vehículo y Tipo ---
    # Usamos exactamente la misma implementación del notebook
    for vehiculo in df['NombreVehiculo'].unique():
        df_vehiculo = df[df['NombreVehiculo'] == vehiculo]
        for tipo in df_vehiculo['TipoMantenimiento'].unique():
            df_tipo = df_vehiculo[df_vehiculo['TipoMantenimiento'] == tipo].copy()
            if len(df_tipo) < 4:  # No entrenar si hay muy pocos datos
                continue

            # Agrupar por mes exactamente como en el notebook
            df_tipo['AñoMes'] = df_tipo['FechaElaboracion'].dt.to_period('M')
            df_hist_grouped = df_tipo.groupby('AñoMes').agg({
                'Debito': 'sum',
                'Categoria': lambda x: x.mode()[0] if not x.empty else None,
                'TipoMatricula': lambda x: x.mode()[0] if not x.empty else None,
                'IdentificacionTercero': lambda x: x.mode()[0] if not x.empty else None
            }).reset_index()

            # Convertir periodo a timestamp como en el notebook
            df_hist_grouped['FechaElaboracion'] = df_hist_grouped['AñoMes'].dt.to_timestamp()
            df_hist_grouped['Año'] = df_hist_grouped['FechaElaboracion'].dt.year
            df_hist_grouped['Mes'] = df_hist_grouped['FechaElaboracion'].dt.month
            df_hist_grouped['TipoMantenimiento'] = tipo
            df_hist_grouped['NombreVehiculo'] = vehiculo

            if len(df_hist_grouped) < 2:  # Necesitamos al menos 2 puntos
                continue

            # Usar las mismas características que el notebook
            X = df_hist_grouped[['Año', 'Mes', 'TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero']]
            y = df_hist_grouped['Debito']

            # Mismo preprocesador que el notebook
            preprocessor = ColumnTransformer([
                ('num', StandardScaler(), ['Año', 'Mes']),
                ('cat', OneHotEncoder(handle_unknown='ignore'), ['TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero'])
            ])

            # Mismo pipeline que el notebook
            pipeline = Pipeline([
                ('preprocessing', preprocessor),
                ('regresion', RandomForestRegressor(n_estimators=100, random_state=42))
            ])

            # Entrenamiento y R² igual que el notebook
            pipeline.fit(X, y)
            y_pred = pipeline.predict(X)
            r2 = r2_score(y, y_pred)

            # Generar predicciones para los próximos 12 meses
            ultima_fecha = df_hist_grouped['FechaElaboracion'].max()
            print(f"Para {vehiculo} - {tipo}, última fecha histórica: {ultima_fecha}")
            # Asegurarse de que usamos la última fecha REAL, no la fecha actual del sistema
            # Generar fechas exactamente como en el notebook original
            fechas_futuras = pd.date_range(start=ultima_fecha + pd.DateOffset(months=1), periods=12, freq='MS')

            # Valores más frecuentes para predicciones futuras
            categoria = df_hist_grouped['Categoria'].mode()[0] if not df_hist_grouped['Categoria'].empty else None
            tipomatricula = df_hist_grouped['TipoMatricula'].mode()[0] if not df_hist_grouped['TipoMatricula'].empty else None
            tercero = df_hist_grouped['IdentificacionTercero'].mode()[0] if not df_hist_grouped['IdentificacionTercero'].empty else None

            # DataFrame futuro
            df_futuro = pd.DataFrame({
                'Año': fechas_futuras.year,
                'Mes': fechas_futuras.month,
                'TipoMantenimiento': tipo,
                'Categoria': categoria,
                'TipoMatricula': tipomatricula,
                'NombreVehiculo': vehiculo,
                'IdentificacionTercero': tercero,
                'FechaElaboracion': fechas_futuras
            })

            # Predecir con las mismas características
            X_futuro = df_futuro[['Año', 'Mes', 'TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero']]
            future_preds = pipeline.predict(X_futuro)

            # DataFrame de resultados para históricos
            df_hist_result = pd.DataFrame({
                'NombreVehiculo': vehiculo,
                'TipoMantenimiento': tipo,
                'Fecha': df_hist_grouped['FechaElaboracion'],
                'Costo': df_hist_grouped['Debito'],
                'Prediccion': y_pred,
                'R2': r2,
                'Origen': 'Histórico'
            })
            
            # DataFrame de resultados para predicciones
            df_pred_result = pd.DataFrame({
                'NombreVehiculo': vehiculo,
                'TipoMantenimiento': tipo,
                'Fecha': fechas_futuras,
                'Costo': future_preds,
                'Prediccion': future_preds,
                'R2': r2,
                'Origen': 'Predicción'
            })
            
            # Unir resultados históricos y predicciones
            df_resultado = pd.concat([df_hist_result, df_pred_result])
            all_preds_vehiculo.append(df_pred_result)
            
            # Preparar datos para el gráfico
            df_plot = df_resultado[['Fecha', 'Costo', 'Origen']]
            
            # Crear y guardar gráfico
            try:
                # Verificar que el diccionario existe
                if 'by_vehicle' not in charts:
                    print(f"Recreando diccionario charts['by_vehicle'] porque no existe")
                    charts['by_vehicle'] = {}
                
                title = f'Vehículo: {vehiculo} - Tipo: {tipo}'
                print(f"\nCreando gráfico por vehículo para {title}")
                print(f"Datos para gráfico: {len(df_plot)} filas")
                print(f"Columnas: {df_plot.columns.tolist()}")
                print(f"Primeras filas:\n{df_plot.head()}")
                
                chart = create_pyechart(df_plot, title, r2)
                if chart:
                    # Guardar en el diccionario con un nombre único
                    key = f'Vehículo: {vehiculo} - Tipo: {tipo}'
                    charts['by_vehicle'][key] = chart
                    print(f"Gráfico para '{key}' agregado correctamente")
                    print(f"Total gráficos por vehículo: {len(charts['by_vehicle'])}")
                    print(f"Claves actuales: {list(charts['by_vehicle'].keys())}")
                else:
                    print(f"ERROR: No se pudo crear el gráfico para {title}")
            except Exception as e:
                print(f"Error al crear/guardar gráfico de vehículo {vehiculo} tipo {tipo}: {e}")
                import traceback
                traceback.print_exc()

    # --- 2. Almacenar todas las predicciones por vehículo en la BD ---
    if all_preds_vehiculo:
        df_pred_vehiculo_db = pd.concat(all_preds_vehiculo, ignore_index=True)
        # Asegurar que las fechas se guardan en el formato correcto
        df_pred_vehiculo_db['Fecha'] = df_pred_vehiculo_db['Fecha'].dt.strftime('%Y-%m-%d')
        df_pred_vehiculo_db['Fecha'] = pd.to_datetime(df_pred_vehiculo_db['Fecha'])
        print(f"Guardando {len(df_pred_vehiculo_db)} predicciones por vehículo. Rango de fechas: {df_pred_vehiculo_db['Fecha'].min()} a {df_pred_vehiculo_db['Fecha'].max()}")
        
        # Asegurarse de que todas las columnas necesarias están presentes
        columns_to_save = ['NombreVehiculo', 'TipoMantenimiento', 'Fecha', 'Costo', 'Origen']
        if 'R2' in df_pred_vehiculo_db.columns:
            columns_to_save.append('R2')
        
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                # Mostrar la estructura de la tabla para debugging
                table_info = pd.read_sql("DESCRIBE Predicciones_Vehiculo_Tipo", connection)
                print("Estructura de la tabla:")
                print(table_info)
                
                connection.execute(text("TRUNCATE TABLE Predicciones_Vehiculo_Tipo"))
                df_pred_vehiculo_db[columns_to_save].to_sql('Predicciones_Vehiculo_Tipo', engine, if_exists='append', index=False)
                trans.commit()
                print("Predicciones por vehículo guardadas correctamente")
            except Exception as e:
                trans.rollback()
                print(f"Error en la transacción de Vehículo/Tipo: {e}")

    # --- 3. Predicciones y Gráficos por Tipo de Mantenimiento (Agregado) ---
    print("\n" + "-"*50)
    print("Generando predicciones por tipo de mantenimiento...")
    tipos_mantenimiento = df['TipoMantenimiento'].unique()
    print(f"Tipos de mantenimiento disponibles: {tipos_mantenimiento}")
    
    # Asegurar que el diccionario de gráficos tiene la estructura correcta
    if 'by_type' not in charts:
        charts['by_type'] = {}
    
    print(f"Estructura actual de charts: {list(charts.keys())}")
    os.makedirs("graficas_rf_agrupado", exist_ok=True)  # Crear directorio para gráficos
    
    all_preds_tipo = []
    for tipo in df['TipoMantenimiento'].unique():
        df_tipo = df[df['TipoMantenimiento'] == tipo].copy()
        if df_tipo.empty:
            continue

        # Agrupar por mes exactamente como en el notebook
        df_tipo['AñoMes'] = df_tipo['FechaElaboracion'].dt.to_period('M')
        df_hist_grouped = df_tipo.groupby('AñoMes').agg({
            'Debito': 'sum',
            'Categoria': lambda x: x.mode()[0] if not x.empty else None,
            'TipoMatricula': lambda x: x.mode()[0] if not x.empty else None,
            'NombreVehiculo': lambda x: x.mode()[0] if not x.empty else None,
            'IdentificacionTercero': lambda x: x.mode()[0] if not x.empty else None
        }).reset_index()
        
        # Convertir periodo a timestamp
        df_hist_grouped['FechaElaboracion'] = df_hist_grouped['AñoMes'].dt.to_timestamp()
        df_hist_grouped['Año'] = df_hist_grouped['FechaElaboracion'].dt.year
        df_hist_grouped['Mes'] = df_hist_grouped['FechaElaboracion'].dt.month
        df_hist_grouped['TipoMantenimiento'] = tipo
        
        df_hist_grouped.dropna(subset=['Debito'], inplace=True)

        if len(df_hist_grouped) < 2:
            continue

        # Entrenamiento del modelo exactamente como en el notebook
        X = df_hist_grouped[['Año', 'Mes', 'TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero']]
        y = df_hist_grouped['Debito']

        # Mismo preprocesador que el notebook
        preprocessor = ColumnTransformer([
            ('num', StandardScaler(), ['Año', 'Mes']),
            ('cat', OneHotEncoder(handle_unknown='ignore'), ['TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero'])
        ])
        
        pipeline = Pipeline([
            ('preprocessing', preprocessor),
            ('regresion', RandomForestRegressor(n_estimators=100, random_state=42))
        ])
        
        pipeline.fit(X, y)
        y_pred = pipeline.predict(X)
        r2 = r2_score(y, y_pred)

        # Generar predicciones futuras con el mismo método
        # Usamos la última fecha real de los datos históricos, no la fecha actual
        ultima_fecha = df_hist_grouped['FechaElaboracion'].max()
        print(f"Tipo {tipo} - Última fecha histórica: {ultima_fecha}")
        # Asegurar que estamos usando la última fecha real de los datos históricos
        fechas_futuras = pd.date_range(start=ultima_fecha + pd.DateOffset(months=1), periods=12, freq='MS')
        print(f"Predicciones para {tipo} desde {fechas_futuras[0]} hasta {fechas_futuras[-1]}")
        
        # Obtener valores más frecuentes para características
        categoria = df_hist_grouped['Categoria'].mode()[0] if not df_hist_grouped['Categoria'].empty else None
        tipomatricula = df_hist_grouped['TipoMatricula'].mode()[0] if not df_hist_grouped['TipoMatricula'].empty else None
        vehiculo = df_hist_grouped['NombreVehiculo'].mode()[0] if not df_hist_grouped['NombreVehiculo'].empty else None
        tercero = df_hist_grouped['IdentificacionTercero'].mode()[0] if not df_hist_grouped['IdentificacionTercero'].empty else None

        # DataFrame futuro con predicciones
        df_futuro = pd.DataFrame({
            'Año': fechas_futuras.year,
            'Mes': fechas_futuras.month,
            'TipoMantenimiento': tipo,
            'Categoria': categoria,
            'TipoMatricula': tipomatricula,
            'NombreVehiculo': vehiculo,
            'IdentificacionTercero': tercero,
            'FechaElaboracion': fechas_futuras
        })
        
        # Predecir costos futuros
        X_futuro = df_futuro[['Año', 'Mes', 'TipoMantenimiento', 'Categoria', 'TipoMatricula', 'NombreVehiculo', 'IdentificacionTercero']]
        y_futuro = pipeline.predict(X_futuro)
        df_futuro['Debito'] = y_futuro
        
        # Crear DataFrames para histórico y predicción
        df_hist_result = pd.DataFrame({
            'TipoMantenimiento': tipo,
            'Fecha': df_hist_grouped['FechaElaboracion'], 
            'Costo': df_hist_grouped['Debito'],
            'Prediccion': y_pred,
            'R2': r2,
            'Origen': 'Histórico'
        })
        
        df_pred_result = pd.DataFrame({
            'TipoMantenimiento': tipo,
            'Fecha': fechas_futuras,
            'Costo': y_futuro,
            'Prediccion': y_futuro,
            'R2': r2,
            'Origen': 'Predicción'
        })
        
        # Unir resultados
        df_resultado = pd.concat([df_hist_result, df_pred_result])
        
        # Preparar datos para el gráfico
        df_chart = df_resultado[['Fecha', 'Costo', 'Origen']]
        
        # Crear gráfico PyEcharts
        try:
            title = f'Tipo de Mantenimiento: {tipo}'
            print(f"\nCreando gráfico para {title}")
            print(f"Datos para el gráfico: {len(df_chart)} filas")
            chart = create_pyechart(df_chart, title, r2)
            
            if chart:
                # Verificar que el diccionario existe
                if 'by_type' not in charts:
                    print(f"Recreando diccionario charts['by_type'] porque no existe")
                    charts['by_type'] = {}
                
                # Guardar el gráfico en el diccionario
                charts['by_type'][title] = chart
                print(f"Gráfico para {title} agregado correctamente")
                
                # Imprimir el contenido actual del diccionario para debug
                tipos = list(charts['by_type'].keys())
                print(f"Gráficos por tipo guardados hasta ahora: {tipos}")
            else:
                print(f"ERROR: No se pudo crear el gráfico para {title}")
                
        except Exception as e:
            print(f"ERROR CRÍTICO al crear/guardar gráfico para {tipo}: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Verificación de contenido
        try:
            print(f"Estado actual de charts['by_type']: {len(charts.get('by_type', {}))} gráficos")
        except Exception as e:
            print(f"Error al verificar charts['by_type']: {e}")
            charts['by_type'] = {}  # Recrear si hay algún problema

        # Guardar predicciones para la BD
        all_preds_tipo.append(df_pred_result[['TipoMantenimiento', 'Fecha', 'Costo', 'Origen']])

    # --- 4. Almacenar predicciones por tipo en la BD ---
    if all_preds_tipo:
        df_final_preds = pd.concat(all_preds_tipo)
        # Asegurar que las fechas se guardan en el formato correcto
        df_final_preds['Fecha'] = df_final_preds['Fecha'].dt.strftime('%Y-%m-%d')
        df_final_preds['Fecha'] = pd.to_datetime(df_final_preds['Fecha'])
        print(f"Guardando {len(df_final_preds)} predicciones por tipo. Rango de fechas: {df_final_preds['Fecha'].min()} a {df_final_preds['Fecha'].max()}")
        
        # Asegurarse de que las columnas coinciden con la estructura de la tabla
        columns_to_save = ['TipoMantenimiento', 'Fecha', 'Costo', 'Origen']
        if 'R2' in df_final_preds.columns:
            columns_to_save.append('R2')
            
        with engine.connect() as connection:
            trans = connection.begin()
            try:
                # Mostrar la estructura de la tabla para debugging
                table_info = pd.read_sql("DESCRIBE Predicciones_Tipo_Mantenimiento", connection)
                print("Estructura de la tabla:")
                print(table_info)
                
                connection.execute(text("TRUNCATE TABLE Predicciones_Tipo_Mantenimiento"))
                df_final_preds[columns_to_save].to_sql('Predicciones_Tipo_Mantenimiento', engine, if_exists='append', index=False)
                trans.commit()
                print("Predicciones por tipo guardadas correctamente")
            except Exception as e:
                trans.rollback()
                print(f"Error en la transacción de Tipo Mantenimiento: {e}")
    
    # Verificar el contenido de los gráficos antes de devolverlos
    print("\n" + "="*50)
    print(f"RESUMEN DE GRÁFICOS GENERADOS:")
    print(f"Gráficos por vehículo: {len(charts.get('by_vehicle', {}))}")
    print(f"Gráficos por tipo: {len(charts.get('by_type', {}))}")
    
    if len(charts.get('by_type', {})) == 0:
        print("ADVERTENCIA: No se generaron gráficos por tipo. Revisar datos y procesos.")
    
    return charts

if __name__ == '__main__':
    load_dotenv()
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    }
    if not all(db_config.values()):
        print("Error: Faltan variables de entorno para la base de datos. Asegúrate de que .env está configurado.")
    else:
        charts = get_prediction_charts_and_update_db(db_config)
        print(f"Generados {len(charts.get('by_vehicle', {}))} gráficos por vehículo.")
        print(f"Generados {len(charts.get('by_type', {}))} gráficos por tipo.")
