import streamlit as st
import os
import pandas as pd
import papermill as pm
import base64
import json
import re
from datetime import datetime
from sqlalchemy import create_engine, text
import hashlib
from streamlit_option_menu import option_menu
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import streamlit.components.v1 as components
from streamlit_echarts import st_pyecharts, st_echarts

# Importar los nuevos módulos de gráficos
from notebooks.generar_graficas import generate_analysis_charts
from notebooks.predicciones_script import get_prediction_charts_and_update_db

# Cargar variables de entorno
load_dotenv()

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Mantenimientos",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de la base de datos
def get_db_engine():
    try:
        db_url = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        st.error(f"Error al crear la conexión con la base de datos: {e}")
        return None

# Funciones de autenticación
def verify_password(username, password):
    engine = get_db_engine()
    if engine:
        with engine.connect() as connection:
            query = text("SELECT password FROM users WHERE username = :user")
            result = connection.execute(query, {"user": username}).fetchone()
            if result:
                stored_hash = result[0]
                password_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
                return password_hash == stored_hash
    return False

def is_admin(username):
    engine = get_db_engine()
    if engine:
        with engine.connect() as connection:
            query = text("SELECT is_admin FROM users WHERE username = :user")
            result = connection.execute(query, {"user": username}).fetchone()
            if result:
                return result[0] == 1
    return False

# Inicialización de la sesión
if 'username' not in st.session_state:
    st.session_state.username = None
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'analysis_charts' not in st.session_state:
    st.session_state.analysis_charts = None
if 'prediction_charts' not in st.session_state:
    st.session_state.prediction_charts = None


def _parse_height_value(height):
    if isinstance(height, (int, float)):
        return int(height)
    if isinstance(height, str):
        digits = ''.join(ch for ch in height if ch.isdigit())
        if digits:
            return int(digits)
    return 500


def _build_component_key(prefix, title, index):
    safe = re.sub(r'[^0-9A-Za-z]+', '_', (title or ''))
    safe = safe.strip('_')
    if not safe:
        safe = 'chart'
    return f"{prefix}_{safe}_{index}"



def render_echarts_chart(chart, key, height=500, prefer='auto'):
    if chart is None:
        st.warning('Gráfico no disponible.')
        return

    height_px = _parse_height_value(height)
    height_css = f"{height_px}px"

    def _try_echarts():
        options = json.loads(chart.dump_options())
        st_echarts(options=options, height=height_css, width='100%', key=f"{key}_echarts")

    def _try_pyecharts():
        st_pyecharts(chart, height=height_css, width='100%', key=f"{key}_pye")

    renderers = []
    if prefer == 'pyecharts':
        renderers = [_try_pyecharts, _try_echarts]
    elif prefer == 'echarts':
        renderers = [_try_echarts, _try_pyecharts]
    else:
        renderers = [_try_echarts, _try_pyecharts]

    for renderer in renderers:
        try:
            renderer()
            return
        except Exception:
            continue

    try:
        html = chart.render_embed()
        components.html(html, height=height_px, scrolling=False)
    except Exception as err:
        st.error(f"No se pudo renderizar el gráfico ({err}).")



# Función para ejecutar notebooks
def ejecutar_notebook(notebook_name, parameters=None):
    notebook_path = f"notebooks/{notebook_name}.ipynb"
    output_path = f"notebooks/output/{notebook_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.ipynb"
    
    # Crear directorio de salida si no existe
    os.makedirs("notebooks/output", exist_ok=True)
    
    try:
        pm.execute_notebook(
            input_path=notebook_path,
            output_path=output_path,
            parameters=parameters,
            kernel_name='mantenimientos-kernel'
        )
        st.success(f"Notebook {notebook_name} ejecutado correctamente")
        return True
    except Exception as e:
        st.error(f"Error al ejecutar el notebook {notebook_name}: {e}")
        return False

# Función para cargar archivos
def upload_file(uploaded_file, file_type):
    if uploaded_file is not None:
        # Crear directorio si no existe
        os.makedirs(f"input_files/{file_type}", exist_ok=True)
        
        # Guardar archivo
        file_path = f"input_files/{file_type}/{uploaded_file.name}"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success(f"Archivo {uploaded_file.name} subido correctamente")
        return True
    return False

# Función para mostrar datos históricos
def show_historical_data():
    engine = get_db_engine()
    if engine:
        try:
            query = "SELECT * FROM Hechos_Mantenimiento ORDER BY MantenimientoID DESC LIMIT 1000"
            df = pd.read_sql(query, engine)
            st.dataframe(df)
            
            if not df.empty:
                csv = df.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()
                href = f'<a href="data:file/csv;base64,{b64}" download="datos_historicos.csv">Descargar CSV</a>'
                st.markdown(href, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error al obtener datos históricos: {e}")
    else:
        st.error("No se pudo conectar a la base de datos")

# Función para mostrar predicciones
def show_predictions():
    st.subheader("Gráficos Interactivos de Predicciones")

    if 'prediction_charts' not in st.session_state:
        st.error("La variable prediction_charts no está en session_state. Ejecute el proceso de predicción.")
        return
    
    charts_data = st.session_state.prediction_charts
    if not charts_data:
        st.info("No hay gráficos de predicciones disponibles. Por favor, ejecute el proceso de predicción en el 'Panel de Procesos'.")
        return
    charts_by_vehicle = charts_data.get('by_vehicle', {})
    charts_by_type = charts_data.get('by_type', {})
    
    st.write(f"Se encontraron {len(charts_by_vehicle)} gráficos por vehículo y {len(charts_by_type)} gráficos por tipo.")

    # Panel de debug (collapsible)
    with st.expander("Ver detalles de depuración"):
        st.json({
            "Total gráficos por vehículo": len(charts_by_vehicle),
            "Total gráficos por tipo": len(charts_by_type),
            "Nombres de gráficos por vehículo": list(charts_by_vehicle.keys()),
            "Nombres de gráficos por tipo": list(charts_by_type.keys())
        })

    if not charts_by_vehicle and not charts_by_type:
        st.info("No se generaron gráficos. Verifique los datos de origen.")
        return

    tab1, tab2, tab3 = st.tabs(["Predicciones por Vehículo", "Predicciones por Tipo", "Datos de Predicción"])

    with tab1:
        if not charts_by_vehicle:
            st.info("No hay predicciones por vehículo.")
        else:
            for i, (title, chart) in enumerate(charts_by_vehicle.items()):
                st.subheader(f"{title}")
                with st.spinner(f"Cargando gráfico {i+1}/{len(charts_by_vehicle)}"):
                    chart_key = _build_component_key("vehicle", title, i)
                    render_echarts_chart(chart, key=chart_key)

    with tab2:
        if not charts_by_type:
            st.info("No hay predicciones por tipo de mantenimiento.")
        else:
            for i, (title, chart) in enumerate(charts_by_type.items()):
                st.subheader(f"{title}")
                with st.spinner(f"Cargando gráfico {i+1}/{len(charts_by_type)}"):
                    chart_key = _build_component_key("type", title, i)
                    render_echarts_chart(chart, key=chart_key)

    with tab3:
        st.subheader("Datos de Predicciones Almacenados")
        engine = get_db_engine()
        if not engine:
            return

        try:
            st.write("##### Predicciones por Vehiculo y Tipo")
            df_pred_vehiculo = pd.read_sql("SELECT * FROM Predicciones_Vehiculo_Tipo ORDER BY NombreVehiculo, TipoMantenimiento, Fecha", engine)
            st.dataframe(df_pred_vehiculo)
            if not df_pred_vehiculo.empty:
                csv = df_pred_vehiculo.to_csv(index=False).encode('utf-8')
                st.download_button("Descargar CSV (Vehículo)", csv, 'predicciones_vehiculo.csv', 'text/csv', key='csv_veh')
        except Exception as e:
            st.warning(f"No se pudieron cargar las predicciones por vehículo: {e}")

        try:
            st.write("##### Predicciones por Tipo de Mantenimiento")
            df_pred_tipo = pd.read_sql("SELECT * FROM Predicciones_Tipo_Mantenimiento ORDER BY TipoMantenimiento, Fecha", engine)
            st.dataframe(df_pred_tipo)
            if not df_pred_tipo.empty:
                csv = df_pred_tipo.to_csv(index=False).encode('utf-8')
                st.download_button("Descargar CSV (Tipo)", csv, 'predicciones_tipo.csv', 'text/csv', key='csv_tipo')
        except Exception as e:
            st.warning(f"No se pudieron cargar las predicciones por tipo: {e}")

def show_analysis_page():
    st.header("📊 Análisis Comparativo: Histórico vs. Predicción")

    if 'analysis_charts' not in st.session_state or not st.session_state.analysis_charts:
        st.info("No hay gráficos de análisis disponibles. Por favor, ejecute el proceso 'Generar Gráficas de Análisis' en el 'Panel de Procesos'.")
        return

    charts = st.session_state.analysis_charts
    tipos_mantenimiento = list(charts.keys())

    if not tipos_mantenimiento:
        st.info("No se encontraron datos para el análisis.")
        return

    # Crear pestañas para cada tipo de mantenimiento
    tabs = st.tabs(tipos_mantenimiento)

    for i, tab in enumerate(tabs):
        with tab:
            tipo_seleccionado = tipos_mantenimiento[i]
            chart_group = charts[tipo_seleccionado]
            
            # Mostrar gráficos verticalmente dentro de cada pestaña
            st.write("##### Comparativo Mensual (Barras)")
            if 'bar' in chart_group and chart_group['bar']:
                bar_key = _build_component_key("analysis_bar", tipo_seleccionado, i)
                render_echarts_chart(chart_group['bar'], key=bar_key, prefer='pyecharts')
            else:
                st.warning("Gráfico de barras no disponible.")

            st.write("##### Análisis de Totales (Pastel)")
            if 'pie' in chart_group and chart_group['pie']:
                pie_key = _build_component_key("analysis_pie", tipo_seleccionado, i)
                render_echarts_chart(chart_group['pie'], key=pie_key, prefer='pyecharts')
            else:
                st.warning("Gráfico de pastel no disponible.")

# Interfaz de login
def login_page():
    st.title("Sistema de Mantenimientos")
    
    with st.form("login_form"):
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        submit = st.form_submit_button("Iniciar sesión")
        
        if submit:
            if verify_password(username, password):
                st.session_state.username = username
                st.session_state.is_admin = is_admin(username)
                st.success("Inicio de sesión exitoso")
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos")

# Pagina principal
def main_page():
    # --- CSS Injection for modern UI ---
    st.markdown("""
    <style>
    .card {
        border: 1px solid #e6e6e6; border-radius: 10px; padding: 20px; margin: 10px 0;
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.1); transition: 0.3s; background-color: #ffffff;
        text-align: center; height: 280px; display: flex; flex-direction: column; justify-content: space-between;
    }
    .card:hover { box-shadow: 0 8px 16px 0 rgba(0,0,0,0.2); }
    .card h3 { margin-top: 0; font-size: 1.8em; }
    .card p { font-size: 14px; color: #666; }
    .stButton>button {
        width: 100%; border-radius: 8px; color: white; border: none; font-weight: bold;
        background-image: linear-gradient(to right, #4facfe 0%, #00f2fe 100%);
    }
    .stButton>button:hover { color: white; border: none; }
    </style>
    """, unsafe_allow_html=True)

    # --- Sidebar --- 
    with st.sidebar:
        st.title(f"Bienvenido, {st.session_state.username}")
        menu = option_menu(
            "Menú Principal",
            ["Panel de Procesos", "Subir Archivos", "Datos Históricos", "Predicciones", "Análisis Comparativo"],
            icons=['gear', 'cloud-upload', 'clock-history', 'graph-up-arrow', 'bar-chart-line'],
            menu_icon="cast", default_index=0,
            styles={
                "container": {"padding": "5!important", "background-color": "#fafafa"},
                "icon": {"color": "orange", "font-size": "25px"}, 
                "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                "nav-link-selected": {"background-color": "#02ab21"},
            }
        )
    
    def logout():
        st.session_state.username = None
        st.session_state.is_admin = False
        st.rerun()
    st.sidebar.button("Cerrar sesión", on_click=logout)

    # --- Main Panel Logic ---
    if menu == "Panel de Procesos":
        st.title("⚙️ Panel de Procesos del Sistema")
        st.markdown("Ejecute los procesos clave del sistema de forma secuencial para asegurar la integridad de los datos y predicciones.")
        st.subheader("Proceso Completo (Recomendado)")
        if st.button("Ejecutar Proceso Completo 🚀"):
            with st.expander("Ver Salida del Proceso Completo", expanded=True):
                with st.spinner('Ejecutando todos los pasos... Esto puede tardar varios minutos.'):
                    try:
                        st.info("Paso 1: Cargando archivos de input...")
                        command_carga = [sys.executable, str(Path("notebooks/carga_archivo_script.py"))]
                        process_carga = subprocess.run(command_carga, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                        st.text_area("Resultado de la Carga", process_carga.stdout, height=150)
                        st.success("Carga de archivos completada.")

                        st.info("Paso 2: Actualizando Hechos y Dimensiones...")
                        command_update = [sys.executable, str(Path("notebooks/update_datos_script.py")), '--db-host', os.getenv('DB_HOST'), '--db-port', os.getenv('DB_PORT'), '--db-user', os.getenv('DB_USER'), '--db-password', os.getenv('DB_PASSWORD'), '--db-name', os.getenv('DB_NAME')]
                        process_update = subprocess.run(command_update, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                        st.text_area("Resultado de la Actualización", process_update.stdout, height=150)
                        st.success("Actualización de datos completada.")

                        st.info("Paso 3: Generando predicciones...")
                        command_pred = [sys.executable, str(Path("notebooks/predicciones_script.py")), '--db-host', os.getenv('DB_HOST'), '--db-port', os.getenv('DB_PORT'), '--db-user', os.getenv('DB_USER'), '--db-password', os.getenv('DB_PASSWORD'), '--db-name', os.getenv('DB_NAME'), '--output-dir-graphs', 'output/images', '--output-dir-excel', 'output/excel']
                        process_pred = subprocess.run(command_pred, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                        st.text_area("Resultado de la Predicción", process_pred.stdout, height=200)
                        st.success("Generación de predicciones completada.")
                        st.balloons()

                    except subprocess.CalledProcessError as e:
                        st.error(f"Falló la ejecución de un script:")
                        st.text_area("Detalles del Error", e.stderr, height=200)

        st.markdown("<hr>", unsafe_allow_html=True)

        st.subheader("Procesos Individuales")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("1. Cargar Inputs"):
                with st.expander("Ver Salida de la Carga", expanded=True):
                    with st.spinner('Cargando archivos...'):
                        try:
                            command_carga = [sys.executable, str(Path("notebooks/carga_archivo_script.py"))]
                            process_carga = subprocess.run(command_carga, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                            st.text_area("Resultado de la Carga", process_carga.stdout, height=150)
                            st.success("Carga de archivos completada.")
                        except subprocess.CalledProcessError as e:
                            st.error(f"Error durante la carga:")
                            st.text_area("Detalles del Error", e.stderr, height=200)

        with col2:
            if st.button("2. Actualizar Datos"):
                with st.expander("Ver Salida de la Actualización", expanded=True):
                    with st.spinner('Actualizando datos...'):
                        try:
                            command_update = [sys.executable, str(Path("notebooks/update_datos_script.py")), '--db-host', os.getenv('DB_HOST'), '--db-port', os.getenv('DB_PORT'), '--db-user', os.getenv('DB_USER'), '--db-password', os.getenv('DB_PASSWORD'), '--db-name', os.getenv('DB_NAME')]
                            process_update = subprocess.run(command_update, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
                            st.text_area("Resultado de la Actualización", process_update.stdout, height=150)
                            st.success("Actualización de datos completada.")
                        except subprocess.CalledProcessError as e:
                            st.error(f"Error durante la actualización:")
                            st.text_area("Detalles del Error", e.stderr, height=200)

        with col3:
            if st.button("3. Ejecutar Predicciones"):
                with st.spinner('Ejecutando predicciones y generando gráficos...'):
                    try:
                        db_config = {
                            'host': os.getenv('DB_HOST'),
                            'port': os.getenv('DB_PORT'),
                            'user': os.getenv('DB_USER'),
                            'password': os.getenv('DB_PASSWORD'),
                            'database': os.getenv('DB_NAME')
                        }
                        st.session_state.prediction_charts = get_prediction_charts_and_update_db(db_config)
                        st.success("Proceso de predicción completado y gráficos generados.")
                        st.info("Navegue a la página de 'Predicciones' para ver los resultados.")
                    except Exception as e:
                        st.error(f"Error durante el proceso de predicción: {e}")

        with col4:
            if st.button("4. Generar Gráficas de Análisis"):
                with st.spinner('Generando gráficas de análisis...'):
                    try:
                        db_config = {
                            'host': os.getenv('DB_HOST'),
                            'port': os.getenv('DB_PORT'),
                            'user': os.getenv('DB_USER'),
                            'password': os.getenv('DB_PASSWORD'),
                            'database': os.getenv('DB_NAME')
                        }
                        st.session_state.analysis_charts = generate_analysis_charts(db_config)
                        st.success("Gráficos de análisis generados.")
                        st.info("Navegue a la página de 'Análisis Comparativo' para ver los resultados.")
                    except Exception as e:
                        st.error(f"Error durante la generación de gráficos de análisis: {e}")
    
    elif menu == "Subir Archivos":
        st.header("📂 Subir Archivos Excel")
        st.info("Esta sección es para cargar manualmente los archivos Excel. El proceso 'Actualizar Datos Base' en el Panel de Procesos ya utiliza los archivos existentes en la carpeta 'input_files'.")
        uploaded_files = st.file_uploader("Seleccionar archivos Excel", type=["xlsx", "xls"], accept_multiple_files=True)
        if uploaded_files:
            for uploaded_file in uploaded_files:
                upload_file(uploaded_file, "mantenimiento")
            st.success(f"{len(uploaded_files)} archivos subidos correctamente a 'input_files/mantenimiento'.")
    
    elif menu == "Datos Históricos":
        st.header("Datos Históricos")
        show_historical_data()
    
    elif menu == "Predicciones":
        st.header("Resultados de Predicciones")
        show_predictions()
    
    elif menu == "Análisis Comparativo":
        show_analysis_page()
    


# Pagina de Administracion (solo para administradores)
def admin_page():
    st.title("Panel de Administracion")
    
    # Crear nuevo usuario
    st.header("Crear nuevo usuario")
    
    with st.form("create_user_form"):
        new_username = st.text_input("Nombre de usuario")
        new_password = st.text_input("Contraseña", type="password")
        is_admin_user = st.checkbox("Es administrador")
        submit = st.form_submit_button("Crear usuario")
        
        if submit:
            engine = get_db_engine()
            if engine:
                try:
                    with engine.connect() as connection:
                        password_hash = hashlib.sha256(new_password.encode('utf-8')).hexdigest()
                        query = text("INSERT INTO users (username, password, is_admin) VALUES (:user, :pwd, :admin)")
                        connection.execute(query, {"user": new_username, "pwd": password_hash, "admin": 1 if is_admin_user else 0})
                        connection.commit()
                        st.success(f"Usuario {new_username} creado correctamente")
                except Exception as e:
                    st.error(f"Error al crear usuario: {e}")

# Enrutamiento principal
def main():
    if st.session_state.username is None:
        login_page()
    else:
        if st.session_state.is_admin:
            tab1, tab2 = st.tabs(["Panel Principal", "Administracion"])

            with tab1:
                main_page()

            with tab2:
                admin_page()
        else:
            main_page()

if __name__ == "__main__":
    main()
