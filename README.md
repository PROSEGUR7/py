# Sistema de Mantenimientos con Streamlit

Aplicación para análisis predictivo y visualización de datos de mantenimiento, desarrollada con Python y Streamlit.

## Características

- Ejecución de notebooks Jupyter para análisis de datos
- Carga de archivos Excel para procesamiento
- Visualización de datos históricos y predicciones
- Generación de gráficas analíticas
- Sistema de autenticación de usuarios

## Requisitos

- Python 3.8+
- MySQL/MariaDB

## Instalación

1. Clonar el repositorio
2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Configurar la base de datos en el archivo `.env`
4. Ejecutar el script de configuración de la base de datos:

```bash
python setup_database.py
```

5. Si vienes de la versión anterior en PHP, ejecuta el script de migración:

```bash
python migrate_data.py
```

## Ejecución

### Local

```bash
streamlit run app.py
```

### Docker

```bash
docker build -t mantenimientos-streamlit .
docker run -p 8501:8501 mantenimientos-streamlit
```

## Estructura del Proyecto

- `app.py`: Aplicación principal de Streamlit
- `setup_database.py`: Script para configurar la base de datos
- `migrate_data.py`: Script para migrar datos de la versión PHP
- `notebooks/`: Notebooks Jupyter para análisis de datos
- `input_files/`: Archivos de entrada (Excel)
- `output/images/`: Imágenes generadas por los análisis