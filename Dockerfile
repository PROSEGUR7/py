FROM python:3.10-slim

WORKDIR /app

# Instala dependencias del sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copia los archivos de requisitos
COPY requirements.txt .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código al contenedor
COPY . .

# Crea directorios necesarios
RUN mkdir -p input_files/mantenimiento input_files/fallas input_files/operacion
RUN mkdir -p notebooks/output
RUN mkdir -p output/images

# Expone el puerto para Streamlit
EXPOSE 8501

# Comando para iniciar la aplicación
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
