FROM python:3.10-slim-bullseye

# Establece el directorio de trabajo
WORKDIR /app

# Copia todo el contenido del proyecto al contenedor
COPY . /app

# Instala las dependencias especificadas en requirements.txt
RUN pip install -r requirements.txt

# Expone el puerto 8000 para el contenedor
EXPOSE 8080

# Comando para ejecutar la aplicación con Uvicorn
ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
