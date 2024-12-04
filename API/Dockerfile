# Usar imagen base de Python
FROM python:3.10-slim-bullseye

# Establecer directorio de trabajo
WORKDIR /app

# Copiar dependencias y código
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Exponer puerto para la aplicación
EXPOSE 8000

# Comando para ejecutar la app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
