
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Evitar archivos .pyc y mostrar logs inmediatamente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependencias
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar el código y los recursos necesarios
COPY src/ ./src/
COPY sql/ ./sql/
COPY contracts/ ./contracts/
COPY tests/ ./tests/

# Crear directorios de trabajo
RUN mkdir -p data reports

# Comando de entrada del pipeline
ENTRYPOINT ["python", "-m", "src.pipeline"]

CMD ["--mes", "2025-03"]
