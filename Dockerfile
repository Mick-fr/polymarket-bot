# Image Python légère basée sur Alpine pour minimiser la taille
FROM python:3.12-slim

# Empêche Python de bufferiser stdout/stderr (logs en temps réel dans Docker)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Installe les dépendances système nécessaires pour py-clob-client (crypto)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Copie et installe les dépendances Python en premier (cache Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie le code source
COPY . .

# Crée le répertoire de données
RUN mkdir -p /app/data

# Le CMD est défini dans docker-compose.yml (bot ou dashboard)
CMD ["python", "run.py", "bot"]
