FROM python:3.10

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les dépendances
COPY requirements.txt .

# Installer les dépendances dans le conteneur
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copier le reste de ton code
COPY . .

# Commande par défaut (à adapter selon Airflow ou ton script)
CMD ["airflow", "scheduler"]
