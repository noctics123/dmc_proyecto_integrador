# Igual que tu otro servicio
FROM python:3.10-slim

WORKDIR /app

# Reusa el mismo requirements.txt (ajusta la ruta según tu repo)
COPY ../landing_run/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia solo lo necesario
COPY ../landing_run/simbad ./simbad
COPY ./main_simbad.py ./main_simbad.py

EXPOSE 8080
CMD ["sh","-c","uvicorn main_simbad:app --host 0.0.0.0 --port ${PORT:-8080}"]
