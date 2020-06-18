FROM python:3.7-stretch

WORKDIR /app

COPY src/ src
COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "/app/src/start.py"]