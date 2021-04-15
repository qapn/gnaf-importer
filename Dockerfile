FROM python:3

WORKDIR /usr/src/app

RUN pip install psycopg2-binary

COPY import_gnaf.py .

CMD ["python", "import_gnaf.py"]