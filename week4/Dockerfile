FROM svizor/zoomcamp-model:mlops-3.10.0-slim

WORKDIR /app

RUN pip install -U pip
RUN pip install pipenv

COPY ["Pipfile", "Pipfile.lock", "./"]

RUN pipenv install --deploy --system

COPY ["starter.py", "./"]

ENTRYPOINT ["python", "starter.py"]
    