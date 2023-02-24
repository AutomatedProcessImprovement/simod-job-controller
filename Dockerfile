FROM python:3.9-alpine

RUN apk update && apk add poetry
RUN pip install --upgrade pip

WORKDIR /usr/src/simod-http
ADD . .
RUN poetry install

ENV PYTHONUNBUFFERED=1
ENV BROKER_URL=amqp://guest:guest@rabbitmq-service:5672
ENV SIMOD_REQUESTS_QUEUE_NAME=requests
ENV SIMOD_RESULTS_QUEUE_NAME=results
ENV SIMOD_DOCKER_IMAGE=nokal/simod-http:0.2.0
ENV KUBERNETES_NAMESPACE=default

CMD ["poetry", "run", "python", "src/simod_queue_worker/main.py"]