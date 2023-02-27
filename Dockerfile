FROM python:3.9-alpine

RUN apk update && apk add poetry
RUN pip install --upgrade pip

WORKDIR /usr/src/simod-http
ADD . .
RUN poetry install

ENV PYTHONUNBUFFERED=1
ENV BROKER_URL=amqp://guest:guest@rabbitmq-service:5672
ENV SIMOD_PENDING_ROUTING_KEY=requests.status.pending
ENV SIMOD_DOCKER_IMAGE=nokal/simod:3.3.0
ENV KUBERNETES_NAMESPACE=default

CMD ["poetry", "run", "python", "src/simod_queue_worker/main.py"]