# Simod Job Controller

![simod-http](https://github.com/AutomatedProcessImprovement/simod-queue-worker/actions/workflows/build.yaml/badge.svg)


This is a worker that listens to `requests.status.pending` messages, starts a job using the Kubernetes API and publishes job statuses back to the message queue.
