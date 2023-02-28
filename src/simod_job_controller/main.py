import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

import pika
from kubernetes import client, config
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import PERSISTENT_DELIVERY_MODE


class Settings:
    broker_url: str
    binding_key: str
    simod_docker_image: str

    def __init__(self):
        self.broker_url = os.environ.get('BROKER_URL')
        self.exchange_name = os.environ.get('SIMOD_EXCHANGE_NAME')
        self.binding_key = os.environ.get('SIMOD_PENDING_ROUTING_KEY')
        self.simod_docker_image = os.environ.get('SIMOD_DOCKER_IMAGE')
        self.kubernetes_namespace = os.environ.get('KUBERNETES_NAMESPACE', 'default')

        if not self.is_valid():
            raise ValueError('Invalid settings')

    def is_valid(self):
        return (
                self.broker_url is not None
                and self.exchange_name is not None
                and self.binding_key is not None
                and self.simod_docker_image is not None
        )


class Worker:
    def __init__(self, settings: Settings):
        self.settings = settings

        self._parameters = pika.URLParameters(self.settings.broker_url)
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()

        self._queue_name = None
        self._background_tasks = set()

        self.init_cluster_config()

    @staticmethod
    def init_cluster_config():
        config.load_incluster_config()

    async def run(self):
        self._channel.exchange_declare(
            exchange=self.settings.exchange_name,
            exchange_type='topic',
            durable=True,
        )

        result = self._channel.queue_declare('', exclusive=True)
        self._queue_name = result.method.queue

        self._channel.queue_bind(
            exchange=self.settings.exchange_name,
            queue=self._queue_name,
            routing_key=self.settings.binding_key,
        )

        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self.on_message)

        logging.info('Worker started')

        try:
            self._channel.start_consuming()
        except Exception as e:
            logging.error(e)
            self._channel.stop_consuming()
        self._connection.close()

        logging.info('Worker stopped')

    def on_message(
            self,
            channel: BlockingChannel,
            method: spec.Basic.Deliver,
            properties: spec.BasicProperties,
            body: bytes,
    ):
        request_id = body.decode()
        routing_key = method.routing_key
        status = routing_key.split('.')[-1]

        logging.info(f'Got message: {request_id} {status}')

        self.submit_job(request_id)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def submit_job(self, job_request_id: str):
        logging.info(f'Submitting a job for {job_request_id}')

        with client.ApiClient() as api_client:
            api_response = self.try_job_with_client(job_request_id, api_client)

            if api_response is None:
                logging.error(f'Failed to submit a job for {job_request_id}')
                self.try_publish_job_status('failed', job_request_id)
                return

            job_name = api_response.metadata.name
            self.add_job_watcher(job_name, job_request_id)

    def try_job_with_client(self, job_request_id: str, api_client: client.ApiClient) -> Optional[client.V1Job]:
        job = self.make_job(job_request_id)

        try:
            api_instance = client.BatchV1Api(api_client)
            api_response = api_instance.create_namespaced_job(namespace=self.settings.kubernetes_namespace, body=job)
            logging.info(api_response)
            return api_response
        except client.rest.ApiException as e:
            logging.exception('Exception when calling BatchV1Api->create_namespaced_job: %s, %s'.format(e, e.body))

        return None

    def add_job_watcher(self, job_name: str, job_request_id: str):
        task = asyncio.create_task(self.watch_task(job_name, job_request_id))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def watch_task(self, job_name: str, job_request_id: str, delay_seconds: int = 10):
        logging.info(f'Watching job {job_name}')

        previous_status = None

        while True:
            with client.ApiClient() as api_client:
                api_instance = client.BatchV1Api(api_client)
                api_response = api_instance.read_namespaced_job(job_name, self.settings.kubernetes_namespace)

                status = self.get_status_from_response(api_response)

                if status is not None:
                    logging.info(f'Job {job_name} status changed to {status}')

                    if previous_status != status:
                        self.try_publish_job_status(status, job_request_id)
                        previous_status = status

                    if status in ['succeeded', 'failed']:
                        break

            await asyncio.sleep(delay_seconds)

        logging.info(f'Job {job_name} watcher stopped')

    def make_job(self, job_request_id) -> client.V1Job:
        request_output_dir = Path(f'/tmp/simod-volume/data/requests/{job_request_id}')
        config_path = request_output_dir / 'configuration.yaml'
        job_name = self.job_name(job_request_id)

        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                # ttl_seconds_after_finished=5,
                backoff_limit=0,
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy='OnFailure',
                        containers=[
                            client.V1Container(
                                name='simod',
                                image=self.settings.simod_docker_image,
                                command=[
                                    'bash',
                                    'run.sh',
                                    str(config_path),
                                    str(request_output_dir),
                                ],
                                resources=client.V1ResourceRequirements(
                                    requests={'cpu': '100m', 'memory': '128Mi'},
                                    limits={'cpu': '1', 'memory': '1Gi'},
                                ),
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        name='simod-data',
                                        mount_path='/tmp/simod-volume',
                                    ),
                                ],
                            )
                        ],
                        volumes=[
                            client.V1Volume(
                                name='simod-data',
                                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name='simod-volume-claim'
                                ),
                            )
                        ],
                    )
                ),
            ),
        )

        return job

    @staticmethod
    def get_status_from_response(api_response: client.V1Job) -> Optional[str]:
        status = api_response.status
        if status.succeeded == 1:
            return 'succeeded'
        elif status.failed == 1:
            return 'failed'
        elif status.active == 1:
            return 'running'
        else:
            return None

    def try_publish_job_status(self, job_status: str, job_request_id: str):
        routing_key = f'requests.status.{job_status}'

        try:
            self._channel.basic_publish(
                exchange=self.settings.exchange_name,
                routing_key=routing_key,
                body=job_request_id.encode(),
                properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
            )
            logging.info(f'Published job status {job_status} for the request {job_request_id}')
        except Exception as e:
            logging.error(f'Failed to publish job status {job_status} for the request {job_request_id}: {e}')

    @staticmethod
    def job_name(job_request_id: str) -> str:
        return f'simod-{job_request_id}'


async def main():
    settings = Settings()
    worker = Worker(settings)
    await worker.run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())
