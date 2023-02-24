import logging
import os
from pathlib import Path

import pika
from kubernetes import client, config


class UserSettings:
    broker_url: str
    requests_queue: str
    results_queue: str
    simod_docker_image: str

    def __init__(self):
        self.broker_url = os.environ.get('BROKER_URL')
        self.requests_queue = os.environ.get('SIMOD_REQUESTS_QUEUE_NAME')
        self.results_queue = os.environ.get('SIMOD_RESULTS_QUEUE_NAME')
        self.simod_docker_image = os.environ.get('SIMOD_DOCKER_IMAGE')
        self.kubernetes_namespace = os.environ.get('KUBERNETES_NAMESPACE', 'default')

        if not self.is_valid():
            raise ValueError('Invalid settings')

    def is_valid(self):
        return (
                self.broker_url is not None
                and self.requests_queue is not None
                and self.results_queue is not None
                and self.simod_docker_image is not None
        )


class Worker:
    def __init__(self, settings: UserSettings):
        self.settings = settings

        self._parameters = pika.URLParameters(self.settings.broker_url)
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()

    def on_message(self, channel, method_frame, header_frame, body):
        logging.info(
            'Received message {} from {}: {}'.format(
                method_frame.delivery_tag, header_frame.app_id, body
            )
        )

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        self.submit_job(body)

    def submit_job(self, job_request_id):
        job_request_id = str(job_request_id, 'utf-8')
        logging.info('Processing request: {}'.format(job_request_id))

        config.load_incluster_config()

        with client.ApiClient() as api_client:
            job = self.make_job(job_request_id)

            try:
                api_instance = client.BatchV1Api(api_client)
                api_instance.create_namespaced_job(namespace=self.settings.kubernetes_namespace, body=job)
            except client.rest.ApiException as e:
                logging.exception('Exception when calling BatchV1Api->create_namespaced_job: %s, %s'.format(e, e.body))

    def make_job(self, job_request_id) -> client.V1Job:
        request_output_dir = Path(f'/tmp/simod-volume/data/requests/{job_request_id}')
        config_path = request_output_dir / 'configuration.yaml'
        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(name=f'simod-{job_request_id}'),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=5,
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
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
                        restart_policy='Never',
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

    def run(self):
        self._channel.queue_declare(queue=self.settings.requests_queue, durable=True)

        self._channel.basic_consume(
            queue=self.settings.requests_queue, on_message_callback=self.on_message
        )

        try:
            self._channel.start_consuming()
        except Exception as e:
            logging.error(e)
            self._channel.stop_consuming()
        self._connection.close()

        logging.info('Worker stopped')


if __name__ == '__main__':
    settings = UserSettings()
    worker = Worker(settings)
    worker.run()
