import threading
import logging

log = logging.getLogger('calrissian.podmonitor')


class PodMonitor(object):
    """
    Keeps track of pods that need to be deleted when killed
    """
    pod_names = []
    lock = threading.Lock()

    @staticmethod
    def add(pod):
        with PodMonitor.lock:
            log.info('PodMonitor adding {}'.format(pod.metadata.name))
            PodMonitor.pod_names.append(pod.metadata.name)

    @staticmethod
    def remove(pod):
        with PodMonitor.lock:
            log.info('PodMonitor removing {}'.format(pod.metadata.name))
            # This has to look up the pod by something unique
            PodMonitor.pod_names.remove(pod.metadata.name)

    @staticmethod
    def cleanup(client_cls):
        with PodMonitor.lock:
            client = client_cls()
            for pod_name in PodMonitor.pod_names:
                log.info('PodMonitor deleting pod {}'.format(pod_name))
                try:
                    client.delete_pod_name(pod_name)
                except Exception:
                    log.error('Error deleting pod named {}, ignoring'.format(pod_name))
            PodMonitor.pod_names = []


def delete_pods():
    # Import inside function here to avoid import loop
    from calrissian.k8s import KubernetesClient
    PodMonitor.cleanup(KubernetesClient)
