"""Watch expiration must not abort tasks or release their GPU reservation (#216/#217)."""
from concurrent.futures import Future
from datetime import datetime, timezone
import logging
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from kubernetes.client import (
    V1Container, V1ContainerState, V1ContainerStateTerminated, V1ContainerStatus,
    V1ObjectMeta, V1Pod, V1PodSpec, V1PodStatus, V1ResourceRequirements,
)
from kubernetes.client.exceptions import ApiException

from calrissian.dask import KubernetesDaskClient
from calrissian.executor import Resources, ThreadPoolJobExecutor
from calrissian.k8s import KubernetesClient


@pytest.fixture(params=[KubernetesClient, KubernetesDaskClient], ids=['ordinary', 'dask'])
def monitor(request, monkeypatch):
    monkeypatch.setenv('CALRISSIAN_DELETE_PODS', 'true')
    with patch('calrissian.k8s.load_config_get_namespace', return_value='test'), \
            patch('calrissian.k8s.client.CoreV1Api'):
        instance = request.param()
    # No log-stream threads or API connection is needed for these terminal events.
    instance._stop_log_thread = Mock()
    instance.pod = make_pod(0, isinstance(instance, KubernetesDaskClient))
    return instance


def make_pod(exit_code, dask=False):
    now = datetime.now(timezone.utc)
    containers = [V1Container(name='main-container', resources=V1ResourceRequirements(
        requests={'cpu': '1', 'memory': '64Mi', 'nvidia.com/gpu': '1'}))]
    statuses = [V1ContainerStatus(
        name='main-container', image='image:tag', image_id='image-id', ready=False,
        restart_count=0, state=V1ContainerState(terminated=V1ContainerStateTerminated(
            exit_code=exit_code, started_at=now, finished_at=now)))]
    # Cleanup success must not hide failure of the Dask main container.
    if not dask:
        return V1Pod(metadata=V1ObjectMeta(name='task', resource_version='300'),
                     spec=V1PodSpec(containers=containers),
                     status=V1PodStatus(container_statuses=statuses))
    containers.append(V1Container(name='cleanup'))
    statuses.append(V1ContainerStatus(
        name='cleanup', image='image:tag', image_id='image-id', ready=False,
        restart_count=0, state=V1ContainerState(terminated=V1ContainerStateTerminated(
            exit_code=0, started_at=now, finished_at=now))))
    return V1Pod(metadata=V1ObjectMeta(name='task', resource_version='300'),
                 spec=V1PodSpec(containers=containers),
                 status=V1PodStatus(container_statuses=statuses))


def make_running_pod(dask=False):
    pod = make_pod(0, dask=dask)
    statuses = pod.status.container_statuses
    statuses[0].state = SimpleNamespace(running=True, waiting=None, terminated=None)
    if dask:
        statuses[1].state = SimpleNamespace(running=None, waiting=True, terminated=None)
    return pod


def wait(monitor):
    if isinstance(monitor, KubernetesDaskClient):
        return monitor.wait_for_completion(cm_name='task-config')
    return monitor.wait_for_completion()


def script_watch(monitor, outcomes, exit_code=0, while_waiting=lambda: None):
    """Bounded responses fail immediately if recovery loops unexpectedly."""
    versions = [str(100 * (i + 1)) for i in range(len(outcomes))]
    monitor.core_api_instance.list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[monitor.pod], metadata=SimpleNamespace(resource_version=v))
        for v in versions]

    def stream(outcome, expected_version, **kwargs):
        assert kwargs['resource_version'] == expected_version
        assert kwargs['timeout_seconds'] == 30
        assert kwargs['field_selector'] == 'metadata.name=task'
        monitor.core_api_instance.delete_namespaced_pod.assert_not_called()
        monitor.core_api_instance.delete_namespaced_config_map.assert_not_called()
        while_waiting()
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == 'complete':
            yield {'object': make_pod(exit_code, isinstance(monitor, KubernetesDaskClient))}
        # Otherwise simulate a normal watch timeout with no events.

    watches = []
    for outcome, version in zip(outcomes, versions):
        watch = Mock()
        watch.stream.side_effect = lambda outcome=outcome, version=version, **kw: stream(outcome, version, **kw)
        watches.append(watch)
    module = 'calrissian.dask' if isinstance(monitor, KubernetesDaskClient) else 'calrissian.k8s'
    return patch(module + '.watch.Watch', side_effect=watches)


@pytest.mark.parametrize('exit_code', [0, 7])
@pytest.mark.parametrize('delete', [True, False])
@pytest.mark.parametrize('interruptions', ['expired', 'timeout', 'repeated'])
def test_watch_recovery(monitor, monkeypatch, exit_code, delete, interruptions):
    monkeypatch.setenv('CALRISSIAN_DELETE_PODS', str(delete).lower())
    outcomes = {
        'expired': [ApiException(status=410)],
        'timeout': ['timeout'],
        'repeated': [ApiException(status=410), ApiException(status=410)],
    }[interruptions] + ['complete']
    with script_watch(monitor, outcomes, exit_code):
        result = wait(monitor)
    assert result.exit_code == exit_code
    assert monitor.core_api_instance.list_namespaced_pod.call_count == len(outcomes)
    assert monitor.pod is None
    assert monitor.core_api_instance.delete_namespaced_pod.call_count == int(delete)
    if isinstance(monitor, KubernetesDaskClient):
        assert monitor.core_api_instance.delete_namespaced_config_map.call_count == int(delete)


@pytest.mark.parametrize('stream_logs', [True, False])
def test_log_streaming_setting_is_respected_across_watch_recovery(monitor, monkeypatch, stream_logs):
    monkeypatch.setenv('CALRISSIAN_STREAM_LOGS', str(stream_logs).lower())
    running_pod = make_running_pod(isinstance(monitor, KubernetesDaskClient))
    completed_pod = make_pod(0, isinstance(monitor, KubernetesDaskClient))
    monitor._ensure_log_thread_started = Mock()
    monitor.core_api_instance.list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[monitor.pod], metadata=SimpleNamespace(resource_version='100')),
        SimpleNamespace(items=[monitor.pod], metadata=SimpleNamespace(resource_version='200')),
    ]

    def stream_then_expire(**kwargs):
        assert kwargs['resource_version'] == '100'
        yield {'object': running_pod}
        raise ApiException(status=410)

    def stream_to_completion(**kwargs):
        assert kwargs['resource_version'] == '200'
        yield {'object': completed_pod}

    first_watch = Mock()
    first_watch.stream.side_effect = stream_then_expire
    second_watch = Mock()
    second_watch.stream.side_effect = stream_to_completion
    module = 'calrissian.dask' if isinstance(monitor, KubernetesDaskClient) else 'calrissian.k8s'

    with patch(module + '.watch.Watch', side_effect=[first_watch, second_watch]):
        result = wait(monitor)

    assert result.exit_code == 0
    assert monitor._ensure_log_thread_started.call_count == int(stream_logs)
    assert monitor.pod is None


def test_nonrecoverable_watch_error(monitor):
    error = ApiException(status=403, reason='Forbidden')
    with script_watch(monitor, [error]):
        with pytest.raises(ApiException) as caught:
            wait(monitor)
    assert caught.value is error
    assert monitor.core_api_instance.list_namespaced_pod.call_count == 1
    monitor.core_api_instance.delete_namespaced_pod.assert_not_called()
    assert monitor.pod is not None


@pytest.mark.parametrize('failure', [False, True])
def test_gpu_reservation_survives_watch_recovery(monitor, failure):
    executor = ThreadPoolJobExecutor(64, 1, total_gpus=1)
    reserved = Resources(64, 1, 1)
    logger = logging.getLogger(__name__)
    executor.allocate(reserved, logger)

    def assert_reserved():
        assert executor.available_resources.gpus == 0

    outcomes = [ApiException(status=410), ApiException(status=403) if failure else 'complete']
    future = Future()
    # Real Future completion invokes the real accounting callback exactly once.
    with patch.object(executor, 'restore', wraps=executor.restore) as restore:
        future.add_done_callback(lambda done: executor.job_done_callback(reserved, logger, done))
        with script_watch(monitor, outcomes, while_waiting=assert_reserved):
            if failure:
                with pytest.raises(ApiException) as caught:
                    wait(monitor)
                assert_reserved()
                future.set_exception(caught.value)
            else:
                result = wait(monitor)
                assert_reserved()
                future.set_result(result)
        restore.assert_called_once_with(reserved, logger)
    assert executor.available_resources == executor.total_resources
    if failure:
        assert executor.exceptions.get_nowait() is caught.value
    assert executor.exceptions.empty()
