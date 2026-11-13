"""Regression tests for Dask requirement evaluation and command status."""
from unittest.mock import patch

import pytest


def test_main_failure_is_preserved_after_successful_cleanup():
    from unittest.mock import Mock
    from calrissian_dask.dask import KubernetesDaskClient

    main = Mock(state=Mock(running=None, waiting=None, terminated=Mock(exit_code=7)))
    main.name = 'main-container'
    sidecar = Mock(state=Mock(running=None, waiting=None, terminated=Mock(exit_code=0)))
    sidecar.name = 'sidecar-container'
    for statuses in ([main, sidecar], [sidecar, main]):
        # Avoid constructing the Kubernetes API client; exercise actual watch handling.
        client = object.__new__(KubernetesDaskClient)
        client.namespace = 'test'
        client.core_api_instance = Mock()
        client.completion_result = None
        client._get_pod_field_selector = Mock(return_value='metadata.name=test')
        client._get_pod_node_selector = Mock(return_value={})
        client.should_delete_pod = Mock(return_value=False)
        client._clear_pod = Mock()
        client._stop_log_thread = Mock()
        pod = Mock()
        pod.status.init_container_statuses = None
        pod.status.container_statuses = statuses
        container = Mock()
        container.name = 'main-container'
        pod.spec.containers = [container]
        def complete(state, container, selectors):
            client.completion_result = state.terminated.exit_code
        client._handle_completion = Mock(side_effect=complete)
        with patch('calrissian_dask.dask.watch.Watch') as watch:
            watch.return_value.stream.return_value = [{'object': pod}]
            assert client.wait_for_completion() == 7
        assert client._handle_completion.call_args.args[0] is main.state


def test_failed_dask_init_container_cleans_up_and_stops_watching():
    from unittest.mock import Mock, patch
    from calrissian.k8s import CalrissianJobException
    from calrissian_dask.dask import KubernetesDaskClient

    client = object.__new__(KubernetesDaskClient)
    client.namespace = 'test'
    client.core_api_instance = Mock()
    client.core_api_instance.read_namespaced_pod_log.return_value = b'Gateway connection refused'
    client.pod = Mock(metadata=Mock(name='task-pod'))
    client.completion_result = None
    client._get_pod_field_selector = Mock(return_value='metadata.name=task-pod')
    client.delete_pod_name = Mock()
    client.delete_configmap_name = Mock()
    client._clear_pod = Mock(side_effect=lambda: setattr(client, 'pod', None))
    pod = Mock()
    pod.metadata.name = 'task-pod'
    pod.metadata.resource_version = '1'
    failed = Mock(state=Mock(terminated=Mock(exit_code=1)))
    pod.status.init_container_statuses = [failed]
    pod.status.container_statuses = []
    watcher = Mock()
    watcher.stream.return_value = [{'object': pod}]

    with patch('calrissian_dask.dask.watch.Watch', return_value=watcher), \
            patch('calrissian_dask.dask.DaskPodMonitor') as monitor:
        monitor.return_value.__enter__.return_value = monitor.return_value
        with pytest.raises(CalrissianJobException, match='init container failed with exit code 1') as error:
            client.wait_for_completion(cm_name='task-config')

    client.delete_pod_name.assert_called_once_with('task-pod')
    client.delete_configmap_name.assert_called_once_with(cm_name='task-config')
    client._clear_pod.assert_called_once()
    assert 'Gateway connection refused' in str(error.value)
    client.core_api_instance.read_namespaced_pod_log.assert_called_once_with(
        name='task-pod', namespace='test', container=failed.name, tail_lines=100,
    )
    watcher.stop.assert_called_once()
    assert monitor.return_value.remove.call_count == 1


def test_requirement_expressions_are_evaluated_without_mutating_workflow():
    from unittest.mock import Mock
    from calrissian_dask.dask import KubernetesDaskPodBuilder
    requirement = {'class': 'https://calrissian-cwl.github.io/schema#DaskGatewayRequirement',
                   'workerCores': '$(inputs.cores)', 'workerMemory': '${return inputs.memory;}',
                   'workerCoresLimit': 2}
    builder = Mock()
    builder.do_eval.side_effect = [1, '256M']
    def initialize(pod, *args, **kwargs):
        pod.builder = builder
        pod.requirements = [requirement]
    with patch('calrissian_dask.dask.KubernetesPodBuilder.__init__', initialize):
        pod = KubernetesDaskPodBuilder('gateway', False)
    assert pod.dask_requirement['workerCores'] == 1
    assert pod.dask_requirement['workerMemory'] == '256M'
    assert pod.dask_requirement['workerCoresLimit'] == 2
    assert requirement['workerCores'] == '$(inputs.cores)'
    assert builder.do_eval.call_count == 2


def test_dask_logs_follow_main_and_cleanup_independently():
    import threading
    from unittest.mock import Mock
    from calrissian_dask.dask import KubernetesDaskClient

    client = object.__new__(KubernetesDaskClient)
    client._log_stop = threading.Event()
    main, cleanup = Mock(), Mock()
    main.name = 'main-container'
    cleanup.name = 'sidecar-container'
    with patch('calrissian_dask.dask.threading.Thread') as thread:
        client._ensure_log_thread_started(main)
        client._ensure_log_thread_started(cleanup)
        client._ensure_log_thread_started(main)
        assert thread.call_count == 2
        assert [c.kwargs['args'] for c in thread.call_args_list] == [(main,), (cleanup,)]
        assert thread.return_value.start.call_count == 2
        client._stop_log_thread()
        assert client._log_stop.is_set()
        assert client._container_log_threads == {}
