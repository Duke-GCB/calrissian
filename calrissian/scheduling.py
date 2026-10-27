"""Configuration of GPU-specific Kubernetes scheduling permissions."""
import copy
import re

import yaml
from cwltool.errors import WorkflowException
from kubernetes.utils.quantity import parse_quantity


_LABEL = re.compile(r'[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?')
_DNS_LABEL = re.compile(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?')


def load_gpu_tolerations(filename):
    """Read and validate a Kubernetes toleration list before pod submission."""
    def invalid(message):
        raise WorkflowException(f'Invalid --pod-gpu-tolerations file {filename!r}: {message}')

    try:
        with open(filename) as stream:
            tolerations = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as error:
        invalid(str(error))
    if not isinstance(tolerations, list):
        invalid('expected a YAML list of toleration objects (use [] for an empty list)')
    fields = {'key', 'operator', 'value', 'effect', 'tolerationSeconds'}
    for index, toleration in enumerate(tolerations):
        prefix = f'entry {index + 1}: '
        if not isinstance(toleration, dict):
            invalid(prefix + 'expected a toleration object')
        if set(toleration) - fields:
            invalid(prefix + 'unknown fields: ' + ', '.join(map(str, set(toleration) - fields)))
        for field in ('key', 'operator', 'value', 'effect'):
            if field in toleration and not isinstance(toleration[field], str):
                invalid(prefix + field + ' must be a string')
        key = toleration.get('key', '')
        operator = toleration.get('operator') or 'Equal'
        value = toleration.get('value', '')
        effect = toleration.get('effect', '')
        if operator not in ('Equal', 'Exists'):
            invalid(prefix + 'operator must be Equal or Exists')
        if not key and operator != 'Exists':
            invalid(prefix + 'an empty key requires operator Exists')
        if key:
            parts = key.split('/')
            if len(parts) > 2 or len(parts[-1]) > 63 or not _LABEL.fullmatch(parts[-1]):
                invalid(prefix + 'key must be a Kubernetes qualified name')
            if len(parts) == 2 and (len(parts[0]) > 253 or any(
                    len(label) > 63 or not _DNS_LABEL.fullmatch(label)
                    for label in parts[0].split('.'))):
                invalid(prefix + 'key prefix must be a DNS subdomain')
        if operator == 'Exists' and value:
            invalid(prefix + 'operator Exists requires an empty value')
        if value and (len(value) > 63 or not _LABEL.fullmatch(value)):
            invalid(prefix + 'value must be a Kubernetes label value')
        if effect not in ('', 'NoSchedule', 'PreferNoSchedule', 'NoExecute'):
            invalid(prefix + 'effect must be NoSchedule, PreferNoSchedule, or NoExecute')
        if 'tolerationSeconds' in toleration:
            seconds = toleration['tolerationSeconds']
            if type(seconds) is not int or not -(2**63) <= seconds < 2**63:
                invalid(prefix + 'tolerationSeconds must be a signed 64-bit integer')
            if effect != 'NoExecute':
                invalid(prefix + 'tolerationSeconds requires effect NoExecute')
    return tolerations


def apply_gpu_tolerations(pod, tolerations):
    """Add configured tolerations only when a task container allocates GPUs."""
    if tolerations and any(
            parse_quantity(str(container.get('resources', {}).get(bound, {}).get('nvidia.com/gpu', 0))) > 0
            for container in pod['spec']['containers']
            for bound in ('requests', 'limits')):
        pod['spec']['tolerations'] = copy.deepcopy(tolerations)
    return pod
