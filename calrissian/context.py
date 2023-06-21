from calrissian.tool import calrissian_make_tool
from cwltool.context import LoadingContext, RuntimeContext
import logging

log = logging.getLogger('calrissian.context')

class CalrissianLoadingContext(LoadingContext):

    def __init__(self, **kwargs):
        kwargs['construct_tool_object'] = calrissian_make_tool
        return super(CalrissianLoadingContext, self).__init__(kwargs)


class CalrissianRuntimeContext(RuntimeContext):

    def __init__(self, kwargs=None):
        # The ContextBase class sets values from kwargs if the class has that attribute
        # So, if we want to capture an init param of pod_labels, we just set it to
        # None and let super() handle the rest.
        self.pod_labels = None
        self.pod_env_vars = None
        self.pod_nodeselectors = None
        self.pod_serviceaccount = None
        self.tool_logs_basepath = None
        self.max_gpus = None
        return super(CalrissianRuntimeContext, self).__init__(kwargs)
