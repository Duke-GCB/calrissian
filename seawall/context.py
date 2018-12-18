from tool import seawall_make_tool
from cwltool.context import LoadingContext, RuntimeContext
import logging

log = logging.getLogger('seawall.context')

class SeawallLoadingContext(LoadingContext):

    def __init__(self, **kwargs):
        log.info('init SeawallLoadingContext')
        kwargs['construct_tool_object'] = seawall_make_tool
        return super(SeawallLoadingContext, self).__init__(kwargs)


class SeawallRuntimeContext(RuntimeContext):
    pass
