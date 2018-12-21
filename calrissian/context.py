from tool import calrissian_make_tool
from cwltool.context import LoadingContext
import logging

log = logging.getLogger('calrissian.context')

class CalrissianLoadingContext(LoadingContext):

    def __init__(self, **kwargs):
        kwargs['construct_tool_object'] = calrissian_make_tool
        return super(CalrissianLoadingContext, self).__init__(kwargs)
