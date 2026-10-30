"""Discovery and dispatch for optional Calrissian extensions."""

from importlib.metadata import entry_points
import logging


EXTENSION_ENTRY_POINT_GROUP = "calrissian.extensions"
EXTENSION_API_VERSION = 1
log = logging.getLogger("calrissian.extensions")


class ExtensionManager:
    """Load installed extensions and dispatch their optional hooks."""

    def __init__(self, extensions=None):
        self.extensions = list(extensions or [])
        self.args = None
        self.runtime_context = None

    @classmethod
    def discover(cls):
        discovered = []
        for entry_point in sorted(
            entry_points(group=EXTENSION_ENTRY_POINT_GROUP), key=lambda item: item.name
        ):
            factory = entry_point.load()
            extension = factory()
            if not callable(getattr(extension, "register_arguments", None)):
                raise TypeError(
                    f"Calrissian extension '{entry_point.name}' must provide register_arguments(parser)"
                )
            if getattr(extension, "api_version", None) != EXTENSION_API_VERSION:
                raise TypeError(
                    f"Calrissian extension '{entry_point.name}' uses API version "
                    f"{getattr(extension, 'api_version', None)!r}; expected {EXTENSION_API_VERSION}"
                )
            discovered.append(extension)
            log.debug("Loaded extension %s from %s", entry_point.name, entry_point.value)
        return cls(discovered)

    def register_arguments(self, parser):
        for extension in self.extensions:
            extension.register_arguments(parser)

    def configure_runtime(self, args, runtime_context):
        self.args = args
        self.runtime_context = runtime_context
        options = getattr(runtime_context, "extension_options", None)
        if not isinstance(options, dict):
            options = {}
            runtime_context.extension_options = options
        for extension in self.extensions:
            hook = getattr(extension, "configure_runtime", None)
            if hook:
                hook(args, runtime_context)

    def custom_schema_callback(self):
        callbacks = []
        for extension in self.extensions:
            hook = getattr(extension, "custom_schema_callback", None)
            callback = hook(self.args) if hook else None
            if callback:
                callbacks.append(callback)
        if not callbacks:
            return None

        def register_schemas():
            for callback in callbacks:
                callback()

        return register_schemas

    def make_job_runner(self, tool, runtime_context):
        for extension in self.extensions:
            hook = getattr(extension, "make_job_runner", None)
            runner = hook(tool, runtime_context) if hook else None
            if runner is not None:
                return runner
        return None

    def cleanup(self):
        handled = False
        for extension in reversed(self.extensions):
            hook = getattr(extension, "cleanup", None)
            if hook:
                try:
                    handled = bool(hook(self.args, self.runtime_context)) or handled
                except Exception:
                    log.exception("Cleanup hook failed for Calrissian extension %s", extension)
        return handled

    def logger_names(self):
        return [
            name
            for extension in self.extensions
            for name in getattr(extension, "logger_names", ())
        ]

