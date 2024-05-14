class DerivaDownloadError(RuntimeError):
    pass


class DerivaDownloadConfigurationError(ValueError):
    pass


class DerivaDownloadAuthenticationError(Exception):
    pass


class DerivaDownloadAuthorizationError(Exception):
    pass


class DerivaDownloadTimeoutError(Exception):
    pass


class DerivaDownloadBaggingError(Exception):
    pass
