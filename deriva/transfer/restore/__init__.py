class DerivaRestoreError(RuntimeError):
    pass


class DerivaRestoreConfigurationError(ValueError):
    pass


class DerivaRestoreAuthenticationError(Exception):
    pass


class DerivaRestoreAuthorizationError(Exception):
    pass
