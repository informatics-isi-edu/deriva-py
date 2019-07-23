from deriva.transfer import DerivaDownloadError, DerivaDownloadAuthorizationError, DerivaDownloadAuthenticationError, \
    DerivaDownloadConfigurationError


class DerivaBackupError(DerivaDownloadError):
    pass


class DerivaBackupConfigurationError(DerivaDownloadConfigurationError):
    pass


class DerivaBackupAuthenticationError(DerivaDownloadAuthenticationError):
    pass


class DerivaBackupAuthorizationError(DerivaDownloadAuthorizationError):
    pass
