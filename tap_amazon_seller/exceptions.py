class PermissionError(Exception):
    """Raised when SP-API returns a permanent authorization failure."""


class InvalidReportParameter(Exception):
    """Raised when SP-API rejects report or replenishment request parameters."""


REPORT_GIVEUP_EXCEPTIONS = (PermissionError, InvalidReportParameter)


def report_giveup(exc: Exception) -> bool:
    """Return True for exceptions that should not be retried."""
    return isinstance(exc, REPORT_GIVEUP_EXCEPTIONS)
