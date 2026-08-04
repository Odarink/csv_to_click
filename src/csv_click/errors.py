class CsvClickError(Exception):
    """Base application error."""


class CertificateError(CsvClickError):
    """Raised when ClickHouse client certificate files are missing or unusable."""


class ClickHouseConnectionError(CsvClickError):
    """Raised when a ClickHouse connection check fails."""


class ExistingTableError(CsvClickError):
    """Raised when the target local or distributed table already exists."""


class CsvSchemaError(CsvClickError):
    """Raised when CSV schema inference or type conversion fails."""


class CsvLoadError(CsvClickError):
    """Raised when converted CSV rows cannot be loaded into ClickHouse."""


class CsvReadCancelled(CsvClickError):
    """Raised when the user stops CSV schema reading."""


class CsvLoadCancelled(CsvClickError):
    """Raised when the operator cancels a running load."""

