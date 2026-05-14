class DunecatError(Exception):
    pass


class ConfigError(DunecatError):
    pass


class TokenExpiredError(DunecatError):
    pass
