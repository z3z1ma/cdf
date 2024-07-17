class ConfigError(RuntimeError):
    pass


class FrozenConfigError(ConfigError):
    pass


class InputConfigError(ConfigError):
    pass


class NewKeyConfigError(ConfigError):
    pass


class SetChildConfigError(ConfigError):
    pass


class PerturbSpecError(ConfigError):
    pass
