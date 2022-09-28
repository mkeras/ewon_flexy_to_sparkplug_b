

class ConfigError(Exception):
    pass


class IllegalTopicError(ValueError):
    pass


class TopicDecodeError(ValueError):
    pass


class DeviceAlreadySavedError(ValueError):
    pass