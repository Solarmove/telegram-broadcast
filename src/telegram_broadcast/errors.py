"""Public errors; callers can handle operational outcomes explicitly."""

from math import isfinite


class BroadcastError(Exception):
    """Base error for the library."""


class ConfigurationError(BroadcastError):
    pass


class BroadcastNotFound(BroadcastError):
    pass


class WrongBot(BroadcastError):
    pass


class BotBusy(BroadcastError):
    """Another runner currently holds this bot's database lock."""


class InvalidTransition(BroadcastError):
    pass


class RecipientUnavailable(BroadcastError):
    """Telegram definitively rejected this recipient."""


class DeliveryUncertain(BroadcastError):
    """Telegram may have accepted the message before its response was lost."""


class PermanentSendError(BroadcastError):
    """Invalid credentials or content; stop this broadcast."""


class RateLimited(BroadcastError):
    def __init__(self, retry_after: float):
        if not isfinite(retry_after) or retry_after < 0:
            raise ValueError("retry_after must be finite and nonnegative")
        self.retry_after = retry_after
        super().__init__(f"Telegram rate limited the bot for {retry_after:g} seconds")
