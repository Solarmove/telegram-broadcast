"""Redact bot credentials from errors persisted for later inspection."""

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ErrorSanitizer:
    bot_token: str = field(repr=False)

    def __call__(self, error: Exception) -> str:
        message = str(error).replace(self.bot_token, "[redacted]")
        return re.sub(r"\d{5,}:[A-Za-z0-9_-]{20,}", "[redacted]", message)[:2000]
