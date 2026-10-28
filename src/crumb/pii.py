"""Pseudonymisation of user identifiers before anything lands in the warehouse.

Identifiers are replaced by a keyed HMAC-SHA256 (salt from ``CRUMB_PII_SALT``),
namespaced per identifier kind so a Navi user id and a Stripe customer id never
collide. The same input always maps to the same key, so joins still work, but
the warehouse can't be joined back to people without the salt.
"""

import hashlib
import hmac


def pseudonymise(salt: bytes, namespace: str, value: object) -> str | None:
    if value is None or value == "":
        return None
    digest = hmac.new(salt, f"{namespace}:{value}".encode(), hashlib.sha256)
    return digest.hexdigest()[:32]
