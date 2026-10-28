"""Runtime configuration, read from the environment (``CRUMB_*``) and ``.env``."""

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices
from pydantic import Field
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class ConfigError(RuntimeError):
    """A required setting is missing or invalid. Never retried."""


class Settings(BaseSettings):
    # Every setting reads CRUMB_<NAME>. The deployment manifests use a few
    # unprefixed names (WAREHOUSE_DSN, NAVI_SOURCE_DSN, TEMPORAL_ADDRESS,
    # TEMPORAL_NAMESPACE, PII_SALT, STRIPE_API_KEY); those are accepted too.
    model_config = SettingsConfigDict(
        env_prefix="CRUMB_", env_file=".env", extra="ignore", populate_by_name=True
    )

    # Warehouse crumb owns (raw / staging / marts / meta schemas).
    warehouse_dsn: SecretStr = Field(
        default=SecretStr("postgresql://crumb:crumb@localhost:5440/crumb"),
        validation_alias=AliasChoices("CRUMB_WAREHOUSE_DSN", "WAREHOUSE_DSN"),
    )
    # Navi's Postgres, read-only. In prod this is a read replica.
    navi_dsn: SecretStr = Field(
        default=SecretStr("postgresql://crumb_reader:crumb_reader@localhost:5441/navi"),
        validation_alias=AliasChoices("CRUMB_NAVI_DSN", "NAVI_SOURCE_DSN"),
    )
    # Owner DSN for the synthetic navi_source DB; only `crumb seed` uses it.
    navi_seed_dsn: SecretStr = SecretStr("postgresql://navi:navi@localhost:5441/navi")

    temporal_address: str = Field(
        default="localhost:7233",
        validation_alias=AliasChoices("CRUMB_TEMPORAL_ADDRESS", "TEMPORAL_ADDRESS"),
    )
    temporal_namespace: str = Field(
        default="default",
        validation_alias=AliasChoices("CRUMB_TEMPORAL_NAMESPACE", "TEMPORAL_NAMESPACE"),
    )
    temporal_ui_url: str = "http://localhost:8233"
    task_queue: str = "crumb"
    create_schedules: bool = True

    api_key: SecretStr | None = None
    pii_salt: SecretStr | None = Field(
        default=None, validation_alias=AliasChoices("CRUMB_PII_SALT", "PII_SALT")
    )
    stripe_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CRUMB_STRIPE_API_KEY", "STRIPE_API_KEY", "STRIPE_SECRET_KEY"
        ),
    )

    # Deployment environment (local / staging / production). Picks the default
    # reconciliation severity, and scopes Stripe objects tagged with
    # metadata.environment to this one.
    environment: str = Field(
        default="local", validation_alias=AliasChoices("CRUMB_ENVIRONMENT", "ENVIRONMENT")
    )
    # Stripe<->Navi mismatches fail the data-quality run ("fail") or are only
    # recorded ("warn"). Unset: "fail" in production or with a live Stripe key,
    # otherwise "warn" (test Stripe accounts are shared across environments).
    reconciliation_severity: Literal["warn", "fail"] | None = None

    extract_page_size: int = 1000
    # Navi changes order/payment status with queryset .update(), which doesn't
    # bump updated_at, so recently created rows are re-read every run as well.
    navi_recent_refresh_hours: int = 72
    # Re-read this far behind the stored watermark so rows committed late (long
    # transactions, replica lag) are still picked up. Upserts make it safe.
    navi_lookback_seconds: int = 300
    # Stripe objects change after creation (charges get refunded), and we page
    # by `created`, so look further back.
    stripe_lookback_seconds: int = 3 * 24 * 3600
    freshness_max_age_hours: float = 3.0
    # Timezone used to bucket timestamps into reporting days.
    reporting_tz: str = "UTC"

    def effective_reconciliation_severity(self) -> Literal["warn", "fail"]:
        if self.reconciliation_severity is not None:
            return self.reconciliation_severity
        live_key = self.stripe_api_key is not None and self.stripe_api_key.get_secret_value()[
            :8
        ] in ("sk_live_", "rk_live_")
        return "fail" if self.environment == "production" or live_key else "warn"

    def require_pii_salt(self) -> bytes:
        if self.pii_salt is None or len(self.pii_salt.get_secret_value()) < 16:
            msg = "CRUMB_PII_SALT / PII_SALT must be set (>= 16 chars) to pseudonymise users"
            raise ConfigError(msg)
        return self.pii_salt.get_secret_value().encode()


@lru_cache
def get_settings() -> Settings:
    return Settings()
