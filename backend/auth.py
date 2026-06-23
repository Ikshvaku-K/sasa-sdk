"""
Optional admin authentication for the management API. (Fixes audit H-1.)

Behaviour:
  • If config.ADMIN_SECRET is empty (the default for local dev), the gate is a
    no-op and the management API stays open — convenient for development.
  • If ADMIN_SECRET is set (production), every /api/* management route requires
    `Authorization: Bearer <ADMIN_SECRET>`; otherwise it returns 401.

⚠️  You MUST set ADMIN_SECRET in any deployment reachable by untrusted clients,
    otherwise project listing (which exposes api_keys) is unauthenticated.
"""
import hmac
from fastapi import Header, HTTPException

import config


def require_admin(authorization: str = Header(default="")):
    """FastAPI dependency: enforce the admin bearer token when configured."""
    secret = config.ADMIN_SECRET
    if not secret:
        # Dev mode — auth disabled. (Documented; do not use in production.)
        return

    expected = f"Bearer {secret}"
    # constant-time comparison to avoid timing side-channels
    if not hmac.compare_digest(authorization, expected):
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized",
                    "message": "Valid admin bearer token required for /api/* routes."},
            headers={"WWW-Authenticate": "Bearer"},
        )
