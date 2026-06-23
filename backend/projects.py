"""
Project + API key registry.
Stored in-memory for simplicity; swap for a DB in production.
"""
import secrets
import time
from dataclasses import dataclass, field
from config import DEMO_API_KEY

@dataclass
class Project:
    id: str
    name: str
    api_key: str
    created_at: float = field(default_factory=time.time)
    color: str = "#4f8ef7"

_PROJECTS: dict[str, Project] = {}
_KEY_TO_PROJECT: dict[str, str] = {}

def _seed():
    """Pre-create a demo project so the dashboard works out of the box."""
    p = Project(id="demo", name="Demo App", api_key=DEMO_API_KEY, color="#34d399")
    _PROJECTS[p.id] = p
    _KEY_TO_PROJECT[p.api_key] = p.id

_seed()


def create_project(name: str, color: str = "#4f8ef7") -> Project:
    pid = name.lower().replace(" ", "-")[:32]
    key = "sf_" + secrets.token_urlsafe(16)
    p   = Project(id=pid, name=name, api_key=key, color=color)
    _PROJECTS[pid]      = p
    _KEY_TO_PROJECT[key] = pid
    return p


def get_project_by_key(api_key: str) -> Project | None:
    pid = _KEY_TO_PROJECT.get(api_key)
    return _PROJECTS.get(pid) if pid else None


def get_project(pid: str) -> Project | None:
    return _PROJECTS.get(pid)


def list_projects() -> list[Project]:
    return list(_PROJECTS.values())


def validate_key(api_key: str) -> str | None:
    """Return project_id if valid, else None."""
    return _KEY_TO_PROJECT.get(api_key)
