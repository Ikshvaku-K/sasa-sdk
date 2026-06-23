"""
Pytest session fixtures.

Creates a dedicated test project at session start so test-suite traffic
does not share the demo project's rate-limit quota.
"""
import pytest
import httpx

BASE = "http://localhost:8001"


@pytest.fixture(scope="session", autouse=True)
def test_project(request):
    """
    Create a fresh project for this test session.
    Exposed as a session-scoped fixture; individual tests can also
    import TEST_KEY / TEST_PROJECT from this module directly.
    """
    r = httpx.post(f"{BASE}/api/projects", json={"name": "pytest-session", "color": "#888"})
    proj = r.json()
    # Stash on the request config so test modules can read it
    request.config._test_project = proj
    return proj


@pytest.fixture(scope="session")
def test_key(test_project):
    return test_project["api_key"]


@pytest.fixture(scope="session")
def test_pid(test_project):
    return test_project["id"]
