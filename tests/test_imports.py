# Test imports from the package to check for missing dependencies
from pathlib import Path


def test_imports():
    from oii import annotation
    from oii import habcam
    from oii import ifcb
    from oii import ifcb2
    from oii import image
    from oii import rbac
    from oii import seabed
    from oii import webapi
    from oii import workflow

    assert True
