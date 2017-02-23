from aioc.suspicion import Suspicion


def test_suspicion_ctor():
    s = Suspicion(3, 1, 2)
    s.confirm("127.0.0.1:8080")
    s.confirm("127.0.0.1:8081")
    s.confirm("127.0.0.1:8081")
    assert s.check_timeout()


def test_suspicion_timeout():
    s = Suspicion(3, 1, 1)
    s.confirm("127.0.0.1:8080")
    s.confirm("127.0.0.1:8081")
    s.confirm("127.0.0.1:8081")
    assert not s.check_timeout()
