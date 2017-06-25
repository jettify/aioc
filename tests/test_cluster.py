import asyncio
import pytest
from aioc import Cluster, Config
from aioc import state as s


@pytest.mark.run_loop
async def test_basic():
    config1 = Config(host='127.0.0.1', port=9900)
    config2 = Config(host='127.0.0.1', port=9901)
    config3 = Config(host='127.0.0.1', port=9902)
    cluster1 = Cluster(config1)
    cluster2 = Cluster(config2)
    cluster3 = Cluster(config3)

    await cluster1.boot()
    await cluster2.boot()
    await cluster3.boot()

    await cluster2.join(s.Node(host='127.0.0.1', port=9900))
    await cluster3.join(s.Node(host='127.0.0.1', port=9900))

    await asyncio.sleep(2)
    print(cluster1._active_view)
    print(cluster2._active_view)
    print(cluster3._active_view)

    await cluster1.close()
    await asyncio.sleep(2)
    print("-" * 100)

    print(cluster1._active_view)
    print(cluster2._active_view)
    print(cluster3._active_view)
    await cluster2.close()
    await cluster3.close()
