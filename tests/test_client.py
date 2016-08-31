# Copyright 2016 ZEROFAIL
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import pytest


@pytest.mark.asyncio
async def test_client_auto_release(cluster):
    client = await cluster.connect()
    resp = await client.submit(gremlin="1 + 1")
    async for msg in resp:
        pass
    await asyncio.sleep(0)
    host = cluster._hosts.popleft()
    assert len(host._pool._available) == 1
    await host.close()
