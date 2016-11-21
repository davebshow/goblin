# Copyright 2016 David M. Brown
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
async def test_pool_init(connection_pool):
    await connection_pool.init_pool()
    assert len(connection_pool._available) == 1
    await connection_pool.close()


@pytest.mark.asyncio
async def test_acquire_release(connection_pool):
    conn = await connection_pool.acquire()
    assert not len(connection_pool._available)
    assert len(connection_pool._acquired) == 1
    assert conn.times_acquired == 1
    connection_pool.release(conn)
    assert len(connection_pool._available) == 1
    assert not len(connection_pool._acquired)
    assert not conn.times_acquired
    await connection_pool.close()


@pytest.mark.asyncio
async def test_acquire_multiple(connection_pool):
    conn1 = await connection_pool.acquire()
    conn2 = await connection_pool.acquire()
    assert not conn1 is conn2
    assert len(connection_pool._acquired) == 2
    await connection_pool.close()


@pytest.mark.asyncio
async def test_share(connection_pool):
    connection_pool._max_conns = 1
    conn1 = await connection_pool.acquire()
    conn2 = await connection_pool.acquire()
    assert conn1 is conn2
    assert conn1.times_acquired == 2
    await connection_pool.close()


@pytest.mark.asyncio
async def test_acquire_multiple_and_share(connection_pool):
    connection_pool._max_conns = 2
    connection_pool._max_times_acquired = 2
    conn1 = await connection_pool.acquire()
    conn2 = await connection_pool.acquire()
    assert not conn1 is conn2
    conn3 = await connection_pool.acquire()
    conn4 = await connection_pool.acquire()
    assert not conn3 is conn4
    assert conn3 is conn1
    assert conn4 is conn2
    await connection_pool.close()


@pytest.mark.asyncio
async def test_max_acquired(connection_pool):
    connection_pool._max_conns = 2
    connection_pool._max_times_acquired = 2
    conn1 = await connection_pool.acquire()
    conn2 = await connection_pool.acquire()
    conn3 = await connection_pool.acquire()
    conn4 = await connection_pool.acquire()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(connection_pool.acquire(), timeout=0.1)
    await connection_pool.close()


@pytest.mark.asyncio
async def test_release_notify(connection_pool):
    connection_pool._max_conns = 2
    connection_pool._max_times_acquired = 2
    conn1 = await connection_pool.acquire()
    conn2 = await connection_pool.acquire()
    conn3 = await connection_pool.acquire()
    conn4 = await connection_pool.acquire()

    async def release(conn):
        conn.release()

    results = await asyncio.gather(
        *[connection_pool.acquire(), release(conn4)])
    conn4 = results[0]
    assert conn4 is conn2
    await connection_pool.close()
