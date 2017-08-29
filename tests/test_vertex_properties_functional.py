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

import pytest
from aiogremlin import Graph
from aiogremlin.gremlin_python.process.traversal import Cardinality


@pytest.mark.asyncio
async def test_add_update_property(app, person):
    session = await app.session()
    person.birthplace = 'Iowa City'
    result = await session.save(person)
    assert result.birthplace.value == 'Iowa City'
    person.birthplace = 'unknown'
    result = await session.save(person)
    assert result.birthplace.value == 'unknown'
    person.birthplace = None
    result = await session.save(person)
    assert not result.birthplace
    await app.close()


@pytest.mark.xfail(pytest.config.getoption('provider') == 'dse', reason='temporary')
@pytest.mark.asyncio
async def test_add_update_list_card_property(app, person):
    session = await app.session()
    person.nicknames = ['db', 'dirtydb']
    result = await session.save(person)
    assert [v.value for v in result.nicknames] == ['db', 'dirtydb']
    person.nicknames.append('davebshow')
    result = await session.save(person)
    assert [v.value for v in result.nicknames] == [
        'db', 'dirtydb', 'davebshow']
    person.nicknames = []
    result = await session.save(person)
    assert not result.nicknames
    person.nicknames = ['none']
    result = await session.save(person)
    assert result.nicknames('none').value == 'none'
    person.nicknames = None
    result = await session.save(person)
    assert not result.nicknames
    await app.close()


@pytest.mark.skipif(pytest.config.getoption('provider') == 'dse', reason='set cardinality unsupported')
@pytest.mark.asyncio
async def test_add_update_set_card_property(app, place):
    session = await app.session()
    place.important_numbers = set([1, 2])
    result = await session.save(place)
    assert {v.value for v in result.important_numbers} == {1, 2}
    place.important_numbers = set([3, 4])
    result = await session.save(place)
    assert {v.value for v in result.important_numbers} == {3, 4}
    place.important_numbers.add(5)
    result = await session.save(place)
    assert {v.value for v in result.important_numbers} == {3, 4, 5}
    place.important_numbers = set()
    result = await session.save(place)
    assert not result.important_numbers
    place.important_numbers = set([1, 2])
    result = await session.save(place)
    assert place.important_numbers(1).value == 1
    place.important_numbers = None
    result = await session.save(place)
    assert not result.important_numbers
    await app.close()


@pytest.mark.asyncio
async def test_metas(app, place, remote_connection):
    g = Graph().traversal().withRemote(remote_connection)
    # Property API
    v = await g.addV('person').property('name', 'dave').next()
    props = await g.V(v.id).properties().toList()
    meta = await g.V(v.id).properties('name').property('nickname', 'davebshow').next()
    nickname = await g.V(v.id).properties('name').valueMap(True).next()
    # List card
    v2 = await g.addV('person').property(Cardinality.list_, 'name', 'dave').property(Cardinality.list_, 'name', 'dave brown').next()
    props2 = await g.V(v2.id).properties().toList()
    meta2 = await g.V(v2.id).properties('name').hasValue('dave').property('nickname', 'davebshow').next()
    nickname2 = await g.V(v2.id).properties('name').valueMap(True).next()

    session = await app.session()
    place.historical_name = ['Detroit']
    place.historical_name('Detroit').notes = 'rock city'
    place.historical_name('Detroit').year = 1900
    detroit = await session.save(place)
    dprops = await g.V(detroit.id).properties().toList()
    trav = g.V(detroit.id).properties('historical_name').valueMap(True)
    dmetas = await trav.next()

    new_session = await app.session()
    new_detroit = await new_session.g.V(detroit.id).next()

    await remote_connection.close()
    await app.close()


@pytest.mark.xfail(pytest.config.getoption('provider') == 'dse', reason='temporary')
@pytest.mark.asyncio
async def test_add_update_metas(app, place):
    session = await app.session()
    place.historical_name = ['Detroit']
    place.historical_name('Detroit').notes = 'rock city'
    place.historical_name('Detroit').year = 1900
    result = await session.save(place)
    assert result.historical_name('Detroit').notes == 'rock city'
    assert result.historical_name('Detroit').year == 1900

    place.historical_name('Detroit').notes = 'comeback city'
    place.historical_name('Detroit').year = 2016
    result = await session.save(place)
    assert result.historical_name('Detroit').notes == 'comeback city'
    assert result.historical_name('Detroit').year == 2016

    new_session = await app.session()
    result = await new_session.g.V(place.id).next()
    assert result.historical_name('Detroit').notes == 'comeback city'
    assert result.historical_name('Detroit').year == 2016

    place.historical_name('Detroit').notes = None
    place.historical_name('Detroit').year = None
    result = await session.save(place)
    assert not result.historical_name('Detroit').notes
    assert not result.historical_name('Detroit').year
    await app.close()


@pytest.mark.xfail(pytest.config.getoption('provider') == 'dse', reason='temporary')
@pytest.mark.asyncio
async def test_add_update_metas_list_card(app, place):
    session = await app.session()
    place.historical_name = ['Hispania', 'Al-Andalus']
    place.historical_name('Hispania').notes = 'romans'
    place.historical_name('Hispania').year = 200
    place.historical_name('Al-Andalus').notes = 'muslims'
    place.historical_name('Al-Andalus').year = 700
    result = await session.save(place)
    assert result.historical_name('Hispania').notes == 'romans'
    assert result.historical_name('Hispania').year == 200
    assert result.historical_name('Al-Andalus').notes == 'muslims'
    assert result.historical_name('Al-Andalus').year == 700

    place.historical_name('Hispania').notes = 'really old'
    place.historical_name('Hispania').year = 200
    place.historical_name('Al-Andalus').notes = 'less old'
    place.historical_name('Al-Andalus').year = 700
    result = await session.save(place)
    assert result.historical_name('Hispania').notes == 'really old'
    assert result.historical_name('Hispania').year == 200
    assert result.historical_name('Al-Andalus').notes == 'less old'
    assert result.historical_name('Al-Andalus').year == 700

    place.historical_name('Hispania').notes = None
    place.historical_name('Hispania').year = None
    place.historical_name('Al-Andalus').notes = None
    place.historical_name('Al-Andalus').year = None
    result = await session.save(place)
    assert not result.historical_name('Hispania').notes
    assert not result.historical_name('Hispania').year
    assert not result.historical_name('Al-Andalus').notes
    assert not result.historical_name('Al-Andalus').year
    await app.close()
