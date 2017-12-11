import pytest
from aiogremlin import Graph
from gremlin_python.process.traversal import Cardinality


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


@pytest.mark.xfail(
    pytest.config.getoption('provider') == 'dse', reason='temporary')
@pytest.mark.asyncio
async def test_add_update_list_card_property(app, person):
    session = await app.session()
    person.nicknames = ['db', 'dirtydb']
    result = await session.save(person)
    assert [v.value for v in result.nicknames] == ['db', 'dirtydb']
    person.nicknames.append('davebshow')
    result = await session.save(person)
    assert [v.value
            for v in result.nicknames] == ['db', 'dirtydb', 'davebshow']
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


@pytest.mark.skipif(
    pytest.config.getoption('provider') == 'dse',
    reason='set cardinality unsupported')
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
    session = await app.session()
    place.zipcode = 98402
    place.historical_name = ['Detroit']
    place.historical_name('Detroit').notes = 'rock city'
    place.historical_name('Detroit').year = 1900
    place.historical_name.append('Other')
    place.historical_name[-1].notes = 'unknown'
    place.historical_name[-1].year = 1700
    detroit = await session.save(place)

    dprops = await g.V(detroit.id).properties().toList()
    assert len(dprops) == 4
    trav = g.V(detroit.id).properties('historical_name').valueMap(True)
    dmetas = await trav.toList()
    assert dmetas[0]['value'] == 'Detroit'
    assert dmetas[0]['notes'] == 'rock city'
    assert dmetas[0]['year'] == 1900
    assert dmetas[1]['value'] == 'Other'
    assert dmetas[1]['notes'] == 'unknown'
    assert dmetas[1]['year'] == 1700
    new_session = await app.session()
    new_detroit = await new_session.g.V(detroit.id).next()
    assert new_detroit.zipcode == detroit.zipcode
    assert new_detroit.historical_name[-1].value == detroit.historical_name[
        -1].value
    assert new_detroit.historical_name[-1].notes == detroit.historical_name[
        -1].notes
    assert new_detroit.historical_name[-1].year == detroit.historical_name[
        -1].year
    assert new_detroit.historical_name[0].value == detroit.historical_name[
        0].value
    assert new_detroit.historical_name[0].notes == detroit.historical_name[
        0].notes
    assert new_detroit.historical_name[0].year == detroit.historical_name[
        0].year
    await remote_connection.close()
    await app.close()


@pytest.mark.xfail(
    pytest.config.getoption('provider') == 'dse', reason='temporary')
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


@pytest.mark.xfail(
    pytest.config.getoption('provider') == 'dse', reason='temporary')
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
