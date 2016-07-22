import pytest


@pytest.mark.asyncio
async def test_add_update_property(session, person):
    async with session:
        person.birthplace = 'Iowa City'
        result = await session.save(person)
        assert result.birthplace.value == 'Iowa City'
        person.birthplace = 'unknown'
        result = await session.save(person)
        assert result.birthplace.value == 'unknown'
        person.birthplace = None
        result = await session.save(person)
        assert not result.birthplace


@pytest.mark.asyncio
async def test_add_update_list_card_property(session, person):
    async with session:
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


@pytest.mark.asyncio
async def test_add_update_set_card_property(session, place):
    async with session:
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


@pytest.mark.asyncio
async def test_add_update_metas(session, place):
    async with session:
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

        place.historical_name('Detroit').notes = None
        place.historical_name('Detroit').year = None
        result = await session.save(place)
        assert not result.historical_name('Detroit').notes
        assert not result.historical_name('Detroit').year




@pytest.mark.asyncio
async def test_add_update_metas_list_card(session, place):
    async with session:
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
