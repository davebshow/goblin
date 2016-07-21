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
async def test_add_update_list_card_property(session, place):
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
