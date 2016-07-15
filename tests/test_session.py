import pytest

from goblin import element


@pytest.mark.asyncio
async def test_session_close(session):
    assert not session.conn.closed
    await session.close()
    assert session.conn.closed


@pytest.mark.asyncio
async def test_session_ctxt_mngr(session):
    async with session:
        assert not session.conn.closed
    assert session.conn.closed


class TestCreationApi:

    @pytest.mark.asyncio
    async def test_create_vertex(self, session, person_class):
        async with session:
            jon = person_class()
            jon.name = 'jonathan'
            jon.age = 38
            leif = person_class()
            leif.name = 'leif'
            leif.age = 28
            session.add(jon, leif)
            assert not hasattr(jon, 'id')
            assert not hasattr(leif, 'id')
            await session.flush()
            assert hasattr(jon, 'id')
            assert session.current[jon.id] is jon
            assert jon.name == 'jonathan'
            assert hasattr(leif, 'id')
            assert session.current[leif.id] is leif
            assert leif.name == 'leif'

    @pytest.mark.asyncio
    async def test_create_edge(self, session, person_class, place_class,
                               lives_in_class):
        async with session:
            jon = person_class()
            jon.name = 'jonathan'
            jon.age = 38
            montreal = place_class()
            montreal.name = 'Montreal'
            lives_in = lives_in_class(jon, montreal)
            session.add(jon, montreal, lives_in)
            await session.flush()
            assert hasattr(lives_in, 'id')
            assert session.current[lives_in.id] is lives_in
            assert lives_in.source is jon
            assert lives_in.target is montreal
            assert lives_in.source.__label__ == 'person'
            assert lives_in.target.__label__ == 'place'

    @pytest.mark.asyncio
    async def test_create_edge_no_source(self, session, lives_in, person):
        async with session:
            lives_in.source = person
            with pytest.raises(Exception):
                await session.save(lives_in)

    @pytest.mark.asyncio
    async def test_create_edge_no_target(self, session, lives_in, place):
        async with session:
            lives_in.target = place
            with pytest.raises(Exception):
                await session.save(lives_in)

    @pytest.mark.asyncio
    async def test_create_edge_no_source_target(self, session, lives_in):
        async with session:
            with pytest.raises(Exception):
                await session.save(lives_in)

    @pytest.mark.asyncio
    async def test_get_vertex(self, session, person_class):
        async with session:
            jon = person_class()
            jon.name = 'jonathan'
            jon.age = 38
            await session.save(jon)
            jid = jon.id
            result = await session.get_vertex(jon)
            assert result.id == jid
            assert result is jon

    @pytest.mark.asyncio
    async def test_get_edge(self, session, person_class, place_class,
                            lives_in_class):
        async with session:
            jon = person_class()
            jon.name = 'jonathan'
            jon.age = 38
            montreal = place_class()
            montreal.name = 'Montreal'
            lives_in = lives_in_class(jon, montreal)
            session.add(jon, montreal, lives_in)
            await session.flush()
            lid = lives_in.id
            result = await session.get_edge(lives_in)
            assert result.id == lid
            assert result is lives_in

    @pytest.mark.asyncio
    async def test_get_vertex_doesnt_exist(self, session, person):
        async with session:
            person.id = 1000000000000000000000000000000000000000000000
            result = await session.get_vertex(person)
            assert not result

    @pytest.mark.asyncio
    async def test_get_edge_doesnt_exist(self, session, knows, person_class):
        async with session:
            jon = person_class()
            leif = person_class()
            works_with = knows
            works_with.source = jon
            works_with.target = leif
            works_with.id = 1000000000000000000000000000000000000000000000
            result = await session.get_edge(works_with)
            assert not result

    @pytest.mark.asyncio
    async def test_remove_vertex(self, session, person):
        async with session:
            person.name = 'dave'
            person.age = 35
            await session.save(person)
            result = await session.g.V(person.id).one_or_none()
            assert result is person
            rid = result.id
            await session.remove_vertex(person)
            result = await session.g.V(rid).one_or_none()
            assert not result

    @pytest.mark.asyncio
    async def test_remove_edge(self, session, person_class, place_class,
                               lives_in_class):
        async with session:
            jon = person_class()
            jon.name = 'jonathan'
            jon.age = 38
            montreal = place_class()
            montreal.name = 'Montreal'
            lives_in = lives_in_class(jon, montreal)
            session.add(jon, montreal, lives_in)
            await session.flush()
            result = await session.g.E(lives_in.id).one_or_none()
            assert result is lives_in
            rid = result.id
            await session.remove_edge(lives_in)
            result = await session.g.E(rid).one_or_none()
            assert not result

    @pytest.mark.asyncio
    async def test_update_vertex(self, session, person):
        async with session:
            person.name = 'dave'
            person.age = 35
            result = await session.save(person)
            assert result.age == 35
            person.name = 'david'
            person.age = None
            result = await session.save(person)
            assert result is person
            assert result.name == 'david'
            assert not result.age

    @pytest.mark.asyncio
    async def test_update_edge(self, session, person_class, knows):
        async with session:
            dave = person_class()
            leif = person_class()
            knows.source = dave
            knows.target = leif
            knows.notes = 'online'
            session.add(dave, leif)
            await session.flush()
            result = await session.save(knows)
            assert knows.notes == 'online'
            knows.notes = None
            result = await session.save(knows)
            assert result is knows
            assert not result.notes


class TestTraversalApi:

    @pytest.mark.asyncio
    async def test_traversal_source_generation(self, session, person_class,
                                               knows_class):
        async with session:
            traversal = session.traversal(person_class)
            assert repr(traversal) == 'g.V().hasLabel("person")'
            traversal = session.traversal(knows_class)
            assert repr(traversal) == 'g.E().hasLabel("knows")'


    @pytest.mark.asyncio
    async def test_all(self, session, person_class):
        async with session:
            dave = person_class()
            leif = person_class()
            jon = person_class()
            session.add(dave, leif, jon)
            await session.flush()
            resp = await session.traversal(person_class).all()
            results = []
            async for msg in resp:
                assert isinstance(msg, person_class)
                results.append(msg)
            assert len(results) > 2

    @pytest.mark.asyncio
    async def test_one_or_none_one(self, session, person_class):
        async with session:
            dave = person_class()
            leif = person_class()
            jon = person_class()
            session.add(dave, leif, jon)
            await session.flush()
            resp = await session.traversal(person_class).one_or_none()
            assert isinstance(resp, person_class)

    @pytest.mark.asyncio
    async def test_one_or_none_none(self, session):
        async with session:
            none = await session.g.V().hasLabel(
                'a very unlikey label').one_or_none()
            assert not none

    @pytest.mark.asyncio
    async def test_vertex_deserialization(self, session, person_class):
        async with session:
            resp = await session.g.addV('person').property(
                person_class.name, 'leif').property('birthplace', 'detroit').one_or_none()
            assert isinstance(resp, person_class)
            assert resp.name == 'leif'
            assert resp.birthplace == 'detroit'

    @pytest.mark.asyncio
    async def test_edge_desialization(self, session, knows_class):
        async with session:
            p1 = await session.g.addV('person').one_or_none()
            p2 = await session.g.addV('person').one_or_none()
            e1 = await session.g.V(p1.id).addE('knows').to(
                session.g.V(p2.id)).property(
                    knows_class.notes, 'somehow').property(
                    'how_long', 1).one_or_none()
            assert isinstance(e1, knows_class)
            assert e1.notes == 'somehow'
            assert e1.how_long == 1

    @pytest.mark.asyncio
    async def test_unregistered_vertex_deserialization(self, session):
        async with session:
            dave = await session.g.addV(
                'unregistered').property('name', 'dave').one_or_none()
            assert isinstance(dave, element.GenericVertex)
            assert dave.name == 'dave'
            assert dave.__label__ == 'unregistered'

    @pytest.mark.asyncio
    async def test_unregistered_edge_desialization(self, session):
        async with session:
            p1 = await session.g.addV('person').one_or_none()
            p2 = await session.g.addV('person').one_or_none()
            e1 = await session.g.V(p1.id).addE('unregistered').to(
                session.g.V(p2.id)).property('how_long', 1).one_or_none()
            assert isinstance(e1, element.GenericEdge)
            assert e1.how_long == 1
            assert e1.__label__ == 'unregistered'
