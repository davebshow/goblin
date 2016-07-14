import pytest


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

    def test_update_vertex(self):
        pass

    def test_update_edge(self):
        pass


class TestTraversalApi:

    def test_all(self):
        pass

    def test_one_or_none_one(self):
        pass

    def test_one_or_none_none(self):
        pass

    def test_vertex_deserialization(self):
        pass

    def test_edge_desialization(self):
        pass

    def test_unregistered_vertex_deserialization(self):
        pass

    def test_unregistered_edge_desialization(self):
        pass
