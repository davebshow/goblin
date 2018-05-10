from goblin import element
from goblin.fileio.graphson import dump, dumps, AdjList


# def test_dump_simple_vertex(person):
#     person.id = 1
#     person.name = 'dave'
#     person.age = 37
#     person.birthplace = 'Iowa City'
#     # import ipdb; ipdb.set_trace()
#     person.nicknames = ['davebshow', 'crustee']
#     person.location = 'London, ON'
#     person.location('London, ON').year = 2010
#     dumped = dumps(person)
#     print(dumped)


def test_dumps(person_class, knows_class):
    person = person_class()
    person.id = 1
    person.name = 'dave'
    person.age = 37
    person.birthplace = 'Iowa City'
    # import ipdb; ipdb.set_trace()
    person.nicknames = ['davebshow', 'crustee']
    person.location = 'London, ON'
    person.location('London, ON').year = 2010

    person2 = person_class()
    person2.id = 2
    person2.name = 'itziri'
    person2.age = 37
    person2.birthplace = 'London'
    # import ipdb; ipdb.set_trace()
    person2.nicknames = ['chong', 'itsilly']
    person2.location = 'Tacoma'
    person2.location('Tacoma').year = 2018

    knows = knows_class()
    knows.source = person
    knows.target = person2
    knows.notes = "married"
    knows.id = 3

    al1 = AdjList(vertex=person, inE=[], outE=[knows])
    al2 = AdjList(vertex=person2, inE=[knows], outE=[])

    print(dumps(al1))
    print(dumps(al2))
    dump('/home/davebshow/test_graph.json', al1, al2)