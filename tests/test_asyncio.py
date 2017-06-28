# -*- coding: utf-8 -*-

from __future__ import absolute_import

from os import path

import thriftpy
from thriftpy.contrib.async import make_client, make_server

import pytest
import asyncio

pytestmark = pytest.mark.asyncio

addressbook = thriftpy.load(path.join(path.dirname(__file__),
                                      "addressbook.thrift"))


class Dispatcher(object):
    def __init__(self):
        self.registry = {}

    @asyncio.coroutine
    def add(self, person):
        """
        bool add(1: Person person);
        """
        if person.name in self.registry:
            return False
        self.registry[person.name] = person
        return True

    @asyncio.coroutine
    def get(self, name):
        """
        Person get(1: string name) throws (1: PersonNotExistsError not_exists);
        """
        if name not in self.registry:
            raise addressbook.PersonNotExistsError(
                'Person "{0}" does not exist!'.format(name))
        return self.registry[name]

    @asyncio.coroutine
    def remove(self, name):
        """
        bool remove(1: string name) throws (1: PersonNotExistsError not_exists)
        """
        # delay action for later
        yield from asyncio.sleep(.1)
        if name not in self.registry:
            raise addressbook.PersonNotExistsError(
                'Person "{0}" does not exist!'.format(name))
        del self.registry[name]
        return True


@pytest.fixture
async def client(request):
    server = await make_server(addressbook.AddressBookService, Dispatcher())
    client = await make_client(addressbook.AddressBookService)

    def teardown():
        client.close()
        server.close()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(server.wait_closed())
    request.addfinalizer(teardown)

    return client


async def test_async_result(client):
    dennis = addressbook.Person(name='Dennis Ritchie')
    success = await client.add(dennis)
    assert success
    success = await client.add(dennis)
    assert not success
    person = await client.get(dennis.name)
    assert person.name == dennis.name


async def test_async_exception(client):
    exc = None
    try:
        await client.get('Brian Kernighan')
    except Exception as e:
        exc = e

    assert isinstance(exc, addressbook.PersonNotExistsError)
