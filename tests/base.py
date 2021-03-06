#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import configparser
import json
import os
import requests
import sys
import unittest

from datetime import datetime

if '..' not in sys.path:
    sys.path.insert(0, '..')

from grimoire_elk.elk import load_identities
from grimoire_elk.utils import get_connectors, get_elastic


CONFIG_FILE = 'tests.conf'
DB_SORTINGHAT = "test_sh"
DB_PROJECTS = "test_projects"


def data2es(items, ocean):
    def ocean_item(item):
        # Hack until we decide the final id to use
        if 'uuid' in item:
            item['ocean-unique-id'] = item['uuid']
        else:
            # twitter comes from logstash and uses id
            item['uuid'] = item['id']
            item['ocean-unique-id'] = item['id']

        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        if 'timestamp' in item:
            ts = datetime.fromtimestamp(item['timestamp'])
            item['metadata__timestamp'] = ts.isoformat()

        # the _fix_item does not apply to the test data for Twitter
        try:
            ocean._fix_item(item)
        except KeyError:
            pass

        return item

    items_pack = []  # to feed item in packs

    for item in items:
        item = ocean_item(item)
        if len(items_pack) >= ocean.elastic.max_items_bulk:
            ocean._items_to_es(items_pack)
            items_pack = []
        items_pack.append(item)
    inserted = ocean._items_to_es(items_pack)

    return inserted


def refresh_identities(enrich_backend):
    total = 0

    for eitem in enrich_backend.fetch():
        roles = None
        try:
            roles = enrich_backend.roles
        except AttributeError:
            pass
        new_identities = enrich_backend.get_item_sh_from_id(eitem, roles)
        eitem.update(new_identities)
        total += 1

    return total


def refresh_projects(enrich_backend):
    total = 0

    for eitem in enrich_backend.fetch():
        new_project = enrich_backend.get_item_project(eitem)
        eitem.update(new_project)
        total += 1

    return total


class TestBaseBackend(unittest.TestCase):
    """Functional tests for GrimoireELK Backends"""

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_con = dict(cls.config.items('ElasticSearch'))['url']
        cls.connectors = get_connectors()

        # Sorting hat settings
        cls.db_user = ''
        cls.db_password = ''
        if 'Database' in cls.config:
            if 'user' in cls.config['Database']:
                cls.db_user = cls.config['Database']['user']
            if 'password' in cls.config['Database']:
                cls.db_password = cls.config['Database']['password']

    def setUp(self):
        with open(os.path.join("data", self.connector + ".json")) as f:
            self.items = json.load(f)

    def tearDown(self):
        delete_raw = self.es_con + "/" + self.ocean_index
        requests.delete(delete_raw)

        delete_enrich = self.es_con + "/" + self.enrich_index
        requests.delete(delete_enrich)

    def _test_items_to_raw(self):
        """Test whether fetched items are properly loaded to ES"""

        clean = True
        perceval_backend = None
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)

        raw_items = data2es(self.items, ocean_backend)

        return {'items': len(self.items), 'raw': raw_items}

    def _test_raw_to_enrich(self, sortinghat=False, projects=False):
        """Test whether raw indexes are properly enriched"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, ocean_backend)

        # populate enriched index
        if not sortinghat and not projects:
            enrich_backend = self.connectors[self.connector][2]()
        elif sortinghat and not projects:
            enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                                db_user=self.db_user,
                                                                db_password=self.db_password)
        elif not sortinghat and projects:
            enrich_backend = self.connectors[self.connector][2](db_projects_map=DB_PROJECTS,
                                                                db_user=self.db_user,
                                                                db_password=self.db_password)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)

        # Load SH identities
        if sortinghat:
            load_identities(ocean_backend, enrich_backend)

        raw_count = len([item for item in ocean_backend.fetch()])
        enrich_count = enrich_backend.enrich_items(ocean_backend)

        return {'raw': raw_count, 'enrich': enrich_count}

    def _test_refresh_identities(self):
        """Test refresh identities"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, ocean_backend)

        # populate enriched index
        enrich_backend = self.connectors[self.connector][2]()
        load_identities(ocean_backend, enrich_backend)
        enrich_backend = self.connectors[self.connector][2](db_sortinghat=DB_SORTINGHAT,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)
        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        enrich_backend.enrich_items(ocean_backend)

        total = refresh_identities(enrich_backend)
        return total

    def _test_refresh_project(self):
        """Test refresh project field"""

        # populate raw index
        perceval_backend = None
        clean = True
        ocean_backend = self.connectors[self.connector][1](perceval_backend)
        elastic_ocean = get_elastic(self.es_con, self.ocean_index, clean, ocean_backend)
        ocean_backend.set_elastic(elastic_ocean)
        data2es(self.items, ocean_backend)

        # populate enriched index
        enrich_backend = self.connectors[self.connector][2](db_projects_map=DB_PROJECTS,
                                                            db_user=self.db_user,
                                                            db_password=self.db_password)

        elastic_enrich = get_elastic(self.es_con, self.enrich_index, clean, enrich_backend)
        enrich_backend.set_elastic(elastic_enrich)
        enrich_backend.enrich_items(ocean_backend)

        total = refresh_projects(enrich_backend)
        return total
