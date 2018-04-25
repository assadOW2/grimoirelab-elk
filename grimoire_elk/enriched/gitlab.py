#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Gitlab to Elastic class helper
#
# Copyright (C) 2015 Bitergia
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
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Assad Montasser <assad.montasser@ow2.org>
#

import logging

from datetime import datetime

from .utils import get_time_diff_days

from .enrich import Enrich, metadata
from ..elastic_mapping import Mapping as BaseMapping

logger = logging.getLogger(__name__)


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        if es_major != '2':
            mapping = """
            {
                "properties": {
                   "title_analyzed": {
                     "type": "text"
                   }
                }
            }
            """
        else:
            mapping = """
            {
                "properties": {
                   "title_analyzed": {
                      "type": "string",
                      "index": "analyzed"
                   }
                }
            }
            """

        return {"items": mapping}


class GitLabEnrich(Enrich):

    mapping = Mapping

    roles = ['assignee_data', 'user_data']

    def __init__(self, db_sortinghat=None,
                 db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)

        self.studies = []
        self.studies.append(self.enrich_onion)

        self.users = {}  # cache users
        self.location = {}  # cache users location
        self.location_not_found = []  # location not found in map api

    def set_elastic(self, elastic):
        self.elastic = elastic
        # Recover cache data from Elastic

    def get_field_author(self):
        return "user_data"

    def get_field_date(self):
        """ Field with the date in the JSON enriched items """
        return "grimoire_creation_date"

    def get_fields_uuid(self):
        return ["assignee_uuid", "user_uuid"]

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        item = item['data']

        for identity in ['user', 'assignee']:
            if item[identity]:
                # In user_data we have the full user data
                user = self.get_sh_identity(item[identity + "_data"])
                if user:
                    identities.append(user)
        return identities

    def get_sh_identity(self, item, identity_field=None):
        identity = {}

        user = item  # by default a specific user dict is expected
        if 'data' in item and type(item) == dict:
            user = item['data'][identity_field]

        if not user:
            return identity

        identity['username'] = user['login']
        identity['email'] = None
        identity['name'] = None
        if 'email' in user:
            identity['email'] = user['email']
        if 'name' in user:
            identity['name'] = user['name']
        return identity

    def get_gitlab_cache(self, kind, key_):
        """ Get cache data for items of _type using key_ as
        the cache dict key """

        cache = {}
        res_size = 100  # best size?
        from_ = 0

        index_gitlab = "gitlab/" + kind

        url = self.elastic.url + "/" + index_gitlab
        url += "/_search" + "?" + "size=%i" % res_size
        r = self.requests.get(url)
        type_items = r.json()

        if 'hits' not in type_items:
            logger.info("No gitlab %s data in ES" % (kind))

        else:
            while len(type_items['hits']['hits']) > 0:
                for hit in type_items['hits']['hits']:
                    item = hit['_source']
                    cache[item[key_]] = item
                from_ += res_size
                r = self.requests.get(url + "&from=%i" % from_)
                type_items = r.json()
                if 'hits' not in type_items:
                    break

        return cache

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        return repo

    @metadata
    def get_rich_item(self, item):
        rich_issue = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                rich_issue[f] = item[f]
            else:
                rich_issue[f] = None
        # The real data
        issue = item['data']

        rich_issue['time_to_close_days'] = \
            get_time_diff_days(issue['created_at'], issue['closed_at'])

        if issue['state'] != 'closed':
            rich_issue['time_open_days'] = \
                get_time_diff_days(issue['created_at'], datetime.utcnow())
        else:
            rich_issue['time_open_days'] = rich_issue['time_to_close_days']

        rich_issue['user_login'] = issue['author']['username']
        user = issue['author']

        if user is not None and user:
            rich_issue['user_name'] = user['username']
            rich_issue['author_name'] = user['name']
            rich_issue['user_web_url'] = user['web_url']
        else:
            rich_issue['user_name'] = None
            rich_issue['author_name'] = None
            rich_issue['user_web_url'] = None

        assignee = None

        if issue['assignee'] is not None:
            assignee = issue['assignee']
            rich_issue['assignee_login'] = assignee["username"]
            rich_issue['assignee_name'] = assignee['name']
            rich_issue['assignee_web_url'] = assignee['web_url']

        else:
            rich_issue['assignee_name'] = None
            rich_issue['assignee_login'] = None
            rich_issue['assignee_web_url'] = None

        rich_issue['id'] = issue['id']
        rich_issue['id_in_repo'] = issue['web_url'].split("/")[-1]
        rich_issue['repository'] = issue['web_url'].rsplit("/", 2)[0]
        rich_issue['title'] = issue['title']
        rich_issue['title_analyzed'] = issue['title']
        rich_issue['state'] = issue['state']
        rich_issue['created_at'] = issue['created_at']
        rich_issue['updated_at'] = issue['updated_at']
        rich_issue['closed_at'] = issue['closed_at']
        rich_issue['url'] = issue['web_url']
        labels = ''
        i = 0
        if 'labels' in issue:
            for label in issue['labels']:
                labels += label[i] + ";;"
                i += 1
        if labels != '':
            labels[:-2]
        rich_issue['labels'] = labels

        rich_issue['pull_request'] = True
        rich_issue['item_type'] = 'pull request'
        if 'head' not in issue.keys() and 'pull_request' not in issue.keys():
            rich_issue['pull_request'] = False
            rich_issue['item_type'] = 'issue'

        rich_issue['gitlab_repo'] = \
            rich_issue['repository'].rsplit("/", 2)[1] + \
            + '/' + \
            rich_issue['repository'].rsplit("/", 2)[2]

        rich_issue["url_id"] = \
            rich_issue['gitlab_repo'] + \
            '/issues/' + rich_issue['id_in_repo']

        if self.prjs_map:
            rich_issue.update(self.get_item_project(rich_issue))

        if 'project' in item:
            rich_issue['project'] = item['project']

        rich_issue.update(self.get_grimoire_fields(
            issue['created_at'], "issue"))

        if self.sortinghat:
            item[self.get_field_date()] = rich_issue[self.get_field_date()]
            rich_issue.update(self.get_item_sh(item, self.roles))

        return rich_issue

    def enrich_items(self, items):
        total = super(GitLabEnrich, self).enrich_items(items)

        return total

    def enrich_onion(self, enrich_backend, no_incremental=False,
                     in_index_iss='gitlab_issues_onion-src',
                     in_index_prs='gitlab_prs_onion-src',
                     out_index_iss='gitlab_issues_onion-enriched',
                     out_index_prs='gitlab_prs_onion-enriched',
                     data_source_iss='gitlab-issues',
                     data_source_prs='gitlab-prs',
                     contribs_field='uuid',
                     timeframe_field='grimoire_creation_date',
                     sort_on_field='metadata__timestamp'):

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_iss,
                             out_index=out_index_iss,
                             data_source=data_source_iss,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental)

        super().enrich_onion(enrich_backend=enrich_backend,
                             in_index=in_index_prs,
                             out_index=out_index_prs,
                             data_source=data_source_prs,
                             contribs_field=contribs_field,
                             timeframe_field=timeframe_field,
                             sort_on_field=sort_on_field,
                             no_incremental=no_incremental)


class GitLabUser(object):
    """ Helper class to manage data from a Gitlab user """

    users = {}  # cache with users from gitlab

    def __init__(self, user):

        self.login = user['username']
        self.email = user['web_url']
        self.name = user['name']
