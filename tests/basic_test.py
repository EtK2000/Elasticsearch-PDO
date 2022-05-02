import os
import unittest

from elastic_pdo.elasticsearch_integration import ElasticsearchIntegration


class TestBasic(unittest.TestCase):
    def setUp(self):
        ElasticsearchIntegration.create_client(os.environ['ELASTIC_ENDPOINT'],
                                               ('elastic', os.environ['ELASTIC_PASSWORD']))

    def tearDown(self):
        ...

    def test_it(self):
        from .cdr import Cdr

        test = Cdr(session_id='banana')
        test.commit()
        test.online = True
        with test.transaction():
            test.states.clean = True
            test.online = False
        test.delete()
        print(f'online calls: {Cdr.count({"online": True})}, total: {Cdr.count()}')
        # cdrs = Cdr.fetch_all()

        dis = Cdr.distinct('language.keyword')
        print(str(dis))

        from .swagger.calls_filter_request import CallsFilterRequest
        all_seconds = Cdr.sum('duration', CallsFilterRequest(caller_number='35095'))

        dummy = Cdr.fetch('90b43306-abd0-11ea-8b61-0242ac170007')
        with dummy.transaction():
            dummy.is_loading = False
            dummy.states.status = -1
            dummy.states.has_marked_transcript = False
        print('yuda is a banana')
