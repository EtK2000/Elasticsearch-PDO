from datetime import datetime
from typing import Dict, List, Tuple, Union

from elastic_pdo.elasticsearch_integration import ElasticsearchIntegration
from elastic_pdo.elasticsearch_model import ElasticsearchModel
from .swagger.call_log_states import CallLogStates
from .swagger.calls_filter_request import CallsFilterRequest
from .swagger.comment import Comment
from .swagger.transcript_line import TranscriptLine


# noinspection GrazieInspection
class Cdr(ElasticsearchModel):
    __index = 'cdrs'
    __primary_key = 'session_id'

    @staticmethod
    def _id_query(value: int):
        return {
            'match': {
                'cdr_id': value
            }
        }

    @classmethod
    def __generate_search_request(cls, request: CallsFilterRequest):
        from elastic_pdo.elasticsearch_model import ElasticsearchQuery
        filter_part, must_part = [], []

        if request.text_to_search:
            if request.text_to_search.isnumeric():
                must_part.append(cls._id_query(value=int(request.text_to_search)))
            else:
                must_part.append(ElasticsearchQuery.text(value=request.text_to_search))

        if request.assigned:
            filter_part.append(ElasticsearchQuery.equals(field='assigned', value=True))

        if request.successful_calls:
            filter_part.append(ElasticsearchQuery.equals(field='successful_call', value=request.successful_calls))

        if request.only_approved_calls:
            filter_part.append(ElasticsearchQuery.exists(field='approved_by'))

        if request.caller_number:
            filter_part.append(ElasticsearchQuery.equals(field='caller', value=request.caller_number))

        if request.callee_number:
            filter_part.append(ElasticsearchQuery.equals(field='callee', value=request.callee_number))

        if request.ai_tag:
            filter_part.append(ElasticsearchQuery.equals(field='has_ai_tags', value=True))

        if request.online:
            filter_part.append(ElasticsearchQuery.equals(field='online', value=True))

        if request.compound_phrases:
            filter_part.append(ElasticsearchQuery.equals(field='has_compound_phrases', value=True))

        if request.simple_phrases:
            filter_part.append(ElasticsearchQuery.equals(field='has_simple_phrases', value=True))

        if request.keywords:
            filter_part.append(ElasticsearchQuery.equals(field='has_keyword_phrases', value=True))

        if request.approved_calls:
            filter_part.append(
                ElasticsearchQuery.equals(field='only_approved_calls', value=request.only_approved_calls))

        if request.call_classifications:
            filter_part.append(
                ElasticsearchQuery.or_(field='states.call_classification', values=request.call_classifications))

        if request.topics:
            must_part.append(ElasticsearchQuery.or_(field='topics', values=request.topics))

        if request.call_tags:
            must_part.append(ElasticsearchQuery.or_(field='tags', values=request.call_tags))

        if request.agent_tags:
            filter_part.append(ElasticsearchQuery.equals(field='agent_tags', value=request.agent_tags))

        if request.customer_tags:
            filter_part.append(ElasticsearchQuery.equals(field='customer_tags', value=request.customer_tags))

        if request.language_filter:
            must_part.append(ElasticsearchQuery.or_(field='language', values=request.language_filter))

        if request.assignees:
            filter_part.append(ElasticsearchQuery.equals(field='assignees', value=request.assignees))

        if request.statuses:
            filter_part.append(ElasticsearchQuery.equals(field='cdr_statuses', value=request.statuses))

        if request.duration_min is not None or request.duration_max is not None:
            filter_part.append(
                ElasticsearchQuery.range(field='duration', gte=request.duration_min, lte=request.duration_max))

        if request.start or request.end:
            filter_part.append(ElasticsearchQuery.range(field='start', gte=request.start, lte=request.end))

        query = {}
        if filter_part:
            query['filter'] = filter_part
        if must_part:
            query['must'] = must_part

        return {
            'query': {
                'bool': query
            }
        }

    @classmethod
    def count(cls, query_or_filter: Union[CallsFilterRequest, Dict[str, any]] = None) -> int:
        """
        Counts all models of this type matching the supplied query or filter

        :param query_or_filter: The query/filter to match against,
        or <span style="color:#0055aa">True</span> if not supplied
        :return: The amount of models matching the supplied query/filter or total amount if no query supplied
        """

        if not isinstance(query_or_filter, CallsFilterRequest):
            return super().count(query_or_filter)

        request_body_search = cls.__generate_search_request(query_or_filter)
        search_response = ElasticsearchIntegration.client.search(index=cls.__index, body=request_body_search,
                                                                 track_total_hits=True,
                                                                 size=query_or_filter.max_elements,
                                                                 from_=query_or_filter.offset)
        return search_response['hits']['total']['value']

    @classmethod
    def search(cls, filter_: CallsFilterRequest) -> Tuple[List['Cdr'], int]:
        request_body_search = cls.__generate_search_request(filter_)
        search_response = ElasticsearchIntegration.client.search(index=cls.__index, body=request_body_search,
                                                                 track_total_hits=True, size=filter_.max_elements,
                                                                 from_=filter_.offset)
        cdrs = [cls().from_elastic_document(cdr['_source']) for cdr in search_response['hits']['hits']]
        number_of_calls = search_response['hits']['total']['value']

        return cdrs, number_of_calls

    @classmethod
    def sum(cls, field: str, filter_: CallsFilterRequest) -> int:
        request_body_search = cls.__generate_search_request(filter_)
        request_body_search['size'] = 0
        request_body_search['aggs'] = {
            '*': {
                'sum': {
                    'field': field
                }
            }
        }

        search_response = ElasticsearchIntegration.client.search(index=cls.__index, body=request_body_search,
                                                                 track_total_hits=True, size=filter_.max_elements,
                                                                 from_=filter_.offset)
        return search_response['aggregations']['*']['value']

    def __init__(self, account_manager_id: int = None, call_score: int = None, callee_phone_number: str = None,
                 callee_phrases: List[str] = None, callee_score: int = None,
                 callee_transcription: List[TranscriptLine] = None, caller_phone_number: str = None,
                 caller_phrases: List[str] = None, caller_score: int = None,
                 caller_transcription: List[TranscriptLine] = None, cdr_id: int = None,
                 compliance_comments: List[Comment] = None, end: datetime = None, is_loading: bool = False,
                 language: str = None, metadata: Dict[str, any] = None, online: bool = False, outgoing: bool = True,
                 premium_transcription_status: int = None, review_comments: List[Comment] = None,
                 session_id: str = None, start: datetime = None, states: CallLogStates = None,
                 successful_call: bool = None, transcript_callee_id: int = None, transcript_caller_id: int = None):
        super().__init__()

        self.account_manager_id = account_manager_id
        self.call_score = call_score
        self.callee_phone_number = callee_phone_number
        self.callee_phrases = callee_phrases
        self.callee_score = callee_score
        self.callee_transcription = callee_transcription
        self.caller_phone_number = caller_phone_number
        self.caller_phrases = caller_phrases
        self.caller_score = caller_score
        self.caller_transcription = caller_transcription
        self.cdr_id = cdr_id
        self.compliance_comments = compliance_comments
        self.duration = (end - start).total_seconds() if start and end else 0  # LOW: make a calculated property?
        self.outgoing = outgoing
        self.end = end
        self.is_loading = is_loading
        self.language = language
        self.metadata = metadata
        self.online = online
        self.premium_transcription_status = premium_transcription_status
        self.review_comments = review_comments
        self.session_id = session_id
        self.start = start
        self.states = states or CallLogStates()
        self.successful_call = successful_call
        self.transcript_callee_id = transcript_callee_id
        self.transcript_caller_id = transcript_caller_id

    # LOW: maybe cache?
    def tagged_lines(self) -> List[TranscriptLine]:
        res = [line for line in self.callee_transcript if line.tags]
        res.extend([line for line in self.caller_transcript if line.tags])
        return res
