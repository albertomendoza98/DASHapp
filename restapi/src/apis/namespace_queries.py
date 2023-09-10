"""
This script defines a Flask RESTful namespace for managing Solr queries.

Author: Lorena Calvo-Bartolomé
Date: 13/04/2023
"""

from flask_restx import Namespace, Resource, reqparse
from src.core.clients.ewb_solr_client import EWBSolrClient

# ======================================================
# Define namespace for managing queries
# ======================================================
api = Namespace(
    'Queries', description='Specfic Solr queries for the EWB.')

# ======================================================
# Namespace variables
# ======================================================
# Create Solr client
sc = EWBSolrClient(api.logger)

# Define parsers to take inputs from user
q1_parser = reqparse.RequestParser()
q1_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q1_parser.add_argument(
    'open_access', help='Specify with 1 to filter by open access documents, 0 otherwise.', required=True)

q2_parser = reqparse.RequestParser()
q2_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)

q3_parser = reqparse.RequestParser()
q3_parser.add_argument(
    'collection', help='Name of the collection', required=True)

q4_parser = reqparse.RequestParser()
q4_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q4_parser.add_argument(
    'year', help='Publication year to filter by', required=True)

q5_parser = reqparse.RequestParser()
q5_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q5_parser.add_argument(
    'continent', help='Continent by which to filter the document collection', required=True)

q6_parser = reqparse.RequestParser()
q6_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q6_parser.add_argument(
    'city', help="City by which to filter the document collection", required=True)

q7_parser = reqparse.RequestParser()
q7_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q7_parser.add_argument(
    'institution', help="Institution by which to filter the document collection", required=True)

q8_parser = reqparse.RequestParser()
q8_parser.add_argument(
    'model_collection', help='Name of the model collection', required=True)
q8_parser.add_argument(
    'start', help='Specifies an offset (by default, 0) into the responses at which Solr should begin displaying content', required=False)
q8_parser.add_argument(
    'rows', help='Controls how many rows of responses are displayed at a time (default value: maximum number of docs in the collection)', required=False)

q9_parser = reqparse.RequestParser()
q9_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q9_parser.add_argument(
    'model_name', help='Name of the model reponsible for the creation of the doc-topic distribution', required=True)
q9_parser.add_argument(
    'topic_id', help="ID of the topic whose top documents according to 'model_name' are being searched", required=True)
q9_parser.add_argument(
    'start', help='Specifies an offset (by default, 0) into the responses at which Solr should begin displaying content', required=False)
q9_parser.add_argument(
    'rows', help='Controls how many rows of responses are displayed at a time (default value: maximum number of docs in the collection)', required=False)

q10_parser = reqparse.RequestParser()
q10_parser.add_argument(
    'model_collection', help='Name of the model collection', required=True)
q10_parser.add_argument(
    'start', help='Specifies an offset (by default, 0) into the responses at which Solr should begin displaying content', required=False)
q10_parser.add_argument(
    'rows', help='Controls how many rows of responses are displayed at a time (default value: maximum number of docs in the collection)', required=False)

q11_parser = reqparse.RequestParser()
q11_parser.add_argument(
    'model_collection', help='Name of the model collection', required=True)
q11_parser.add_argument(
    'topic_id', help='ID of the topic whose whose word-topic distribution is to be retrieved', required=False)

q12_parser = reqparse.RequestParser()
q12_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q12_parser.add_argument(
    'lower_limit', help='Lower limit to filter by number of citations', required=True)
q12_parser.add_argument(
    'upper_limit', help='Upper limit to filter by number of citations', required=True)

q13_parser = reqparse.RequestParser()
q13_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q13_parser.add_argument(
    'fund_sponsor', help='Funding Sponsor by which to filter the document collection', required=True)

q15_parser = reqparse.RequestParser()
q15_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q15_parser.add_argument(
    'doc_id', help='ID of the document whose whose doc-topic distribution associated to a specific model is to be retrieved', required=True)

q16_parser = reqparse.RequestParser()
q16_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q16_parser.add_argument(
    'model_name', help='Name of the model reponsible for the creation of the doc-topic distribution to be retrieved', required=True)
q16_parser.add_argument(
    'start', help='Specifies an offset (by default, 0) into the responses at which Solr should begin displaying content', required=False)
q16_parser.add_argument(
    'rows', help='Controls how many rows of responses are displayed at a time (default value: maximum number of docs in the collection)', required=False)

q17_parser = reqparse.RequestParser()
q17_parser.add_argument(
    'model_name', help='Name of the model to retrieve the topic-word distribution.', required=True)
q17_parser.add_argument(
    'tpc_id', help='ID of the specific topic to retrieve the topic-word distribution.', required=True)
q17_parser.add_argument(
    'word', help='Word of interest to retrieve its topic-word distribution in the specified topic. If the word is not present, the distribution is 0.', required=True)


@api.route('/getOpenAccess/')
class getOpenAccess(Resource):
    @api.doc(parser=q1_parser)
    def get(self):
        args = q1_parser.parse_args()
        corpus_collection = args['corpus_collection']
        open_access = args['open_access']

        return sc.do_Q1(corpus_col=corpus_collection,
                        open_access=open_access)


@api.route('/getCorpusMetadataFields/')
class getCorpusMetadataFields(Resource):
    @api.doc(parser=q2_parser)
    def get(self):
        args = q2_parser.parse_args()
        corpus_collection = args['corpus_collection']

        return sc.do_Q2(corpus_col=corpus_collection)


@api.route('/getNrDocsColl/')
class getNrDocsColl(Resource):
    @api.doc(parser=q3_parser)
    def get(self):
        args = q3_parser.parse_args()
        collection = args['collection']

        return sc.do_Q3(col=collection)


@api.route('/getDocsByYear/')
class getDocsByYear(Resource):
    @api.doc(parser=q4_parser)
    def get(self):
        args = q4_parser.parse_args()
        corpus_collection = args['corpus_collection']
        year = args['year']

        return sc.do_Q4(corpus_col=corpus_collection,
                        year=year)


@api.route('/getDocsByContinent/')
class getDocsByContinent(Resource):
    @api.doc(parser=q5_parser)
    def get(self):
        args = q5_parser.parse_args()
        corpus_collection = args['corpus_collection']
        continent = args['continent']

        return sc.do_Q5(corpus_col=corpus_collection,
                        continent=continent)


@api.route('/getDocsByCity/')
class getDocsByCity(Resource):
    @api.doc(parser=q6_parser)
    def get(self):
        args = q6_parser.parse_args()
        corpus_collection = args['corpus_collection']
        city = args['city']

        return sc.do_Q6(corpus_col=corpus_collection,
                        city=city)


@api.route('/getDocsByInstitution/')
class getDocsByInstitution(Resource):
    @api.doc(parser=q7_parser)
    def get(self):
        args = q7_parser.parse_args()
        corpus_collection = args['corpus_collection']
        institution = args['institution']

        return sc.do_Q7(corpus_col=corpus_collection,
                        institution=institution)


@api.route('/getTopicsLabels/')
class getTopicsLabels(Resource):
    @api.doc(parser=q8_parser)
    def get(self):
        args = q8_parser.parse_args()
        model_collection = args['model_collection']
        start = args['start']
        rows = args['rows']

        return sc.do_Q8(model_col=model_collection,
                        start=start,
                        rows=rows)


@api.route('/getTopicTopDocs/')
class getTopicTopDocs(Resource):
    @api.doc(parser=q9_parser)
    def get(self):
        args = q9_parser.parse_args()
        corpus_collection = args['corpus_collection']
        model_name = args['model_name']
        topic_id = args['topic_id']
        start = args['start']
        rows = args['rows']

        return sc.do_Q9(corpus_col=corpus_collection,
                        model_name=model_name,
                        topic_id=topic_id,
                        start=start,
                        rows=rows)


@api.route('/getModelInfo/')
class getModelInfo(Resource):
    @api.doc(parser=q10_parser)
    def get(self):
        args = q10_parser.parse_args()
        model_collection = args['model_collection']
        start = args['start']
        rows = args['rows']

        return sc.do_Q10(model_col=model_collection,
                         start=start,
                         rows=rows)


@api.route('/getBetasTopicById/')
class getBetasTopicById(Resource):
    @api.doc(parser=q11_parser)
    def get(self):
        args = q11_parser.parse_args()
        model_collection = args['model_collection']
        topic_id = args['topic_id']

        return sc.do_Q11(model_col=model_collection,
                         topic_id=topic_id)


@api.route('/getDocsByCitedCount/')
class getDocsByCitedCount(Resource):
    @api.doc(parser=q12_parser)
    def get(self):
        args = q12_parser.parse_args()
        corpus_collection = args['corpus_collection']
        lower_limit = args['lower_limit']
        upper_limit = args['upper_limit']

        return sc.do_Q12(corpus_col=corpus_collection,
                         lower_limit=lower_limit,
                         upper_limit=upper_limit)

@api.route('/getDocsByFundSponsor/')
class getDocsByFundSponsor(Resource):
    @api.doc(parser=q13_parser)
    def get(self):
        args = q13_parser.parse_args()
        corpus_collection = args['corpus_collection']
        fund_sponsor = args['fund_sponsor']

        return sc.do_Q13(corpus_col=corpus_collection,
                        fund_sponsor=fund_sponsor)


@api.route('/getLemmasDocById/')
class getLemmasDocById(Resource):
    @api.doc(parser=q15_parser)
    def get(self):
        args = q15_parser.parse_args()
        corpus_collection = args['corpus_collection']
        doc_id = args['doc_id']

        return sc.do_Q15(corpus_col=corpus_collection,
                         doc_id=doc_id)


@api.route('/getThetasAndDateAllDocs/')
class getThetasAndDateAllDocs(Resource):
    @api.doc(parser=q16_parser)
    def get(self):
        args = q16_parser.parse_args()
        corpus_collection = args['corpus_collection']
        model_name = args['model_name']
        start = args['start']
        rows = args['rows']

        return sc.do_Q16(corpus_col=corpus_collection,
                         model_name=model_name,
                         start=start,
                         rows=rows)


@api.route('/getBetasByWordAndTopicId/')
class getBetasByWordAndTopicId(Resource):
    @api.doc(parser=q17_parser)
    def get(self):
        args = q17_parser.parse_args()
        model_name = args['model_name']
        tpc_id = args['tpc_id']
        word = args['word']

        return sc.do_Q17(model_name=model_name,
                         tpc_id=tpc_id,
                         word=word)
