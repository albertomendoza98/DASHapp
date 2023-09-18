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

q9_parser = reqparse.RequestParser()
q9_parser.add_argument(
    'model_name', help='Name of the model reponsible for the creation of the doc-topic distribution', required=True)
q9_parser.add_argument(
    'topic_label', help="Label of the topic whose id is retrieved", required=True)

q10_parser = reqparse.RequestParser()
q10_parser.add_argument(
    'corpus_collection', help='Name of the corpus collection', required=True)
q10_parser.add_argument(
    'model_collection', help='Name of the model collection', required=True)
q10_parser.add_argument(
    'topic_label', help="Label of the topic whose id is retrieved", required=True)

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

q14_parser = reqparse.RequestParser()
q14_parser.add_argument(
    'model_collection', help='Name of the model collection', required=True)


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


@api.route('/getIdOfTopicLabel/')
class getIdOfTopicLabel(Resource):
    @api.doc(parser=q9_parser)
    def get(self):
        args = q9_parser.parse_args()
        model_col = args['model_name']
        topic_label = args['topic_label']

        return sc.do_Q9(model_col=model_col,
                        topic_label=topic_label)


@api.route('/getDocsByTopicLabel/')
class getDocsByTopicLabel(Resource):
    @api.doc(parser=q10_parser)
    def get(self):
        args = q10_parser.parse_args()
        corpus_collection = args['corpus_collection']
        model_collection = args['model_collection']
        topic_label = args['topic_label']

        return sc.do_Q10(corpus_col=corpus_collection,
                         model_col=model_collection,
                         topic_label=topic_label)

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
    
@api.route('/getTopicMap/')
class getTopicMap(Resource):
    @api.doc(parser=q14_parser)
    def get(self):
        args = q14_parser.parse_args()
        model_collection = args['model_collection']

        return sc.do_Q14(model_col=model_collection)