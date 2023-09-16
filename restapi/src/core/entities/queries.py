"""
This module defines a class with the EWB-specific queries used to interact with Solr.


Author: Lorena Calvo-Bartolomé
Date: 19/04/2023
"""


class Queries(object):

    def __init__(self) -> None:

        # ================================================================
        # # Q1: getOpenAccess  ##################################################################
        # # Get collection filter by Open Access or Subscription
        # ================================================================
        self.Q1 = {
            'q': 'openaccess:{}',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q2: getCorpusMetadataFields  ##################################################################
        # # Get the name of the metadata fields available for
        # a specific corpus collection (not all corpus have
        # the same metadata available)
        # http://localhost:8983/solr/#/Corpora/query?q=corpus_name:Cordis&q.op=OR&indent=true&fl=fields&useParams=
        # ================================================================
        self.Q2 = {
            'q': 'corpus_name:{}',
            'fl': 'fields',
        }

        # ================================================================
        # # Q3: getNrDocsColl ##################################################################
        # # Get number of documents in a collection
        # http://localhost:8983/solr/{col}/select?q=*:*&wt=json&rows=0
        # ================================================================
        self.Q3 = {
            'q': '*:*',
            'rows': '0',
        }

        # ================================================================
        # # Q4: getDocsByYear ##################################################################
        # # Get collection filter by year
        # ================================================================
        self.Q4 = {
            'q': "date:[{}-01-01T00:00:00Z TO {}-12-31T23:59:59Z]",
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q5: getDocsByContinent
        # ################################################################
        # # Get collection filter by continent.
        # # They are customize below.
        # ================================================================

        # ================================================================
        # # Q6: getDocsByCity
        # ################################################################
        # # Get collection filter by affiliation city
        # ================================================================
        self.Q6 = {
            'q': 'affiliation_city:\"{}\"',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q7: getDocsByInstitution
        # ################################################################
        # # Get collection filter by affiliation name
        # ================================================================
        self.Q7 = {
            'q': 'affilname:\"{}\"',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q8: getTopicsLabels
        # ################################################################
        # # Get the label associated to each of the topics in a given model
        # ================================================================
        self.Q8 = {
            'q': '*:*',
            'fl': 'tpc_labels',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q9: getIdOfTopicLabel
        # ################################################################
        # # Get the Id of a given topic label
        # ================================================================
        self.Q9 = {
            'q': 'tpc_labels:\"{}\"',
            'fl': 'id',
            'start': '0',
            'rows': '1'
        }

        # ================================================================
        # # Q10: getModelInfo
        # ################################################################
        # # Get the information (chemical description, label, statistics,
        # top docs, etc.) associated to each topic in a model collection
        # ================================================================
        self.Q10 = {
            'q': '*:*',
            'fl': 'id,betas,alphas,topic_entropy,topic_coherence,ndocs_active,tpc_descriptions,tpc_labels,coords',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q11: getBetasTopicById  ##################################################################
        # # Get word distribution of a selected topic in a
        # # model collection
        # http://localhost:8983/solr/{col}/select?fl=betas&q=id:t{id}
        # ================================================================
        self.Q11 = {
            'q': 'id:t{}',
            'fl': 'betas',
        }

        # ================================================================
        # # Q12: getMostCorrelatedTopics
        # ################################################################
        # # Get the most correlated topics to a given one in a selected
        # model
        # ================================================================
        self.Q12 = {
            'q': "citedby_count:[{} TO {}]",
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q13: getDocsByFundSponsor
        # ################################################################
        # # Get collection filter by funding sponsor
        # ================================================================
        self.Q13 = {
            'q': 'fund_sponsor:\"{}\"',
            'start': '{}',
            'rows': '{}'
        }


        # ================================================================
        # # Q15: getLemmasDocById  ##################################################################
        # # Get lemmas of a selected document in a corpus collection
        # http://localhost:8983/solr/{col}/select?fl=all_lemmas&q=id:{id}
        # ================================================================
        self.Q15 = {
            'q': 'id:{}',
            'fl': 'all_lemmas',
        }

    def customize_Q1(self,
                     open_access: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q1 'getOpenAccess'.

        Parameters
        ----------
        open_access: str
            Number to filter the collection by open access or subscription.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q1: dict
            Customized query Q1.
        """

        custom_q1 = {
            'q': self.Q1['q'].format(open_access),
            'start': self.Q1['start'].format(start),
            'rows': self.Q1['rows'].format(rows),
        }
        return custom_q1

    def customize_Q2(self,
                     corpus_name: str) -> dict:
        """Customizes query Q2 'getCorpusMetadataFields'

        Parameters
        ----------
        corpus_name: str
            Name of the corpus collection whose metadata fields are to be retrieved.

        Returns
        -------
        custom_q2: dict
            Customized query Q2.
        """

        custom_q2 = {
            'q': self.Q2['q'].format(corpus_name),
            'fl': self.Q2['fl'],
        }

        return custom_q2

    def customize_Q3(self) -> dict:
        """Customizes query Q3 'getNrDocsColl'

        Returns
        -------
        self.Q3: dict
            The query Q3 (no customization is needed).
        """

        return self.Q3

    def customize_Q4(self,
                     year: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q4 'getDocsByYear'

        Parameters
        ----------
        year: str
            Publication year to filter by
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q4: dict
            Customized query Q4.
        """

        custom_q4 = {
            'q': self.Q4['q'].format(year,year),
            'start': self.Q4['start'].format(start),
            'rows': self.Q4['rows'].format(rows),
        }
        return custom_q4

    def customize_Q5(self,
                     continent: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q5 'getDocsByContinent'

        Parameters
        ----------
        continent: str
            Continent by which to filter the document collection
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q5: dict
            Customized query Q5.
        """
        if continent == "europe":
            Q5_europe = {
                'q': 'affiliation_country:(Italy OR \"United Kingdom\" OR Germany OR France OR Netherlands OR Portugal OR Switzerland OR Belgium OR Sweden OR Denmark OR Poland OR \"Russian Federation\" OR Austria OR Greece OR Norway OR Finland OR \"Czech Republic\" OR Ireland OR Slovenia OR Croatia OR Serbia OR Estonia OR Lithuania OR Cyprus OR Hungary OR Slovakia OR Bulgaria OR Latvia OR Romania OR Malta OR Luxembourg OR Iceland OR Belarus OR Ukraine OR \"Bosnia and Herzegovina\" OR Moldova OR Albania OR \"North Macedonia\")',
                'start': start,
                'rows': rows,
            }
            return Q5_europe
        elif continent == "asia":
            Q5_asia = {
                'q': 'affiliation_country:(China OR Japan OR India OR \"South Korea\" OR Israel OR Iran OR Turkey OR \"Saudi Arabia\" OR \"United Arab Emirates\" OR Taiwan OR Pakistan OR Singapore OR \"Hong Kong\" OR Thailand OR Malaysia OR \"Viet Nam\" OR Lebanon OR Qatar OR Bangladesh OR Armenia OR Jordan OR Georgia OR Philippines OR Kazakhstan OR Iraq OR Afghanistan OR Kyrgyzstan OR Tajikistan OR \"Brunei Darussalam\" OR Myanmar OR Laos OR Indonesia OR Cambodia OR Singapore OR Yemen OR Oman OR \"Syrian Arab Republic\" OR Azerbaijan OR Turkmenistan OR Uzbekistan OR \"Sri Lanka\" OR Mongolia OR Nepal OR Bhutan OR Kuwait OR Cyprus)',
                'start': start,
                'rows': rows,
            }
            return Q5_asia
        elif continent == "africa":
            Q5_africa = {
                'q': 'affiliation_country:(\"South Africa\" OR Morocco OR Egypt OR Nigeria OR Algeria OR Tunisia OR Ethiopia OR Kenya OR Ghana OR Namibia OR Sudan OR \"Libyan Arab Jamahiriya\" OR Mauritania OR Mozambique OR Zimbabwe OR Mali OR Angola OR Gambia OR Togo OR Senegal OR Cameroon OR Mauritius OR Congo OR Zambia OR Uganda OR Botswana OR Gabon OR Rwanda OR Madagascar OR Niger OR Malawi OR \"Burkina Faso\" OR \"Cape Verde\" OR Guinea OR \"Cote d\'Ivoire\" OR Benin OR Chad OR \"Guinea-Bissau\" OR \"Sierra Leone\" OR Zimbabwe OR Burundi OR Liberia OR \"Central African Republic\" OR Djibouti OR \"Equatorial Guinea\" OR \"Democratic Republic Congo\" OR Tanzania)',
                'start': start,
                'rows': rows,
            }
            return Q5_africa
        elif continent == "north america":
            Q5_north_america = {
                'q': 'affiliation_country:(\"United States\" OR Canada OR Mexico OR Guatemala OR Haiti OR Honduras OR \"El Salvador\" OR Nicaragua OR \"Costa Rica\" OR Panama OR Cuba OR \"Dominican Republic\" OR Jamaica OR \"Puerto Rico\" OR Bahamas OR Greenland OR \"Trinidad and Tobago\")',
                'start': start,
                'rows': rows,
            }
            return Q5_north_america
        elif continent == "south america":
            Q5_south_america = {
                'q': 'affilcountry:(Brazil OR Chile OR Argentina OR Colombia OR Ecuador OR Peru OR Venezuela OR Uruguay OR Paraguay OR Bolivia OR Guyana OR Suriname OR \"Falkland Islands (Malvinas)\")',
                'start': start,
                'rows': rows,
            }            
            return Q5_south_america
        else:
            Q5_world = {
                'q': "*:*",
                'start': self.Q6['start'].format(start),
                'rows': self.Q6['rows'].format(rows),
            }
            return Q5_world
        

    def customize_Q6(self,
                     city: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q6 'getDocsByCity'


        Parameters
        ----------
        city: str
            City by which to filter the document collection.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.


        Returns
        -------
        custom_q6: dict
            Customized query Q6.
        """

        custom_q6 = {
            'q': self.Q6['q'].format(city),
            'start': self.Q6['start'].format(start),
            'rows': self.Q6['rows'].format(rows),
        }
        return custom_q6

    def customize_Q7(self,
                     institution: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q7 'getDocsByInstitution'

        Parameters
        ----------
        institution: str
            Institution by which to filter the document collection.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q7: dict
            Customized query Q7.
        """

        custom_q7 = {
            'q': self.Q7['q'].format(institution),
            'start': self.Q7['start'].format(start),
            'rows': self.Q7['rows'].format(rows),
        }
        return custom_q7

    def customize_Q8(self,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q8 'getTopicsLabels'

        Parameters
        ----------
        rows: str
            Number of rows to retrieve.
        start: str
            Start value.

        Returns
        -------
        self.Q8: dict
            The query Q8
        """

        custom_q8 = {
            'q': self.Q8['q'],
            'fl': self.Q8['fl'],
            'start': self.Q8['start'].format(start),
            'rows': self.Q8['rows'].format(rows),
        }

        return custom_q8

    def customize_Q9(self,
                     topic_label: str) -> dict:
        """Customizes query Q9 'getIdOfTopicLabel'

        Parameters
        ----------
        topic_label: str
            Topic label.

        Returns
        -------
        custom_q9: dict
            Customized query Q9.
        """

        custom_q9 = {
            'q': self.Q9['q'].format(topic_label),
            'fl': self.Q9['fl'],
            'start': self.Q9['start'],
            'rows': self.Q9['rows'],
        }
        
        return custom_q9

    def customize_Q10(self,
                      start: str,
                      rows: str) -> dict:
        """Customizes query Q10 'getModelInfo'

        Parameters
        ----------
        start: str
            Start value.
        rows: str

        Returns
        -------
        custom_q10: dict
            Customized query Q10.
        """

        custom_q10 = {
            'q': self.Q10['q'],
            'fl': self.Q10['fl'],
            'start': self.Q10['start'].format(start),
            'rows': self.Q10['rows'].format(rows),
        }

        return custom_q10

    def customize_Q11(self,
                      topic_id: str) -> dict:
        """Customizes query Q11 'getBetasTopicById'.

        Parameters
        ----------
        topic_id: str
            Topic id.

        Returns
        -------
        custom_q11: dict
            Customized query Q11.
        """

        custom_q11 = {
            'q': self.Q11['q'].format(topic_id),
            'fl': self.Q11['fl']
        }
        return custom_q11

    def customize_Q12(self,
                      lower_limit: str,
                      upper_limit: str,
                      start: str,
                      rows: str) -> dict:
        """Customizes query Q12 'getMostCorrelatedTopics'

        Parameters
        ----------
        lower_limit: str
            Lower limit to filter by number of citations
        upper_limit: str
            Upper limit to filter by number of citations
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q12: dict
            Customized query q12.
        """

        custom_q12 = {
            'q': self.Q12['q'].format(lower_limit, upper_limit),
            'start': self.Q12['start'].format(start),
            'rows': self.Q12['rows'].format(rows),
        }
        return custom_q12

    def customize_Q13(self,
                      fund_sponsor: str,
                      start: str,
                      rows: str) -> dict:
        
        """Customizes query Q13 'getDocsByFundSponsor'

        Parameters
        ----------
        fund_sponsor: str
            Funding Sponsor by which to filter the document collection
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q13: dict
            Customized query Q13.
        """

        custom_q13 = {
            'q': self.Q13['q'].format(fund_sponsor),
            'start': self.Q13['start'].format(start),
            'rows': self.Q13['rows'].format(rows),
        }
        
        return custom_q13

    def customize_Q15(self,
                      id: str) -> dict:
        """Customizes query Q15 'getLemmasDocById'.

        Parameters
        ----------
        id: str
            Document id.

        Returns
        -------
        custom_q15: dict
            Customized query Q15.
        """

        custom_q15 = {
            'q': self.Q15['q'].format(id),
            'fl': self.Q15['fl'],
        }
        return custom_q15