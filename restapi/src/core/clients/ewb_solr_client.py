"""
This module provides a specific class for handeling the Solr API responses and requests of the EWB.

Author: Lorena Calvo-Bartolomé
Date: 17/04/2023
"""

import configparser
import logging
import pathlib
import pandas as pd
from typing import List, Union

from src.core.clients.base.solr_client import SolrClient
from src.core.clients.ewb_inferencer_client import EWBInferencerClient
from src.core.entities.corpus import Corpus
from src.core.entities.model import Model
from src.core.entities.queries import Queries


class EWBSolrClient(SolrClient):

    def __init__(self,
                 logger: logging.Logger,
                 config_file: str = "/config/config.cf") -> None:
        super().__init__(logger)

        # Read configuration from config file
        cf = configparser.ConfigParser()
        cf.read(config_file)
        self.batch_size = int(cf.get('restapi', 'batch_size'))
        self.corpus_col = cf.get('restapi', 'corpus_col')
        self.no_meta_fields = cf.get('restapi', 'no_meta_fields').split(",")
        self.max_sum = int(cf.get('restapi', 'max_sum'))

        # Create Queries object for managing queries
        self.querier = Queries()

        # Create InferencerClient to send requests to the Inferencer API
        self.inferencer = EWBInferencerClient(logger)

        return

    # ======================================================
    # CORPUS-RELATED OPERATIONS
    # ======================================================
    def index_corpus(self,
                     corpus_logical_path: str) -> None:
        """Given the string path of corpus file, it creates a Solr collection with such the stem name of the file (i.e., if we had '/data/source.Cordis.json' as corpus_logical_path, 'Cordis' would be the stem), reades the corpus file, extracts the raw information of each document, and sends a POST request to the Solr server to index the documents in batches.

        Parameters
        ----------
        corpus_logical_path : str
            The path of the logical corpus file to be indexed.
        """

        # 1. Get full path and stem of the logical corpus
        corpus_to_index = pathlib.Path(corpus_logical_path)
        corpus_logical_name = corpus_to_index.stem.lower()

        # 2. Create collection
        corpus, err = self.create_collection(col_name=corpus_logical_name)
        if err == 409:
            self.logger.info(
                f"-- -- Collection {corpus_logical_name} already exists.")
            return
        else:
            self.logger.info(
                f"-- -- Collection {corpus_logical_name} successfully created.")

        # 3. Add corpus collection to self.corpus_col. If Corpora has not been created already, create it
        corpus, err = self.create_collection(col_name=self.corpus_col)
        if err == 409:
            self.logger.info(
                f"-- -- Collection {self.corpus_col} already exists.")

            # 3.1. Do query to retrieve last id in self.corpus_col
            # http://localhost:8983/solr/#/{self.corpus_col}/query?q=*:*&q.op=OR&indent=true&sort=id desc&fl=id&rows=1&useParams=
            sc, results = self.execute_query(q='*:*',
                                             col_name=self.corpus_col,
                                             sort="id desc",
                                             rows="1",
                                             fl="id")
            if sc != 200:
                self.logger.error(
                    f"-- -- Error getting latest used ID. Aborting operation...")
                return
            # Increment corpus_id for next corpus to be indexed
            corpus_id = int(results.docs[0]["id"]) + 1
        else:
            self.logger.info(
                f"Collection {self.corpus_col} successfully created.")
            corpus_id = 1

        # 4. Create Corpus object and extract info from the corpus to index
        corpus = Corpus(corpus_to_index)
        json_docs = corpus.get_docs_raw_info()
        corpus_col_upt = corpus.get_corpora_update(id=corpus_id)

        # 5. Index corpus and its fiels in CORPUS_COL
        self.logger.info(
            f"-- -- Indexing of {corpus_logical_name} info in {self.corpus_col} starts.")
        self.index_documents(corpus_col_upt, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing of {corpus_logical_name} info in {self.corpus_col} completed.")

        # 6. Index documents in corpus collection
        self.logger.info(
            f"-- -- Indexing of {corpus_logical_name} in {corpus_logical_name} starts.")
        self.index_documents(json_docs, corpus_logical_name, self.batch_size)
        self.logger.info(
            f"-- -- Indexing of {corpus_logical_name} in {corpus_logical_name} completed.")

        return

    def list_corpus_collections(self) -> Union[List, int]:
        """Returns a list of the names of the corpus collections that have been created in the Solr server.

        Returns
        -------
        corpus_lst: List
            List of the names of the corpus collections that have been created in the Solr server.
        """

        sc, results = self.execute_query(q='*:*',
                                         col_name=self.corpus_col,
                                         fl="corpus_name")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus collections in {self.corpus_col}. Aborting operation...")
            return

        corpus_lst = [doc["corpus_name"] for doc in results.docs]

        return corpus_lst, sc

    def get_corpus_coll_fields(self, corpus_col: str) -> Union[List, int]:
        """Returns a list of the fields of the corpus collection given by 'corpus_col' that have been defined in the Solr server.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose fields are to be retrieved.

        Returns
        -------
        models: list
            List of fields of the corpus collection
        sc: int
            Status code of the request
        """
        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="fields")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting fields of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["fields"], sc

    def get_corpus_models(self, corpus_col: str) -> Union[List, int]:
        """Returns a list with the models associated with the corpus given by 'corpus_col'

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose models are to be retrieved.

        Returns
        -------
        models: list
            List of models associated with the corpus
        sc: int
            Status code of the request
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="models")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting models of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["models"], sc

    def delete_corpus(self,
                      corpus_logical_path: str) -> None:
        """Given the string path of corpus file, it deletes the Solr collection associated with it. Additionally, it removes the document entry of the corpus in the self.corpus_col collection and all the models that have been trained with such a logical corpus.

        Parameters
        ----------
        corpus_logical_path : str
            The path of the logical corpus file to be indexed.
        """

        # 1. Get stem of the logical corpus
        corpus_logical_name = pathlib.Path(corpus_logical_path).stem.lower()

        # 2. Delete corpus collection
        _, sc = self.delete_collection(col_name=corpus_logical_name)
        if sc != 200:
            self.logger.error(
                f"-- -- Error deleting corpus collection {corpus_logical_name}")
            return

        # 3. Get ID and associated models of corpus collection in self.corpus_col
        sc, results = self.execute_query(q='corpus_name:'+corpus_logical_name,
                                         col_name=self.corpus_col,
                                         fl="id,models")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus ID. Aborting operation...")
            return

        # 4. Delete all models associated with the corpus if any
        # TODO: Check
        if "models" in results.docs[0].keys():
            for model in results.docs[0]["models"]:
                _, sc = self.delete_collection(col_name=model)
                if sc != 200:
                    self.logger.error(
                        f"-- -- Error deleting model collection {model}")
                    return

        # 5. Remove corpus from self.corpus_col
        sc = self.delete_doc_by_id(
            col_name=self.corpus_col, id=results.docs[0]["id"])
        if sc != 200:
            self.logger.error(
                f"-- -- Error deleting corpus from {self.corpus_col}")
        return

    def check_is_corpus(self, corpus_col) -> bool:
        """Checks if the collection given by 'corpus_col' is a corpus collection.

        Parameters
        ----------
        corpus_col : str
            Name of the collection to be checked.

        Returns
        -------
        is_corpus: bool
            True if the collection is a corpus collection, False otherwise.
        """

        corpus_colls, sc = self.list_corpus_collections()
        if corpus_col not in corpus_colls:
            self.logger.error(
                f"-- -- {corpus_col} is not a corpus collection. Aborting operation...")
            return False

        return True

    def check_corpus_has_model(self, corpus_col, model_name) -> bool:
        """Checks if the collection given by 'corpus_col' has a model with name 'model_name'.

        Parameters
        ----------
        corpus_col : str
            Name of the collection to be checked.
        model_name : str
            Name of the model to be checked.

        Returns
        -------
        has_model: bool
            True if the collection has the model, False otherwise.
        """

        corpus_fields, sc = self.get_corpus_coll_fields(corpus_col)
        if 'doctpc_' + model_name not in corpus_fields:
            self.logger.error(
                f"-- -- {corpus_col} does not have the field doctpc_{model_name}. Aborting operation...")
            return False
        return True

    # ======================================================
    # MODEL-RELATED OPERATIONS
    # ======================================================
    def index_model(self, model_path: str) -> None:
        """
        Given the string path of a model created with the ITMT (i.e., the name of one of the folders representing a model within the TMmodels folder), it extracts the model information and that of the corpus used for its generation. It then adds a new field in the corpus collection of type 'VectorField' and name 'doctpc_{model_name}, and index the document-topic proportions in it. At last, it index the rest of the model information in the model collection.

        Parameters
        ----------
        model_path : str
            Path to the folder of the model to be indexed.
        """

        # 1. Get stem of the model folder
        model_to_index = pathlib.Path(model_path)
        model_name = pathlib.Path(model_to_index).stem.lower()

        # 2. Create collection
        _, err = self.create_collection(col_name=model_name)
        if err == 409:
            self.logger.info(
                f"-- -- Collection {model_name} already exists.")
            return
        else:
            self.logger.info(
                f"-- -- Collection {model_name} successfully created.")
            
        metadata = self.do_Q2("cordis")
        self.logger.info(
            f"-- -- Metadata of {self.corpus_col} before creating the model: {metadata}")

        # 3. Create Model object and extract info from the corpus to index
        model = Model(model_to_index)
        json_docs, corpus_name = model.get_model_info_update(action='set')
        sc, results = self.execute_query(q='corpus_name:'+corpus_name,
                                         col_name=self.corpus_col,
                                         fl="id")
        if sc != 200:
            self.logger.error(
                f"-- -- Corpus collection not found in {self.corpus_col}")
            return
        field_update = model.get_corpora_model_update(
            id=results.docs[0]["id"], action='add')
        
        metadata = self.do_Q2("cordis")
        self.logger.info(
            f"-- -- Metadata of {self.corpus_col} before adding the doc-tpc distribution: {metadata}")

        # 4. Add field for the doc-tpc distribution associated with the model being indexed in the document associated with the corpus
        self.logger.info(
            f"-- -- Indexing model information of {model_name} in {self.corpus_col} starts.")
        self.index_documents(field_update, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing of model information of {model_name} info in {self.corpus_col} completed.")
        
        metadata = self.do_Q2("cordis")
        self.logger.info(
            f"-- -- Metadata of {self.corpus_col} before modifying the schema: {metadata}")  

        # 5. Modify schema in corpus collection to add field for the doc-tpc distribution and the similarities associated with the model being indexed
        model_key = 'doctpc_' + model_name
        sim_model_key = 'sim_' + model_name
        self.logger.info(
            f"-- -- Adding field {model_key} in {corpus_name} collection")
        _, err = self.add_field_to_schema(
            col_name=corpus_name, field_name=model_key, field_type='VectorField')
        self.logger.info(
            f"-- -- Adding field {sim_model_key} in {corpus_name} collection")
        _, err = self.add_field_to_schema(
            col_name=corpus_name, field_name=sim_model_key, field_type='VectorFloatField')
        

        metadata = self.do_Q2("cordis")
        self.logger.info(
            f"-- -- Metadata of {self.corpus_col} before indexing doc-tpc information: {metadata}")  

        # 6. Index doc-tpc information in corpus collection
        self.logger.info(
            f"-- -- Indexing model information in {corpus_name} collection")
        self.index_documents(json_docs, corpus_name, self.batch_size)

        self.logger.info(
            f"-- -- Indexing model information in {model_name} collection")
        json_tpcs = model.get_model_info()
        self.index_documents(json_tpcs, model_name, self.batch_size)

        return

    def list_model_collections(self) -> Union[List[str], int]:
        """Returns a list of the names of the model collections that have been created in the Solr server.

        Returns
        -------
        models_lst: List[str]
            List of the names of the model collections that have been created in the Solr server.
        sc: int
            Status code of the request.
        """
        sc, results = self.execute_query(q='*:*',
                                         col_name=self.corpus_col,
                                         fl="models")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus collections in {self.corpus_col}. Aborting operation...")
            return

        models_lst = [model for doc in results.docs for model in doc["models"]]

        return models_lst, sc

    def delete_model(self, model_path: str) -> None:
        """
        Given the string path of a model created with the ITMT (i.e., the name of one of the folders representing a model within the TMmodels folder), 
        it deletes the model collection associated with it. Additionally, it removes the document-topic proportions field in the corpus collection and removes the fields associated with the model and the model from the list of models in the corpus document from the self.corpus_col collection.

        Parameters
        ----------
        model_path : str
            Path to the folder of the model to be indexed.
        """

        # 1. Get stem of the model folder
        model_to_index = pathlib.Path(model_path)
        model_name = pathlib.Path(model_to_index).stem.lower()

        # 2. Delete model collection
        _, sc = self.delete_collection(col_name=model_name)
        if sc != 200:
            self.logger.error(
                f"-- -- Error occurred while deleting model collection {model_name}. Stopping...")
            return
        else:
            self.logger.info(
                f"-- -- Model collection {model_name} successfully deleted.")

        # 3. Create Model object and extract info from the corpus associated with the model
        model = Model(model_to_index)
        json_docs, corpus_name = model.get_model_info_update(action='remove')
        sc, results = self.execute_query(q='corpus_name:'+corpus_name,
                                         col_name=self.corpus_col,
                                         fl="id")
        if sc != 200:
            self.logger.error(
                f"-- -- Corpus collection not found in {self.corpus_col}")
            return
        field_update = model.get_corpora_model_update(
            id=results.docs[0]["id"], action='remove')

        # 4. Remove field for the doc-tpc distribution associated with the model being deleted in the document associated with the corpus
        self.logger.info(
            f"-- -- Deleting model information of {model_name} in {self.corpus_col} starts.")
        self.index_documents(field_update, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Deleting model information of {model_name} info in {self.corpus_col} completed.")

        # 5. Delete doc-tpc information from corpus collection
        self.logger.info(
            f"-- -- Deleting model information from {corpus_name} collection")
        self.index_documents(json_docs, corpus_name, self.batch_size)

        # 6. Modify schema in corpus collection to delete field for the doc-tpc distribution and similarities associated with the model being indexed
        model_key = 'doctpc_' + model_name
        sim_model_key = 'sim_' + model_name
        self.logger.info(
            f"-- -- Deleting field {model_key} in {corpus_name} collection")
        _, err = self.delete_field_from_schema(
            col_name=corpus_name, field_name=model_key)
        self.logger.info(
            f"-- -- Deleting field {sim_model_key} in {corpus_name} collection")
        _, err = self.delete_field_from_schema(
            col_name=corpus_name, field_name=sim_model_key)

        return

    def check_is_model(self, model_col) -> bool:
        """Checks if the model_col is a model collection. If not, it aborts the operation.

        Parameters
        ----------
        model_col : str
            Name of the model collection.

        Returns
        -------
        is_model : bool
            True if the model_col is a model collection, False otherwise.
        """

        model_colls, sc = self.list_model_collections()
        if model_col not in model_colls:
            self.logger.error(
                f"-- -- {model_col} is not a model collection. Aborting operation...")
            return False
        return True

    # ======================================================
    # AUXILIARY FUNCTIONS
    # ======================================================
    def custom_start_and_rows(self, start, rows, col) -> Union[str, str]:
        """Checks if start and rows are None. If so, it returns the number of documents in the collection as the value for rows and 0 as the value for start.

        Parameters
        ----------
        start : str
            Start parameter of the query.
        rows : str
            Rows parameter of the query.
        col : str
            Name of the collection.

        Returns
        -------
        start : str
            Final start parameter of the query.
        rows : str
            Final rows parameter of the query.
        """
        if start is None:
            start = str(0)
        if rows is None:
            numFound_dict, sc = self.do_Q3(col)
            rows = str(numFound_dict['ndocs'])

            if sc != 200:
                self.logger.error(
                    f"-- -- Error executing query Q3. Aborting operation...")
                return

        return start, rows
    
    def indexes_filter(self, row):
        """Auxiliary function to filter the 'similarities' column by the 'indexes' column.
        It is used inside an apply function in pandas, so it iterates over the rows of the DataFrame.
        """
        indexes = str(row['score']).split('.')
        lower_limit = int(indexes[0])
        upper_limit = int(indexes[1]) + 1
        similarities = row['similarities'].split(" ")
        filtered_similarities = similarities[lower_limit:upper_limit]

        return ' '.join(filtered_similarities)

    def pairs_sims_process(self, df: pd.DataFrame, model_name: str, num_records: int):
        """Function to process the pairs of documents in descendent order by the similarities for a given year.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with documents id, similarities and score.

        Returns
        -------
        df_sims: list
            List like dictionary [{column -> value}, … , {column -> value}] with the pairs of documents in descendent order by the similarities for a given year
        """
        # 0. Rename the 'sim_{model_name}' column to 'similarities'
        sim_model_key = 'sim_' + model_name
        df.rename(columns={sim_model_key: 'similarities'}, inplace=True)
        # 1. Remove rows with score = 0.00
        df_filtered = df.loc[df['score'] != 0.00].copy()
        # 2. Apply the score filter to the 'similarities' column
        df_filtered['similarities'] = df_filtered.apply(self.indexes_filter, axis=1)
        # 3. Remove the 'score' column
        df_filtered.drop(['score'], axis=1, inplace=True)
        # 4. Split the 'similarities' column and create multiple rows
        df_sims = df_filtered.assign(similarities=df_filtered['similarities'].str.split(' ')).explode('similarities')
        # 5. Divide the 'similarities' column into two columns: id_similarities and similarities
        df_sims[['id_similarities', 'similarities']] = df_sims['similarities'].str.split('|', expand=True)
        # 6. Convert the 'id_similarities' and 'similarities' columns to numeric types
        df_sims['id'] = df_sims['id'].astype(int)
        df_sims['id_similarities'] = df_sims['id_similarities'].astype(int)
        df_sims['similarities'] = df_sims['similarities'].astype(float)
        # 7. Remove rows where id_similarities is not in the 'id' column (not in the year specified by the user)
        df_sims = df_sims[df_sims['id_similarities'].isin(df_sims['id'])]
        # 8. Remove rows where the values of "id" and "id_similarities" match (same document)
        df_sims = df_sims[df_sims['id'] != df_sims['id_similarities']]
        # 9. Sort the DataFrame from highest to lowest based on the "similarities" field
        df_sims = df_sims.sort_values(by='similarities', ascending=False)
        # 10. Reset the DataFrame index
        df_sims.reset_index(drop=True, inplace=True)
        # 11. Keep only the first num_records rows
        df_sims = df_sims.head(num_records)
        # 12. Rename the columns
        df_sims.rename(columns={'id': 'id_1', 'id_similarities': 'id_2', 'similarities': 'score'}, inplace=True)
        # 13. Reorder the columns
        columns_order = ['id_1', 'id_2', 'score']
        df_sims = df_sims.reindex(columns=columns_order)
        
        return df_sims.to_dict('records')

    # ======================================================
    # QUERIES
    # ======================================================

    def do_Q1(self,
              corpus_col: str,
              open_access: str) -> Union[dict, int]:
        """Executes query Q1.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection.
        open_access : str
            Filter the collection by open access documents if value equal to 1. Otherwise if 0.

        Returns
        -------
        collection: dict
            JSON object with the filtered document collection
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q1 = self.querier.customize_Q1(open_access=open_access, start=start, rows=10)
        params = {k: v for k, v in q1.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q1['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q1. Aborting operation...")
            return

        return results.docs, sc
    
    def do_Q2(self, corpus_col: str) -> Union[dict, int]:
        """
        Executes query Q2.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection

        Returns
        -------
        json_object: dict
            JSON object with the metadata fields of the corpus collection in the form: {'metadata_fields': [field1, field2, ...]}
        sc: int
            The status code of the response
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Execute query (to self.corpus_col)
        q2 = self.querier.customize_Q2(corpus_name=corpus_col)
        params = {k: v for k, v in q2.items() if k != 'q'}
        sc, results = self.execute_query(
            q=q2['q'], col_name=self.corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q2. Aborting operation...")
            return

        # Filter out metadata fields that we don't consider metadata
        #meta_fields = [field for field in results.docs[0]
                       #['fields'] if field not in self.no_meta_fields and not field.startswith("doctpc_")]
        meta_fields = [field for field in results.docs[0]
                       ['fields'] if field not in self.no_meta_fields]
        
        return {'metadata_fields': meta_fields}, sc
    
    def do_Q3(self, col: str) -> Union[dict, int]:
        """Executes query Q3.

        Parameters
        ----------
        col : str
            Name of the collection

        Returns
        -------
        json_object : dict
            JSON object with the number of documents in the corpus collection
        sc : int
            The status code of the response
        """

        # 0. Convert collection name to lowercase
        col = col.lower()

        # 1. Check that col is either a corpus or a model collection
        if not self.check_is_corpus(col) and not self.check_is_model(col):
            return

        # 2. Execute query
        q3 = self.querier.customize_Q3()
        params = {k: v for k, v in q3.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q3['q'], col_name=col, **params)

        # 3. Filter results
        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q3. Aborting operation...")
            return

        return {'ndocs': int(results.hits)}, sc

    def do_Q4(self,
              corpus_col: str,
              year: str,
              ) -> Union[dict, int]:
        """Executes query Q4.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection
        year: str
            Publication year to filter by
        
        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 4. Execute query
        q4 = self.querier.customize_Q4(
            year=year, start=start, rows=rows)
        params = {k: v for k, v in q4.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q4['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q4. Aborting operation...")
            return

        return results.docs, sc

    def do_Q5(self,
              corpus_col: str,
              continent: str) -> Union[dict, int]:
        """Executes query Q5.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection
        continent: str
            Continent through which the document collection is to be filtered

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q5 = self.querier.customize_Q5(continent=continent, start=start, rows=rows)
        params = {k: v for k, v in q5.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q5['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q5. Aborting operation...")
            return

        return results.docs, sc

    def do_Q6(self,
              corpus_col: str,
              city: str) -> Union[dict, int]:
        """Executes query Q6.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        city: str
            City by which to filter the document collection

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q6 = self.querier.customize_Q6(city=city, start=start, rows=rows)
        params = {k: v for k, v in q6.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q6['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q6. Aborting operation...")
            return

        return results.docs, sc

    def do_Q7(self,
              corpus_col: str,
              institution: str) -> Union[dict, int]:
        """Executes query Q7.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        institution: str
            Institution by which to filter the document collection

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q7 = self.querier.customize_Q7(institution=institution, start=start, rows=rows)
        params = {k: v for k, v in q7.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q7['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q7. Aborting operation...")
            return

        return results.docs, sc

    def do_Q8(self,
              model_col: str,
              start: str,
              rows: str) -> Union[dict, int]:
        """Executes query Q8.

        Parameters
        ----------
        model_col: str
            Name of the model collection
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model name to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 3. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, model_col)

        # 4. Execute query
        q8 = self.querier.customize_Q8(start=start, rows=rows)
        params = {k: v for k, v in q8.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q8['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q8. Aborting operation...")
            return

        return results.docs, sc

    def do_Q9(self,
              model_col: str,
              topic_label: str) -> Union[dict, int]:
        """Executes query Q9.

        Parameters
        ----------
        model_name: str
            Name of the model collection on which the search will be based
        topic_label: str
            Label of the topic whose id will be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model name to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 5. Execute query
        q9 = self.querier.customize_Q9(topic_label=topic_label)
        
        params = {k: v for k, v in q9.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q9['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q9. Aborting operation...")
            return
        
        return results.docs, sc

    def do_Q10(self,
               model_col: str,
               start: str,
               rows: str) -> Union[dict, int]:
        """Executes query Q10.

        Parameters
        ----------
        model_col: str
            Name of the model collection whose information is being retrieved
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model name to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 3. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, model_col)

        # 4. Execute query
        q10 = self.querier.customize_Q10(start=start, rows=rows)
        params = {k: v for k, v in q10.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q10['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q10. Aborting operation...")
            return
            
        for el in results.docs:
            desc = el['tpc_descriptions'].split(", ")
            tpc_id = el['id'].split("t")[1]
            betas_list = [word + "|" + str(self.do_Q17(model_name=model_col, tpc_id=tpc_id, word=word)[0]['betas']) for word in desc]
            el['top_words_betas'] = ' '.join(betas_list)
              
        return results.docs, sc

    def do_Q11(self,
               model_col: str,
               topic_id: str) -> Union[dict, int]:
        """Executes query Q11.

        Parameters
        ----------
        model_col : str
            Name of the model collection.
        topic_id : str
            ID of the topic to be retrieved.

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus and model names to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 3. Execute query
        q11 = self.querier.customize_Q11(
            topic_id=topic_id)
        params = {k: v for k, v in q11.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q11['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q11. Aborting operation...")
            return

        return {'betas': results.docs[0]['betas']}, sc

    def do_Q12(self,
               corpus_col: str,
               lower_limit: str,
               upper_limit: str) -> Union[dict, int]:
        """Executes query Q12.

        Parameters
        ----------
        corpus_col: str
           Name of the corpus collection.
        lower_limit: str
            Lower limit to filter by number of citations
        upper_limit: str
            Upper limit to filter by number of citations

        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q12 = self.querier.customize_Q12(lower_limit=lower_limit, upper_limit=upper_limit, start=start, rows=10)
        params = {k: v for k, v in q12.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q12['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q12. Aborting operation...")
            return

        return results.docs, sc

    def do_Q13(self,
               corpus_col: str,
               fund_sponsor: str) -> Union[dict, int]:
        
        """Executes query Q13.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection.
        fund_sponsor: str
            Funding Sponsor by which to filter the document collection.

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Customize start and rows
        start, rows = self.custom_start_and_rows(start=None, rows=None, col=corpus_col)

        # 3. Execute query
        q13 = self.querier.customize_Q13(fund_sponsor=fund_sponsor, start=start, rows=rows)
        params = {k: v for k, v in q13.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q13['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q13. Aborting operation...")
            return

        return results.docs, sc

    def do_Q15(self,
               corpus_col: str,
               doc_id: str) -> Union[dict, int]:
        """Executes query Q15.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection.
        id : str
            ID of the document to be retrieved.

        Returns
        -------
        lemmas: dict
            JSON object with the document's lemmas.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Execute query
        q15 = self.querier.customize_Q15(id=doc_id)
        params = {k: v for k, v in q15.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q15['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q15. Aborting operation...")
            return

        return {'lemmas': results.docs[0]['all_lemmas']}, sc