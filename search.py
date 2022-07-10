#!/usr/bin/env python
# coding: utf-8

# # GDMA Project
# Author: Julian Schelb (1069967)

# In[1]:


from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ### Connection to the database instance

# In[2]:


class Search():
    
    def __init__(self, url = "bolt://localhost:7687", user = "neo4j", password = "", database_name = "cddb"):
    
        self.driver = GraphDatabase.driver(url, auth=(user, password))
        self.database_name = database_name
        self.session = self.driver.session(database = database_name)

        
    def createUser(self, user_id: int = 1):

        query_create_user = """
        MERGE (u:User {id:  $user_id})
        RETURN u.id as user_id
        """

        self.session.run(query_create_user, user_id = user_id)



    def deletePrefProj(self):

        query_pref_delete_proj = """
        // DELETE EXISTING PROJECTION
        CALL gds.graph.drop('searchdomain_preference', false) 
        YIELD graphName 
        RETURN graphName
        """

        self.session.run(query_pref_delete_proj)


    def createPrefProj(self, user_id: int = 1):

        query_pref_create_proj = f"""
        // CREATE NEW PROJECTION WITH SEARCH RELEVANT SUB GRAPH
        CALL gds.graph.project.cypher(
          'searchdomain_preference',
          ' // Liked Artists, Albums and Songs
            MATCH (u:User)-[:LIKES]->(n) 
            WHERE u.id = {user_id}
                AND (n:Song OR n:Album OR n:Artist) 
            RETURN id(n) AS id, labels(n) AS labels 
            LIMIT 100000

            UNION

            // CDs linked to liked Artists, Albums and Songs
            MATCH (u:User)-[:LIKES]->(x)-[:APPEARED_ON]->(n:CD) 
            WHERE u.id = {user_id}
            RETURN id(n) AS id, labels(n) AS labels 
            LIMIT 100000',

            'MATCH (u:User)-[:LIKES]->(n)
            WHERE u.id = {user_id}
            AND (n:CD OR n:Song OR n:Album OR n:Artist) 
            MATCH (n)-[r:APPEARED_ON]->(m:CD) 
            RETURN id(n) AS source, id(m) AS target, type(r) AS type 
            LIMIT 100000' 
        )
        YIELD
          graphName, nodeCount AS nodes, relationshipCount AS rels
        RETURN graphName, nodes, rels
        """

        self.session.run(query_pref_create_proj)



    def calcPrefScore(self):

        # https://neo4j.com/docs/graph-data-science/current/algorithms/eigenvector-centrality/
        query_pref_calc_score = """
        CALL gds.eigenvector.mutate('searchdomain_preference',  {
          mutateProperty: 'score_eig'
        })
        YIELD centralityDistribution, nodePropertiesWritten, ranIterations
        RETURN centralityDistribution.min AS minimumScore, 
        centralityDistribution.mean AS meanScore, nodePropertiesWritten
        """

        self.session.run(query_pref_calc_score)


    def deleteContProj(self):

        query_cont_delete_proj = """
        // DELETE EXISTING PROJECTION
        CALL gds.graph.drop('searchdomain_content', false) 
        YIELD graphName 
        RETURN graphName
        """

        self.session.run(query_cont_delete_proj)


    def createContProj(self, search_input: str = "", search_mask: str = ""):

        query_cont_create_proj = f"""
        // CREATE NEW PROJECTION WITH SEARCH RELEVANT SUB GRAPH
        CALL gds.graph.project.cypher(
          "searchdomain_content",

          " // Artists, Albums and Songs which match query

                CALL {{
                    CALL db.index.fulltext.queryNodes('artists', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'artists'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('songs', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'songs'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('albums', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'albums'
                    RETURN node, score
                }}

                WITH node, score
                ORDER BY score desc
                LIMIT 100
                MATCH (node)-[r:APPEARED_ON]->(c:CD)
                RETURN id(node) AS id, labels(node) AS labels 

            UNION

            // CDS linked to Artists, Albums and Songs which match query

                CALL {{
                    CALL db.index.fulltext.queryNodes('artists', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'artists'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('songs', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'songs'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('albums', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'albums'
                    RETURN node, score
                }}

                WITH node, score
                ORDER BY score desc
                LIMIT 100
                MATCH (node)-[r:APPEARED_ON]->(c:CD)
                RETURN id(c) AS id, labels(c) AS labels 
                ",

            "  CALL {{
                    CALL db.index.fulltext.queryNodes('artists', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'artists'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('songs', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'songs'
                    RETURN node, score
                    UNION 
                    CALL db.index.fulltext.queryNodes('albums', '{search_input}') 
                    YIELD node, score
                    WHERE '{search_mask}' = 'all' or '{search_mask}' = 'albums'
                    RETURN node, score
                }}

                WITH node, score
                ORDER BY score desc
                LIMIT 100
                MATCH (node)-[r:APPEARED_ON]->(c:CD)
                RETURN id(node) AS source, id(c) AS target, type(r) AS type 
            "
        )
        YIELD
          graphName, nodeCount AS nodes, relationshipCount AS rels
        RETURN graphName, nodes, rels
        """

        self.session.run(query_cont_create_proj)


    def calcContScore(self):

        # https://neo4j.com/docs/graph-data-science/current/algorithms/eigenvector-centrality/
        query_cont_calc_score = """
        CALL gds.eigenvector.mutate('searchdomain_content',  {
          mutateProperty: 'score_eig'
        })
        YIELD centralityDistribution, nodePropertiesWritten, ranIterations
        RETURN centralityDistribution.min AS minimumScore, 
        centralityDistribution.mean AS meanScore, nodePropertiesWritten
        """

        self.session.run(query_cont_calc_score)


    def getResults(self):

        query_results = """
        CALL {

            CALL {
                CALL gds.eigenvector.stream('searchdomain_content')
                YIELD nodeId, score
                WHERE gds.util.asNode(nodeId):CD
                WITH gds.util.asNode(nodeId).id AS nodeId, score as score_cont
                RETURN nodeId, score_cont
            }
            RETURN nodeId, score_cont, 0 as score_pref

            UNION 

            CALL {
                CALL gds.eigenvector.stream('searchdomain_preference')
                YIELD nodeId, score
                WHERE gds.util.asNode(nodeId):CD
                WITH gds.util.asNode(nodeId).id AS nodeId, score as score_pref
                RETURN nodeId, score_pref
            }
            RETURN nodeId, 0 as score_cont, score_pref

        }

        WITH DISTINCT nodeId, count(*) as count, sum(score_cont) as score_cont, sum(score_pref) as score_pref

        MATCH (n:CD)
        WHERE n.id = nodeId
        OPTIONAL MATCH (n)-[:CONTAINS]->(ar:Artist)
        OPTIONAL MATCH (n)-[:CONTAINS]->(ab:Album)
        OPTIONAL MATCH (n)-[:CONTAINS]->(so:Song)

        RETURN nodeId, 
        count,
        score_cont, score_pref,
        collect(DISTINCT ar.artist) as artists,
        collect(DISTINCT ab.album) as albums, 
        collect(DISTINCT so.song) as songs


        ORDER BY (2 * score_cont) + ( 1 * score_pref) DESC
        LIMIT 10
        """

        dtf_data = pd.DataFrame([dict(_) for _ in self.session.run(query_results)])
        return dtf_data


    def searchInGraph(self, user_id: int = 1, search_input: str = "", search_mask: str = "all"):

        # Make sure user exists
        self.createUser(user_id)

        # Preference
        self.deletePrefProj()
        self.createPrefProj()
        self.calcPrefScore()

        # Match with query 
        self.deleteContProj()
        self.createContProj(search_input, search_mask)
        self.calcContScore()

        return self.getResults()     



