#!/usr/bin/env python
# coding: utf-8


from neo4j import GraphDatabase
import pandas as pd


class SearchEngine():

    def __init__(self, url="bolt://localhost:7687", user="neo4j",
                 password="", database_name="cddb"):

        self.driver = GraphDatabase.driver(url, auth=(user, password))
        self.database_name = database_name
        self.session = self.driver.session(database=database_name)

    # ---------------------------------------------------------------------------
    #                            CREATE EXAMPLE USER
    # ---------------------------------------------------------------------------

    def createUser(self, user_id: int = None):

        if type(user_id) is not int:
            raise TypeError('User ID must be a Number')
        if user_id < 0:
            raise ValueError('User ID must be a positive Number')

        query = """
        MERGE (u:User {id:  $user_id})
        RETURN u.id as user_id
        """

        with self.driver.session(database=self.database_name) as session:
            session.run(query, user_id=user_id)

    # ---------------------------------------------------------------------------

    def deleteUser(self, user_id: int = None):

        if type(user_id) is not int:
            raise TypeError('User ID must be a Number')
        if user_id < 0:
            raise ValueError('User ID must be a positive Number')

        query = """
        MATCH (u:User)
        WHERE u.id = $user_id
        DETACH DELETE u
        """

        with self.driver.session(database=self.database_name) as session:
            session.run(query, user_id=user_id)

    # ---------------------------------------------------------------------------

    def addLike(self, user_id: int = None, node_id: int = None, node_label: str = None):

        # Validate user_id
        if type(user_id) is not int:
            raise TypeError('User ID must be a Number')
        if user_id < 0:
            raise ValueError('User ID must be a positive Number')

        # Validate node_id
        if type(node_id) is not int:
            raise TypeError('Node ID must be a Number')
        if node_id < 0:
            raise ValueError('Node ID must be a positive Number')

        # Validate node_label
        if node_label not in ["Song", "Album", "Artist"]:
            raise ValueError('Node type must be Song, Album or Artist')

        self.createUser(user_id=user_id)

        query = f"""
        MATCH (u:User)
        WHERE u.id = {user_id}
        MATCH (n: {node_label})
        WHERE n.id = {node_id}
        MERGE (u)-[r:LIKES]->(n)
        """

        with self.driver.session(database=self.database_name) as session:
            session.run(query)

    # ---------------------------------------------------------------------------

    def createExampleUser(self, user_id: int = 1, genre: str = "rock", limit: int = 50):

        ####### CREATE USER #######

        self.deleteUser(user_id=user_id)
        self.createUser(user_id=user_id)

        ####### LIKE SONGS #######

        query = """
        MATCH (g:Genre)<-[r:BELONGS_TO]-(c:CD)
        MATCH (c)-[r2:CONTAINS]->(s:Song)
        WHERE g.genre = $genre
        WITH DISTINCT s
        LIMIT $limit
        RETURN s.id as id
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query, user_id=user_id,
                                  genre=genre, limit=limit)
            for row in results:
                self.addLike(user_id=user_id,
                             node_id=row["id"], node_label="Song")

        ####### LIKE ALBUMS #######

        query = """
        MATCH (g:Genre)<-[r:BELONGS_TO]-(c:CD)
        MATCH (c)-[r2:CONTAINS]->(s:Album)
        WHERE g.genre = $genre
        WITH DISTINCT s
        LIMIT $limit
        RETURN s.id as id
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query, user_id=user_id,
                                  genre=genre, limit=limit)
            for row in results:
                self.addLike(user_id=user_id,
                             node_id=row["id"], node_label="Album")

        ####### LIKE ARTISTS #######

        query = """
        MATCH (g:Genre)<-[r:BELONGS_TO]-(c:CD)
        MATCH (c)-[r2:CONTAINS]->(s:Artist)
        WHERE g.genre = $genre
        WITH DISTINCT s
        LIMIT $limit
        RETURN s.id as id
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query, user_id=user_id,
                                  genre=genre, limit=limit)
            for row in results:
                self.addLike(user_id=user_id,
                             node_id=row["id"],
                             node_label="Artist")

    # ---------------------------------------------------------------------------
    #                            STAGE 1: COMPUTE USER PREFERENCE
    # ---------------------------------------------------------------------------

    def loadUserPreferences(self, user_id: int = None):

        ####### DELETE EXISTING PROJECTION #######

        query_delete = """
        CALL gds.graph.drop('searchdomain_preference', false)
        YIELD graphName
        RETURN graphName
        """

        ####### CREATE NEW PROJECTION #######

        query_create = f"""
        // CREATE NEW PROJECTION WITH SEARCH RELEVANT SUB GRAPH
        CALL gds.graph.project.cypher(
          'searchdomain_preference',

          ' // Liked Artists, Albums and Songs
            MATCH (u:User)-[:LIKES]->(n)
            WHERE u.id = {user_id}
                AND (n:Song OR n:Album OR n:Artist)
            RETURN id(n) AS id, labels(n) AS labels
            UNION
            // CDs linked to liked Artists, Albums and Songs
            MATCH (u:User)-[:LIKES]->(x)-[:APPEARED_ON]->(n:CD)
            WHERE u.id = {user_id}
            RETURN id(n) AS id, labels(n) AS labels ',

            'MATCH (u:User)-[:LIKES]->(n)
            WHERE u.id = {user_id}
            AND (n:CD OR n:Song OR n:Album OR n:Artist)
            MATCH (n)-[r:APPEARED_ON]->(m:CD)
            RETURN id(n) AS source, id(m) AS target, type(r) AS type'
        )
        YIELD
          graphName, nodeCount AS nodes, relationshipCount AS rels
        RETURN graphName, nodes, rels
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query_delete)
            results = session.run(query_create)

    # ---------------------------------------------------------------------------

    def calcPreferredCD(self, user_id: int = None):

        ####### CHECK IF PROJECTION EXISTS #######

        query_check = """
        CALL gds.graph.exists("searchdomain_preference")
        YIELD graphName, exists
        RETURN graphName, exists
        """

        ####### DELETE EXISTING RELATIONS #######

        query_delete = """
        WITH $user_id as userID
        MATCH (:User {id: userID})-[r:PREFERRES]->(c:CD)
        DELETE r
        """

        ####### CREATE NEW RELATIONS #######

        query_create = """
        WITH $user_id as userId
        CALL gds.eigenvector.stream('searchdomain_preference')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId).id AS nodeId, score, userId
        MATCH (c:CD {id: nodeId})
        MATCH (u:User {id: userId})
        MERGE (c)<-[r:PREFERRES {score: score}]-(u)
        """

        with self.driver.session(database=self.database_name) as session:

            # Check if projection exists
            results = session.run(query_check, user_id=user_id)
            graph_exists = bool(results.single()["exists"])

            if graph_exists:
                results = session.run(query_delete, user_id=user_id)
                results = session.run(query_create, user_id=user_id)

    # ---------------------------------------------------------------------------

    def calcPreferredGenre(self, user_id: int = None):

        ####### DELETE EXISTING RELATIONS #######

        query_delete = """
        WITH $user_id as userID
        MATCH (:User {id: userID})-[r:PREFERRES]->(g:Genre)
        DELETE r
        """

        ####### CREATE NEW RELATIONS #######

        query_create = """
        WITH $user_id as userID

        // Determine total count for normalization
        CALL {
            MATCH (u:User)-[r1:LIKES]->(n:Album)
            MATCH (n)-[r2:APPEARED_ON]-(c:CD)
            MATCH (c)-[:BELONGS_TO]->(g:Genre)
            WHERE u.id = 1
            RETURN COUNT(DISTINCT c.id) as countTotal
        }

        // Determine count per genre
        MATCH (u:User)-[r1:LIKES]->(n:Album)
        MATCH (n)-[r2:APPEARED_ON]-(c:CD)
        MATCH (c)-[:BELONGS_TO]->(g:Genre)
        WHERE u.id = userID
        WITH g, c, u, countTotal, userID
        WITH
            DISTINCT g.id as id, g.genre as genre, u, g,
            count(DISTINCT c.id) as countGenre, countTotal,
            userID
        WITH id, genre, userID, u, g,  countGenre, countTotal,
        (toFloat(countGenre) / toFloat(countTotal)) as prob

        // Create relation between user and genre
        MERGE (u)-[:PREFERRES {score: prob}]->(g)
        RETURN *
        ORDER BY countGenre DESC
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query_delete, user_id=user_id)
            results = session.run(query_create, user_id=user_id)

    # ---------------------------------------------------------------------------

    def processUserPreference(self):

        query = """
        MATCH (u:User)
        RETURN u.id as userID
        """

        with self.driver.session(database=self.database_name) as session:
            results = session.run(query)
            for row in results:
                user_id = row["userID"]
                print(f"Precompute Preferences for User {user_id}")
                self.loadUserPreferences(user_id=user_id)
                self.calcPreferredCD(user_id=user_id)
                self.calcPreferredGenre(user_id=user_id)

    # ---------------------------------------------------------------------------
    #                            STAGE 2: SEARCH BY QUERY
    # ---------------------------------------------------------------------------

    def searchFor(self, user_id=1, search_input="", search_mask="all", limit=10):

        query = """// Search input
        WITH
        //1 as userID,
        //'Jimi Hendrix purple haze are you experienced' as searchQuery,
        //'Ludwig van Beethoven Für Elise Symphony' as searchQuery,
        //'all' as searchMask
        $user_id as userID,
        $search_input as searchQuery,
        $search_mask as searchMask

        // Find matching artists, songs and albums
        // based on text similarity
        CALL {
            // Artists
            WITH searchQuery, searchMask
            CALL db.index.fulltext.queryNodes('artists', searchQuery)
            YIELD node, score
            WHERE searchMask = 'all' or searchMask = 'artists'
            RETURN node, score
            UNION

            // Songs
            WITH searchQuery, searchMask
            CALL db.index.fulltext.queryNodes('songs', searchQuery)
            YIELD node, score
            WHERE searchMask = 'all' or searchMask = 'songs'
            RETURN node, score
            UNION

            // Albums
            WITH searchQuery, searchMask
            CALL db.index.fulltext.queryNodes('albums', searchQuery)
            YIELD node, score
            WHERE searchMask = 'all' or searchMask = 'albums'
            RETURN node, score
        }
        WITH node, score, userID
        MATCH (node)-[r:APPEARED_ON]->(c:CD)
        WITH DISTINCT c.id as id, c,  sum(score) as score_content, userID

        // Get more information about the CDs
        MATCH (c)-[:CONTAINS]->(ar:Artist)
        MATCH (c)-[:CONTAINS]->(ab:Album)
        MATCH (c)-[:CONTAINS]->(so:Song)
        MATCH (c)-[:BELONGS_TO]->(ge:Genre)
        OPTIONAL MATCH (c)<-[r:PREFERRES]-(u:User {id: userID})
        OPTIONAL MATCH (ge)<-[r2:PREFERRES]-(u2:User {id: userID})

        // Compile final list of search results
        RETURN
        id, userID as user, score_content, sum(r.score) as score_pref, r2.score as score_genre ,
        score_content + (sum(r.score)  * score_content) + (COALESCE(r2.score, 0)  * score_content) as score_combined,
        collect(DISTINCT ge.genre) as genres,
        collect(DISTINCT ar.artist) as artists,
        collect(DISTINCT ab.album) as albums,
        collect(DISTINCT so.song) as songs

        ORDER BY score_combined DESC
        LIMIT 100"""

        results = pd.DataFrame([dict(_) for _ in self.session.run(query, user_id=user_id,
                                                                  search_input=search_input,
                                                                  search_mask=search_mask)])
        return results.head(limit)
