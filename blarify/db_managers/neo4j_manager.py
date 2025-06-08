import os
import time
from typing import Any, List, Optional, Tuple
from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase, exceptions
import logging

from blarify.db_managers.models.node_result import NodeResult, NeighborResult
logger = logging.getLogger(__name__)

load_dotenv()


class Neo4jManager:
    entity_id: str
    repo_id: str
    driver: Driver

    def __init__(
        self,
        repo_id: str = None,
        entity_id: str = None,
        max_connections: int = 50,
        uri: str = None,
        user: str = None,
        password: str = None,
    ):
        uri = uri or os.getenv("NEO4J_URI")
        user = user or os.getenv("NEO4J_USERNAME")
        password = password or os.getenv("NEO4J_PASSWORD")

        retries = 3
        for attempt in range(retries):
            try:
                self.driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=max_connections)
                break
            except exceptions.ServiceUnavailable as e:
                if attempt < retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    raise e

        self.create_indexes_and_constraints()
        self.repo_id = repo_id if repo_id is not None else "default_repo"
        self.entity_id = entity_id if entity_id is not None else "default_user"

    def create_indexes_and_constraints(self):
        self.create_function_name_index()
        self.create_node_id_index()
        self.create_entityId_index()
        # self.create_unique_constraint()

    def close(self):
        # Close the connection to the database
        self.driver.close()

    def save_graph(self, nodes: List[Any], edges: List[Any]):
        self.create_nodes(nodes)
        self.create_edges(edges)

    def create_function_name_index(self):
        # Creates a fulltext index on the name and path properties of the nodes
        with self.driver.session() as session:
            node_query = """
            CREATE FULLTEXT INDEX functionNames IF NOT EXISTS FOR (n:CLASS|FUNCTION|FILE) ON EACH [n.name, n.path, n.node_id]
            """
            session.run(node_query)

    def create_node_id_index(self):
        with self.driver.session() as session:
            node_query = """
            CREATE INDEX node_id_NODE IF NOT EXISTS FOR (n:NODE) ON (n.node_id)
            """
            session.run(node_query)

    def create_entityId_index(self):
        with self.driver.session() as session:
            user_query = """
            CREATE INDEX entityId_INDEX IF NOT EXISTS FOR (n:NODE) ON (n.entityId)
            """
            session.run(user_query)

    def create_unique_constraint(self):
        with self.driver.session() as session:
            constraint_query = """
            CREATE CONSTRAINT user_node_unique IF NOT EXISTS FOR (n:NODE)
            REQUIRE (n.entityId, n.node_id) IS UNIQUE
            """
            session.run(constraint_query)

    def create_nodes(self, nodeList: List[Any]):
        # Function to create nodes in the Neo4j database
        with self.driver.session() as session:
            session.write_transaction(
                self._create_nodes_txn, nodeList, 100, repoId=self.repo_id, entityId=self.entity_id
            )

    def create_edges(self, edgesList: List[Any]):
        # Function to create edges between nodes in the Neo4j database
        with self.driver.session() as session:
            session.write_transaction(self._create_edges_txn, edgesList, 100, entityId=self.entity_id)

    @staticmethod
    def _create_nodes_txn(tx, nodeList: List[Any], batch_size: int, repoId: str, entityId: str):
        node_creation_query = """
        CALL apoc.periodic.iterate(
            "UNWIND $nodeList AS node RETURN node",
            "CALL apoc.merge.node(
            node.extra_labels + [node.type, 'NODE'],
            apoc.map.merge(node.attributes, {repoId: $repoId, entityId: $entityId}),
            {},
            {}
            )
            YIELD node as n RETURN count(n) as count",
            {batchSize: $batchSize, parallel: false, iterateList: true, params: {nodeList: $nodeList, repoId: $repoId, entityId: $entityId}}
        )
        YIELD batches, total, errorMessages, updateStatistics
        RETURN batches, total, errorMessages, updateStatistics
        """

        result = tx.run(node_creation_query, nodeList=nodeList, batchSize=batch_size, repoId=repoId, entityId=entityId)

        # Fetch the result
        for record in result:
            logger.info(f"Created {record['total']} nodes")
            print(record)

    @staticmethod
    def _create_edges_txn(tx, edgesList: List[Any], batch_size: int, entityId: str):
        # Cypher query using apoc.periodic.iterate for creating edges
        edge_creation_query = """
        CALL apoc.periodic.iterate(
            'WITH $edgesList AS edges UNWIND edges AS edgeObject RETURN edgeObject',
            'MATCH (node1:NODE {node_id: edgeObject.sourceId}) 
            MATCH (node2:NODE {node_id: edgeObject.targetId}) 
            CALL apoc.merge.relationship(
            node1, 
            edgeObject.type, 
            {scopeText: edgeObject.scopeText}, 
            {}, 
            node2, 
            {}
            ) 
            YIELD rel RETURN rel',
            {batchSize:$batchSize, parallel:false, iterateList: true, params:{edgesList: $edgesList, entityId: $entityId}}
        )
        YIELD batches, total, errorMessages, updateStatistics
        RETURN batches, total, errorMessages, updateStatistics
        """
        # Execute the query
        result = tx.run(edge_creation_query, edgesList=edgesList, batchSize=batch_size, entityId=entityId)

        # Fetch the result
        for record in result:
            logger.info(f"Created {record['total']} edges")

    def detatch_delete_nodes_with_path(self, path: str):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (n {path: $path})
                DETACH DELETE n
                """,
                path=path,
            )
            return result.data()


    """
    ------ READ functions
    """

    def get_node_by_id(self, node_id: str) -> Tuple[Optional[NodeResult], List[NeighborResult]]:
        # Query to retrieve a node by its ID

        query = """
        MATCH (n)
        WHERE n.node_id = $node_id
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query, {"node_id": node_id})
            records = result.data()

            if not records:
                return None, []

            node_data = records[0]["n"]
            neighbors = self.get_1_hop_neighbors_and_relations(node_id)

            node_result = NodeResult(
                node_id=node_data.get("node_id"),
                name=node_data.get("name"),
                node_path=node_data.get("node_path"),
                start_line=node_data.get("start_line"),
                end_line=node_data.get("end_line"),
                text=node_data.get("text"),
            )
            return node_result, neighbors

    def search_nodes_by_text(self, query: str) -> Tuple[Optional[NodeResult], List[NeighborResult]]:
        formatted_query = self._format_query(query)
        node_query = """
        CALL db.index.fulltext.queryNodes("functionNames", $formatted_query) YIELD node, score
        WHERE node.repoId = $repoId
        RETURN node.text, node.node_id, node.name, node.node_path, node.start_line, node.end_line, score
        """

        with self.driver.session() as session:
            # Try with wildcard first
            result = session.run(node_query, formatted_query=f"*{formatted_query}*", repoId=self.repo_id)
            first_result = result.peek()

            # If no results, try without wildcard
            if first_result is None:
                result = session.run(node_query, formatted_query=formatted_query, repoId=self.repo_id)
                first_result = result.peek()

            if first_result is None:
                return None, []

            neighbors = self.get_1_hop_neighbors_and_relations(first_result["node.node_id"])

            node_result = NodeResult(
                node_id=first_result["node.node_id"],
                name=first_result["node.name"],
                node_path=first_result["node.node_path"],
                start_line=first_result.get("node.start_line"),
                end_line=first_result.get("node.end_line"),
                text=first_result.get("node.text"),
            )
            return node_result, neighbors

    def get_1_hop_neighbors_and_relations(self, node_id: str) -> List[NeighborResult]:
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (p {node_id: $node_id})-[r]->(p2)
                RETURN
                    type(r) as relationship_type,
                    p2.node_id AS node_id,
                    p2.name AS node_name,
                    labels(p2) AS node_type
                """,
                node_id=node_id,
            )
            data = result.data()

            return [
                NeighborResult(
                    node_id=record["node_id"],
                    name=record["node_name"],
                    node_type=record["node_type"],
                    relationship_type=record["relationship_type"],
                )
                for record in data
            ]
    
    def _format_query(self, query: str):
        # Function to format the query to be used in the fulltext index
        special_characters = [
            "+",
            "-",
            "&&",
            "||",
            "!",
            "(",
            ")",
            "{",
            "}",
            "[",
            "]",
            "^",
            '"',
            "~",
            "*",
            "?",
            ":",
            "\\",
            "/",
        ]
        for character in special_characters:
            query = query.replace(character, f"\\{character}")
        return query

