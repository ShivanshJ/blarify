from typing import Any, Dict, List, Optional, Tuple
from agno.tools import Toolkit

from blarify.db_managers.neo4j_manager import Neo4jManager
from blarify.db_managers.models.node_result import NodeResult, NeighborResult


class GraphQueryTool(Toolkit):
    """
    Separate class for Neo4j query/retrieval operations.
    Keeps query logic separate from the main Neo4jManager which focuses on write operations.
    """

    def __init__(self, 
                db_manager: Neo4jManager,
                get_node_by_id: bool = True,
                search_nodes_by_text: bool = True,
                **kwargs,
            ):
        """Initialize with an existing Neo4jManager instance to reuse connection and settings"""

        self.db_manager = db_manager
        self.repo_id = db_manager.repo_id
        self.entity_id = db_manager.entity_id

        tools: List[Any] = []
        if get_node_by_id:
            tools.append(self.get_node_by_id)
        if search_nodes_by_text:
            tools.append(self.search_nodes_by_text)

        super().__init__(name="neo4j_tools", tools=tools, **kwargs)


    def get_node_by_id(self, query: str) ->  List[Dict[str, Any]]:
        # Query to retrieve a node by its ID
        result = self.db_manager.get_node_by_id(query)
        if not result:
            return "No code found for the given query"
        result = result if result else "No result found"
        return result


    def search_nodes_by_text(self, query: str) ->  List[Dict[str, Any]]:
        """Returns a function code given a node_id. returns the node text and the neighbors of the node."""
        code, neighbours = self.db_manager.search_nodes_by_text(query)
        if not code:
            return "No code found for the given query"
        res = f"current node code:\n {code.text} \n\n current node neighbours: {neighbours}"

        return res