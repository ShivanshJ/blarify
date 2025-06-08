from dataclasses import dataclass
from typing import List, Optional


@dataclass
class NodeResult:
    """Data class representing a node result from Neo4j queries"""
    node_id: str
    name: str
    node_path: str
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    text: Optional[str] = None


@dataclass
class NeighborResult:
    """Data class representing a neighbor node with relationship information"""
    node_id: str
    name: str
    node_type: List[str]
    relationship_type: str