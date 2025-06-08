from agno.agent import Agent
from agno.models.anthropic import Claude

from blarify.agent.tools.GraphQueryTool import GraphQueryTool
from blarify.db_managers.neo4j_manager import Neo4jManager

prompt ="""
You are a code assistant that makes solid and extensive unit test. You only respond with the unit test code and the test cases made in python.
You can traverse the graph by calling the function search_nodes_by_text.
You are given a graph of code functions, We purposly omitted some code If the code has the comment '# Code replaced for brevity. See node_id ..... '.
Prefer calling the function search_nodes_by_text with query = node_id, only call it with starting nodes or neighbours.
Extensivley traverse the graph before giving an answer
"""

def unit_test_agent(graph_manager) -> Agent:
    agent = Agent(
        model=Claude(id="claude-3-7-sonnet-20250219"),
        instructions=prompt,
        tools=[GraphQueryTool(graph_manager)],
        show_tool_calls=True,
        markdown=True,
    )
    return agent

