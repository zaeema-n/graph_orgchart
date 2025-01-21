import pandas as pd

import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from neo4j_util.neo4j_interface import Neo4jInterface

# Initialize Neo4j interface
neo4j_interface = Neo4jInterface()

def create_constraints(driver: Neo4jInterface):
    """Create unique constraints for nodes."""
    constraints = [
        "CREATE CONSTRAINT government_name_unique IF NOT EXISTS FOR (g:government) REQUIRE g.name IS UNIQUE",
        "CREATE CONSTRAINT minister_name_unique IF NOT EXISTS FOR (m:minister) REQUIRE m.name IS UNIQUE",
        "CREATE CONSTRAINT department_name_unique IF NOT EXISTS FOR (d:department) REQUIRE d.name IS UNIQUE"
    ]
    for query in constraints:
        driver.execute_query(query)
    print("Constraints created successfully.")


# Create nodes and relationships from CSV files
def create_government_nodes(driver: Neo4jInterface, gov_file: str):
    """Create Government nodes."""
    governments = pd.read_csv(gov_file)
    for _, row in governments.iterrows():
        query = """
        CREATE (:government {id: $id, name: $name})
        """
        parameters = {
            "id": row["id"],
            "name": row["name"]
        }
        driver.execute_query(query, parameters)

def create_minister_nodes(driver: Neo4jInterface, min_file: str):
    """Create Minister nodes."""
    ministers = pd.read_csv(min_file)
    for _, row in ministers.iterrows():
        query = """
        CREATE (:minister {id: $id, name: $name})
        """
        parameters = {
            "id": row["id"],
            "name": row["name"]
        }
        driver.execute_query(query, parameters)

def create_department_nodes(driver: Neo4jInterface, dep_file: str):
    """Create Department nodes."""
    departments = pd.read_csv(dep_file)
    for _, row in departments.iterrows():
        query = """
        CREATE (:department {id: $id, name: $name})
        """
        parameters = {
            "id": row["id"],
            "name": row["name"]
        }
        driver.execute_query(query, parameters)

def create_gov_min_relationships(driver: Neo4jInterface, gov_min_file: str):
    """Create relationships between Government and Ministry."""
    gov_min = pd.read_csv(gov_min_file)
    for _, row in gov_min.iterrows():
        query = """
        MATCH (gov:government {id: $gov_id}), (min:minister {id: $min_id})
        CREATE (gov)-[:HAS_MINISTER {start_time: $start_time, end_time: $end_time}]->(min)
        """
        parameters = {
            "gov_id": row["gov_id"],
            "min_id": row["min_id"],
            "start_time": row["start_time"],
            "end_time": row.get("end_time", None)  # Handle empty end_time
        }
        driver.execute_query(query, parameters)

def create_min_dep_relationships(driver: Neo4jInterface, min_dep_file: str):
    """Create relationships between Ministry and Department."""
    min_dep = pd.read_csv(min_dep_file)
    for _, row in min_dep.iterrows():
        query = """
        MATCH (min:minister {id: $min_id}), (dep:department {id: $dep_id})
        CREATE (min)-[:HAS_DEPARTMENT {start_time: $start_time, end_time: $end_time}]->(dep)
        """
        parameters = {
            "min_id": row["min_id"],
            "dep_id": row["dep_id"],
            "start_time": row["start_time"],
            "end_time": row.get("end_time", None)  # Handle empty end_time
        }
        driver.execute_query(query, parameters)

# Main execution
def load_data_to_neo4j():
    
    # Create constraints to ensure unique names
    create_constraints(neo4j_interface)

    # File paths (replace with your actual file paths)
    government_file = "../data/2015-09-21/government.csv"
    ministry_file = "../data/2015-09-21/minister.csv"
    gov_min_file = "../data/2015-09-21/gov-min.csv"
    department_file = "../data/2015-09-21/department.csv"
    min_dep_file = "../data/2015-09-21/min-dep.csv"

    # Create nodes
    create_government_nodes(neo4j_interface, government_file)
    create_minister_nodes(neo4j_interface, ministry_file)
    create_department_nodes(neo4j_interface, department_file)

    # Create relationships
    create_gov_min_relationships(neo4j_interface, gov_min_file)
    create_min_dep_relationships(neo4j_interface, min_dep_file)

    print("Data successfully loaded into Neo4j.")

if __name__ == "__main__":
    load_data_to_neo4j()
