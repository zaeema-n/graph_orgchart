import pandas as pd
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')

from neo4j_util.neo4j_interface import Neo4jInterface

# Initialize Neo4j interface
neo4j_interface = Neo4jInterface()

# Function to load and process transactions from files
def load_transactions():
    base_folder = os.path.join("..", "data/2015-10-15_2")  # Adjust path if needed
    files = {
        "Rename": os.path.join(base_folder, "RENAME.csv"),
        "Move": os.path.join(base_folder, "MOVE.csv"),
        "Add": os.path.join(base_folder, "ADD.csv")
    }

    transactions = []
    for file_type, file_path in files.items():
        df = pd.read_csv(file_path)
        df["file_type"] = file_type  # Add file type to each transaction
        transactions.extend(df.to_dict("records"))

    # Sort transactions by transaction_id
    transactions.sort(key=lambda x: x["transaction_id"])
    return transactions

# Function to handle renaming of a minister
def rename_minister(tx, transaction, entity_counters):
    try:

        # Increment minister counter and generate a new ID
        new_minister_id = f"gzt_min_{entity_counters['Minister']+1}"
        

        # Create new minister
        query_create_new = """
        MERGE (new:Minister {name: $new})
        ON CREATE SET new.id = $new_id
        """
        
        result = tx.run(query_create_new, new=transaction["new"], new_id=new_minister_id)
        print(f"Created new minister: {transaction['new']}, Result: {result.consume().counters.nodes_created} node(s) created")

        # Create relationship from Government (Government of Sri Lanka) to new Minister (HAS_MINISTER)
        query_create_relationship = """
        MATCH (gov:Government {name: 'Government of Sri Lanka'}), (new:Minister {name: $new})
        CREATE (gov)-[:HAS_MINISTER {start_time: $start_time, end_time: $end_time}]->(new)
        """

        result = tx.run(query_create_relationship, new=transaction["new"], start_time=transaction["date"], end_time=-1)
        print(f"Created HAS_MINISTER relationship, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Create relationships between the old minister's departments and the new minister
        query_transfer = """
        MATCH (old:Minister {name: $old})-[r:HAS_DEPARTMENT]->(d: Department)
        WHERE r.end_time = -1
        MATCH (new:Minister {name: $new})
        MERGE (new)-[:HAS_DEPARTMENT {start_time: $start_time, end_time: -1}]->(d)
        """

        result = tx.run(query_transfer, old=transaction["old"], new=transaction["new"], start_time=transaction["date"])
        print(f"Transferred departments to {transaction['new']}, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Terminate old government minister relationship
        query_terminate = """
        MATCH (:Government)-[r:HAS_MINISTER]->(old:Minister {name: $old})
        WHERE r.end_time = -1
        SET r.end_time = $end_time
        """

        result = tx.run(query_terminate, old=transaction["old"], end_time=transaction["date"])
        print(f"Terminated old minister relationships, Result: {result.consume().counters.properties_set} property(ies) set")

        # Terminate old minister to department relationships
        query_terminate_departments = """
        MATCH (old:Minister {name: $old})-[r:HAS_DEPARTMENT]->(d)
        WHERE r.end_time = -1
        SET r.end_time = $end_time
        """

        result = tx.run(query_terminate_departments, old=transaction["old"], end_time=transaction["date"])
        print(f"Terminated department relationships, Result: {result.consume().counters.properties_set} property(ies) set")

        # Create RENAMED_TO relationship between old and new ministers
        query_rename_rel = """
        MATCH (old:Minister {name: $old}), (new:Minister {name: $new})
        MERGE (old)-[:RENAMED_TO {start_time: $start_time}]->(new)
        """

        result = tx.run(query_rename_rel, old=transaction["old"], new=transaction["new"], start_time=transaction["date"])
        print(f"Created RENAMED_TO relationship, Result: {result.consume().counters.relationships_created} relationship(s) created")

        return entity_counters['Minister'] + 1
    
    except Exception as e:
        print(f"Error processing rename transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback
    

# Function to handle moving a department
def move_department(tx, transaction):
    try:
        # Create new relationships between the new minister parent and the department child
        query_create = """
        MATCH (new_parent:Minister {name: $new_parent}), (child:Department {name: $child})
        MERGE (new_parent)-[:HAS_DEPARTMENT {start_time: $start_time, end_time: -1}]->(child)
        """
        result = tx.run(query_create, new_parent=transaction["new_parent"], child=transaction["child"], start_time=transaction["date"])
        print(f"Created new department relationship, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Terminate the old parent to department relationships
        query_terminate = """
        MATCH (old_parent:Minister {name: $old_parent})-[r:HAS_DEPARTMENT]->(child:Department {name: $child})
        WHERE r.end_time = -1
        SET r.end_time = $end_time
        """
        result = tx.run(query_terminate, old_parent=transaction["old_parent"], child=transaction["child"], end_time=transaction["date"])
        print(f"Terminated old department relationship, Result: {result.consume().counters.properties_set} property(ies) set")

    except Exception as e:
        print(f"Error processing move transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback

def add_entity(tx, transaction, entity_counters):
    try:
        # Extract details from the transaction
        parent = transaction["parent"]
        child = transaction["child"]
        date = transaction["date"]
        parent_type = transaction["parent_type"]
        child_type = transaction["child_type"]
        rel_type = transaction["rel_type"]

        # Determine the ID for the new child entity using the child type
        if child_type not in entity_counters:
            raise ValueError(f"Unknown child type: {child_type}")
        
        prefix = f"gzt_{child_type[:3].lower()}"  # Use the first three letters of the type
        # print(child_type)
        # print(entity_counters[child_type])
        # print(type(entity_counters[child_type]))
        # print=f"entity counter: {entity_counters[child_type]}"
        entity_counter = entity_counters[child_type]+1
        new_entity_id = f"{prefix}_{entity_counter}"

        # Create the new child entity
        query_create_entity = f"""
        MERGE (child:{child_type} {{name: $child}})
        ON CREATE SET child.id = $entity_id
        """
        result = tx.run(query_create_entity, child=child, entity_id=new_entity_id)
        print(f"Created entity: {child} ({child_type}), Result: {result.consume().counters.nodes_created} node(s) created")

        # Create the relationship from the parent to the child
        query_create_relationship = f"""
        MATCH (parent:{parent_type} {{name: $parent}}), (child:{child_type} {{name: $child}})
        MERGE (parent)-[:{rel_type} {{start_time: $start_time, end_time: -1}}]->(child)
        """
        result = tx.run(query_create_relationship, parent=parent, child=child, start_time=date)
        print(f"Created relationship from {parent} to {child}, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Increment the counter for the specific entity type
        return entity_counter

    except Exception as e:
        print(f"Error processing add transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback



# Main function to load transactions and execute them in order
def execute_transactions():
    transactions = load_transactions()
    
    # minister_counter = 1  # Counter for new ministers (e.g., gzt_min_1)
    # department_counter = 1  # Counter for new departments (e.g., gzt_dep_1)

    entity_counters = {"Minister": 0, "Department": 0}  # Initialize counters for entity types
    # print(entity_counters["Minister"])
    # print(entity_counters["Department"])
    
    
    with neo4j_interface.driver.session() as session:
        with session.begin_transaction() as tx:
            for transaction in transactions:
                try:
                    # Identify the correct function to call based on file type and type
                    if transaction["file_type"] == "Rename" and transaction["type"] == "minister":
                        entity_counters["Minister"] = rename_minister(tx, transaction, entity_counters)
                        print(f"Processed Rename Minister transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Move" and transaction["type"] == "department":
                        move_department(tx, transaction)
                        print(f"Processed Move Department transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Add":
                        new_counter = add_entity(tx, transaction, entity_counters)
                        entity_counters[transaction["child_type"]] = new_counter
                        print(f"Processed Add transaction: {transaction['transaction_id']}")
                except Exception as e:
                    print(f"Error processing transaction: {transaction['transaction_id']}, Error: {e}")
                    tx.rollback()
                    return  # Exit early on failure

            # Commit the transaction if no errors occurred
            tx.commit()
            print("All transactions successfully committed")

if __name__ == "__main__":
    execute_transactions()
