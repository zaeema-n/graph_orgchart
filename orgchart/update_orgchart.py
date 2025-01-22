import pandas as pd
import sys
import os
from datetime import datetime

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
        "Add": os.path.join(base_folder, "ADD.csv"),
        "Terminate": os.path.join(base_folder, "TERMINATE.csv"),
        "Merge": os.path.join(base_folder, "MERGE.csv")
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
        
        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

        # Create new minister
        add_entity_transaction = {
            "parent": "Government of Sri Lanka",
            "child": transaction["new"],
            "date": transaction["date"],
            "parent_type": "government",
            "child_type": "minister",
            "rel_type": "HAS_MINISTER"
        }

        # Call add_entity to create the new minister and establish the relationship
        new_minister_counter = add_entity(tx, add_entity_transaction, entity_counters)

        # Create relationships between the old minister's departments and the new minister
        query_transfer = """
        MATCH (old:minister {name: $old})-[r:HAS_DEPARTMENT]->(d)
        WHERE r.end_date IS NULL
        MATCH (new:minister {name: $new})
        MERGE (new)-[:HAS_DEPARTMENT {start_date: date($start_date)}]->(d)
        """
        result = tx.run(query_transfer, old=transaction["old"], new=transaction["new"], start_date=transaction["date"])
        print(f"Transferred departments to {transaction['new']}, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Terminate old government-to-minister relationship
        terminate_gov_minister_transaction = {
            "parent": "Government of Sri Lanka",
            "child": transaction["old"],
            "date": transaction["date"],
            "parent_type": "government",
            "child_type": "minister",
            "rel_type": "HAS_MINISTER"
        }
        terminate_entity(tx, terminate_gov_minister_transaction)

        # Terminate old minister-to-department relationships
        query_terminate_departments = """
        MATCH (old:minister {name: $old})-[r:HAS_DEPARTMENT]->(d)
        WHERE r.end_date IS NULL
        SET r.end_date = date($end_date)
        """
        result = tx.run(query_terminate_departments, old=transaction["old"], end_date=transaction["date"])
        print(f"Terminated department relationships, Result: {result.consume().counters.properties_set} property(ies) set")

        # Create RENAMED_TO relationship between old and new ministers
        query_rename_rel = """
        MATCH (old:minister {name: $old}), (new:minister {name: $new})
        MERGE (old)-[:RENAMED_TO {date: date($date)}]->(new)
        """
        result = tx.run(query_rename_rel, old=transaction["old"], new=transaction["new"], date=transaction["date"])
        print(f"Created RENAMED_TO relationship, Result: {result.consume().counters.relationships_created} relationship(s) created")

        return new_minister_counter

    except Exception as e:
        print(f"Error processing rename transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback
    

# Function to handle moving a department
def move_department(tx, transaction):
    try:

        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

        # Create new relationships between the new minister parent and the department child
        query_create = """
        MATCH (new_parent:minister {name: $new_parent}), (child:department {name: $child})
        MERGE (new_parent)-[:HAS_DEPARTMENT {start_date: date($start_date)}]->(child)
        """
        result = tx.run(query_create, new_parent=transaction["new_parent"], child=transaction["child"], start_date=transaction["date"])
        print(f"Created new department relationship, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Terminate the old parent to department relationships
        terminate_relationship_transaction = {
            "parent": transaction["old_parent"],
            "child": transaction["child"],
            "date": transaction["date"],
            "parent_type": "minister",
            "child_type": "department",
            "rel_type": "HAS_DEPARTMENT"
        }
        terminate_entity(tx, terminate_relationship_transaction)

    except Exception as e:
        print(f"Error processing move transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback

def add_entity(tx, transaction, entity_counters):
    try:

        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

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
        MERGE (parent)-[:{rel_type} {{start_date: date($start_date)}}]->(child)
        """
        result = tx.run(query_create_relationship, parent=parent, child=child, start_date=date)
        print(f"Created relationship from {parent} to {child}, Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Increment the counter for the specific entity type
        return entity_counter

    except Exception as e:
        print(f"Error processing add transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback

def terminate_entity(tx, transaction):
    try:

        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

        # Extract details from the transaction
        parent = transaction["parent"]
        child = transaction["child"]
        date = transaction["date"]
        parent_type = transaction["parent_type"]
        child_type = transaction["child_type"]
        rel_type = transaction["rel_type"]

        # Match the parent and child nodes and the specified relationship
        query_terminate_relationship = f"""
        MATCH (parent:{parent_type} {{name: $parent}})-[rel:{rel_type}]-(child:{child_type} {{name: $child}})
        WHERE rel.end_date is NULL
        SET rel.end_date = date($end_date)
        """
        result = tx.run(query_terminate_relationship, parent=parent, child=child, end_date=date)
        print(f"Terminated relationship from {parent} to {child}, Result: {result.consume().counters.properties_set} property(ies) set")

    except Exception as e:
        print(f"Error processing terminate transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback

def merge_ministers(tx, transaction, entity_counters):
    try:

        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

        # Parse the old ministers array from the transaction
        old_ministers = eval(transaction["old"])  # Convert string representation of array to list
        new_minister = transaction["new"]
        date = transaction["date"]
        
        # Generate a unique ID for the new minister
        if "minister" not in entity_counters:
            raise ValueError("Entity counter for 'Minister' is not initialized")
        

        # Create the new minister entity

        # Use add_entity to create the new minister entity and establish the relationship with the government
        add_entity_transaction = {
            "parent": "Government of Sri Lanka",
            "child": new_minister,
            "date": date,
            "parent_type": "government",
            "child_type": "minister",
            "rel_type": "HAS_MINISTER"
        }
        
        new_minister_counter = add_entity(tx, add_entity_transaction, entity_counters)
    
        
        # Loop through each old minister to transfer relationships and terminate them
        for old_minister in old_ministers:
            # Transfer relationships from old minister's departments to new minister
            query_transfer_departments = """
            MATCH (old:minister {name: $old})-[r:HAS_DEPARTMENT]->(dept:department)
            WHERE r.end_date is NULL
            MATCH (new:minister {name: $new})
            MERGE (new)-[:HAS_DEPARTMENT {start_date: date($date)}]->(dept)
            """
            result = tx.run(query_transfer_departments, old=old_minister, new=new_minister, date=date)
            print(f"Created relationship(s) from {old_minister} department(s) to {new_minister}, "
                  f"Result: {result.consume().counters.relationships_created} relationship(s) created")

            # Terminate government -> old minister relationship

            terminate_entity_transaction = {
                "parent": "Government of Sri Lanka",
                "child": old_minister,
                "date": date,
                "parent_type": "government",
                "child_type": "minister",
                "rel_type": "HAS_MINISTER"
            }

            terminate_entity(tx, terminate_entity_transaction)

            # Terminate old minister -> department relationships
            query_terminate_department_relations = """
            MATCH (old:minister {name: $old})-[r:HAS_DEPARTMENT]->(dept:department)
            WHERE r.end_date is NULL
            SET r.end_date = date($date)
            """
            result = tx.run(query_terminate_department_relations, old=old_minister, date=date)
            print(f"Terminated department relationships for {old_minister}, "
                  f"Result: {result.consume().counters.properties_set} property(s) updated")

            # Create old minister -> new minister MERGED_INTO relationship
            query_create_merged_into = """
            MATCH (old:minister {name: $old}), (new:minister {name: $new})
            MERGE (old)-[:MERGED_INTO {date: date($date)}]->(new)
            """
            result = tx.run(query_create_merged_into, old=old_minister, new=new_minister, date=date)
            print(f"Created MERGED_INTO relationship from {old_minister} to {new_minister}, "
                  f"Result: {result.consume().counters.relationships_created} relationship(s) created")

        return new_minister_counter

    except Exception as e:
        print(f"Error processing merge transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback

def merge_departments(tx, transaction, entity_counters):
    try:

        if isinstance(transaction["date"], str):
            transaction["date"] = datetime.strptime(transaction["date"].strip(), "%Y-%m-%d").date()

        # Extract details from the transaction
        print(transaction["old"])
        old_departments = eval(transaction["old"])  # Convert string representation of list to actual list
        new_department = transaction["new"]
        date = transaction["date"]

        # Determine the ID for the new department
        if "department" not in entity_counters:
            raise ValueError("Department counter not initialized")
        
        # entity_counters["Department"] += 1
        new_department_id = f"gzt_dep_{entity_counters['department']+1}"

        # Step 1: Create the new department
        query_create_department = """
        MERGE (new:department {name: $new})
        ON CREATE SET new.id = $new_id
        """
        result = tx.run(query_create_department, new=new_department, new_id=new_department_id)
        print(f"Created new department: {new_department}, Result: {result.consume().counters.nodes_created} node(s) created")

        # Create relationship from the minister of old department to the new department
        query_create_minister_relationship = """
        MATCH (minister:minister)-[rel:HAS_DEPARTMENT]->(old:department {name: $old})
        WHERE rel.end_date is NULL
        WITH minister, old
        MATCH (new:department {name: $new})
        MERGE (minister)-[:HAS_DEPARTMENT {start_date: date($date)}]->(new)
        """
        result = tx.run(query_create_minister_relationship, old=old_departments[0], new=new_department, date=date)
        print(f"Created relationship from minister of {old_departments[0]} to {new_department}, "
                f"Result: {result.consume().counters.relationships_created} relationship(s) created")

        # Step 2: Handle relationships for each old department
        for old_department in old_departments:
            
            # Terminate relationship between minister and old department
            query_terminate_minister_relationship = """
            MATCH (minister:minister)-[rel:HAS_DEPARTMENT]->(old:department {name: $old})
            WHERE rel.end_date is NULL
            SET rel.end_date = $date
            """
            result = tx.run(query_terminate_minister_relationship, old=old_department, date=date)
            print(f"Terminated relationship from minister to {old_department}, "
                  f"Result: {result.consume().counters.properties_set} property(s) updated")

            # Create MERGED_INTO relationship
            query_create_merged_into = """
            MATCH (old:department {name: $old}), (new:department {name: $new})
            CREATE (old)-[:MERGED_INTO {date: date($date)}]->(new)
            """
            result = tx.run(query_create_merged_into, old=old_department, new=new_department, date=date)
            print(f"Created MERGED_INTO relationship from {old_department} to {new_department}, "
                  f"Result: {result.consume().counters.relationships_created} relationship(s) created")

        return entity_counters["department"]+1

    except Exception as e:
        print(f"Error processing merge departments transaction: {transaction['transaction_id']}, Error: {e}")
        raise  # Rethrow the exception to allow rollback


# Main function to load transactions and execute them in order
def execute_transactions():
    transactions = load_transactions()
    
    entity_counters = {"minister": 0, "department": 0}  # Initialize counters for entity types
    
    
    with neo4j_interface.driver.session() as session:
        with session.begin_transaction() as tx:
            for transaction in transactions:
                try:
                    # Identify the correct function to call based on file type and type
                    if transaction["file_type"] == "Rename" and transaction["type"] == "minister":
                        entity_counters["minister"] = rename_minister(tx, transaction, entity_counters)
                        print(f"Processed Rename Minister transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Move" and transaction["type"] == "department":
                        move_department(tx, transaction)
                        print(f"Processed Move Department transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Add":
                        new_counter = add_entity(tx, transaction, entity_counters)
                        entity_counters[transaction["child_type"]] = new_counter
                        print(f"Processed Add transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Terminate":
                        terminate_entity(tx, transaction)
                        print(f"Processed Terminate transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Merge" and transaction["type"] == "minister":
                        entity_counters['minister'] = merge_ministers(tx, transaction, entity_counters)
                        print(f"Processed Merge Ministers transaction: {transaction['transaction_id']}")
                    elif transaction["file_type"] == "Merge" and transaction["type"] == "department":
                        entity_counters["department"] = merge_departments(tx, transaction, entity_counters)
                        print(f"Processed Merge Departments transaction: {transaction['transaction_id']}")

                except Exception as e:
                    print(f"Error processing transaction: {transaction['transaction_id']}, Error: {e}")
                    tx.rollback()
                    return  # Exit early on failure

            # Commit the transaction if no errors occurred
            tx.commit()
            print("All transactions successfully committed")

if __name__ == "__main__":
    execute_transactions()
