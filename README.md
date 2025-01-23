# OrgChart 2.0

## Prerequisites

```bash
mamba create -n graph_orgchart
```

## Setup Neo4j

### Environment Variables

Before using the `Neo4jInterface`, ensure the following environment variables are set:

- `NEO4J_URI`: The URI of your Neo4j database.
- `NEO4J_USER`: The username for your Neo4j database.
- `NEO4J_PASSWORD`: The password for your Neo4j database.

You can set these variables in your shell like this:

```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j_username
export NEO4J_PASSWORD=your_password
```

```bash
docker build --build-arg NEO4J_USER=$NEO4J_USER --build-arg NEO4J_PASSWORD=$NEO4J_PASSWORD -t graph_orgchart .
```

Ensure you have a `.env` file in your project directory with the following content:

```plaintext
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

### Running the Docker Container

To run the Docker container with the environment variables from the `.env` file (for the first time running the container after building), use the following command:

```bash
docker run -p 7474:7474 -p 7687:7687 --name graph_orgchart_server \
    --env-file .env \
    -v neo4j_data:/data graph_orgchart:latest
```

To run the Docker container again after it has been created, use the following command:

```bash
docker start graph_orgchart_server
```

This allows you to interact with the running Docker container by opening a shell (terminal) session inside it.

```bash
docker exec -it graph_orgchart_server bash
```

## Inserting data into the database

To insert the initial data, run `orgchart/setup_db.py`. This file inserts the full tabular data from the `2015-09-21` gazette (csv files found at `data/2015-09-21`).

To update the db with a new amendment, run `orgchart/update_orgchart.py`. This file modifies the db according to the `2015-10-15` gazette amendment (csv files found at `data/2015-10-15_2`).

### Viewing data

To directly view and interact with the database, visit `localhost:7474`. Try out the following cypher query:

```cypher
match(g:government)-[r]->(m:minister)-[y]->(d:department)
where m.name="Minister of University Education and Highways" or m.name="Minister of Higher Education and Highways" or m.name="Minister of Higher Education and Highways" or m.name="Minister of Skills Development and Vocational Training"
return m,g,d
```
