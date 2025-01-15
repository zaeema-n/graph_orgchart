# doctracer

## Prerequisites

```bash
mamba create -n graph_orgchart
```

### Setup Neo4j

#### Environment Variables

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


#### Running the Docker Container

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