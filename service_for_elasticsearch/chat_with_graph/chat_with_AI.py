from langchain.chains import GraphCypherQAChain
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
import os
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define the cypher generation template
cypher_generation_template = """
You are an expert Neo4j Cypher translator who converts English to Cypher based on the Neo4j Schema provided, following the instructions below:
1. Generate Cypher query compatible ONLY for Neo4j Version 5
2. Do not use EXISTS, SIZE, HAVING keywords in the cypher. Use alias when using the WITH keyword
3. Use only Nodes and relationships mentioned in the schema
4. Always do a case-insensitive and fuzzy search for any properties related search. Eg: to search for a Client, use `toLower(client.id) contains 'neo4j'`. To search for Slack Messages, use 'toLower(SlackMessage.text) contains 'neo4j'`. To search for a project, use `toLower(project.summary) contains 'logistics platform' OR toLower(project.name) contains 'logistics platform'`.)
5. Never use relationships that are not mentioned in the given schema
6. When asked about projects, Match the properties using case-insensitive matching and the OR-operator, E.g, to find a logistics platform -project, use `toLower(project.summary) contains 'logistics platform' OR toLower(project.name) contains 'logistics platform'`.
7. In every request, you should select the project_id that is given at that moment.

schema: {schema}

Examples:
Question: Can you tell me about my graph? (project_id : '478fdffc-8384-4da9-8496-0d530faa76cda0')
Answer: ```MATCH (n)
WHERE n.project_id = '478fdffc-8384-4da9-8496-0d530faa76cda0'
WITH DISTINCT 'Node' AS elementType, labels(n) AS label_or_type, keys(n) AS properties
RETURN elementType, label_or_type AS labelOrType, properties

UNION

MATCH ()-[r]->()
WHERE r.project_id = '478fdffc-8384-4da9-8496-0d530faa76cda0'
WITH DISTINCT 'Relationship' AS elementType, type(r) AS label_or_type, keys(r) AS properties
RETURN elementType, label_or_type AS labelOrType, properties

Question: Tell me about a person named \"Marie Curie\"? (project_id : '478fdffc-8384-4da9-8496-0d530faa76cda0')
Answer: ```MATCH (p:Person)-[r]-(s)
WHERE toLower(p.name) contains 'marie curie' AND toLower(p.project_id) = '321hg2h1g3123'
RETURN p, r, s

Question: Which client's projects use most of our people? (project_id : '478fdffc-8384-4da9-8496-0d530faa76cda0')
Answer: ```MATCH (c:CLIENT)<-[:HAS_CLIENT]-(p:Project)-[:HAS_PEOPLE]->(person:Person)
WHERE toLower(c.project_id) = toLower('31hg2h1g31231') AND toLower(p.project_id) = toLower('31hg2h1g31231') and toLower(person.project_id) = toLower('31hg2h1g31231') AND toLower(HAS_CLIENT.project_id) = toLower('31hg2h1g31231') AND toLower(HAS_PEOPLE.project_id) = toLower('31hg2h1g31231')
RETURN c.name AS Client, COUNT(DISTINCT person) AS NumberOfPeople
ORDER BY NumberOfPeople DESC```

Question: Which person uses the largest number of different technologies?
Answer: ```MATCH (person:Person)-[:USES_TECH]->(tech:Technology)
RETURN person.name AS PersonName, COUNT(DISTINCT tech) AS NumberOfTechnologies
WHERE toLower(person.project_id) = toLower('31hg2h1g31231')
ORDER BY NumberOfTechnologies DESC```

Question: {question}
"""

# Define the PromptTemplate for Cypher generation
cypher_prompt = PromptTemplate(
    template=cypher_generation_template,
    input_variables=["schema", "question"]
)

# Define the QA Template
CYPHER_QA_TEMPLATE = """You are an assistant that helps to form nice and human understandable answers.
The questions will be asked through the AI chat of the Araks system, during which you have to answer the questions asked by the user.
Answers must be specific to the graph with project_id only.
The information part contains the provided information that you must use to construct an answer.
The provided information is authoritative, you must never doubt it or try to use your internal knowledge to correct it.
Make the answer sound as a response to the question. Do not mention that you based the result on the given information.
If the provided information is empty, say that you don't know the answer.
Final answer should be easily readable and structured.
project_id is not provided by the user but is added by the system, so it should not be specified
The response should not specify which project ID it is referring to and it is not allowed to mention project id in the response in any way!
In case of a question about specific nodes, give answers about specific nodes.
All graphs are located in Araks, which is a web-graph service that works with graphs, you can mention about Araks sometimes in your response.
Answers must be formulated taking into account the limits of the model (4096 tokens).
In case of a question about a particular node or connection, try to give all the information about it, taking into account its properties and connections.
Information:
{context}

Question: {question}
"""

# Define the PromptTemplate for QA
qa_prompt = PromptTemplate(
    input_variables=["context", "question"], template=CYPHER_QA_TEMPLATE
)

# Print the OPENAI_API_KEY to ensure it's loaded
print("OpenAI API Key:", os.getenv('OPENAI_API_KEY'))

# Fetch and print Neo4j connection details to debug
neo4j_url = os.getenv('NEO4JURL')
neo4j_user = os.getenv('NEO4JUSER')
neo4j_password = os.getenv('NEO4JPASSWORD')

print("Neo4j URL:", neo4j_url)
print("Neo4j User:", neo4j_user)
# Avoid printing the password for security reasons

try:
    # Initialize Neo4jGraph
    graph = Neo4jGraph(
        url=neo4j_url, 
        username=neo4j_user, 
        password=neo4j_password
    )
    
    graph.refresh_schema()

    # Initialize GraphCypherQAChain
    cypher_chain = GraphCypherQAChain.from_llm(
        llm=ChatOpenAI(temperature=0, model_name='gpt-4'), # type: ignore
        graph=graph,
        verbose=True,
        return_intermediate_steps=False,
        cypher_prompt=cypher_prompt,
        qa_prompt=qa_prompt
    )

except Exception as e:
    raise ValueError("Error initializing GraphCypherQAChain: " + str(e))

async def get_bot_response(message, project_id):
    try:
        response = cypher_chain(message + f" (project_id : {project_id})")
    except Exception as e:
        print("Error during Cypher query generation:", str(e))
        try:
            response = {"result": re.search(r"\"Answer: (.*)\"", str(e)).group(1)} # type: ignore
        except Exception as nested_e:
            print("Error during response extraction:", str(nested_e))
            response = {"result": "Sorry, but I don't know the answer to your question, please try to ask the question in a slightly different way."}
    return response["result"]
