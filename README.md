# SAVANA - tabular-to-graph-data ETL process

Scala project to transform given tabular data into suitable dataset for graph databases

## The project
The whole project has been developed in scala, and the data-processing is done with Spark.
The data-processing is managed in two layers:
* Harmonization:
  * Generates the calculated field _patient_days_age_
  * Transformation of the fields _direct_parents_ and _direct_children_ from StringType into Spark ArrayType
* Functional: 
  * Selects the required fields from both datasets (_apparitions_ and _concepts_) to create the final tables.  
  * For the relationship between concepts the code expands the array in _direct_parents_ for each concept and creates a new record for each parent.

All the operations in the different layers are done in simultaneously.<br>
For testing purposes, the input data is located in the folder
```
src/test/resources
```

And the output of the process can be found at
```
src/test/resources/svn_output
```

Since this project has been developed without knowledge of the environment where it would be executed, some adjustments in the code must be done in order to get the final data instead of the testing data before the release.

## Why Spark and Scala?
Spark is currently the go-to solution for projects like this, where the data can be up to several terabytes on each run.
<br>Spark supports both Scala and Python languages, but scala is more efficient than Python and supports true multithreading.
    
## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites
Next items need to be installed and properly configured to ensure the tests work without any problem:
* sbt
* Java SDK 1.8
* Scala 2.12.13

Before executing any test the following command must be executed
```
sbt compile
```

## Running the tests
```
sbt clean test
```

## Output
To verify the result, set up a Graph DB and load the output files. In __Neo4j__:
* Create a new project
* Rename the files and copy them into the import folder of the project
* Run the following command
```
LOAD CSV WITH HEADERS FROM "file:///document.csv" as row CREATE (d:Document) SET d.id = row.document_id, d.date = row.document_date;
LOAD CSV WITH HEADERS FROM "file:///concept.csv" as row CREATE (c:Concept) SET c.id = row.concept_id, c.fsn = row.fsn;
LOAD CSV WITH HEADERS FROM "file:///patient.csv" as row CREATE (p:Patient) SET p.id = row.patient_id, p.gender = row.gender;
LOAD CSV WITH HEADERS FROM "file:///is_mentioned_in.csv" AS row MATCH (c:Concept {id: row.concept_id}) MERGE (d:Document {id: row.document_id}) WITH c, d MERGE (c)-[:MENTIONED_IN]->(d);
LOAD CSV WITH HEADERS FROM "file:///applies_to.csv" AS row MATCH (c:Document {id: row.document_id}) MERGE (d:Patient {id: row.patient_id}) WITH c, d MERGE (c)-[:APPLIES_TO]->(d);
LOAD CSV WITH HEADERS FROM "file:///is_a.csv" AS row MATCH (c:Concept {id: row.concept_id}) MERGE (d:Concept {id: row.is_a}) WITH c, d MERGE (c)-[:IS_A]->(d);
LOAD CSV WITH HEADERS FROM "file:///is_related_to.csv" AS row MATCH (c:Concept {id: row.concept_id}) MERGE (d:Patient {id: row.patient_id}) WITH c, d, row MERGE (c)-[:IS_RELATED_TO {DocumentDate: row.document_date, PatientDaysAge: row.patient_days_age, DocumentID: row.document_id}]->(d);
```

The result must show an graph with a similar appearance to this:<br>
![alt text](src/test/resources/img/svnGraph.png?raw=true)

## Other considerations
Since the input data has been given as a text sample, some modifications to it had to be done in order to avoid parsing issues:
* Replace the default delimiter (',' is a common character that usually can be found in the values, the \t character avoids this problem)
* Quote the text fields. Instead of
    ```
    document_date,patient_id,gender,birthdate,document_id,concept_id
    2018-08-11,5487589844736575979,male,1952-04-05,1400000001241646,105
    ```
    it should be
    ```
    document_date,patient_id,gender,birthdate,document_id,concept_id
    "2018-08-11",5487589844736575979,"male","1952-04-05",1400000001241646,105
    ```
* Replace the delimiter in the array-like data (_direct_parents_ and _direct_children_ fields)