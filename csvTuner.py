# WORKS FOR EXACTLY 1 RELATION TYPE "relates".
# ANY NUMBER OF NODES/RELATIONSHIPS ALLOWED.
# ARGUMENTS: 1. CSV file of exported neo4j data. 2. CSV file to create and import into postgres.
# Run "CALL apoc.export.csv.all("test.csv", {})" in neo4J and pass in "test.csv" as argument 1.

# FORMAT OF THIS CONVERTOR:
# Everything in same table
# 1 column ("relates") will have type integer[] storing nodeID's of nodes the given node is connected to.

# PSQL SCHEMA:
# _id [PK] integer, _labels character varying, uuid, character varying, relates integer[]

import re
import sys

graphExportCsvFile = sys.argv[1]
relationalImportCsvFile = sys.argv[2]

csvFile = open(graphExportCsvFile, 'r')
charList = csvFile.read()
# Removing all quotation marks Neo4j leaves behind.
charList = charList.replace("\"", "")

with open(graphExportCsvFile, "w") as file:
    file.write(charList)

csvFile.close()

csvFile = open(graphExportCsvFile, 'r')
lineList = csvFile.readlines()
csvFile.close()

nodesStartIndex = 1
relationshipStartIndex = 999  # Will Change.
counter = 0
headers = lineList[0]

# Loop that goes through the lines until they start with a comma.
# This is when the relationships start getting listed.
for line in lineList:
    if line[0] == ',':
        relationshipStartIndex = counter
        break
    counter += 1

numberOfNodes = relationshipStartIndex - 1
numberOfRelationships = (len(lineList) - numberOfNodes - 1)

nodesList = lineList[nodesStartIndex:relationshipStartIndex]
relationshipsList = lineList[relationshipStartIndex:]

# Clean up headers
headers = headers.replace("_start,_end,_type", "relates")  # If relationship isn't relates, this won't work (easy fix).

# Clean up lists
nodesList = [node.strip() for node in nodesList]
relationshipsList = [rel.strip() for rel in relationshipsList]

for node in range(len(nodesList)):
    nodesList[node] = nodesList[node].replace(",,,", ",{")  # Closing bracket left out for ease of adding values.

for relationship in range(len(relationshipsList)):
    relationshipsList[relationship] = relationshipsList[relationship].replace(",,,", "")
    relationshipsList[relationship] = relationshipsList[relationship].replace(",Relates", "")

# Create a dictionary. The keys are all the node ID's. The values initialized to empty list.
relationshipsDict = {}
for node in range(numberOfNodes):
    relationshipsDict[node] = []

for relationship in relationshipsList:
    fromAndToNodes = relationship.split(',')
    fromNode = fromAndToNodes[0]
    toNode = fromAndToNodes[1]
    relationshipsDict[int(fromNode)].append(toNode)


###################################################################
# Clean up of NEO4J export finished. Extracted all important info:
# numberOfNodes
# numberOfRelationships
# nodesList
# relationshipsList
# headers
# relationshipsDict
###################################################################


updatedCsvFile = open(relationalImportCsvFile, "w")

headers = headers.replace(",", "|")
updatedCsvFile.write(headers)

counter = 0
for node in nodesList:
    line = node
    for connection in relationshipsDict[counter]:
        line = line + connection + ","
    if line[-1:] == ',':
        line = line[:-1]
    line = line + "}\n"
    counter += 1

# DELIMITER ISSUE - NEED TO CHANGE COMMA TO BAR
    line = re.sub(",", "|", line, count=3)

    updatedCsvFile.write(line)

updatedCsvFile.close()
