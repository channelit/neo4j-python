from neo4j import GraphDatabase
import json
import glob
import os

root_dir = '/Users/hp/workbench/data/json_dir'


class IngestNeo4J(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create(self, stmt):
        with self._driver.session() as session:
            result = session.write_transaction(self._create, stmt)
            print(result.value(0))

    @staticmethod
    def _create(tx, stmt):
        result = tx.run(stmt)
        return result


ingest = IngestNeo4J(uri="bolt://localhost:7687", user="neo4j", password="password")
ctr = 0


def jsonToCreate(json, objectname):
    creates = []
    for item in json:
        fields = ""
        for a, v in item.items():
            if v:
                if not fields == "":
                    fields += ","
                fields += a + ":" + '"' + str(v).strip() + '"'
            create = "create (:" + objectname + "{" + fields + "})"
        creates.append(create)
    return creates


for filename in glob.iglob(root_dir + '**/*/*.json', recursive=True):
    ctr += 1
    with open(filename) as f:
        print(filename)
        data = json.load(f)
        creates = jsonToCreate(data, os.path.splitext(os.path.basename(f.name))[0])
        for create in creates:
            ingest.create(stmt=create)
