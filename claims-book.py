from neo4j import GraphDatabase
import os

root_dir = '/Users/hp/workbench/data/json_dir'
quads_file = '/Users/hp/workbench/data/json_dir/quads.txt'


class NeoExec(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def exec(self, stmt):
        with self._driver.session() as session:
            result = session.write_transaction(self._exec, stmt)
            return result

    @staticmethod
    def _exec(tx, stmt):
        result = tx.run(stmt)
        return result


neo = NeoExec(uri="bolt://localhost:7687", user="neo4j", password="password")


def extract_quads(pages):
    if os.path.exists(quads_file):
        os.remove(quads_file)
    op_file = open(quads_file, "a")
    has_more = True
    skip = 0
    limit = 10
    while has_more == True and pages > 0:
        stmt: str = f"""match (v1:vendors)-[r1:worked_with]-(v2:vendors)-[r2:worked_with]-(v3:vendors)-[r3:worked_with]-(v4:vendors)
                where r1.claimnumber = r2.claimnumber = r3.claimnumber and id(v1) < id(v2) < id(v3) < id(v4)
                return v1.id, v2.id, v3.id, v4.id, r1.claimnumber, r2.claimnumber, r3.claimnumber skip {skip} limit {limit}"""
        result = neo.exec(stmt)
        pages -= 1
        if result == "none":
            has_more = False
        else:
            skip += limit
            limit += limit
        for record in result:
            result_line = ""
            for key in record.keys():
                result_line += record[key] + ","
            op_file.write(result_line + '\n')

extract_quads(100000)