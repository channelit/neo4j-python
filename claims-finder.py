from neo4j import GraphDatabase

root_dir = '/Users/hp/workbench/data/json_dir'


class NeoExec(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def exec(self, stmt):
        with self._driver.session() as session:
            result = session.write_transaction(self._exec, stmt)
            print(result.value(0))

    @staticmethod
    def _exec(tx, stmt):
        result = tx.run(stmt)
        return result


neo = NeoExec(uri="bolt://localhost:7687", user="neo4j", password="password")
ctr = 0


def find_overlap():
    stmt = """match p=(v1:vendors)-[r1:worked_with]-(v2:vendors)-[r2:worked_with]-(v3:vendors)-[r3:worked_with]-(
    v4:vendors) where r1.claimnumber = r2.claimnumber = r3.claimnumber and id(v1) < id(v2) < id(v3) < id(v4) 
    with p, collect([[v1.id, v2.id, v3.id, v4.id], [r1.claimnumber, r2.claimnumber, r3.claimnumber]]) as rs 
    return p, rs """
    return neo.exec(stmt)

print (find_overlap())
