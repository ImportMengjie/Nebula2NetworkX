#! /bin/python3
from neo4j import GraphDatabase
import json


def dict2json(data:dict):
    parse_str = ''
    for key, value in data.items():
        parse_str+=','+key+":"+('"'+str(value)+'"' if isinstance(value, str) else str(value))+" "
    return "{ "+parse_str[1:]+"}"

def addVertex(tx, element:dict):
    for data in element['data']:
        statement = "MERGE (:{} {})".format(element["name"], dict2json(data))
        tx.run(statement)

def addEdge(tx, element:dict):
    for data in element["data"]:
        from_statement = "(a:{} {})".format(data["from"]["vertex"], dict2json(data["from"]["match"]))
        to_statement = "(b:{} {})".format(data["to"]["vertex"], dict2json(data["to"]["match"]))
        edge_statement = '[c:{} {}]'.format(element["name"], dict2json(data["data"]))
        statement = "MATCH {} MATCH {} MERGE (a)-{}->(b) return a,b,c".format(from_statement, to_statement, edge_statement)
        tx.run(statement)

if __name__=='__main__':

    import argparse

    parser = argparse.ArgumentParser(description='import json file to neo4j')

    parser.add_argument('-f', '--file', help='json file path', type=str, required=True)
    parser.add_argument('-a', '--address', help='neo4j address', type=str, required=True)
    parser.add_argument('-p', '--password', help='neo4j password', type=str, required=True)
    parser.add_argument('-u', '--user', help='neo4j user name', type=str, default="neo4j")
    args = parser.parse_args()

    # driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "limengjie"))

    driver = GraphDatabase.driver(args.address, auth=(args.user, args.password))
    with open(args.file) as f, driver.session() as session:
        data = json.load(f)
        for v in data["vertex"]:
            session.write_transaction(addVertex, v)
        for e in data["edge"]:
            session.write_transaction(addEdge, e)
