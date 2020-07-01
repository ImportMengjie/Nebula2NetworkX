import sys

from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient
from nebula.Common import *
from graph import ttypes

import nebula2networkx


def do_simple_execute(client, cmd):
    print("do execute %s" %cmd)
    resp = client.execute(cmd)
    if resp.error_code != 0:
        print('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))
        raise ExecutionException('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))


def test_response():
    client = GraphClient(connection_pool)
    auth_resp = client.authenticate('user', 'password')
    if auth_resp.error_code:
        raise AuthException("Auth failed")

    query_resp = client.execute_query('SHOW SPACES')
    do_simple_execute(client, 'use nba')
    query_resp = client.execute_query('GO FROM 100 OVER follow,serve yield follow._dst,serve._dst')
                
    query_resp = client.execute_query('fetch prop on * 100')
    for row in query_resp.rows:
        for ids, col in enumerate(row.columns):
            print(col)
    # query_resp = client.execute_query('FETCH PROP ON follow 100->102')
    # print(query_resp.column_names)
    # print(query_resp.rows)


if __name__=='__main__':

    g_ip = '127.0.0.1'
    g_port = 3699

    # init connection pool
    connection_pool = ConnectionPool(g_ip, g_port)
    test_response()


