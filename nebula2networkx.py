import sys

from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient
from nebula.Common import *
from graph import ttypes
# from nebula.AsyncClient import AsyncGraphClient

import networkx as nx


def do_simple_execute(client, cmd):
    print("do execute %s" %cmd)
    resp = client.execute(cmd)
    if resp.error_code != 0:
        print('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))
        raise ExecutionException('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))

def fetch_info(client:GraphClient, key:str):
    return client.execute_query("FETCH PROP ON "+key)

def handle_fetch_resp(resp):
    if not resp.rows:
        return []
    ret = []
    for row in resp.rows:
        if len(resp.column_names) != len(row.columns):
            continue
        name_value_list = {}
        for name, col in zip(resp.column_names[:], row.columns[:]):
            name = str(name)
            if '.' in name:
                name = name.split('.')[1]

            if col.getType() == ttypes.ColumnValue.__EMPTY__:
                print('ERROR: type is empty')
            elif col.getType() == ttypes.ColumnValue.BOOL_VAL:
                name_value_list[name] = col.get_bool_val()
            elif col.getType() == ttypes.ColumnValue.INTEGER:
                name_value_list[name] = col.get_integer()
            elif col.getType() == ttypes.ColumnValue.ID:
                name_value_list[name] = col.get_id()
            elif col.getType() == ttypes.ColumnValue.STR:
                name_value_list[name] = col.get_str().decode('utf-8')
            elif col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                name_value_list[name] = col.get_double_precision()
            elif col.getType() == ttypes.ColumnValue.TIMESTAMP:
                name_value_list[name] = col.get_timestamp()
            else:
                print('ERROR: Type unsupported')
        ret.append(name_value_list)
    return ret
    

def nebula2networkx(client:GraphClient, nebula_space:str ,graph:nx.MultiDiGraph ,vertex_list, edge_types):
    do_simple_execute(client, 'use '+nebula_space)
    yield_statement = ",".join([edge_type+"._dst" for edge_type in edge_types])
    seen_vertex = set()
    queue_vertex = []
    all_edges = {}
    for v in vertex_list:
        queue_vertex.insert(0, v)
    while len(queue_vertex):
        vertex = queue_vertex.pop()
        seen_vertex.add(vertex)
        get_edge_go_statement = "GO FROM {} OVER {} YIELD ".format(vertex, ','.join(edge_types))+yield_statement
        edges_resp = client.execute_query(get_edge_go_statement)
        edges = [[] for _ in edge_types]
        if edges_resp.rows is not None:
            for row in edges_resp.rows:
                for ids,col in enumerate(row.columns):
                    if(col.getType() == ttypes.ColumnValue.ID) and col.get_id()!=0:
                        edges[ids].append(col.get_id())
                        if col.get_id() not in seen_vertex:
                            seen_vertex.add(col.get_id())
                            queue_vertex.insert(0, col.get_id())
        all_edges[vertex] = edges
        # build networkX graph Node
        vertex_info_resp = fetch_info(client, "* "+str(vertex))
        vertex_info = handle_fetch_resp(vertex_info_resp)
        graph.add_node(vertex, **vertex_info[0] if len(vertex_info)>0 else {})

    # build networkX graph Edge
    for vertex_src, edges in all_edges.items():
        for edge_type_ids, vertexs_dst in enumerate(edges):
            if len(vertexs_dst)!=0:
                edge_info_fetch_statement = edge_types[edge_type_ids]+ ' '+','.join([str(vertex_src)+"->"+str(dst) for dst in vertexs_dst])

                edges_info = handle_fetch_resp(fetch_info(client, edge_info_fetch_statement))
                graph.add_edges_from([(vertex_src, vertexs_dst[i], edges_info[i]) for i in range(len(edges_info))])


def draw_networkx_graph(graph):
    import matplotlib.pyplot as plt
    nx.draw(graph, with_labels=True, font_weight='bold')
    plt.show()


if __name__=='__main__':
    g_ip = '127.0.0.1'
    g_port = 3699

    # init connection pool
    connection_pool = ConnectionPool(g_ip, g_port)
    G = nx.MultiDiGraph()
    client = GraphClient(connection_pool)
    auth_resp = client.authenticate('user', 'password')

    if auth_resp.error_code:
        raise AuthException("Auth failed")
    nebula2networkx(client, 'nba', G, [100], ['follow', 'serve'])
    draw_networkx_graph(G)