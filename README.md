# Nebula2NetworkX

## NetworkX 特性

python包并不是数据库, 数据都在内存中.

图类型: 无向图(nx.Graph), 有向图(nx.DiGraph), MultiGraphs/MultiDiGraphs=>两个节点之间可以添加多条边, 每个边可以拥有不同的属性, 和Neo4j一致.

节点: 以一个可以hash的对象作为key(id)标识, 带有一个python字典属性(也就是说属性字典中可以包含python对象,列表中可以包含不同类型的元素)

边: 指定两个节点key, 有向图则有向, 无向图则无向. 同样也含有一个python字典属性.

## Neo4j和NetworkX导入导出调研

### NetworkX&&Neo4j(neonx)

[neonx](https://github.com/ducky427/neonx) 提供NetworkX和Neo4j之间相互导入.

Neo4j通信没有采用py2neo,甚至连简单的driver都没有用, 直接通过http接口发送cypher语句完成取数据, 而创建节点/边调用的是HTTP API, 连cypher都未使用. [代码](https://github.com/ducky427/neonx/blob/master/neonx/neo.py).

由于Neo4j和NetworkX都是没有强schema的, 所以边/点的属性都很简单相互转换.

当NetworkX的graph是无向图导入到Neo4j时, 会将节点之间的边正反方向都导入一次.

## NetworkX和Nebula

### Nebula=>NetworkX

从强类型=>弱类型转换应该没什么冲突.
