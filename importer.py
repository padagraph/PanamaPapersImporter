import pandas
import collections.namedtuple

from botapi import Botagraph, BotApiError
from reliure.types import Text


PDG_HOST = "g0v-tw.padagraph.io"
PDG_KEY = ""
GRAPHNAME = "Panama Papers"


nodetypes = ["addresses",]

NodeType = namedtuple("NodeType", "name description properties")
EdgeType = namedtuple("EdgeType", "name description properties")


class NodesImporter:

    def __init__(self, basename):
        self.df = pandas.read_csv("./data/" + basename + ".csv")
        self.basename = basename


    def buildNodeType(self):
        properties = {name: Text() for name in self.df.columns}
        return NodeType(self.basename, "", properties)


    def iterNodes(self, nodetypeUuid):
        for _, row in df.iterrows():
            properties = row.to_dict() 
            yield { 'nodetype': nodetypeUuid,
                    'properties': properties}



class AllEdgesImporter:

    def __init__(self):
        self.df = pandas.read_csv("./data/all_edges.csv")

    def buildEdgeTypes(self):
        for name in set(self.df.rel_type):
            yield EdgeType(name, "", {})

    def iterEdges(self,nodes_uuids, rels_uuids):
        for _, row in self.df.iterrows():
            n1_uuid = nodes_uuids[row.node_1]
            n2_uuid = nodes_uuids[row.node_2]
            rel_uuid = rels_uuids[row.rel_type]
            yield {'edgetype': rel_uuid,
                    'source': n1_uuid,
                    'target': n2_uuid,
                    'properties': {}}

bot = Botagraph(PDG_HOST, PDG_KEY)
bot.create_graph(GRAPHNAME, {'description': "IJIC's data",
    "tags": ["panama", "leak"]})

nodes_uuids = {}

for nodetype in nodetypes:
    nImporter = NodesImporter(nodetype)
    nt = nImporter.buildNodeType()
    type_uuid = bot.post_nodetype(GRAPHNAME,nt.name, nt.description, nt.properties)['uuid']
    for node, uuid in bot.post_nodes(GRAPHNAME,nImporter.iterNodes(type_uuid)):
        nodes_uuids[node['properties']['node_id']] = uuid

eImporter = AllEdgesImporter()
types_uuid = {}
for et in eImporter.buildEdgeTypes():
    types_uuid[et.name] = bot.post_edgetype(GRAPHNAME, et.name, et.description, et.properties)['uuid']

list(bot.post_edges(GRAPHNAME, eImporter.iterEdges(nodes_uuids, types_uuid)))
