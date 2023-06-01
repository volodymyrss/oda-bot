import rdflib
import json
from pathlib import Path

class TurtleFileGraph:
    def __init__(self, filepath, default_fn = 'kg.ttl'):
        p = Path(filepath)
        self.filepath = filepath
        if p.is_dir():
            p.joinpath(default_fn).touch()
            self.filepath = str(p.joinpath(default_fn))
        if not p.exists():
            p.touch()
        self.graph = rdflib.Graph()
        self.graph.parse(self.filepath)
        self.graph.bind('oda',rdflib.Namespace('http://odahub.io/ontology#'))
    
    def insert(self, query_string):
        self.graph.update(f"INSERT DATA {{ {query_string} }}")
        self.graph.serialize(destination=self.filepath)

    def construct(self, query_string, jsonld=False):
        qres = self.graph.query(f"CONSTRUCT WHERE {{ {query_string} }}")
        if jsonld:
            try:
                j = qres.serialize(format="json-ld", indent=4, sort_keys=True).decode()
            except:
                j = qres.serialize(format="json-ld", indent=4, sort_keys=True)
            return json.loads(j)
        else:
            raise NotImplementedError
    
