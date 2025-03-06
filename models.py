class Edge:
    def __init__(self, source, destination, cluster_id):
        self.source = source
        self.destination = destination
        self.cluster_id = cluster_id

    def __str__(self):
        return self.source + " -> " + self.destination + " (" + self.cluster_id + ")"

    def __eq__(self, other):
        if isinstance(other, Edge):
            return (self.source, self.destination) == (other.source, other.destination)
        return False
    
    def __hash__(self):
        return hash((self.source, self.destination))
    
    def __repr__(self):
        return f"({self.source}, {self.destination}): {self.cluster_id}"
    
    def to_dict(self):
        return {
            "source": self.source,
            "destination": self.destination,
            "cluster_id": self.cluster_id
        }

class TestResult:
    def __init__(self, name: str, edges: list[Edge]):
        self.name = name
        self.edges = edges
        self.found_relationships = 0
        self.ratio_found = 0
        self.nmi = 0
        self.ari = 0
        self.f1 = 0
        self.accuracy = 0
        self.custom_found = 0

    def to_dict(self):
        return {
            "name": self.name,
            "edges": [edge.to_dict() for edge in self.edges],  # Convert list of Edge objects
            "found_relationships": self.found_relationships,
            "ratio_found": self.ratio_found,
            "nmi": self.nmi,
            "ari": self.ari,
            "f1": self.f1,
            "accuracy": self.accuracy,
            "custom_found": self.custom_found
        }
    
    def to_dict_slim(self):
        return {
            "name": self.name,
            "found_relationships": self.found_relationships,
            "ratio_found": self.ratio_found,
            "nmi": self.nmi,
            "ari": self.ari,
            "f1": self.f1,
            "accuracy": self.accuracy,
            "custom_found": self.custom_found
        }

    def __repr__(self):
        return f"TestResult({self.name}, {len(self.edges)} edges)"