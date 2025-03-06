import pandas as pd
import os

class Edge:
    def __init__(self, source, destination, relationship):
        self.source = source
        self.destination = destination
        self.relationship = relationship

    def __str__(self):
        return self.source + " -> " + self.destination + " (" + self.relationship + ")"

class TestResult:
    def __init__(self, name: str, edges: list[Edge]):
        self.name = name
        self.edges = edges

def get_ground_truth():
    ground_truth = pd.read_csv("./data/sorted_logfile.csv")
    #print(ground_truth.head())

    # goal -> find associations/relations within the ground truth data
    # check if associations were found in the clustered data
    # print meaningful statistics

    # get distinct concepts
    raw_concepts = ground_truth["concept:name"].unique()
    #print(raw_concepts)

    clusters = ground_truth["case:concept:name"].unique()
    # print(clusters)

    nodes: list[str] = raw_concepts
    edges: list[tuple[str, str]] = [] # (src, dst)
    enhanced_edges: list[Edge] = [] # (src, dst, cluster_name)

    for cluster_name in clusters:
        concepts_in_cluster = ground_truth.loc[ground_truth["case:concept:name"] == cluster_name]["concept:name"].unique()
        
        for i in range(len(concepts_in_cluster) - 1):
            edges.append(tuple((concepts_in_cluster[i], concepts_in_cluster[i + 1])))
            enhanced_edges.append(Edge(concepts_in_cluster[i], concepts_in_cluster[i + 1], cluster_name))

    # print(enhanced_edges)
    return enhanced_edges

def get_cluster_result(file_path: str):
    # read file and convert to node/edge structure
    edges = []
    enhanced_edges = []

    with open(file_path) as file:
        lines = [line.rstrip() for line in file]
        for l in lines:
            splitter = l.split(":")
            cluster_id = splitter[0]
            activities = splitter[1].split(",")

            for i in range(len(activities) - 1):
                edges.append(tuple((activities[i], activities[i + 1])))
                enhanced_edges.append(Edge(activities[i], activities[i + 1], cluster_id))

    # print(enhanced_edges)

    return enhanced_edges

def main():
    ground_edges = get_ground_truth()

    test_results: list[TestResult] = []

    file_names = os.listdir("./results")
    for file_name in file_names:
        loaded_edges = get_cluster_result("./results/" + file_name)
        test_results.append(TestResult(file_name, loaded_edges))

    print(ground_edges)
    print(test_results[0].name)
    print(test_results[0].edges[0])


if __name__ == "__main__":
    main()


# if possible -> create graph visualizations for DFG for nice presentation