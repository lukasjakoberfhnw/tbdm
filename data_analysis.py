import pandas as pd
import os
from helper import map_predicted_clusters_to_ground_truth, evaluate_clustering
from models import Edge, TestResult
import json
import random
import networkx as nx
import matplotlib.pyplot as plt

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

def check_commonality(ground_edges, test_edges, directional=False):
    found: int = 0
    total: int = 0

    if not directional:
        for edge in ground_edges:
            total += 1
            for test in test_edges:
                if (edge.source == test.source and edge.destination == test.destination) or (edge.destination == test.source and edge.source == test.destination):
                    # print("Commonality found")
                    found += 1
                    continue
    else:
        for edge in ground_edges:
            total += 1
            for test in test_edges:
                if edge.source == test.source and edge.destination == test.destination:
                    # print("Commonality found")
                    found += 1
                    continue

    return found, total

def check_tp_fp_tn_fn(ground_edges, test_edges):
    tp = 0
    fp = 0
    tn = 0
    fn = 0

    ground_truth = pd.read_csv("./data/sorted_logfile.csv")
    raw_concepts = ground_truth["concept:name"].unique()
    all_edges = []

    for i in range(len(raw_concepts) - 1):
        all_edges.append(Edge(raw_concepts[i], raw_concepts[i+1], "all"))

    total = 0
    for edge in all_edges:
        # if in ground & 
        in_ground = False
        in_pred = False

        total += 1

        for g in ground_edges:
            if (edge.source == g.source and edge.destination == g.destination) or (edge.destination == g.source and edge.source == g.destination):
                in_ground = True
        for test in test_edges:
            if (edge.source == test.source and edge.destination == test.destination) or (edge.destination == test.source and edge.source == test.destination):
                in_pred = True

        if in_ground and in_pred:
            tp += 1
        elif in_ground and not in_pred:
            fp += 1
        elif not in_ground and in_pred:
            fn += 1
        elif not in_ground and not in_pred:
            tn += 1

    # print("F1 - Customized")
    # # precision = tp / tp + fp
    # # recall = tp / tp + fn
    # precision = tp / total
    # recall = tp / total
    # f1_score = 2/((1/precision) + (1/recall))
    # print(precision)
    # print(recall)
    # print(f1_score)
    # print(tp)
    # print(fp)
    # print(fn)
    # print(tn)
    # print(tp/(fp+fn))
    # print((tp+tn)/(fp+fn))

    accuracy = (tp+tn)/(fp+fn+tp+tn)
    found_compared_all = tp / (fp+fn+tp+tn)

    return accuracy, found_compared_all

def main():
    ground_edges = get_ground_truth()
    
    test_results: list[TestResult] = []

    file_names = os.listdir("./results")
    for file_name in file_names:
        loaded_edges = get_cluster_result("./results/" + file_name)
        test_results.append(TestResult(file_name, loaded_edges))

    # print(ground_edges)
    # print(test_results[0].name)
    # print(test_results[0].edges[0])

    test_results.sort(key=lambda x: x.name)

    # compare the edges and see how many we have found
    for test in test_results:
        # print(test.name)
        try:
            found, total = check_commonality(ground_edges, test.edges)
            # print(test.name)
            # print("Relationships found: " + str(found))
            # print("Total Relationships: " + str(total))
            # print("Ratio: " + str(found/total))
            results = evaluate_clustering(ground_edges, test.edges)
            others_performances = check_tp_fp_tn_fn(ground_edges, test.edges)
            test.found_relationships = found
            test.ratio_found = found/total
            test.nmi = results["NMI"]
            test.ari = results["ARI"]
            test.f1 = results["F1-score"]
            test.accuracy = others_performances[0]
            test.custom_found = others_performances[1]

            # still try the TP, FP, TN, FN
        except Exception as ex:
            print("Something went wrong in ", test.name)
            print(ex.with_traceback())


    test_results[0].name
    others_performances = check_tp_fp_tn_fn(ground_edges, test_results[0].edges)

    final_evaluation = json.dumps([result.to_dict() for result in test_results], indent=4)
    final_evaluation_slim = json.dumps([result.to_dict_slim() for result in test_results], indent=4)
    # print(final_evaluation)

    with open("final_results.json", "w") as file:
        file.write(final_evaluation)

    with open("final_results_slim.json", "w") as file:
        file.write(final_evaluation_slim)

    # sort the best algorithms on top
    test_results.sort(key=lambda x: x.custom_found, reverse=True)

    sorted_final_evaluation_slim = json.dumps([result.to_dict_slim() for result in test_results], indent=4)
    with open("final_results_slim_sorted.json", "w") as file:
        file.write(sorted_final_evaluation_slim)

def draw_graph(edges):
    G = nx.Graph()

    # Add edges and assign clusters
    clusters = {}
    for edge in edges:
        G.add_edge(edge.source, edge.destination)
        clusters[edge.source] = edge.cluster_id
        clusters[edge.destination] = edge.cluster_id

    # Generate unique colors for clusters
    cluster_ids = list(set(clusters.values()))
    cluster_colors = {cid: f"#{random.randint(0, 0xFFFFFF):06x}" for cid in cluster_ids}

    # Assign colors based on cluster
    node_colors = [cluster_colors[clusters[node]] for node in G.nodes()]

    # Draw the graph
    plt.figure(figsize=(8, 6), clear=True)
    pos = nx.spring_layout(G, seed=42)  # Layout for visualization
    nx.draw(G, pos, node_color=node_colors, with_labels=True, edge_color="gray", node_size=700, font_size=10)
    plt.title("Clustered Graph Visualization")
    plt.show()


if __name__ == "__main__":
    # ground_edges = get_ground_truth()
    test_results: list[TestResult] = []
    file_names = os.listdir("./results")
    for file_name in file_names:
        loaded_edges = get_cluster_result("./results/" + file_name)
        test_results.append(TestResult(file_name, loaded_edges))
        print(len(loaded_edges))

    get_good_edges = get_cluster_result("./results/A1_activity_clustering.txt")


    # print(test_results[3].name)
    draw_graph(get_good_edges)


# if possible -> create graph visualizations for DFG for nice presentation