import numpy as np
from scipy.optimize import linear_sum_assignment
from sklearn.metrics import normalized_mutual_info_score, adjusted_rand_score, f1_score
import numpy as np
from scipy.optimize import linear_sum_assignment
from collections import defaultdict

from models import Edge

def map_predicted_clusters_to_ground_truth(ground_truth_edges, predicted_edges):
    """
    Maps predicted cluster labels to the best-matching ground truth cluster labels using the Hungarian algorithm.
    
    :param ground_truth_edges: List of Edge(source, destination, cluster_id) for ground truth.
    :param predicted_edges: List of Edge(source, destination, cluster_id) for predicted clustering.
    :return: Dictionary mapping predicted cluster IDs to ground truth cluster IDs.
    """
    
    # Convert edges to dictionary format {(source, destination): cluster_id}
    gt_dict = {(e.source, e.destination): e.cluster_id for e in ground_truth_edges}
    pred_dict = {(e.source, e.destination): e.cluster_id for e in predicted_edges}

    # Get all unique edges (some might be missing in one set)
    all_edges = set(gt_dict.keys()).union(set(pred_dict.keys()))

    # Create lists of corresponding cluster IDs
    gt_labels = []
    pred_labels = []
    
    for edge in all_edges:
        gt_labels.append(gt_dict.get(edge, -1))  # -1 for missing edges
        pred_labels.append(pred_dict.get(edge, -1))

    # Create mapping between predicted and ground truth cluster IDs
    unique_gt_labels = list(set(gt_labels))
    unique_pred_labels = list(set(pred_labels))

    # Create a confusion matrix
    confusion_matrix = np.zeros((len(unique_gt_labels), len(unique_pred_labels)), dtype=int)
    
    for gt, pred in zip(gt_labels, pred_labels):
        if gt != -1 and pred != -1:  # Ignore unmatched edges
            i = unique_gt_labels.index(gt)
            j = unique_pred_labels.index(pred)
            confusion_matrix[i, j] += 1

    # Use Hungarian Algorithm to find the best cluster mapping
    row_ind, col_ind = linear_sum_assignment(-confusion_matrix)  # Min cost = Max accuracy

    # Map predicted clusters to the best-matching ground truth clusters
    mapping = {unique_pred_labels[col]: unique_gt_labels[row] for row, col in zip(row_ind, col_ind)}

    return mapping

def evaluate_clustering(ground_truth_edges, predicted_edges):
    """
    Evaluates clustering using NMI, ARI, and F1-score after relabeling predicted clusters.
    """
    
    # Map predicted clusters to match ground truth
    mapping = map_predicted_clusters_to_ground_truth(ground_truth_edges, predicted_edges)

    # Relabel predicted clusters
    predicted_edges_mapped = [Edge(e.source, e.destination, mapping.get(e.cluster_id, -1)) for e in predicted_edges]

    # Convert to dictionaries for easy label comparison
    gt_dict = {(e.source, e.destination): e.cluster_id for e in ground_truth_edges}
    pred_dict = {(e.source, e.destination): e.cluster_id for e in predicted_edges_mapped}

    all_edges = set(gt_dict.keys()).union(set(pred_dict.keys()))

    true_labels = []
    predicted_labels = []

    for edge in all_edges:
        true_labels.append(gt_dict.get(edge, -1))  # -1 for missing edges
        predicted_labels.append(pred_dict.get(edge, -1))

    # Ensure uniform label types (convert all to strings)
    true_labels = [str(label) for label in true_labels]
    predicted_labels = [str(label) for label in predicted_labels]   

    # Compute clustering metrics
    nmi = normalized_mutual_info_score(true_labels, predicted_labels)
    ari = adjusted_rand_score(true_labels, predicted_labels)
    f1 = f1_score(true_labels, predicted_labels, average='weighted')

    return {
        "NMI": nmi,
        "ARI": ari,
        "F1-score": f1
    }