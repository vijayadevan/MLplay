import csv
import warnings

import numpy as np
import pandas as pd
import sklearn.metrics.pairwise
import tqdm
from matplotlib import pyplot as plt

warnings.simplefilter(action='ignore', category=FutureWarning)


def object_fn(data, indices, centers):
    total = 0
    for d, cluster in zip(data, indices):
        total = total + np.sum(np.square(d - centers[cluster]))
    return total


def Kmeans(data, k):
    centroids = data.sample(k)
    while True:
        distances = sklearn.metrics.pairwise.euclidean_distances(data, centroids)
        indices = np.argsort(distances)[:, 0]
        data["cluster"] = indices
        new_centers = data.groupby(by=["cluster"]).agg('mean').reset_index(drop=True)
        data.drop('cluster', axis=1, inplace=True)
        if (new_centers.values == centroids.values).all():
            return indices, new_centers
        centroids = new_centers


if __name__ == '__main__':
    file = open("CSE575-HW03-Data.csv", 'r')
    reader = csv.reader(file)
    data = []
    for row in reader:
        data.append(row)

    data = pd.DataFrame(data).astype(float)
    print(data.shape)

    k_vals = [2, 3, 4, 5, 6, 7, 8, 9]
    values = []
    for k in tqdm.tqdm(k_vals):
        clusters, centers = Kmeans(data, k)
        values.append(object_fn(data, clusters, centers))

    # plt.plot(k_vals, values)
    # plt.xlabel("K values")
    # plt.ylabel("Objective function")
    # plt.show()

    K = 2
    clusters, _ = Kmeans(data, K)
    colors = ["red", "blue", "green", "yellow", "pink", "black", "orange", "violet", "grey"]

    for i in range(K):
        plt.scatter(data.loc[clusters == i, 0], data.loc[clusters == i, 1],
                    color=colors[i], label='class ' + str(i))

    plt.title('Clustering')
    plt.show()
