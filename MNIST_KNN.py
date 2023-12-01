import operator

import numpy as np
from matplotlib import pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.metrics.pairwise import euclidean_distances
import tqdm
from sklearn.datasets import fetch_openml


class KNN:
    def __init__(self, K):
        self.K = K
        self.x_train = None
        self.y_train = None
        self.distance_matrix_of_test_to_train = None

    def fit(self, x_train, y_train):
        self.x_train = x_train
        self.y_train = y_train

    def predict(self, x_test):
        predictions = []
        for i in tqdm.tqdm(range(len(x_test))):
            dist_sorted = np.argsort(distance_matrix_of_test_to_train[i])[:self.K]
            neigh_count = {}
            for idx in dist_sorted:
                if self.y_train[idx] in neigh_count:
                    neigh_count[self.y_train[idx]] += 1
                else:
                    neigh_count[self.y_train[idx]] = 1
            sorted_neigh_count = sorted(neigh_count.items(),
                                        key=operator.itemgetter(1), reverse=True)
            predictions.append(sorted_neigh_count[0][0])
        return predictions


if __name__ == "__main__":
    # Load dataset
    mnist = fetch_openml('mnist_784', as_frame=False)
    train_data = mnist.data[:60000].reshape(60000, 28,28)
    test_data = mnist.data[60000:].reshape(10000, 28,28)

    train_labels = mnist.target[:60000]
    test_labels = mnist.target[60000:]

    # Reshape and transform data
    train_data = train_data.reshape(train_data.shape[0], train_data.shape[1] * train_data.shape[2])
    test_data = test_data.reshape(test_data.shape[0], test_data.shape[1] * test_data.shape[2])

    train_labels = train_labels.reshape(-1, 1)
    test_labels = test_labels.reshape(-1, 1)

    train_X = np.asarray(train_data)
    test_X = np.asarray(test_data)

    train_Y = np.squeeze(np.asarray(train_labels))
    test_Y = np.squeeze(np.asarray(test_labels))

    k_list = [1, 3, 5, 10, 20, 30, 40, 50, 60]


    distance_matrix_of_test_to_train = euclidean_distances(test_X, train_X)

    k_vals = [1, 3, 5, 10, 20, 30, 40, 50, 60]
    dict1 = {}
    accuracies = []
    for k in k_vals:
        print("Clustering with K = " + str(k))
        model = KNN(K=k)
        model.fit(train_X, train_Y)
        pred = model.predict(test_X)
        acc = accuracy_score(test_Y, pred)
        accuracies.append(acc)
        print("K = " + str(k) + "; Accuracy: " + str(acc))

    plt.plot(k_vals, accuracies, '*-', color='turquoise')
    plt.xlabel('Value of k')
    plt.ylabel('Test Accuracy')
    plt.title('Accuracy vs Value of k')
    plt.show()
