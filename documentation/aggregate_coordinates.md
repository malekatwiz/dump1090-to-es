# Aggregate captured coordinates

## K-Means clustering

[Coordinates Clustering notebook](../notebooks/coordinates_clustering.ipynb)

The K-Means clustering algorithm is used to group the captured coordinates into a specified number of clusters. The algorithm is initialized with the number of clusters and the coordinates. The algorithm then iteratively assigns each coordinate to the nearest cluster and recalculates the cluster centroids. The algorithm stops when the cluster centroids do not change significantly between iterations.

The K-Means clustering algorithm is implemented using the `KMeans` class from the `sklearn.cluster` module. The `KMeans` class takes the following parameters:

- `n_clusters`: The number of clusters to create.
- `init`: The method used to initialize the cluster centroids. The default value is `k-means++`, which uses a smart initialization method to speed up convergence.
- `n_init`: The number of times the algorithm will be run with different initializations. The default value is 10.
- `max_iter`: The maximum number of iterations for the algorithm. The default value is 300.
- `tol`: The tolerance for convergence. The algorithm stops when the cluster centroids do not change by more than this value between iterations. The default value is 1e-4.

The `KMeans` class has the following methods:

- `fit`: Run the K-Means clustering algorithm on the input data.
- `predict`: Assign each coordinate to the nearest cluster.
- `cluster_centers_`: The coordinates of the cluster centroids.

## DBSCAN clustering

The DBSCAN clustering algorithm is used to group the captured coordinates into clusters of varying shapes and sizes. The algorithm is initialized with two parameters: `eps` and `min_samples`. The `eps` parameter specifies the maximum distance between two samples for them to be considered as in the same neighborhood. The `min_samples` parameter specifies the number of samples in a neighborhood for a point to be considered as a core point.

The DBSCAN clustering algorithm is implemented using the `DBSCAN` class from the `sklearn.cluster` module. The `DBSCAN` class takes the following parameters:

- `eps`: The maximum distance between two samples for them to be considered as in the same neighborhood.
- `min_samples`: The number of samples in a neighborhood for a point to be considered as a core point.
- `metric`: The distance metric to use. The default value is `euclidean`.

