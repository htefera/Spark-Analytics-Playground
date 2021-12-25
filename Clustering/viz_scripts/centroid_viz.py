import matplotlib.pyplot as plt
import numpy as np

CENTROID_VIZ_NAME = "centroids.png"


def visualize_centroids(centroids: np.array):
    fig = plt.figure(figsize=(50, 50))
    plt.title("KMeans Centroids for MNIST Dataset", fontsize=150, color="green")
    n_rows = int(np.sqrt(len(centroids)))
    n_cols = int(len(centroids) / n_rows) + 1

    for i in range(len(centroids)):
        c = centroids[i]
        ax = fig.add_subplot(n_rows, n_cols, i + 1)
        ax.axis('off')
        ax.imshow(c.reshape(28, 28),
                  interpolation='none', cmap=plt.get_cmap('gray'), vmin=0, vmax=255)
    plt.savefig(CENTROID_VIZ_NAME)
    print(f"Visualization Saved at {CENTROID_VIZ_NAME}")


def main():
    centroids = np.genfromtxt("../data/kmeans_centroids.csv", delimiter=",", skip_header=False)
    visualize_centroids(centroids)


if __name__ == '__main__':
    main()
