# Scalable Data Science- Technical University of Berlin

The goal of the project is to process various data using various data mining tasks with Scala and Apache Spark.

### Tools
* Language: Scala, Java, Python
* IDE: IntelliJ IDEA, Google Colab
* Build tool: Maven
* Apache RDDs, Spark DataFrames and GeoSpark Spatial RDDs


The project is decomposed into the following sub tasks

### [1. Classification](https://github.com/htefera/Scalable-Data-Science-Assignment-2/tree/master/Classification)

The goal is to classify email texts as spam or not. To do so, we use logistic regression and SVM algorithms and compare their accuracies. 

Approach

1. We read the CSV files as dataframe and add a column “spam” to denote if the read record is spam or not
1. We union training records for spam and no spam into a single dataframe, the same operation is done for the training set too.
1. We normalize the email text, we apply normalization as we term frequency as the basis
for classification and having discrete email addresses,
2. We construct a pipeline to apply the transformation. We perform word tokoneziation the words and stop word removal operation. We use a hashing term frequency transform to count the word occurrences and we calculate the inverse document frequency and this forms our feature vector.
3.  We train a Logistic Regression and SVM model with transformed training data
4.  We use the testing set to make predictions using the model
5.  We compute RoC, Accuracy, Precision and Recall metrics and display a confusion matrix for the benefit of the user

![Logistic Regression](Images/lg.png)
![SVM](Images/svm.png)
### [2. Clustering](https://github.com/htefera/Scalable-Data-Science-Assignment-2/tree/master/Clustering)
The goal is to apply k means clustering  on MNIST handwritten numbers dataset.

Approach Clustering

1. Read the mnist CSV files as raw text files
2. Convert each line into a Row of dense vectors
3. Create a dataframe from RDD from step 2
4. Since the grayscale values can range from 0 to 255 and we don’t want to give undue weightage during distance computation, we apply mean scaling
5. Scaled dataset is used to training the developed K-Means model
6. We set the number of clusters as 10 based on intuition, there are 10 numbers (0-9) and the aim was to see if vectors with the same image are clustered together
7. The final centroid values are scaled back and written to a CSV file

After we classify the handwritten datasets, the next task is to visualize the centeroids 

Centroid Visualization
1. We read the centroids CSV file
2. Create a matplotlib plot with 10 subplots within it, one for each centroid
3. We reshape the flattened MNIST data into 28x28 numpy array
4. This array is then projected as a grayscale image
5. The plot is saved

### [3. Recommednation System](https://github.com/htefera/Scalable-Data-Science-Assignment-2/tree/master/Recommendation%20Systems)
### [4. Spatial Data Analysis](https://github.com/htefera/Scalable-Data-Science-Assignment-2/tree/master/Spatial%20Data%20Analysis)




