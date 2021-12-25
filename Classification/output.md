# Output

## Compilation Result

Below screenshot serves as documentation for successful compilation.
![](A_compilation.png)

## Result

### Logistic Regression

```text
org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS@54644470
RoC: 0.8350036512849045
  a\p |  spam |   ham
 spam |   104 |    45
  ham |    27 |   938
Accuracy: 0.9353680430879713
Precision: 0.7938931297709924
Recall: 0.697986577181208
```

### SVM SGD

```text
org.apache.spark.mllib.classification.SVMWithSGD@5304aeb
RoC: 0.9114441701151025
  a\p |  spam |   ham
 spam |   124 |    25
  ham |     9 |   956
Accuracy: 0.9694793536804309
Precision: 0.9323308270676691
Recall: 0.8322147651006712
```