# Output

## Compilation Result

Below screenshot serves as documentation for successful compilation of Spatial Join.
![](D_compilation.png)

## Result

### Printing the names of the top-10 movies with the largest number of ratings

Output will also be written to [data/c1.csv](data/c1.csv)

```csv
Forrest Gump (1994)
"Shawshank Redemption, The (1994)"
Pulp Fiction (1994)
"Silence of the Lambs, The (1991)"
"Matrix, The (1999)"
Star Wars: Episode IV - A New Hope (1977)
Jurassic Park (1993)
Braveheart (1995)
Terminator 2: Judgment Day (1991)
Schindler's List (1993)
```

### Printing the names of the top-10 movies with the highest average rating grouped by genre

Output will also be written to [data/c2.csv](data/c2.csv)

|title                                      |genre  |
|-------------------------------------------|-------|
|Investigation Held by Kolobki (1986)       |Animation|
|Empties (2007)                             |Comedy |
|Sonatine (Sonachine) (1993)                |Crime  |
|L.A. Slasher (2015)                        |Fantasy|
|Sisters (Syostry) (2001)                   |Crime  |
|My Sassy Girl (Yeopgijeogin geunyeo) (2001)|Comedy |
|Get Low (2009)                             |Drama  |
|Cherish (2002)                             |Drama  |
|Goodbye Charlie (1964)                     |Fantasy|
|Get Low (2009)                             |Comedy |

### Finding the common support for all pair of the first 100 movies.

Output will also be written to [data/c3.csv](data/c3.csv)
We present a snippet of the results here.

|movieId1|movieId2|support |
|--------|--------|--------|
|7       |21      |22      |
|7       |87      |3       |
|11      |63      |1       |
|12      |93      |2       |
|13      |10      |5       |
|15      |24      |4       |
|15      |105     |2       |
|21      |79      |4       |
|28      |107     |2       |
|36      |47      |32      |
|38      |58      |1       |
|42      |27      |1       |
|45      |71      |2       |
|48      |75      |1       |
|48      |94      |1       |
|50      |80      |1       |
|52      |62      |15      |

### RMES for Baseline and Collaborative Filtering using ALS

Root-mean-square error baseline = **0.9226554416827888**

Root-mean-square error collaborative filtering = **0.8953246814811533**

### Top Movie Recommendation using ALS model

Output will also be written to [data/recommendations.csv](data/recommendations.csv)
We present a snippet of the results here.

|userId|recommendedMovie|
|------|----------------|
|210   |4429            |
|183   |92259           |
|436   |720             |
|28    |7842            |



