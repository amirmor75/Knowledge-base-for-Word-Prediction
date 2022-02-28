# Knowledge-base for Word Prediction
assignment 2 of distributed systems 1 course.
Amir Mor and Shelly Talis.

## Description:
In this project we generate a knowledge-base for Hebrew word-prediction system, based on
Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR). The produced
knowledge-base indicates for each pair of words the probability of their possible next words. In
addition, we examine the quality of the algorithm according to statistic measures and manual
analysis.

## Running instructions:


1. Create an S3 bucket.
2. Compile the project and create a jar with `src/main/java/jobs/MR.java` as your main class.
3. Upload your jar to your S3 bucket.
4. Fill `config.properties` file with the followings:
    4.1. `bucketName` - The name of the bucket you want all step outputs and logs to go to.
    4.2. `jarBucketName` - The name of the bucket your jar file resides in.
    4.3.`jarFileName` - The name of the jar you've created at step 2 **without extension**.
5. Run `src/main/java/Main.java`.
6. The final output will be presented inside `finalOutput` folder.

## Map-Reduce Program Flow:

### Step 1 to 3 : Filter & Sum 
* At every step mentioned above we aggregate the 1-gram, 2-gram, and 3-gram respectively to the step index.
* Step1Count1Gram also, counts C0, the sum of all single word occurrences in the data set.
* input : [https://aws.amazon.com/datasets/google-books-ngrams/](https://aws.amazon.com/datasets/google-books-ngrams/) explained here.
* output of each step: <w1,count> or <w1 w2, count> or <w1w2w3, count>
### Step 4: Zipping Step1 And Step2 Outputs
* this step receives as inputs step1 and step2 outputs.
* map-input: receives <w,count> or <w1 w2, w1w2count> as inputs.
* map-output: <w,wcount> or <w2 w1, w1w2count> (inverted w1 w2 so we get w2 count of pair)
* reduce input: <w,wcount> or <w2 w1, w1w2count>
* reduce output: <w1 w2,w2count w1w2count>

### Step 5: Probability Calculation
* this step receives as inputs step3's and step4's outputs.
* map-input: receives <w1 w2,w2count w1w2count> or <w1 w2 w3, w1w2w3count> as inputs.
* map-output: <w1 w2,w2count w1w2count> or <w2 w1, w1w2count> (inverted w1 w2 so we get w2 count of pair)
* reduce input: <w,wcount> or <w2 w1, w1w2count>
* reduce output: <w1 w2,w2count w1w2count>

### Step 6: Probability Calculation

* We calculate the probability and match it with it's Trigram.

![](formula.png)


### Step 7: Sorting

* Sorting the records as follows:
    (1) by w1w2, ascending;
    (2) by the probability for w3, descending.

## Statistics With | Without Combiners:


1. Map-Reduce Job1Count1Gram:

   |                               | With combiners | Without combiners |
   |-------------------------------|----------------|-------------------|
   | Map input records             | 163,471,963    | 163,471,963       |
   | Map output records            | 71,119,513     | 71,119,513        |
   | Map output bytes              | 2,400,202,188  | 2,400,202,188     |
   | Combine input records         | 71,119,513     | Non existing      |
   | Combine output records        | 3,372,257      | Non existing      |
   | Map output materialized bytes | 42,386,294     | 307,746,702       |
   | Reduce input records          | 3,372,257      | 71,119,513        |
   | Reduce output records         | 1,686,118      | 1,686,118         |

2. Map-Reduce Job2Count2Gram:

3. Map-Reduce Job3Count3Gram:

4. Map-Reduce Job4Zip1With2:

5. Map-Reduce Job5Zip3With4:

6. Map-Reduce Job6CalcProb:

7. Map-Reduce Job7Sort:



## Analysis:




