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
* At every step that is mentioned we aggregate the 1-gram, 2-gram, and 3-gram respectively to the step index.
* Step1Count1Gram also, counts C0, the sum of all single word occurrences in the data set.
* input : [https://aws.amazon.com/datasets/google-books-ngrams/](https://aws.amazon.com/datasets/google-books-ngrams/) explained here.
* output of each step: <w1,count> or <w1 w2, count> or <w1w2w3, count>
### Step 4: Zipping Step1 And Step2 Outputs
* this step recieves
### Step 5: Probability Calculation

### Step 6: Probability Calculation

* We calculate the probability and match it with it's Trigram.

![The Formula](del.png "The Formula")


### Step 7: Sorting


## Statistics With | Without Combiners:


1. Map-Reduce Job1Count1Gram:

  




4. Map-Reduce Job6CalcProb:

  


5. Map-Reduce Job7Sort:



## Analysis:




