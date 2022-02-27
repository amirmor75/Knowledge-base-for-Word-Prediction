aws s3api create-bucket --bucket amir-knowledge-base-for-word-prediction-jar --region us-east-1

aws s3 rm s3://amir-knowledge-base-for-word-prediction/withoutCombiners/step1output --recursive
aws s3 rm s3://amir-knowledge-base-for-word-prediction/withoutCombiners/step4output --recursive
aws s3 rm s3://amir-knowledge-base-for-word-prediction/withoutCombiners/step5output --recursive
aws s3 rm s3://amir-knowledge-base-for-word-prediction/withoutCombiners/step6output --recursive
aws s3 rm s3://amir-knowledge-base-for-word-prediction/withoutCombiners/finaloutput --recursive
aws s3 rm s3://amir-knowledge-base-for-word-prediction/logs --recursive

aws s3 rm s3://amir-knowledge-base-for-word-prediction-jar --recursive
aws s3 mv out/artifacts/ass2Dsp2_jar/ass2Dsp2.jar s3://amir-knowledge-base-for-word-prediction-jar
