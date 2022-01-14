aws s3api create-bucket --bucket amir-knowledge-base-for-word-prediction --region us-east-1
aws s3 rm s3://amir-knowledge-base-for-word-prediction --recursive
aws s3 mv out/artifacts/ass2Dsp2_jar/ass2Dsp2.jar s3://amir-knowledge-base-for-word-prediction
