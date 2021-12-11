aws emr create-default-roles

aws emr create-cluster --name cc-iit-emr-word-count --use-default-roles --release-label emr-5.28.0 --instance-count 3 --instance-type m4.large --applications Name=JupyterHub Name=Spark Name=Hadoop --ec2-attributes KeyName=iit-vivo  --log-uri s3://cc-iit-emr/logs/

