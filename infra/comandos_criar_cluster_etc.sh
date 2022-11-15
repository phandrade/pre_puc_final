eksctl create cluster --version=1.21 --name=kbpaulo2 --timeout=60m --managed --instance-types=m5.xlarge --alb-ingress-access --node-private-networking --region=us-east-2 --nodes-min=2 --nodes-max=3 --full-ecr-access --asg-access --nodegroup-name=ng-kbpaulo2

helm install airflow apache-airflow/airflow -f airflow_custom_values.yaml -n airflow --create-namespace --version 1.6.0 --timeout 5m --debug