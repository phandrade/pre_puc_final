from io import BytesIO
import zipfile
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

def s3_list_folder_content (bucket: str, folder: str, filetype:str='*', maxitens:int=200):
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3_objects = s3.list_objects_v2(
        Bucket=bucket,
        Prefix =folder,
        Delimiter = '/',
        MaxKeys=maxitens
    )

    keyitens = []

    for keys in s3_objects.get('Contents'):
        if (filetype != "*"):
            if (keys.get('Key').endswith('.'+filetype)):
                keyitens.append((keys.get('Key')))
        else:
            keyitens.append((keys.get('Key')))
    return keyitens

def s3_unzip (src_bucket: str, dst_bucket: str = '', dst_folder: str='raw', zipfiles:list=[], ext_extensions:tuple=('.csv'), maxitens:int=200):
    
    
    if dst_bucket == '':
        dst_bucket = src_bucket

    if dst_folder.endswith('/'):
        dst_folder = dst_folder[:-1]
    
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    for filename in zipfiles:
        if (filename.endswith('.zip')):
            zip_obj = s3_resource.Object(bucket_name=src_bucket, key=filename)
            buffer = BytesIO(zip_obj.get()["Body"].read())

            z = zipfile.ZipFile(buffer)

            print('Opened zipfile:', filename)

            for filenamezip in z.namelist():
                if filenamezip.endswith(ext_extensions):
                    #file_info = z.getinfo(filenamezip)
                    #print (file_info)
                    print('Uploading file:', filenamezip)
                    s3_resource.meta.client.upload_fileobj(
                        z.open(filenamezip),
                        Bucket=dst_bucket,
                        Key=f'{dst_folder}/{filenamezip}'
                    )
                    print('Uploaded file:', filenamezip)

client = boto3.client(
    'emr', region_name='us-east-2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'Gregori',
    'start_date': datetime(2022, 11, 9)
}

unzip_args = {
    'srcbucket': 'prepuceleicoes2022',
    'srcfolder': 'landing/',
    'dstbucket': 'prepuceleicoes2022',
    'dstfolder': 'raw/',
    'maxitens': 100,
    'ext_exten': ('.csv')
}

@dag(default_args=default_args, schedule_interval="@once", description="Executa um job Spark no EMR", catchup=False, tags=['Spark','EMR'])
def indicadores_eleicoes2022():

    inicio = DummyOperator(task_id='inicio')

    @task
    def tarefa_inicial():
        print("Início do pipeline")

    @task
    def unzip_raw():
        #zip_list = ['landing/bweb_1t_AC_051020221321.zip','landing/bweb_1t_AL_051020221321.zip','landing/bweb_1t_AM_051020221321.zip']      
        zip_list = s3_list_folder_content(bucket=unzip_args.get('srcbucket'), folder=unzip_args.get('srcfolder'), maxitens=unzip_args.get('maxitens'), filetype='zip')
        s3_unzip(src_bucket=unzip_args.get('srcbucket'), dst_folder=unzip_args.get('dstfolder'), zipfiles=zip_list, ext_extensions=unzip_args.get('ext_exten'))
        return ', '.join(zip_list)
        
        

    @task
    def emr_create_cluster():
        cluster_id = client.run_job_flow( # Cria um cluster EMR
            Name='Automated_EMR_Gregori',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://prepuceleicoes2022/Automated_EMR_Gregori_logs/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'gregori_pem',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0c0fe84fea4012875'
            },

            Applications=[{'Name': 'Spark'}],
        )
        return cluster_id["JobFlowId"]


    @task
    def wait_emr_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True


    
    @task
    def emr_process_eleicoes2022(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Processa Eleicoes 2022',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--deploy-mode', 'cluster',
                                's3://prepuceleicoes2022/pyscripts/Eleicoes2022_pipeline.py'
                                ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]

    @task
    def wait_emr_job(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )
    
    @task
    def terminate_emr_cluster(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = DummyOperator(task_id="fim")

    # Orquestração
    tarefainicial = tarefa_inicial()
    cluster = emr_create_cluster()
    #unzip = unzip_raw()
    
    #inicio >> tarefainicial >> unzip >> cluster
    inicio >> tarefainicial >> cluster

    esperacluster = wait_emr_cluster(cluster)

    indicadores = emr_process_eleicoes2022(cluster)
    esperacluster >> indicadores

    wait_step = wait_emr_job(cluster, indicadores)

    terminacluster = terminate_emr_cluster(cluster)
    wait_step >> terminacluster >> fim
    #---------------

execucao = indicadores_eleicoes2022()