from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import ElasticKubernetesService as EKS
from diagrams.aws.integration import SQS
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from IPython.display import Image
from diagrams.aws.network import APIGateway


import os

os.environ["PATH"] += os.pathsep + 'C:/IDE/Graphviz/bin/'

with Diagram("Система скрытия лиц", show=False, direction="LR",
             filename="face-hider"):
    user = User("Client")

    api = APIGateway("API Layer / Gateway")

    raw_storage = S3("Raw Video Storage")

    with Cluster("Parallel Processing"):
        splitter = Server("Video Splitter")
        queue = SQS("Task Queue")

        with Cluster("GPU Workers"):
            workers = [EKS("Worker 1"), EKS("Worker 2")]

        processed_chunks = S3("Processed Chunks")

    merger = Server("Video Merger")
    final_video = S3("Final Video Storage")


    user >> Edge(
        label="Upload") >> api >> raw_storage >> splitter >> queue >> workers >> processed_chunks >> merger >> final_video >> Edge(
        label="Result Link") >> api >> user

# Отображаем результат
Image("face-hider.png")
