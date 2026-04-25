from diagrams import Cluster, Diagram
from diagrams.aws.storage import S3
from diagrams.aws.compute import ElasticKubernetesService as EKS
from diagrams.aws.integration import SQS
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from IPython.display import Image


import os
# Укажите путь к папке bin, где лежит dot.exe
os.environ["PATH"] += os.pathsep + 'C:/IDE/Graphviz/bin/'

# Создаем объект диаграммы
with Diagram("ML_Face_Blurring_System", show=False, direction="LR",
             filename="system_schema"):
    user = User("Client Data")
    raw_storage = S3("S3 Raw Video")
    processed_storage = S3("S3 Blurred Video")

    with Cluster("Processing Pipeline"):
        orchestrator = Server("Splitter\n(FFmpeg)")
        queue = SQS("Task Queue")

        with Cluster("GPU Workers"):
            workers = [EKS("Worker 1"),
                       EKS("Worker 2")]

        merger = Server("Merger\n(FFmpeg)")

    user >> raw_storage >> orchestrator >> queue >> workers >> processed_storage
    processed_storage >> merger >> processed_storage

# Отображаем результат
Image("system_schema.png")
