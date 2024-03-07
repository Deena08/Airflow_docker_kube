Airflow on Kubernetes

Overview

This repository contains configurations and instructions for deploying Apache Airflow on Kubernetes. Apache Airflow is an open-source workflow automation and scheduling platform. By deploying Airflow on Kubernetes, you can leverage Kubernetes' scalability, reliability, and ease of management to run your workflows.

Pre requisites

Before you begin, ensure you have the following installed:
    • Kubernetes cluster: You need a Kubernetes cluster to deploy Airflow. You can use a local cluster (e.g., Minikube or Kind) for testing or a production-grade cluster like GKE, EKS, or AKS.
    • kubectl: The Kubernetes command-line tool (kubectl) should be installed and configured to connect to your Kubernetes cluster.
    • Helm: Helm is a package manager for Kubernetes. We'll use Helm to install Airflow and manage its configurations.

Deployment Steps

Step 1: Configure Airflow
Before deploying Airflow, you need to configure its settings. You can customize configurations such as executor type, database backend, and worker resources. Refer to the Airflow documentation for detailed configuration options.

Step 2: Deploy Airflow using Helm

Once you've configured Airflow, deploy it using Helm. Helm simplifies the deployment process by managing charts (packages of pre-configured Kubernetes resources).

helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm install airflow apache-airflow/airflow

This command installs Airflow using the default configuration provided by the Helm chart. You can customize the deployment by specifying your configuration values.

Step 3: Access Airflow UI

After the deployment is successful, you can access the Airflow web UI using the following command:

kubectl port-forward namespace/airflow-webserver 8080:8080

Open a web browser and go to http://localhost:8080 to access the Airflow UI. Log in using the credentials specified during deployment.

Step 4: Manage DAGs

You can manage your workflows (DAGs) using the Airflow UI. Upload your DAG files to the DAGs folder specified in the Airflow configuration. Airflow will automatically detect and schedule your workflows based on the defined schedule intervals.

Customization

    • Configuration: Modify the Airflow Helm chart values (values.yaml) to customize Airflow's configuration.
    • Scaling: Adjust the number of Airflow workers based on your workload and resource requirements.
    • Security: Secure Airflow by enabling authentication, configuring RBAC, and using TLS for communication.

Monitoring and Logging

Monitor the health and performance of your Airflow deployment using Kubernetes monitoring tools .Collect and analyze Airflow logs using centralized logging solutions.

Maintenance

    • Upgrades: Upgrade Airflow to newer versions using Helm.
    • Backup and Restore: Regularly back up Airflow metadata database and DAG files to prevent data loss.
    • Troubleshooting: Debug issues by checking Airflow logs, Kubernetes events, and resource utilization.

Resources

    • Apache Airflow Documentation
    • Helm Charts Repository

