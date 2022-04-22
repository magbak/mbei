# Performance tests on Google Kubernetes Engine
Note that following this guide can incur costs if you forget to tear down the resources you create in Google Cloud Platform (GCP).
I recommend deleting the whole project afterwards to avoid any surprises. I will assume a linux environment with docker installed. 
## Setting up GCP resources
We will assume that you have a Google Cloud account.
1. [Create a project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
3. Run ```gcloud init``` and choose the project you created as the default.
4. Enable the Artifact Registry API in [GCP console](https://console.cloud.google.com/): Activate your project > Hamburger menu > Artifact Registry > Enable 
5. Enable the Google Kubernetes Engine (GKE) API in [GCP console](https://console.cloud.google.com/): Activate your project > Hamburger menu > Artifact Registry > Enable

## Create an Artifact Repository
Follow the guide [here](https://cloud.google.com/artifact-registry/docs/manage-repos#console) with the following settings: The default settings should be fine: 
   * Use a name you remember
   * Regional
   * Format: Docker
   * Note the region you picked here for later. I picked _europe-west3 (Frankfurt)_	and will use that for the remainder of this document.

   
## Build and push the docker images to Artifact Repository
First we will use gcloud to configure docker as described [here](https://cloud.google.com/artifact-registry/docs/docker/authentication#gcloud-helper).

```shell
$ gcloud auth configure-docker europe-west3-docker.pkg.dev
```

The ```gcp_docker_build_tag.sh``` script in the repository root builds the images we need and pushes them to Artifact Repository. 

Edit the ```gcp_docker_build_tag.sh``` before you run it:
Fill in values appropriate for your project and repository:
```shell
LOCATION="europe-west3"
PROJECT_ID="mbei-test"
REPOSITORY_NAME="mbeirepo"
```
Save the script and run it. This can easily take 10 minutes the first time for rust compilation.
Afterwards there will be a cache in Docker, and consecutive builds will take much less time. 
```shell
~/mbei $ chmod +x gcp_docker_build_tag.sh
~/mbei $ ./gcp_docker_build_tag.sh 
```

## Cluster creation
You may have to increase the quota to create your cluster to run the performance benchmark. 
In this case cluster creation will give you an error with an URL to request quota increases. 
These are typically approved within minutes. 

To create the kubernetes cluster it is easiest to navigate to [Kubernetes clusters](https://console.cloud.google.com/kubernetes) in Cloud Console.
Create a new regional cluster with a memorable name (I used "mbei-cluster") in one of the regions of your location. I chose "europe-west-3-b".
In the default node pool, choose the machine type. I have used the AMD EPYC-based "n2d-standard-2" which has 2 vCPUs and 8GB of RAM per node. 
Set the number of nodes to the number of MBEI-nodes you want to run + 1. The data producer is set up to be scheduled on a node without any components, explaining the need for another node.

After the cluster is created, we configure the kubectl command to manage it from your local machine as described [here](https://cloud.google.com/kubernetes-engine/docs/quickstart).
First, we need to install the ```kubectl``` component. 

```shell
$ gcloud components install kubectl
```

The command above might work for you, but I have installed gcloud through snap.
I followed the comment [here](https://stackoverflow.com/questions/60023834/unable-to-change-kubectl-context-to-my-google-kubernetes-cluster), and ran the command below instead.

```shell
$ snap install kubectl --classic
```

Next we need to set up the kubectl command so that it manages our cluster as described [here](https://cloud.google.com/kubernetes-engine/docs/quickstart). We need to supply a location argument since we have not set a default compute region. 
```shell
$ gcloud container clusters get-credentials mbei-cluster --region europe-west-3-b
```

Next, navigate to the "gcp-perftest"-folder and install the Python requirements.
```shell
$ pip install -r requirements.txt
```

Then, edit "testscript.py" to run with the desired scenario size and number of nodes.
Note that scenario sizes are even numbers >= 2. 
```shell
$ python testscript.py
```
The table below describes what test scripts were used in what experiments.
In order to run experiment C it is necessary to check out a special branch which modifies our code. 

|Script name| Branch       |Experiment|
|-------------|--------------|-----------|
|testscript.py| main         |Experiment A|
|testscript-reverse.py | main         | Experiment B |
|testscript.py| experimentC  | Experiment C |
