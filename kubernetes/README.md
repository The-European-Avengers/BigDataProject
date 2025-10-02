Setup
export KUBECONFIG="$(pwd)/kubernetes/bd-gr-05-sa-bd-bd-gr-05-kubeconfig.yaml"
kubectl config get-contexts
kubectl get namespace
kubectl config use-context bd-bd-gr-05-context
kubectl get pods

## Do not use the following commands, everything should be set up automatically
### To deploy Kafka cluster:
helm install --values kafka-values.yaml kafka-g5 oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4

### To deploy HDFS cluster:
cd kubernetes/hadoop-config
kubectl apply -f .