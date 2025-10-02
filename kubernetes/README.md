Setup
export KUBECONFIG="$(pwd)/kubernetes/bd-gr-05-sa-bd-bd-gr-05-kubeconfig.yaml"
kubectl config get-contexts
kubectl get namespace
kubectl config use-context bd-bd-gr-05-context
kubectl get pods