# AmbCQ-GitOps
Manifests para criação do Ambiente Cezão de Qualidade

## Instalação do K3s
### Criar diretórios para snapshots e backups do etcd
```
sudo mkdir /data && sudo mkdir /data/etcd && sudo mkdir /data/etcd/etcd-snapshots && sudo mkdir /data/etcd/etcd-backups && sudo chmod -R 777 /data
```

### Instalar o K3s configurando o intervalo dos snapshots (3h) e o total de retenção (72)
```
curl -sfL https://get.k3s.io | sh -s server - --cluster-init --write-kubeconfig-mode 644 --data-dir=/data/etcd/etcd-backups --etcd-snapshot-retention=72 --etcd-snapshot-dir=/data/etcd/etcd-snapshots --etcd-snapshot-schedule-cron="*/3 * * * *"
```

### Adicionar um alias para o comando kubectl
Editar o arquivo .bashrc
```
cd && nano .bashrc
```
Inserir a seguinte linha:
```
alias k=kubectl
```

### Instalar o Helm
```
sudo snap install helm --classic && export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

### Clonar o repositório do projeto abaixo
```
cd && git clone https://github.com/cablespaghetti/k3s-monitoring.git && cd k3s-monitoring
```


### Adicionar o repositório do Helm Chart do Prometheus
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
```

### Editar o arquivo kube-prometheus-stack-values.yaml inserindo informações relevantes
Prestar especial atenção para a imagem do kube-state-metrics, caso seja necessário substituir pela registry.k8s.io/kube-state-metrics/kube-state-metrics:2.14.0
```
nano kube-prometheus-stack-values.yaml
```

### Instalar Prometheus e Grafana
```
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack --version 39.13.3 --values kube-prometheus-stack-values.yaml
```

### Editar o service do Grafana para usar NodePort
Alterar o tipo de ClusterIP para NodePort
```
kubectl edit service/prometheus-grafana
```

### Verificar qual porta foi alocada para o Grafana com o comando abaixo:
```
k get service prometheus-grafana
```

### Acessar a console do Grafana
http://<your-k3s-node-ip>:<nodeport>
Utilizar a seguinte credencial:
Login: admin
Password: prom-operator
