# AmbCQ-GitOps
Manifests para criação do Ambiente Cezão de Qualidade

## Instalação do K3s
### Atualizar pacotes apt
```
sudo apt update && sudo apt upgrade -y
```
### Criar diretórios para snapshots e backups do etcd
```
sudo mkdir /data && sudo mkdir /data/etcd && sudo mkdir /data/etcd/etcd-snapshots && sudo mkdir /data/etcd/etcd-backups && sudo chmod -R 777 /data
```

### Instalar o K3s configurando o intervalo dos snapshots (3h) e o total de retenção (72)
```
sudo curl -sfL https://get.k3s.io | sh -s server - --cluster-init --write-kubeconfig-mode 644 --data-dir=/data/etcd/etcd-backups --etcd-snapshot-retention=72 --etcd-snapshot-dir=/data/etcd/etcd-snapshots --etcd-snapshot-schedule-cron="*/3 * * * *"
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

### Reinicializar o servidor
```
sudo shutdown -r now
```

## Instalar o Helm
```
sudo snap install helm --classic && export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

## Instalar o Prometheus e Grafana
### Clonar os repositórios dos projetos abaixo
```
cd && git clone https://github.com/cablespaghetti/k3s-monitoring.git
cd && git clone https://github.com/acrsantana/AmbCQ-GitOps.git
```

### Adicionar o repositório do Helm Chart do Prometheus
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
```

### Editar o arquivo kube-prometheus-stack-values.yaml inserindo informações relevantes
Prestar especial atenção para a imagem do kube-state-metrics, caso seja necessário substituir pela registry.k8s.io/kube-state-metrics/kube-state-metrics:2.14.0
```
cd && cd k3s-monitoring && nano kube-prometheus-stack-values.yaml
```
![image](https://github.com/user-attachments/assets/0b385c16-2e57-438f-8749-abc4341d7d6b)

### Instalar Prometheus e Grafana
```
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack --version 39.13.3 --values kube-prometheus-stack-values.yaml
```

### Editar o service do Grafana para usar NodePort
Alterar o tipo (type) de ClusterIP para NodePort
```
k edit svc -n default prometheus-grafana
```
![image](https://github.com/user-attachments/assets/8ccc3b60-970d-40b9-bb3c-58094f707f6b)

### Verificar qual porta foi alocada para o Grafana com o comando abaixo:
PORT(S)
80:**XXXXX**/TCP
```
k get service prometheus-grafana
```
![image](https://github.com/user-attachments/assets/1335b592-8167-43f3-8064-c21636e0fca1)

### Acessar a console do Grafana
http://\<your-k3s-node-ip>:\<nodeport>  
Utilizar a seguinte credencial:
Login: admin
Password: prom-operator

## Instalar o Docker Engine
### Desinstalar qualquer pacote conflitante, caso exista:
```
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done
```
### Configurar o repositório APT do docker
```
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```
### Instalar a versão mais recente do docker
```
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```
### Criar um grupo docker e adicione seu usuário
```
sudo groupadd docker
sudo usermod -aG docker $USER && newgrp docker
```
### Configure o docker para sempre iniciar junto com o sistema
```
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
```
### Verifique que o docker está instalado corretamente
```
docker run hello-world
```
![image](https://github.com/user-attachments/assets/d2688461-2580-403b-af7c-cfa75b6be650)

## Deploy da suite Fractal
### Instalar o Airflow
O Airflow é o único componente que ainda não está rodando nativamente no kubernetes, e deve ser instalado via docker compose.
```
cd && mkdir airflow && cd airflow
sudo curl -O https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/docker-compose.yaml
docker compose up -d
```

### Criar a namespace do fractal
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/00%20-%20fractal-namespace.yaml
```

### Instalar o Active MQ
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/01%20-%20active-mq.yaml
```

### Instalar o Keycloak
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/02%20-%20keycloak.yaml
```

### Instalar o Postgres (Postgis)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/03%20-%20fractal-postgres.yaml
```

### Criação do volume compartilhado (core e api)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/04%20-%20fractal-shared-volumes.yaml
```

### Deploy do fractal-core (latest)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/05%20-%20fractal_core.yaml
```

### Deploy do fractal-api (latest)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/06%20-%20fractal-api.yaml
```

### Deploy do fractal-webui (latest)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/07%20-%20fractal-webui.yaml
```
