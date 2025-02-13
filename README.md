# AmbCQ-GitOps
Segue abaixo procedimento detalhado para a montagem do ambiente do projeto Fractal. Todo o ambiente foi montado considerando o Ubuntu 22.04 como sistema operacional de base, portanto os comandos são baseados neste ambiente. Os mesmos comandos também foram testados na versão 24.04 e funcionam perfeitamente.

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
Os seguintes diretórios devem ser criados, e terem as devidas permissões atribuidas:  
**./dags** - As dags devem ser armazenadas neste diretório.  
**./logs** - contém os logs de execução das task e scheduler.  
**./config** - you can add custom log parser or add airflow_local_settings.py to configure cluster policy.  
**./plugins** - Plugins customizados devem ser armazenados neste diretório.  

```
cd && mkdir airflow && cd airflow
mkdir -p ./logs ./plugins ./config
cp -r ../AmbCQ-GitOps/dags .
cp ../AmbCQ-GitOps/dags/connections.json ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

O Airflow é o único componente que ainda não está rodando nativamente no kubernetes, e deve ser instalado via docker compose.
```
cd && cd airflow
sudo curl -O https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/docker-compose.yaml
docker compose up airflow-init
```

Após instalar e configurar, o airflow pode ser executado
```
cd && cd airflow
docker compose up -d
cd
```

Acesse a interface web do airflow para validar a instalação. O servidor web está disponível em http://\<ip-servidor>:8085. A conta padrão tem o login **airflow** e a senha **airflow**.
![image](https://github.com/user-attachments/assets/db2105d8-a65d-421b-82bb-848bd25900db)

Durante o processo de instalação, algumas dags do projeto Fractal já foram copiadas para a pasta correspondente. É necessário verificar se as mesmas estão atualizadas na versão correta. Após acessar o airflow, habilitar todas as dags e executar a dag **Import Airflow Connections**
![image](https://github.com/user-attachments/assets/209b8bf1-7574-4ee5-80c5-7e03eac38672)


### Criar a namespace do fractal
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/00%20-%20fractal-namespace.yaml
```

### Instalar o Active MQ
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/01%20-%20active-mq.yaml
```
Validar que o Active MQ encontra-se acessivel em http://\<ip-servidor>:30161. A conta padrão tem o login **admin** e a senha **admin**.
![image](https://github.com/user-attachments/assets/7048048b-416c-4fbe-bcc8-f6b62a0f74ff)

### Instalar o Postgres (Postgis)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/03%20-%20fractal-postgres.yaml
```

### Configurar a connection fractal_db do Airflow e executar as DAGs
No airflow, clicar em Admin / Connections conforme imagem abaixo, e depois clicar no ícone de editar
![image](https://github.com/user-attachments/assets/da84012b-7a58-4cf4-ba7c-484069d148bc)
![image](https://github.com/user-attachments/assets/adb99e19-6edd-4c3e-89ee-9809b4d5187c)
Ajustar o campo Host com o IP do node, e preencher novamente o campo Password com o valor root
![image](https://github.com/user-attachments/assets/0ded73d2-c5ff-4f0d-806e-d6c543d1a03b)
Voltar para a aba DAGs e executar as dags ETL TLF_Brasil e City Data Transfer

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
Editar o configmap, alterando a variável **FRACTAL_ETL_SERVICE** para refletir o IP do airflow na API  
http://\<ip-servidor>:8085
```
k edit cm -n fractal configmap-fractal-api
```
![image](https://github.com/user-attachments/assets/e89b0a37-960e-4fa7-a5cc-c4abe50a9841)

Verificar o nome do pod do fractal api, para que o mesmo possa ser reiniciado após mudança do configmap
```
k get pod -n fractal
```
![image](https://github.com/user-attachments/assets/bdba994b-11ea-4a60-a873-4fe5576003a2)

Restartar o pod do fractal-api para refletir as mudanças no configmap
```
k get pod <pod_name> -n fractal -o yaml | kubectl replace --force -f -
```
Validar que o deploy ocorreu com sucesso acessando a documentação da API no link: http://\<ip-servidor>:31080/swagger-ui/index.html
![image](https://github.com/user-attachments/assets/9484fc5d-09cd-425c-a888-5c7fb11e4647)

### Deploy do fractal-webui (latest)
É necessário configurar o projeto frontend e o keycloak para que o redirect uri seja apontado para o endereço do deploy da aplicação http://\<ip-servidor>:31000, conforme imagens abaixo:

**Keycloak**  
![image](https://github.com/user-attachments/assets/458c0b7c-9845-4943-ad0c-5809a1389c13)
![image](https://github.com/user-attachments/assets/12b720e4-1662-4848-ba10-82c1041c182b)

**Frontend (fractal-webui)**
![image](https://github.com/user-attachments/assets/c46b8190-7c94-4e4a-8b7b-616715a03af2)
![image](https://github.com/user-attachments/assets/d22ca6e7-959e-4f29-a301-357abf0045f3)

Por característica, o projeto do frontend precisa que as configurações sejam feitas hardcoded, o que exige que a imagem docker seja reconstruída após mudanças no projeto. Caso seja necessário, clone o repositório do projeto no servidor, realize as mudanças informadas acima e reconstrua a imagem de forma que a mesma fique armazenada de forma local, ou altere o arquivo 07 - fractal-webui.yaml para buscar a imagem nova em um outro image registry (não será possível modificar a imagem cezaodabahia/fractal-webui:latest sem autenticar com o usuário cezaodabahia no docker hub).
```
npm run build
docker image build -t cezaodabahia/fractal-webui:latest .
```

Realizar o deploy da aplicação
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/07%20-%20fractal-webui.yaml
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/08%20-%20fly-webui.yaml
```

Ou caso tenha optado por modificar o arquivo de deployment, executar o comando abaixo

**Arquivo Deployment**
![image](https://github.com/user-attachments/assets/828d9595-11fe-4d5f-83c6-4fea24c36885)

```
k apply -f <caminho-do-arquivo-modificado.yaml>
```

Validar que o deploy ocorreu com sucesso acessando o frontend no link: http://\<ip-servidor>:31000  
**Observação:** O frontend utiliza o keycloak Minsait, instalado no ambiente de Desenvolvimento. Para que a tela de autenticação apareça, é necessário ter conectividade com o ambiente Dev Minsait, seja diretamente ou através de VPN.  


### Instalar o Keycloak (opcional)
```
k apply -f https://raw.githubusercontent.com/acrsantana/AmbCQ-GitOps/refs/heads/main/02%20-%20keycloak.yaml
```
Validar que o Keycloak encontra-se acessivel em http://\<ip-servidor>:30080. A conta padrão tem o login **admin** e a senha **admin**.  
É necessário realizar as configurações tanto no keycloak quanto no frontend para que a aplicação consiga utilizar corretamente o Keycloak instalado no passo anterior.
