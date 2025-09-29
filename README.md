# lg-logistica

Sistema para organização logística da Logos Editora, com integração de produtos, pedidos e ofertas entre plataformas como **Guru**, **Shopify** e **Frete Barato**.

---

## 📦 Pré-requisitos

- Python **3.13** (para execução local)  
- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/) (para execução em container)  
- Arquivo `.env` preenchido com as chaves e URLs de API necessárias  
  - Um modelo está disponível em `.env.example`

---

## 🚀 Uso local (desenvolvimento)

1. Crie e ative o ambiente virtual:

   ```bash
   python3 -m venv .venv
   # Linux/Mac
   source .venv/bin/activate
   # Windows (PowerShell)
   .venv\Scripts\Activate.ps1

2. Instale as dependências:

pip install -r requirements.txt

3. Execute a aplicação (FastAPI com Uvicorn):

python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

4. Acesse a documentação interativa em:

Swagger: http://localhost:8000/docs

ReDoc: http://localhost:8000/redoc

🐳 Uso com Docker
Construir a imagem localmente
docker build -t lg-logistica:local .
docker run --rm -p 8000:8000 --env-file .env lg-logistica:local

Usando Docker Compose (desenvolvimento)
docker compose up --build


O serviço ficará disponível em http://localhost:8000

🏗️ Uso com imagem publicada (GHCR)

Sempre que houver mudanças na branch main, a imagem mais recente é publicada automaticamente em:

ghcr.io/jfioreze-logos/lg-logistica-v2:latest

Para rodar no servidor (usuários autorizados):
# login no GitHub Container Registry
echo <TOKEN> | docker login ghcr.io -u <seu-usuario> --password-stdin

# baixar e executar
docker pull ghcr.io/jfioreze-logos/lg-logistica-v2:latest
docker run --rm -p 8000:8000 --env-file .env ghcr.io/jfioreze-logos/lg-logistica-v2:latest


🔒 O <TOKEN> é um Personal Access Token (PAT) com permissão read:packages.

📝 Logs

Os logs são inicializados automaticamente via sitecustomize.py.

Formato: JSON no console e em arquivo

Arquivo de log: sistema.log na raiz do projeto

Nível de log: controlado por DEBUG=1 (ou LOG_LEVEL=DEBUG)

Desativar captura de print()/stderr: defina LOG_CAPTURE_STDOUT=0 no .env

Exemplos:

DEBUG=1 python -m uvicorn app.main:app --reload
LOG_CAPTURE_STDOUT=0 python -m uvicorn app.main:app --reload