# Aplicação de Leilão Distribuída (REST + RabbitMQ + SSE)

## 1. Título do Projeto
Aplicação de Leilão Distribuída com microserviços, API Gateway, notificações em tempo real via SSE e integração com sistema externo de pagamentos.

## 2. Descrição
Este projeto implementa um ecossistema de microserviços para um sistema de leilões:
- `ms_leilao`: expõe REST para criar/consultar leilões e gerencia o ciclo de vida (início/encerramento), publicando eventos no RabbitMQ.
- `ms_lance`: expõe REST para processar lances, valida contra leilões ativos, publica eventos de lances válidos/invalidos e determina o vencedor no encerramento.
- `api_gateway`: ponto único para o frontend, expõe REST, mantém conexões SSE e consome/redistribui eventos do RabbitMQ como notificações para clientes.
- `ms_pagamento`: consome evento de vencedor, inicia transação no sistema externo, publica link de pagamento e recebe webhooks com status, repassando-os via RabbitMQ.
- `sist_pagamento`: simula um sistema externo de pagamentos (REST + webhook assíncrono).
- `client`: front-end leve (HTML/CSS/JS) que usa o API Gateway por REST e SSE.

Arquitetura focada em código essencial, separação de responsabilidades e manutenção simples.

## 3. Funcionalidades
- REST (via API Gateway):
  - Criar leilões: `POST /leiloes`
  - Consultar leilões ativos: `GET /leiloes/ativos`
  - Efetuar lances: `POST /lances`
  - Registrar interesse em notificações: `POST /notificacoes/registrar`
  - Cancelar interesse: `POST /notificacoes/cancelar`
- SSE (via API Gateway `GET /stream/<client_id>`):
  - `lance`: novos lances válidos
  - `lance_invalido`: lances rejeitados (broadcast)
  - `vencedor`: anúncio do vencedor ao encerrar o leilão
  - `link_pagamento`: link para pagamento gerado para o vencedor
  - `status_pagamento`: resultado do pagamento (`aprovado`/`recusado`)
  - `connected` e `ping` (manutenção de conexão)

## 4. Pré-requisitos
- Python 3.10+
- `pip` para instalar dependências
- RabbitMQ rodando localmente (`localhost`)
- Navegador moderno para o cliente

Dependências Python (instalar):
```
pip install flask pika requests
```

## 5. Instalação
1. Clone o repositório ou copie os arquivos para um diretório local.
2. (Opcional) Crie e ative um ambiente virtual:
   - Windows PowerShell:
     ```powershell
     python -m venv .venv
     .venv\Scripts\Activate.ps1
     ```
   - Instale dependências:
     ```powershell
     pip install flask pika requests
     ```
3. Garanta RabbitMQ ativo em `localhost` (porta padrão).

## 6. Uso
Abra 5 terminais (PowerShell) na pasta do projeto e inicie os serviços:

1) `ms_leilao` (REST + ciclo de vida) – porta `5001`:
```powershell
python src\services\ms_leilao.py
```

2) `ms_lance` (REST + eventos) – porta `5002`:
```powershell
python src\services\ms_lance.py
```

3) `ms_pagamento` (REST webhook + consumidor) – porta `5003`:
```powershell
python src\services\ms_pagamento.py
```

4) `sist_pagamento` (sistema externo simulado) – porta `5004`:
```powershell
python sist_pagamento\sist_pagamento.py
```

5) `api_gateway` (REST + SSE + consumidores) – porta `5000`:
```powershell
python src\api_gateway.py
```

6) Cliente (HTML/CSS/JS): abra `http://localhost:5000` no navegador. 

> Observação: se o navegador bloquear por CORS (origem diferente), considere servir o cliente via o próprio gateway (ver seção Configuração) ou habilitar CORS.

### Exemplos de REST (via API Gateway `http://localhost:5000`)

Criar leilão:
```bash
curl -X POST http://localhost:5000/leiloes \
  -H "Content-Type: application/json" \
  -d '{
    "nome_produto": "Notebook",
    "descricao": "i7, 16GB RAM",
    "valor_inicial": 1000.00,
    "data_inicio": "2025-01-01T10:00:00",
    "data_fim": "2025-01-01T10:05:00"
  }'
```
Resposta (201):
```json
{
  "id": "leilao_01",
  "descricao": "Notebook - i7, 16GB RAM",
  "valor_minimo": 1000.0,
  "data_inicio": "2025-01-01T10:00:00",
  "data_fim": "2025-01-01T10:05:00",
  "status": "pendente"
}
```

Consultar ativos:
```bash
curl http://localhost:5000/leiloes/ativos
```
Resposta (200):
```json
[
  {
    "id": "leilao_01",
    "nome_produto": "Notebook - i7, 16GB RAM",
    "descricao": "Notebook - i7, 16GB RAM",
    "valor_atual": 1000.0,
    "data_inicio": "2025-01-01T10:00:00",
    "data_fim": "2025-01-01T10:05:00",
    "status": "ativo"
  }
]
```

Efetuar lance:
```bash
curl -X POST http://localhost:5000/lances \
  -H "Content-Type: application/json" \
  -d '{
    "id_leilao": "leilao_01",
    "id_usuario": "user_123",
    "valor_do_lance": 1200.00
  }'
```
Resposta (201, válido):
```json
{
  "status": "validado",
  "id_leilao": "leilao_01",
  "id_usuario": "user_123",
  "valor_do_lance": 1200.0
}
```
Resposta (400, inválido):
```json
{
  "status": "invalidado",
  "motivo": "valor_insuficiente",
  "ultimo_lance": 1200.0
}
```

Registrar interesse em notificações:
```bash
curl -X POST http://localhost:5000/notificacoes/registrar \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "cliente_001",
    "leilao_id": "leilao_01",
    "tipos": ["lance", "vencedor", "pagamento"]
  }'
```

Cancelar interesse:
```bash
curl -X POST http://localhost:5000/notificacoes/cancelar \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "cliente_001",
    "leilao_id": "leilao_01",
    "tipos": ["lance", "vencedor", "pagamento"]
  }'
```

### SSE no cliente (exemplo JS)
```javascript
const es = new EventSource('http://localhost:5000/stream/cliente_001');
es.addEventListener('connected', () => console.log('SSE conectado'));
es.addEventListener('lance', (e) => console.log('Lance', JSON.parse(e.data)));
es.addEventListener('vencedor', (e) => console.log('Vencedor', JSON.parse(e.data)));
es.addEventListener('link_pagamento', (e) => {
  const data = JSON.parse(e.data);
  console.log('Link de pagamento', data.payment_link);
});
es.addEventListener('status_pagamento', (e) => console.log('Pagamento', JSON.parse(e.data)));
```

## 7. Configuração
- Portas padrão:
  - `api_gateway`: `5000`
  - `ms_leilao`: `5001`
  - `ms_lance`: `5002`
  - `ms_pagamento`: `5003`
  - `sist_pagamento`: `5004`
- RabbitMQ: `localhost` (ajuste se necessário).
- Constantes editáveis:
  - `src/api_gateway.py`: `MS_LEILAO_URL`, `MS_LANCE_URL`.
  - `src/services/ms_pagamento.py`: `EXTERNAL_PAYMENT_URL`, `WEBHOOK_URL`.
  - `client/main.js`: `API_BASE` (por padrão `http://localhost:5000`).

### CORS / Servir cliente no Gateway
Para evitar problemas de CORS, há duas opções:
1. Habilitar CORS (ex.: com `flask-cors`):
   ```python
   # api_gateway.py
   from flask_cors import CORS
   CORS(app, resources={r"/*": {"origins": "*"}})
   ```
2. Servir os arquivos do cliente no próprio gateway (mesma origem):
   ```python
   # api_gateway.py
   from flask import send_from_directory

   @app.get('/')
   def serve_home():
       return send_from_directory('client', 'home.html')

   @app.get('/client/<path:p>')
   def serve_static(p):
       return send_from_directory('client', p)
   ```

## 8. Contribuição
- Padrões:
  - Código simples e focado no essencial.
  - Evitar redundâncias e manter arquitetura limpa.
- Como contribuir:
  1. Faça um fork do projeto.
  2. Crie um branch para sua feature/ajuste: `git checkout -b feat/minha-feature`.
  3. Implemente e teste localmente (rodando todos os serviços).
  4. Abra um Pull Request descrevendo claramente o que foi alterado.
- Sugestões futuras:
  - Persistência (DB) para leilões e lances.
  - Reconexão robusta a RabbitMQ.
  - Autenticação e autorização.
  - Observabilidade (métricas/logs estruturados).

---

### Estrutura do Projeto
```
Aplicacao_Leilao_REST/
├── client/
│   ├── home.css
│   ├── home.html
│   └── main.js
├── sist_pagamento/
│   └── sist_pagamento.py
└── src/
    ├── api_gateway.py
    └── services/
        ├── ms_lance.py
        ├── ms_leilao.py
        └── ms_pagamento.py
```

### Fluxo de Pagamento (resumo)
1. `ms_lance` publica `leilao_vencedor` ao encerrar.
2. `ms_pagamento` consome e chama `POST /transacoes` em `sist_pagamento`.
3. `sist_pagamento` responde com `transaction_id` e `payment_link`.
4. `ms_pagamento` publica `pagamento_link` (e `link_pagamento` para compatibilidade).
5. `api_gateway` emite `link_pagamento` via SSE para clientes inscritos.
6. `sist_pagamento` processa e envia webhook para `ms_pagamento` (`POST /webhook`).
7. `ms_pagamento` publica `pagamento_status` (e `status_pagamento` para compatibilidade).
8. `api_gateway` emite `status_pagamento` via SSE.

### Boas Práticas
- Serviços isolados e de responsabilidade única.
- Threads dedicadas para consumo de eventos e manutenção de SSE.
- Estruturas em memória com locks (simples e suficiente para este escopo).
- Publicação em filas de compatibilidade para evitar quebra de integrações.