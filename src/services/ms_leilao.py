import datetime
import json
import time
import threading
from typing import Dict, Any

import pika
from flask import Flask, request, jsonify, abort

# ---------------------------------------------
# Configuração básica
# ---------------------------------------------
app = Flask(__name__)

# Armazenamento simples em memória (substituível por DB futuramente)
leiloes: Dict[str, Dict[str, Any]] = {}
_id_seq = 1
_lock = threading.Lock()

# ---------------------------------------------
# RabbitMQ (publisher)
# ---------------------------------------------

def init_rabbitmq():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        # Declarar filas essenciais usadas pelo serviço
        channel.queue_declare(queue='leilao_iniciado')
        channel.queue_declare(queue='leilao_finalizado')
        return connection, channel
    except Exception:
        # Mantém o serviço REST funcional mesmo sem RabbitMQ
        return None, None

RMQ_CONNECTION, RMQ_CHANNEL = init_rabbitmq()
_RMQ_LOCK = threading.Lock()


def publish(queue_name: str, payload: Dict[str, Any]):
    global RMQ_CONNECTION, RMQ_CHANNEL
    body = json.dumps(payload).encode('utf-8')
    with _RMQ_LOCK:
        if RMQ_CHANNEL is None:
            RMQ_CONNECTION, RMQ_CHANNEL = init_rabbitmq()
            if RMQ_CHANNEL is None:
                return
        try:
            RMQ_CHANNEL.basic_publish(exchange='', routing_key=queue_name, body=body)
        except Exception:
            # Reconecta e tenta novamente uma vez
            RMQ_CONNECTION, RMQ_CHANNEL = init_rabbitmq()
            if RMQ_CHANNEL is None:
                return
            try:
                RMQ_CHANNEL.basic_publish(exchange='', routing_key=queue_name, body=body)
            except Exception:
                # Falha de publicação é ignorada para manter o serviço operante
                return


# ---------------------------------------------
# Helpers
# ---------------------------------------------

def serialize_leilao(leilao: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": leilao["id"],
        "descricao": leilao["descricao"],
        "valor_minimo": leilao["valor_minimo"],
        "data_inicio": leilao["data_inicio"].isoformat(),
        "data_fim": leilao["data_fim"].isoformat(),
        "status": leilao["status"],
    }


def parse_datetime(dt_str: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromisoformat(dt_str)
    except Exception:
        abort(400, description="Formato de data inválido. Use ISO 8601.")


# ---------------------------------------------
# API REST: criação e consulta de leilões
# ---------------------------------------------
@app.post('/leiloes')
def criar_leilao():
    global _id_seq

    data = request.get_json(silent=True)
    if not data:
        abort(400, description="JSON de entrada é obrigatório")

    descricao = data.get('descricao')
    valor_minimo = data.get('valor_minimo')
    data_inicio_s = data.get('data_inicio')
    data_fim_s = data.get('data_fim')

    if not isinstance(descricao, str) or descricao.strip() == "":
        abort(400, description="Campo 'descricao' obrigatório e deve ser string")

    try:
        valor_minimo = float(valor_minimo)
    except (TypeError, ValueError):
        abort(400, description="Campo 'valor_minimo' obrigatório e numérico")

    if not isinstance(data_inicio_s, str) or not isinstance(data_fim_s, str):
        abort(400, description="Campos 'data_inicio' e 'data_fim' devem ser strings ISO 8601")

    data_inicio = parse_datetime(data_inicio_s)
    data_fim = parse_datetime(data_fim_s)

    print(f"Recebido Leilão: descricao={descricao}, valor_minimo={valor_minimo}, data_inicio={data_inicio}, data_fim={data_fim}")

    if data_inicio >= data_fim:
        abort(400, description="'data_inicio' deve ser anterior a 'data_fim'")

    with _lock:
        leilao_id = f"leilao_{_id_seq:02d}"
        _id_seq += 1

        leilao = {
            "id": leilao_id,
            "descricao": descricao.strip(),
            "valor_minimo": valor_minimo,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "status": "pendente",
        }
        leiloes[leilao_id] = leilao

    return jsonify(serialize_leilao(leilao)), 201


@app.get('/leiloes')
def listar_leiloes():
    with _lock:
        return jsonify([serialize_leilao(l) for l in leiloes.values()])


@app.get('/leiloes/<leilao_id>')
def obter_leilao(leilao_id: str):
    with _lock:
        leilao = leiloes.get(leilao_id)
        if not leilao:
            abort(404, description="Leilão não encontrado")
        return jsonify(serialize_leilao(leilao))


# ---------------------------------------------
# Ciclo de vida dos leilões
# ---------------------------------------------

def lifecycle_loop():
    while True:
        now = datetime.datetime.now()
        with _lock:
            for l in leiloes.values():
                # Ativar leilão ao atingir início
                if l['status'] == 'pendente' and now >= l['data_inicio'] and now < l['data_fim']:
                    l['status'] = 'ativo'
                    publish('leilao_iniciado', {
                        "id_leilao": l['id'],
                        "descricao": l['descricao'],
                        "valor_minimo": l['valor_minimo'],
                        "data_inicio": l['data_inicio'].isoformat(),
                        "data_fim": l['data_fim'].isoformat(),
                    })
                # Finalizar leilão ao atingir término
                elif l['status'] == 'ativo' and now >= l['data_fim']:
                    l['status'] = 'encerrado'
                    publish('leilao_finalizado', {
                        "id_leilao": l['id']
                    })
        time.sleep(1)


# Thread em background para o ciclo de vida
_lifecycle_thread = threading.Thread(target=lifecycle_loop, daemon=True)
_lifecycle_thread.start()


# ---------------------------------------------
# Inicialização
# ---------------------------------------------
if __name__ == '__main__':
    # Executa o serviço REST
    app.run(host='0.0.0.0', port=5001)