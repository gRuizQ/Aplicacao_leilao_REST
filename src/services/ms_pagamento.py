import json
import threading
from typing import Dict, Any

import pika
import requests
from flask import Flask, request, jsonify, abort

# ---------------------------------------------
# Configuração
# ---------------------------------------------
EXTERNAL_PAYMENT_URL = 'http://localhost:5004'
WEBHOOK_URL = 'http://localhost:5003/webhook'

app = Flask(__name__)

# ---------------------------------------------
# RabbitMQ publishers/consumers
# ---------------------------------------------

def init_publisher():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        ch = conn.channel()
        # Filas de publicação (compatibilidade com nomes diferentes no gateway)
        ch.queue_declare(queue='link_pagamento')
        ch.queue_declare(queue='status_pagamento')
        # Garante existência da fila consumida pelo serviço
        ch.queue_declare(queue='leilao_vencedor')
        return conn, ch
    except Exception:
        return None, None

PUB_CONN, PUB_CH = init_publisher()
_PUB_LOCK = threading.Lock()


def publish(queue_name: str, payload: Dict[str, Any]):
    """Publica com reconexão e lock para suportar múltiplas threads (webhook/consumidor).

    Em caso de falha (canal fechado/conexão perdida), tenta reestabelecer a conexão
    e publicar novamente uma única vez. Se persistir erro, ignora para manter serviço REST.
    """
    global PUB_CONN, PUB_CH
    body = json.dumps(payload).encode('utf-8')
    with _PUB_LOCK:
        if PUB_CH is None:
            PUB_CONN, PUB_CH = init_publisher()
            if PUB_CH is None:
                return
        try:
            PUB_CH.basic_publish(exchange='', routing_key=queue_name, body=body)
        except Exception:
            # Tenta reconectar e publicar novamente
            PUB_CONN, PUB_CH = init_publisher()
            if PUB_CH is None:
                return
            try:
                PUB_CH.basic_publish(exchange='', routing_key=queue_name, body=body)
            except Exception:
                # Falha persistente é ignorada para não derrubar o serviço
                return


# ---------------------------------------------
# Consumidor de leilao_vencedor
# ---------------------------------------------

def on_leilao_vencedor(ch, method, properties, body):
    try:
        data = json.loads(body.decode('utf-8'))
    except Exception as e:
        print(f"ms_pagamento: Erro ao processar leilão vencedor: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    id_leilao = data.get('id_leilao')
    id_usuario = data.get('id_usuario')
    valor = data.get('valor_do_lance')

    if not id_leilao or not id_usuario or valor is None:
        print(f"ms_pagamento: Vencedor inválido para leilão {id_leilao} do usuário {id_usuario} com valor {valor}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"ms_pagamento: Processando vencedor para leilão {id_leilao} do usuário {id_usuario} com valor {valor}")

    # Chama sistema externo para iniciar transação
    payload = {
        'valor': valor,
        'moeda': 'BRL',
        'cliente_id': id_usuario,
        'leilao_id': id_leilao,
        'webhook_url': WEBHOOK_URL,
    }

    try:
        r = requests.post(f"{EXTERNAL_PAYMENT_URL}/transacoes", json=payload, timeout=5)
        resp = r.json()
        transaction_id = resp.get('transaction_id')
        payment_link = resp.get('payment_link')
    except Exception:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    if not transaction_id or not payment_link:
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            pass
        return

    event = {
        'id_leilao': id_leilao,
        'id_usuario': id_usuario,
        'valor': valor,
        'moeda': 'BRL',
        'transaction_id': transaction_id,
        'payment_link': payment_link,
    }

    # Publica na fila de link de pagamento
    publish('link_pagamento', event)
    try:
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        pass


def start_consumer():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        ch = conn.channel()
        # Exchange fanout para receber eventos de vencedores por instância
        ch.exchange_declare(exchange='vencedores_exchange', exchange_type='fanout')
        result = ch.queue_declare(queue='', exclusive=True)
        fila_vencedor = result.method.queue
        ch.queue_bind(exchange='vencedores_exchange', queue=fila_vencedor)
        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(queue=fila_vencedor, on_message_callback=on_leilao_vencedor, auto_ack=False)
        ch.start_consuming()
    except Exception:
        # Serviço continua rodando o REST mesmo sem RabbitMQ
        pass


_consumer_thread = threading.Thread(target=start_consumer, daemon=True)
_consumer_thread.start()


# ---------------------------------------------
# Webhook de status de pagamento
# ---------------------------------------------
@app.post('/webhook')
def pagamento_webhook():
    data = request.get_json(silent=True)
    if not data:
        abort(400, description='JSON de entrada é obrigatório')

    id_transacao = data.get('id_transacao')
    status = data.get('status')  # 'aprovado' ou 'recusado'
    valor = data.get('valor')
    cliente_id = data.get('cliente_id')
    leilao_id = data.get('leilao_id')

    if not all([id_transacao, status]):
        abort(400, description='Campos obrigatórios: id_transacao, status')

    event = {
        'id_transacao': id_transacao,
        'status': status,
        'valor': valor,
        'cliente_id': cliente_id,
        'leilao_id': leilao_id,
    }

    # Publica na fila de status de pagamento
    publish('status_pagamento', event)

    return jsonify({'ok': True})


# ---------------------------------------------
# Inicialização
# ---------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, use_reloader=False)