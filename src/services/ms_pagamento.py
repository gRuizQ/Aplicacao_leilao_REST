import json
import threading
import datetime
from typing import Dict, Any

import pika
import requests
from flask import Flask, request, jsonify, abort

# ---------------------------------------------
# Configuração
# ---------------------------------------------
EXTERNAL_PAYMENT_URL = 'http://localhost:5004'
WEBHOOK_URL = 'http://localhost:5003/webhook'
RABBIT_HOST = 'localhost'

app = Flask(__name__)

# ---------------------------------------------
# RabbitMQ publishers
# ---------------------------------------------

def publish_link_pagamento(id_leilao: str, id_vencedor: str, link: str):
    """Publica evento de link de pagamento na fila 'link_pagamento'."""
    message = {
        'id_leilao': id_leilao,
        'id_vencedor': id_vencedor,
        'id_usuario': id_vencedor,  # compat
        'link_pagamento': link,
        'timestamp': datetime.datetime.now().isoformat(),
    }
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST)) as connection:
            channel = connection.channel()
            channel.queue_declare(queue='link_pagamento')
            channel.basic_publish(exchange='', routing_key='link_pagamento', body=json.dumps(message).encode('utf-8'))
        print(f"[ms_pagamento] Link de pagamento publicado para leilão {id_leilao}")
    except pika.exceptions.AMQPError as e:
        print(f"[ms_pagamento] ERRO ao publicar link de pagamento: {e}")
        raise e


def publish_status_pagamento(id_leilao: str, id_vencedor: str, status: str, valor: float):
    """Publica evento de status de pagamento na fila 'status_pagamento'."""
    message = {
        'id_leilao': id_leilao,
        'id_vencedor': id_vencedor,
        'cliente_id': id_vencedor,  # compat
        'status': status,
        'valor': valor,
        'timestamp': datetime.datetime.now().isoformat(),
    }
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST)) as connection:
            channel = connection.channel()
            channel.queue_declare(queue='status_pagamento')
            channel.basic_publish(
                exchange='',
                routing_key='status_pagamento',
                body=json.dumps(message).encode('utf-8')
            )
        print(f"[ms_pagamento] Status de pagamento '{status}' publicado para leilão {id_leilao}")
    except pika.exceptions.AMQPError as e:
        print(f"[ms_pagamento] ERRO ao publicar status de pagamento: {e}")
        raise e


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
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Publica link de pagamento
    try:
        publish_link_pagamento(id_leilao=id_leilao, id_vencedor=id_usuario, link=payment_link)
    except Exception:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return
    
    ch.basic_ack(delivery_tag=method.delivery_tag)


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


    publish_status_pagamento(id_leilao=leilao_id or '', id_vencedor=cliente_id or '', status=status, valor=valor or 0.0)

    return jsonify({'ok': True})


# ---------------------------------------------
# Inicialização
# ---------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, use_reloader=False)