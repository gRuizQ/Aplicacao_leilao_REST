import json
import threading
import time
from typing import Dict, Any

import pika
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# Estado em memória
leiloes_ativos: Dict[str, Dict[str, Any]] = {}
ultimos_lances: Dict[str, Dict[str, Any]] = {}
_lock = threading.Lock()

# ---------------------------------------------
# RabbitMQ: publishers e consumers
# ---------------------------------------------

def init_publisher():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        ch = conn.channel()
        # Filas usadas para publicação
        ch.queue_declare(queue='lance_validado')
        ch.queue_declare(queue='lance_invalidado')
        ch.queue_declare(queue='leilao_vencedor')
        # Filas consumidas (garante existência caso ms_leilao não as declare primeiro)
        ch.queue_declare(queue='leilao_iniciado')
        ch.queue_declare(queue='leilao_finalizado')
        return conn, ch
    except Exception:
        return None, None

PUB_CONN, PUB_CH = init_publisher()


def init_consumer():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        ch = conn.channel()
        # Filas consumidas
        ch.queue_declare(queue='leilao_iniciado')
        ch.queue_declare(queue='leilao_finalizado')
        # Também pode publicar vencedor a partir do mesmo canal
        ch.queue_declare(queue='leilao_vencedor')
        return conn, ch
    except Exception:
        return None, None

SUB_CONN, SUB_CH = init_consumer()


def publish_pub(queue_name: str, payload: Dict[str, Any]):
    global PUB_CH
    try:
        if PUB_CH is None:
            return
        body = json.dumps(payload).encode('utf-8')
        PUB_CH.basic_publish(exchange='', routing_key=queue_name, body=body)
    except Exception:
        # Em caso de falha no RabbitMQ, desativa o publisher e segue sem interromper o fluxo REST
        PUB_CH = None
        return


# ---------------------------------------------
# Consumers: leilao_iniciado e leilao_finalizado
# ---------------------------------------------

def on_leilao_iniciado(ch, method, properties, body):
    try:
        data = json.loads(body.decode('utf-8'))
    except Exception:
        return
    leilao_id = data.get('id_leilao')
    valor_minimo = data.get('valor_minimo')
    with _lock:
        leiloes_ativos[leilao_id] = {
            'ativo': True,
            'valor_minimo': valor_minimo,
        }


def on_leilao_finalizado(ch, method, properties, body):
    try:
        data = json.loads(body.decode('utf-8'))
    except Exception:
        return
    leilao_id = data.get('id_leilao')

    with _lock:
        vencedor = ultimos_lances.get(leilao_id)
        # limpezas
        leiloes_ativos.pop(leilao_id, None)
        ultimos_lances.pop(leilao_id, None)

    # Publica vencedor (ou ninguém)
    msg = {
        'id_leilao': leilao_id,
        'id_usuario': vencedor['id_usuario'] if vencedor else 'ninguem',
        'valor_do_lance': vencedor['valor_do_lance'] if vencedor else 0.0,
    }
    # Usa o canal do consumer para evitar concorrência entre threads
    ch.basic_publish(exchange='', routing_key='leilao_vencedor', body=json.dumps(msg).encode('utf-8'))


def start_consumers():
    if SUB_CH is None:
        return
    SUB_CH.basic_consume(queue='leilao_iniciado', on_message_callback=on_leilao_iniciado, auto_ack=True)
    SUB_CH.basic_consume(queue='leilao_finalizado', on_message_callback=on_leilao_finalizado, auto_ack=True)
    SUB_CH.start_consuming()


_consumer_thread = threading.Thread(target=start_consumers, daemon=True)
_consumer_thread.start()


# ---------------------------------------------
# API REST: processamento de lances
# ---------------------------------------------
@app.post('/lances')
def receber_lance():
    data = request.get_json(silent=True)
    if not data:
        abort(400, description='JSON de entrada é obrigatório')

    id_leilao = data.get('id_leilao')
    id_usuario = data.get('id_usuario')
    valor_do_lance = data.get('valor_do_lance')

    if not isinstance(id_leilao, str) or not isinstance(id_usuario, str):
        abort(400, description='Campos id_leilao e id_usuario devem ser strings')

    try:
        valor_do_lance = float(valor_do_lance)
    except (TypeError, ValueError):
        abort(400, description='Campo valor_do_lance deve ser numérico')

    with _lock:
        ativo = leiloes_ativos.get(id_leilao, {}).get('ativo', False)
        if not ativo:
            # Lance inválido: leilão não está ativo
            msg_inv = {
                'id_leilao': id_leilao,
                'id_usuario': id_usuario,
                'valor_do_lance': valor_do_lance,
            }
            publish_pub('lance_invalidado', msg_inv)
            return jsonify({'status': 'invalidado', 'motivo': 'leilao_inativo'}), 400

        ultimo_valor = (ultimos_lances.get(id_leilao) or {}).get('valor_do_lance', float('-inf'))
        if valor_do_lance <= ultimo_valor:
            # Lance inválido: não supera o último lance
            msg_inv = {
                'id_leilao': id_leilao,
                'id_usuario': id_usuario,
                'valor_do_lance': valor_do_lance,
            }
            publish_pub('lance_invalidado', msg_inv)
            return jsonify({'status': 'invalidado', 'motivo': 'valor_insuficiente', 'ultimo_lance': ultimo_valor}), 400

        # Lance válido
        ultimos_lances[id_leilao] = {
            'id_usuario': id_usuario,
            'valor_do_lance': valor_do_lance,
        }
        msg_val = {
            'id_leilao': id_leilao,
            'id_usuario': id_usuario,
            'valor_do_lance': valor_do_lance,
        }
        publish_pub('lance_validado', msg_val)

    return jsonify({'status': 'validado', 'id_leilao': id_leilao, 'id_usuario': id_usuario, 'valor_do_lance': valor_do_lance}), 201


# ---------------------------------------------
# Inicialização
# ---------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, use_reloader=False)