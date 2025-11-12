import json
import datetime
import threading
import time
import queue
from typing import Dict, Set, Any, List

import requests
import pika
from flask import Flask, request, jsonify, abort, Response, send_from_directory
import os

app = Flask(__name__)

# ---------------------------------------------
# Configuração dos serviços
# ---------------------------------------------
MS_LEILAO_URL = 'http://localhost:5001'
MS_LANCE_URL = 'http://localhost:5002' 

# ---------------------------------------------
# Servir cliente estático (mesma origem)
# ---------------------------------------------
# Resolve caminho absoluto da pasta do cliente, robusto a cwd diferentes
_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_CLIENT_DIR = os.path.join(_ROOT_DIR, 'client')

@app.get('/')
def serve_home():
    return send_from_directory(_CLIENT_DIR, 'home.html')

@app.get('/client/<path:p>')
def serve_static(p):
    return send_from_directory(_CLIENT_DIR, p)

# ---------------------------------------------
# Estado em memória (notificações, SSE, agregados)
# ---------------------------------------------
# Interesses: leilao_id -> tipo -> set(client_id)
# tipos aceitos: 'lance', 'vencedor', 'pagamento'
interesses: Dict[str, Dict[str, Set[str]]] = {}

# Mapeia client_id -> fila SSE
sse_clients: Dict[str, queue.Queue] = {}

# Último valor de lance por leilão (alimentado pelos eventos)
ultimo_valor_por_leilao: Dict[str, float] = {}

# Metadados dos leilões (cacheados a partir dos eventos do MS Leilão)
leilao_meta: Dict[str, Dict[str, Any]] = {}

_lock = threading.Lock()

# ---------------------------------------------
# Helpers
# ---------------------------------------------

def ensure_client_queue(client_id: str) -> queue.Queue:
    with _lock:
        if client_id not in sse_clients:
            sse_clients[client_id] = queue.Queue(maxsize=1000)
        return sse_clients[client_id]


def sse_format(event_name: str, payload: Dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"


def notify_clients(leilao_id: str, event_type: str, payload: Dict[str, Any], only_registered: bool = True):
    # Determina quais clientes devem receber a notificação
    with _lock:
        watchers = set()
        if only_registered:
            watchers = set(interesses.get(leilao_id, {}).get(event_type, set()))
        else:
            # Broadcast para todos clientes conectados
            watchers = set(sse_clients.keys())

    msg = sse_format(event_type, payload)
    for cid in watchers:
        q = sse_clients.get(cid)
        if q:
            try:
                q.put_nowait(msg)
            except queue.Full:
                # Se a fila estiver cheia, descarta silenciosamente para proteger o gateway
                pass


# ---------------------------------------------
# Endpoints REST
# ---------------------------------------------
@app.post('/leiloes')
def gateway_criar_leilao():
    body = request.get_json(silent=True)
    if not body:
        abort(400, description='JSON de entrada é obrigatório')

    nome_produto = body.get('nome_produto')
    descricao = body.get('descricao')
    valor_inicial = body.get('valor_inicial')
    data_inicio = body.get('data_inicio')
    data_fim = body.get('data_fim')

    if not isinstance(nome_produto, str) or not isinstance(data_inicio, str) or not isinstance(data_fim, str):
        abort(400, description='Campos obrigatórios: nome_produto, data_inicio, data_fim')

    try:
        valor_inicial = float(valor_inicial)
    except (TypeError, ValueError):
        abort(400, description='valor_inicial deve ser numérico')

    # Mapeia para payload esperado pelo MS Leilão
    descricao_final = nome_produto if not descricao else f"{nome_produto} - {descricao}"
    payload = {
        'descricao': descricao_final,
        'valor_minimo': valor_inicial,
        'data_inicio': data_inicio,
        'data_fim': data_fim,
    }

    try:
        r = requests.post(f"{MS_LEILAO_URL}/leiloes", json=payload, timeout=5)
        return jsonify(r.json()), r.status_code
    except requests.RequestException:
        abort(503, description='MS Leilão indisponível')


@app.get('/leiloes/ativos')
def gateway_leiloes_ativos():
    try:
        r = requests.get(f"{MS_LEILAO_URL}/leiloes", timeout=5)
        lista = r.json()
    except requests.RequestException:
        abort(503, description='MS Leilão indisponível')

    ativos = [l for l in lista if l.get('status') == 'ativo']

    # Enriquecer com valor atual a partir dos eventos recebidos
    enriched: List[Dict[str, Any]] = []
    with _lock:
        for l in ativos:
            lid = l.get('id')
            valor_atual = ultimo_valor_por_leilao.get(lid, l.get('valor_minimo'))
            enriched.append({
                'id': lid,
                'nome_produto': l.get('descricao'),
                'descricao': l.get('descricao'),
                'valor_atual': valor_atual,
                'data_inicio': l.get('data_inicio'),
                'data_fim': l.get('data_fim'),
                'status': l.get('status'),
            })

    return jsonify(enriched)


@app.post('/lances')
def gateway_lances():
    body = request.get_json(silent=True)
    if not body:
        abort(400, description='JSON de entrada é obrigatório')

    id_leilao = body.get('id_leilao')
    id_usuario = body.get('id_usuario')
    valor_do_lance = body.get('valor_do_lance')

    if not isinstance(id_leilao, str) or not isinstance(id_usuario, str):
        abort(400, description='Campos id_leilao e id_usuario devem ser strings')

    try:
        valor_do_lance = float(valor_do_lance)
    except (TypeError, ValueError):
        abort(400, description='valor_do_lance deve ser numérico')

    payload = {
        'id_leilao': id_leilao,
        'id_usuario': id_usuario,
        'valor_do_lance': valor_do_lance,
    }

    try:
        r = requests.post(f"{MS_LANCE_URL}/lances", json=payload, timeout=5)
        return jsonify(r.json()), r.status_code
    except requests.RequestException:
        abort(503, description='MS Lance indisponível')


# ---------------------------------------------
# Notificações: registrar/cancelar interesse
# ---------------------------------------------
@app.post('/notificacoes/registrar')
def registrar_interesse():
    body = request.get_json(silent=True)
    if not body:
        abort(400, description='JSON de entrada é obrigatório')

    client_id = body.get('client_id')
    leilao_id = body.get('leilao_id')
    tipos = body.get('tipos')  # lista: ['lance', 'vencedor', 'pagamento']

    if not isinstance(client_id, str) or not isinstance(leilao_id, str) or not isinstance(tipos, list):
        abort(400, description='Campos client_id, leilao_id e tipos são obrigatórios')

    ensure_client_queue(client_id)

    with _lock:
        if leilao_id not in interesses:
            interesses[leilao_id] = {'lance': set(), 'vencedor': set(), 'pagamento': set()}
        for t in tipos:
            if t in interesses[leilao_id]:
                interesses[leilao_id][t].add(client_id)

    return jsonify({'status': 'registrado', 'client_id': client_id, 'leilao_id': leilao_id, 'tipos': tipos})


@app.post('/notificacoes/cancelar')
def cancelar_interesse():
    body = request.get_json(silent=True)
    if not body:
        abort(400, description='JSON de entrada é obrigatório')

    client_id = body.get('client_id')
    leilao_id = body.get('leilao_id')
    tipos = body.get('tipos')

    if not isinstance(client_id, str) or not isinstance(leilao_id, str) or not isinstance(tipos, list):
        abort(400, description='Campos client_id, leilao_id e tipos são obrigatórios')

    with _lock:
        reg = interesses.get(leilao_id)
        if reg:
            for t in tipos:
                if t in reg:
                    reg[t].discard(client_id)

    return jsonify({'status': 'cancelado', 'client_id': client_id, 'leilao_id': leilao_id, 'tipos': tipos})


# ---------------------------------------------
# SSE: conexões persistentes
# ---------------------------------------------
@app.get('/stream/<client_id>')
def sse_stream(client_id: str):
    q = ensure_client_queue(client_id)

    def gen():
        try:
            # mensagem de boas-vindas
            yield sse_format('connected', {'client_id': client_id})
            while True:
                msg = q.get()
                yield msg
        except GeneratorExit:
            # desconexão do cliente: limpeza leve
            with _lock:
                sse_clients.pop(client_id, None)
                # Remove client_id de todos os interesses
                for reg in interesses.values():
                    for s in reg.values():
                        s.discard(client_id)
        except Exception:
            # Em caso de erro, encerra o stream silenciosamente
            pass

    headers = {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        # CORS pode ser configurado externamente se necessário
    }
    return Response(gen(), headers=headers)


# Heartbeat simples para manter conexões vivas

def heartbeat_loop():
    while True:
        with _lock:
            cids = list(sse_clients.keys())
        for cid in cids:
            q = sse_clients.get(cid)
            if q:
                try:
                    q.put_nowait(sse_format('ping', {'t': int(time.time())}))
                except queue.Full:
                    pass
        time.sleep(15)

_heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
_heartbeat_thread.start()


# ---------------------------------------------
# RabbitMQ: consumo de eventos e despacho SSE
# ---------------------------------------------

def start_event_consumers():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        ch = conn.channel()
        # Declara filas de interesse
        ch.queue_declare(queue='lance_validado')
        ch.queue_declare(queue='lance_invalidado')
        # Exchange fanout para vencedores; gateway usa fila exclusiva
        ch.exchange_declare(exchange='vencedores_exchange', exchange_type='fanout')
        result = ch.queue_declare(queue='', exclusive=True)
        _fila_vencedores_gateway = result.method.queue
        ch.queue_bind(exchange='vencedores_exchange', queue=_fila_vencedores_gateway)
        # Pagamento (se existir)
        ch.queue_declare(queue='link_pagamento')
        ch.queue_declare(queue='status_pagamento')
        ch.basic_qos(prefetch_count=1)

        def cb_lance_validado(ch_, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                lid = data.get('id_leilao')
                valor = data.get('valor_do_lance')
                if lid and isinstance(valor, (int, float)):
                    with _lock:
                        ultimo_valor_por_leilao[lid] = float(valor)
                # Enriquecer payload com nome e tempo restante
                descricao = None
                tempo_restante_segundos = None
                with _lock:
                    meta = leilao_meta.get(lid) or {}
                    descricao = meta.get('descricao')
                    df_s = meta.get('data_fim')
                if df_s:
                    try:
                        df = datetime.datetime.fromisoformat(df_s)
                        now = datetime.datetime.now(df.tzinfo) if df.tzinfo else datetime.datetime.now()
                        tempo_restante_segundos = int((df - now).total_seconds())
                        if tempo_restante_segundos < 0:
                            tempo_restante_segundos = 0
                    except Exception:
                        tempo_restante_segundos = None

                enriched = dict(data)
                if descricao is not None:
                    enriched['nome'] = descricao
                enriched['tempo_restante_segundos'] = tempo_restante_segundos
                notify_clients(lid, 'lance', enriched, only_registered=True)
                ch_.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                print(f"ApiGateway: Erro ao processar lance válido para leilão {lid}: {e}")
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def cb_lance_invalidado(ch_, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                lid = data.get('id_leilao')
                notify_clients(lid, 'lance_invalido', data, only_registered=False)
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                print(f"ApiGateway: Erro ao processar lance inválido para leilão {lid}: {e}")
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def cb_leilao_vencedor(ch_, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                lid = data.get('id_leilao')

                print(f"ApiGateway: Recebido vencedor para leilão {lid}: {data.get('id_usuario')}")
                # Enriquecer com nome do leilão, se disponível
                enriched = dict(data)
                try:
                    with _lock:
                        meta = leilao_meta.get(lid) or {}
                        if meta.get('descricao'):
                            enriched['nome'] = meta['descricao']
                except Exception:
                    pass

                notify_clients(lid, 'vencedor', enriched, only_registered=True)
                ch_.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"ApiGateway: Erro ao processar vencedor para leilão {lid}: {e}")
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


        def cb_pagamento_link(ch_, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                lid = data.get('id_leilao')
                notify_clients(lid, 'link_pagamento', data, only_registered=True)
            except Exception as e:
                print(f"ApiGateway: Erro ao processar link de pagamento para leilão {lid}: {e}")
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def cb_pagamento_status(ch_, method, properties, body):
            try:
                data = json.loads(body.decode('utf-8'))
                lid = data.get('id_leilao')
                notify_clients(lid, 'status_pagamento', data, only_registered=True)
            except Exception as e:
                print(f"ApiGateway: Erro ao processar status de pagamento para leilão {lid}: {e}")
                ch_.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        ch.basic_consume(queue='lance_validado', on_message_callback=cb_lance_validado, auto_ack=False)
        ch.basic_consume(queue='lance_invalidado', on_message_callback=cb_lance_invalidado, auto_ack=False)
        # Consome vencedores via exchange fanout com confirmação manual
        ch.basic_consume(queue=_fila_vencedores_gateway, on_message_callback=cb_leilao_vencedor, auto_ack=False)
        ch.basic_consume(queue='link_pagamento', on_message_callback=cb_pagamento_link, auto_ack=False)
        ch.basic_consume(queue='status_pagamento', on_message_callback=cb_pagamento_status, auto_ack=False)

        ch.start_consuming()
    except Exception:
        # Se RabbitMQ estiver indisponível, o gateway continua servindo REST/SSE sem eventos
        pass

_event_thread = threading.Thread(target=start_event_consumers, daemon=True)
_event_thread.start()


# ---------------------------------------------
# Inicialização
# ---------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, use_reloader=False)