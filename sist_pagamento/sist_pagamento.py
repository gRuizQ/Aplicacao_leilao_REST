import random
import threading
import time
from typing import Dict, Any

import requests
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# Armazenamento simples em memória
_transactions: Dict[str, Dict[str, Any]] = {}
_tx_seq = 0


def _next_tx_id() -> str:
    global _tx_seq
    _tx_seq += 1
    return f"txn_{_tx_seq:06d}"


def _process_async(tx_id: str, valor: float, moeda: str, cliente_id: str, leilao_id: str, webhook_url: str):
    # Simula processamento de pagamento
    time.sleep(random.uniform(2.0, 5.0))
    status = random.choice(['aprovado', 'recusado'])
    payload = {
        'id_transacao': tx_id,
        'status': status,
        'valor': valor,
        'moeda': moeda,
        'cliente_id': cliente_id,
        'leilao_id': leilao_id,
    }
    try:
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception:
        # Ignora erros de callback; sistema externo é best-effort
        pass


@app.post('/transacoes')
def iniciar_transacao():
    data = request.get_json(silent=True)
    if not data:
        abort(400, description='JSON de entrada é obrigatório')

    valor = data.get('valor')
    moeda = data.get('moeda')
    cliente_id = data.get('cliente_id')
    leilao_id = data.get('leilao_id')
    webhook_url = data.get('webhook_url')

    if valor is None or not moeda or not cliente_id or not webhook_url:
        abort(400, description='Campos obrigatórios: valor, moeda, cliente_id, webhook_url')

    tx_id = _next_tx_id()
    payment_link = f"http://localhost:5004/pagar/{tx_id}"

    print(f"Transação iniciada: {tx_id} - Valor: {valor} {moeda} - Cliente: {cliente_id} - Leilão: {leilao_id} - Webhook: {webhook_url}")

    _transactions[tx_id] = {
        'valor': valor,
        'moeda': moeda,
        'cliente_id': cliente_id,
        'leilao_id': leilao_id,
        'webhook_url': webhook_url,
        'payment_link': payment_link,
        'status': 'pendente',
    }

    # Dispara processamento assíncrono
    t = threading.Thread(target=_process_async, args=(tx_id, valor, moeda, cliente_id, leilao_id, webhook_url), daemon=True)
    t.start()

    return jsonify({'transaction_id': tx_id, 'payment_link': payment_link})


@app.get('/health')
def health():
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, use_reloader=False)