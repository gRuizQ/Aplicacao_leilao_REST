// Configuração
// Usa mesma origem quando servido pelo gateway; se abrir via file://, faz fallback
const API_BASE = (window.location.origin && window.location.origin !== 'null')
  ? window.location.origin
  : 'http://127.0.0.1:5000';

// Helpers
const $ = (id) => document.getElementById(id);
const fmtISO = (dtLocal) => {
  // datetime-local => ISO (YYYY-MM-DDTHH:mm:ss)
  if (!dtLocal) return '';
  const s = dtLocal.trim();
  if (!s) return '';
  return s.length === 16 ? `${s}:00` : s; // adiciona segundos se ausentes
};

const state = {
  clientId: '',
  selectedLeilaoId: '',
  es: null,
};

function log(msg, kind = 'info') {
  const el = document.createElement('div');
  el.className = 'item';
  el.textContent = `[${kind}] ${msg}`;
  $('notifLog').prepend(el);
}

async function api(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`${API_BASE}${path}`, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.description || `Erro ${res.status}`);
  return data;
}

// SSE
function connectSSE() {
  const cid = $('clientId').value.trim() || `cliente_${Math.random().toString(36).slice(2)}`;
  state.clientId = cid;
  $('clientId').value = cid;
  $('connStatus').textContent = 'conectando...';
  if (state.es) state.es.close();

  const es = new EventSource(`${API_BASE}/stream/${encodeURIComponent(cid)}`);
  state.es = es;

  es.addEventListener('connected', (e) => {
    $('connStatus').textContent = 'conectado';
    log(`SSE conectado como ${cid}`, 'sse');
  });
  es.addEventListener('ping', () => {});

  es.addEventListener('lance', (e) => {
    const data = JSON.parse(e.data);
    log(`Novo lance válido em ${data.id_leilao}: R$ ${data.valor_do_lance} por ${data.id_usuario}`, 'lance');
  });
  es.addEventListener('lance_invalido', (e) => {
    const data = JSON.parse(e.data);
    log(`Lance inválido em ${data.id_leilao}: R$ ${data.valor_do_lance} por ${data.id_usuario}`, 'lance_invalido');
  });
  es.addEventListener('vencedor', (e) => {
    const data = JSON.parse(e.data);
    log(`Leilão ${data.id_leilao} vencedor: ${data.id_usuario} (R$ ${data.valor_do_lance})`, 'vencedor');
  });
  es.addEventListener('link_pagamento', (e) => {
    const data = JSON.parse(e.data);
    const el = document.createElement('div');
    el.className = 'item';
    el.innerHTML = `[pagamento] Link gerado para ${data.id_usuario}: <a class="link" target="_blank" href="${data.payment_link}">${data.payment_link}</a>`;
    $('notifLog').prepend(el);
  });
  es.addEventListener('status_pagamento', (e) => {
    const data = JSON.parse(e.data);
    log(`Pagamento ${data.id_transacao}: ${data.status} (R$ ${data.valor})`, 'status_pagamento');
  });

  es.onerror = () => {
    $('connStatus').textContent = 'erro';
    log('Erro no SSE, tente reconectar.', 'erro');
  };
}

// Criar leilão
async function criarLeilao(ev) {
  ev.preventDefault();
  $('criarStatus').textContent = '';
  try {
    const body = {
      nome_produto: $('nomeProduto').value.trim(),
      descricao: $('descricao').value.trim(),
      valor_inicial: parseFloat($('valorInicial').value),
      data_inicio: fmtISO($('dataInicio').value),
      data_fim: fmtISO($('dataFim').value),
    };
    const resp = await api('POST', '/leiloes', body);
    $('criarStatus').textContent = `Criado: ${resp.id || ''}`;
    await listarAtivos();
  } catch (e) {
    $('criarStatus').textContent = `Erro: ${e.message}`;
  }
}

// Listar ativos
async function listarAtivos() {
  $('listaAtivos').innerHTML = 'Carregando...';
  try {
    const lista = await api('GET', '/leiloes/ativos');
    const container = document.createElement('div');
    container.className = 'lista';
    lista.forEach((l) => {
      const card = document.createElement('div');
      card.className = 'card';
      card.innerHTML = `
        <div class="title">${l.nome_produto}</div>
        <div class="meta">ID: ${l.id} • Valor atual: R$ ${l.valor_atual}</div>
        <div class="actions">
          <button data-id="${l.id}" class="btnSel">Selecionar</button>
          <label><input type="checkbox" class="chkInt" data-t="lance" checked> Lance</label>
          <label><input type="checkbox" class="chkInt" data-t="vencedor" checked> Vencedor</label>
          <label><input type="checkbox" class="chkInt" data-t="pagamento" checked> Pagamento</label>
          <button data-id="${l.id}" class="btnReg">Registrar interesse</button>
          <button data-id="${l.id}" class="btnCancel">Cancelar interesse</button>
        </div>`;
      container.appendChild(card);
    });
    $('listaAtivos').innerHTML = '';
    $('listaAtivos').appendChild(container);

    // Bind actions
    document.querySelectorAll('.btnSel').forEach((b) => {
      b.addEventListener('click', () => {
        state.selectedLeilaoId = b.getAttribute('data-id');
        $('leilaoSelecionado').value = state.selectedLeilaoId;
        log(`Selecionado leilão ${state.selectedLeilaoId}`, 'sel');
      });
    });
    document.querySelectorAll('.btnReg').forEach((b) => {
      b.addEventListener('click', async () => {
        const id = b.getAttribute('data-id');
        const card = b.closest('.card');
        const tipos = Array.from(card.querySelectorAll('.chkInt'))
          .filter((c) => c.checked)
          .map((c) => c.getAttribute('data-t'));
        try {
          await api('POST', '/notificacoes/registrar', {
            client_id: state.clientId || $('clientId').value.trim(),
            leilao_id: id,
            tipos,
          });
          log(`Interesse registrado em ${id}: ${tipos.join(', ')}`, 'interesse');
        } catch (e) {
          log(`Falha ao registrar interesse: ${e.message}`, 'erro');
        }
      });
    });
    document.querySelectorAll('.btnCancel').forEach((b) => {
      b.addEventListener('click', async () => {
        const id = b.getAttribute('data-id');
        const card = b.closest('.card');
        const tipos = Array.from(card.querySelectorAll('.chkInt'))
          .map((c) => c.getAttribute('data-t'));
        try {
          await api('POST', '/notificacoes/cancelar', {
            client_id: state.clientId || $('clientId').value.trim(),
            leilao_id: id,
            tipos,
          });
          log(`Interesse cancelado em ${id}: ${tipos.join(', ')}`, 'interesse');
        } catch (e) {
          log(`Falha ao cancelar interesse: ${e.message}`, 'erro');
        }
      });
    });
  } catch (e) {
    $('listaAtivos').innerHTML = `Erro: ${e.message}`;
  }
}

// Efetuar lance
async function efetuarLance(ev) {
  ev.preventDefault();
  $('lanceStatus').textContent = '';
  try {
    const idLeilao = state.selectedLeilaoId || $('leilaoSelecionado').value.trim();
    const body = {
      id_leilao: idLeilao,
      id_usuario: $('idUsuario').value.trim(),
      valor_do_lance: parseFloat($('valorLance').value),
    };
    const resp = await api('POST', '/lances', body);
    $('lanceStatus').textContent = resp.status || 'ok';
    // Atualização da lista é opcional; eventos SSE refletem os lances
    await listarAtivos();
  } catch (e) {
    $('lanceStatus').textContent = `Erro: ${e.message}`;
  }
}

// Bind inicial
window.addEventListener('DOMContentLoaded', () => {
  $('btnConnect').addEventListener('click', connectSSE);
  $('formCriar').addEventListener('submit', criarLeilao);
  $('btnRefresh').addEventListener('click', listarAtivos);
  $('formLance').addEventListener('submit', efetuarLance);
  listarAtivos();
});