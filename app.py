import os
import re
import io
import json
import time
from typing import List, Dict, Any, Tuple, Optional

import requests
import pandas as pd
import streamlit as st

# =========================
# Config & Constants
# =========================
st.set_page_config(page_title="Quem pode assinar? • CNPJ", layout="wide")

APP_TITLE = "🔎 Quem pode assinar pelo CNPJ?"
APP_CAPTION = "Consulta pública via BrasilAPI (fallback) + opção de Gateway para esteiras/bureaus. Heurística de cargos e link da Junta Comercial."

# Optional backend (gateway) to unify/secure calls to bureaus (Serasa/Quod/Boa Vista...)
GATEWAY_URL = os.getenv("GATEWAY_URL")        # ex.: https://seu-backend.render.app
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")  # chave interna para autenticar no gateway

# Heurística simples de cargos mais comuns que costumam ter poderes de assinatura
LIKELY_SIGNER_KEYWORDS = [
    "administrador", "sócio-administrador", "sócio administrador",
    "diretor", "presidente", "procurador", "gerente", "sócio gerente",
    "representante", "administradora", "diretora", "presidenta", "procuradora", "gerenta"
]

JUNTAS_BY_UF = {
    "AC": "https://www.juceac.ac.gov.br/",
    "AL": "https://www.juceal.al.gov.br/",
    "AM": "https://www.jucea.am.gov.br/",
    "AP": "https://www.jucap.ap.gov.br/",
    "BA": "https://www.juceb.ba.gov.br/",
    "CE": "https://www.jucec.ce.gov.br/",
    "DF": "https://www.jucis.df.gov.br/",
    "ES": "https://www.jucees.es.gov.br/",
    "GO": "https://www.juceg.go.gov.br/",
    "MA": "https://www.jucema.ma.gov.br/",
    "MG": "https://www.jucemg.mg.gov.br/",
    "MS": "https://www.jucems.ms.gov.br/",
    "MT": "https://www.jucemat.mt.gov.br/",
    "PA": "https://www.jucepa.pa.gov.br/",
    "PB": "https://www.jucep.pb.gov.br/",
    "PE": "https://www.jucepe.pe.gov.br/",
    "PI": "https://www.jucepi.pi.gov.br/",
    "PR": "https://www.juntacommercial.pr.gov.br/",
    "RJ": "https://www.jucerja.rj.gov.br/",
    "RN": "https://www.jucern.rn.gov.br/",
    "RO": "https://www.jucer.ro.gov.br/",
    "RR": "https://www.jucerr.rr.gov.br/",
    "RS": "https://www.jucisrs.rs.gov.br/",
    "SC": "https://www.jucesc.sc.gov.br/",
    "SE": "https://www.jucese.se.gov.br/",
    "SP": "https://www.jucesp.sp.gov.br/",
    "TO": "https://www.jucetins.to.gov.br/",
}

DIARIO_OFICIAL_HINTS = {
    "SP": {
        "municipal": "https://www.imprensaoficial.com.br/",
        "estadual": "https://www.imprensaoficial.com.br/",
        "transparencia_municipio": "https://transparencia.prefeitura.sp.gov.br/",
    }
}

# =========================
# Utils
# =========================
def only_digits(s: str) -> str:
    return re.sub(r"\\D+", "", s or "")

def format_cnpj(cnpj: str) -> str:
    d = only_digits(cnpj).zfill(14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def short_join(values: List[str], sep: str = ", ") -> str:
    return sep.join([v for v in values if v])

def get_junta_url(uf: Optional[str]) -> str:
    if uf and uf.upper() in JUNTAS_BY_UF:
        return JUNTAS_BY_UF[uf.upper()]
    return ""

def is_public_entity(natureza_juridica: Optional[str], natureza_code: Optional[str]) -> bool:
    # Heurística: códigos que começam com '1' (Administração Pública) ou texto contendo "Administração Pública"
    if natureza_code and str(natureza_code).startswith("1"):
        return True
    if natureza_juridica and "administra" in natureza_juridica.lower():
        return True
    return False

def extract_likely_signers(qsa_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results = []
    for item in qsa_list or []:
        nome = item.get("nome_socio") or item.get("nome") or item.get("nome_rep_legal") or ""
        qual = item.get("qualificacao_socio") or item.get("qualificacao") or item.get("qual") or ""
        cargo = (qual or "").strip().lower()
        is_likely = any(k in cargo for k in LIKELY_SIGNER_KEYWORDS)
        results.append({
            "nome": nome or "(sem nome)",
            "qualificacao": qual or "(sem qualificação)",
            "provavel_assinante": bool(is_likely),
        })
    return results

# =========================
# Data fetchers (Gateway first, fallback BrasilAPI)
# =========================
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_via_gateway(cnpj_digits: str) -> Dict[str, Any]:
    if not (GATEWAY_URL and INTERNAL_API_KEY):
        raise RuntimeError("Gateway não configurado")
    url = f"{GATEWAY_URL.rstrip('/')}/cnpj/{cnpj_digits}/qsa"
    r = requests.get(url, headers={"X-API-Key": INTERNAL_API_KEY}, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Gateway retornou {r.status_code}: {r.text[:200]}")
    return r.json()  # esperado: {"qsa":[...], "raw":{...}}

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_brasilapi(cnpj_digits: str) -> Dict[str, Any]:
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_digits}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"BrasilAPI retornou {r.status_code}: {r.text[:200]}")
    return r.json()

def normalize_result_from_gateway(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw = payload.get("raw") or {}
    qsa = payload.get("qsa") or raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def normalize_result_from_brasilapi(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    qsa = raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def fetch_cnpj(cnpj_digits: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str]:
    """
    Returns: (raw_data, qsa_list, source_label)
    """
    if GATEWAY_URL and INTERNAL_API_KEY:
        try:
            data = fetch_via_gateway(cnpj_digits)
            raw, qsa = normalize_result_from_gateway(data)
            return raw, qsa, "gateway"
        except Exception as e:
            # fallback to brasilapi
            pass
    raw = fetch_brasilapi(cnpj_digits)
    raw, qsa = normalize_result_from_brasilapi(raw)
    return raw, qsa, "brasilapi"

# Optional: bureau lookup via gateway (e.g., Serasa) — illustrative
@st.cache_data(show_spinner=False, ttl=900)
def fetch_bureau_serasa(cnpj_digits: str) -> Optional[Dict[str, Any]]:
    if not (GATEWAY_URL and INTERNAL_API_KEY):
        return None
    url = f"{GATEWAY_URL.rstrip('/')}/pj/{cnpj_digits}/bureau/serasa"
    r = requests.get(url, headers={"X-API-Key": INTERNAL_API_KEY}, timeout=40)
    if r.status_code == 404:
        return None
    if r.status_code >= 400:
        raise RuntimeError(f"Gateway/Bureau retornou {r.status_code}: {r.text[:200]}")
    return r.json()

# =========================
# UI
# =========================
st.title(APP_TITLE)
st.caption(APP_CAPTION)

with st.sidebar:
    st.subheader("⚙️ Configurações")
    if GATEWAY_URL and INTERNAL_API_KEY:
        st.success("Gateway configurado")
        st.write(f"**Gateway:** {GATEWAY_URL}")
        use_bureau = st.checkbox("Consultar Bureau (via gateway)", value=False, help="Ex.: Serasa/Quod (requer contrato e credenciais configuradas no gateway).")
    else:
        st.info("Sem gateway configurado. Usando BrasilAPI pública.")
        use_bureau = False

    st.markdown("---")
    st.write("**Modos de consulta**")
    mode = st.radio("Escolha:", ["Consulta única", "Lote (CSV de CNPJs)"], index=0, horizontal=False)

def render_single_result(cnpj_input: str):
    cnpj_digits = only_digits(cnpj_input)
    if len(cnpj_digits) != 14:
        st.error("CNPJ inválido. Digite 14 dígitos (com ou sem máscara).")
        return

    with st.spinner("Consultando dados..."):
        try:
            raw, qsa, source = fetch_cnpj(cnpj_digits)
        except Exception as e:
            st.error(f"Erro na consulta: {e}")
            return

    razao = raw.get("razao_social") or raw.get("nome_fantasia") or "(sem razão social)"
    natureza = raw.get("natureza_juridica")
    natureza_code = str(raw.get("natureza_juridica_codigo") or "")

    est = raw.get("estabelecimento") or {}
    uf = est.get("estado") or est.get("uf") or est.get("estado_nf") or ""
    municipio = est.get("cidade") or est.get("municipio") or ""
    logradouro = " ".join([v for v in [est.get("tipo_logradouro", ""), est.get("logradouro", "")] if v]).strip()
    numero = est.get("numero", "")
    complemento = est.get("complemento", "")
    cep = est.get("cep", "")

    st.subheader(f"📄 {razao}")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("**CNPJ:**", format_cnpj(cnpj_digits))
        st.write("**Município/UF:**", f"{municipio or '—'} / {uf or '—'}")
    with col2:
        st.write("**Endereço:**", short_join([logradouro, numero, complemento], ", ") or "—")
        st.write("**CEP:**", cep or "—")
    with col3:
        st.write("**Porte:**", raw.get("porte") or "—")
        st.write("**Natureza Jurídica:**", natureza or "—")

    is_public = is_public_entity(natureza, natureza_code)

    st.markdown("---")
    st.subheader("👥 Quadro de Sócios e Administradores (QSA)")
    if not qsa:
        st.info("A API não retornou QSA. Para confirmar cargos/assinaturas, será necessário consultar a Junta (para privadas) ou atos oficiais (para entes públicos).")
    else:
        likely = extract_likely_signers(qsa)
        st.dataframe(likely, use_container_width=True)
        possiveis = [p["nome"] for p in likely if p.get("provavel_assinante")]
        if possiveis:
            st.success(f"Prováveis signatários: {', '.join(possiveis)}")
        else:
            st.warning("Nenhum cargo típico de assinante encontrado no QSA. Verifique contrato/alterações na Junta ou procurações.")

    st.markdown("---")
    st.subheader("🏛️ Onde confirmar quem assina")
    if is_public:
        st.write("**Entidade pública / administração:** confirme por **Lei/Estatuto + Portarias/Decretos de nomeação**.")
        if uf and uf.upper() in DIARIO_OFICIAL_HINTS:
            hints = DIARIO_OFICIAL_HINTS[uf.upper()]
            st.write("- Diário Oficial / Atos: ", hints.get("municipal") or hints.get("estadual"))
            if hints.get("transparencia_municipio"):
                st.write("- Portal da Transparência: ", hints["transparencia_municipio"])
        else:
            st.write("Busque no Diário Oficial do ente federativo (municipal/estadual) e no respectivo Portal da Transparência.")
    else:
        junta_url = get_junta_url(uf)
        if junta_url:
            st.write(f"Portal da Junta do estado **{uf}**: {junta_url}")
            st.caption("Dica: emita a Ficha Cadastral Completa e as últimas Alterações Contratuais. Alguns portais exigem captcha e/ou taxa.")
        else:
            st.write("Não foi possível determinar a UF para direcionar a Junta. Consulte manualmente a Junta do estado onde a empresa foi registrada.")

    # Optional Bureau (via gateway)
    if use_bureau:
        st.markdown("---")
        st.subheader("📊 Indicadores de Bureau (ex.: Serasa)")
        try:
            bureau = fetch_bureau_serasa(cnpj_digits)
            if not bureau:
                st.info("Nenhum retorno do bureau (endpoint não configurado ou sem dados).")
            else:
                # Exibição simples e ilustrativa — depende do contrato/schema do fornecedor
                score = bureau.get("score")
                pend = bureau.get("pendencias") or bureau.get("debts")
                qtd_pend = len(pend) if isinstance(pend, list) else (pend if isinstance(pend, int) else None)
                st.write("**Score:**", score if score is not None else "—")
                st.write("**Pendências (qtd.):**", qtd_pend if qtd_pend is not None else "—")
                st.json(bureau)  # mostrar bruto para inspeção inicial
                st.caption("Obs.: o schema real do bureau depende do seu contrato. Padronize isso no gateway.")
        except Exception as e:
            st.error(f"Erro consultando bureau: {e}")

def render_batch_mode():
    st.write("Envie um **CSV** com uma coluna chamada **cnpj** (14 dígitos ou formatado).")
    file = st.file_uploader("CSV com CNPJs", type=["csv"])
    if not file:
        return
    try:
        df = pd.read_csv(file, dtype=str)
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}")
        return

    if "cnpj" not in df.columns:
        st.error("CSV deve conter coluna 'cnpj'.")
        return

    cnpjs = [only_digits(x) for x in df["cnpj"].fillna("").astype(str).tolist()]
    cnpjs = [c for c in cnpjs if len(c) == 14]
    if not cnpjs:
        st.error("Nenhum CNPJ válido (14 dígitos) encontrado na coluna 'cnpj'.")
        return

    rows = []
    progress = st.progress(0)
    status = st.empty()
    total = len(cnpjs)

    for i, cnpj_digits in enumerate(cnpjs, start=1):
        status.text(f"Consultando {format_cnpj(cnpj_digits)} ({i}/{total})")
        try:
            raw, qsa, source = fetch_cnpj(cnpj_digits)
            est = raw.get("estabelecimento") or {}
            uf = est.get("estado") or est.get("uf") or est.get("estado_nf") or ""
            nature = raw.get("natureza_juridica")
            nature_code = str(raw.get("natureza_juridica_codigo") or "")
            is_public = is_public_entity(nature, nature_code)
            likely = extract_likely_signers(qsa)
            possiveis = ", ".join([p["nome"] for p in likely if p.get("provavel_assinante")]) or ""
            rows.append({
                "cnpj": format_cnpj(cnpj_digits),
                "razao_social": raw.get("razao_social") or raw.get("nome_fantasia") or "",
                "uf": uf or "",
                "municipio": (est.get("cidade") or est.get("municipio") or ""),
                "porte": (raw.get("porte") or ""),
                "natureza_juridica": (nature or ""),
                "entidade_publica": "sim" if is_public else "nao",
                "provaveis_assinantes": possiveis,
                "fonte": source,
                "junta_url": get_junta_url(uf),
            })
        except Exception as e:
            rows.append({
                "cnpj": format_cnpj(cnpj_digits),
                "razao_social": "",
                "uf": "",
                "municipio": "",
                "porte": "",
                "natureza_juridica": "",
                "entidade_publica": "",
                "provaveis_assinantes": "",
                "fonte": "erro",
                "junta_url": "",
                "erro": str(e)[:200],
            })
        progress.progress(i / total)

    status.text("Concluído.")
    out = pd.DataFrame(rows)
    st.dataframe(out, use_container_width=True)

    # botão para download CSV
    csv_bytes = out.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Baixar resultados (CSV)", data=csv_bytes, file_name="resultado_cnpjs.csv", mime="text/csv")

# =========================
# Main Interaction
# =========================
st.markdown("—")
if mode == "Consulta única":
    with st.form("consulta_unica"):
        cnpj_input = st.text_input("Digite o CNPJ", placeholder="00.000.000/0001-00")
        submitted = st.form_submit_button("Consultar")
    if submitted:
        render_single_result(cnpj_input)
else:
    render_batch_mode()

st.markdown("---")
st.caption("Aviso: A identificação de **quem assina** depende do contrato social/estatuto, das últimas alterações e de eventuais procurações. Esta ferramenta indica **prováveis signatários** via QSA quando disponível, e direciona para a fonte oficial (Junta/Diário Oficial). Para bureaus, utilize um **gateway** com as devidas credenciais e base legal (LGPD).")
# quem-assina-cnpjstreamlit run app.py --server.port 8501 --server.address 0.0.0.0
