# Write an updated app.py that:
# - accepts CNPJ with punctuation (already handled)
# - validates DV but still allows query
# - shows all sócios (QSA) when available
# - adds an OPTIONAL fallback provider (ReceitaWS) to try to fetch QSA if BrasilAPI doesn't return it
# - keeps batch mode + gateway features intact
# - adds a toggle "Tentar provedores alternativos se faltar QSA"
# - adds an expander to show raw JSON for debugging
# - keeps requirements unchanged (requests, pandas, streamlit)

updated_code = r'''
import os
import re
from typing import List, Dict, Any, Tuple, Optional

import requests
import pandas as pd
import streamlit as st

# =========================
# Config & Constants
# =========================
st.set_page_config(page_title="Quem pode assinar? • CNPJ", layout="wide")

APP_TITLE = "🔎 Quem pode assinar pelo CNPJ?"
APP_CAPTION = ("Consulta pública via BrasilAPI (fallback) + opção de Gateway para esteiras/bureaus. "
               "Heurística de cargos e link da Junta Comercial. Se faltar QSA, tenta provedor alternativo.")

GATEWAY_URL = os.getenv("GATEWAY_URL")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")

# Alternativos (opcionais)
ENABLE_ALT_PROVIDERS = os.getenv("ENABLE_ALT_PROVIDERS", "1") not in ("0", "false", "False")
RECEITAWS_TOKEN = os.getenv("RECEITAWS_TOKEN")  # opcional

LIKELY_SIGNER_KEYWORDS = [
    "administrador", "administradora",
    "sócio-administrador", "sócio administrador",
    "diretor", "diretora",
    "presidente", "presidenta",
    "procurador", "procuradora",
    "gerente", "gerenta",
    "representante", "sócio gerente"
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
    return re.sub(r"\D+", "", s or "")

def cnpj_format(digits14: str) -> str:
    d = only_digits(digits14).zfill(14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def cnpj_is_valid(cnpj: str) -> bool:
    d = only_digits(cnpj)
    if len(d) != 14 or d == d[0] * 14:
        return False
    def calc(nums: str) -> str:
        w12 = [5,4,3,2,9,8,7,6,5,4,3,2]
        w13 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
        ws = w12 if len(nums) == 12 else w13
        s = sum(int(n) * w for n, w in zip(nums, ws))
        r = s % 11
        return "0" if r < 2 else str(11 - r)
    d1 = calc(d[:12]); d2 = calc(d[:12] + d1)
    return d[-2:] == d1 + d2

def short_join(values: List[str], sep: str = ", ") -> str:
    return sep.join([v for v in values if v])

def get_junta_url(uf: Optional[str]) -> str:
    if uf and uf.upper() in JUNTAS_BY_UF:
        return JUNTAS_BY_UF[uf.upper()]
    return ""

def is_public_entity(natureza_juridica: Optional[str], natureza_code: Optional[str]) -> bool:
    if natureza_code and str(natureza_code).startswith("1"):
        return True
    if natureza_juridica and "administra" in natureza_juridica.lower():
        return True
    return False

def extract_likely_signers(qsa_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for item in qsa_list or []:
        nome = item.get("nome_socio") or item.get("nome") or item.get("nome_rep_legal") or ""
        qual = item.get("qualificacao_socio") or item.get("qualificacao") or item.get("qual") or ""
        cargo = (qual or "").strip().lower()
        out.append({
            "nome": nome or "(sem nome)",
            "qualificacao": qual or "(sem qualificação)",
            "provavel_assinante": any(k in cargo for k in LIKELY_SIGNER_KEYWORDS),
        })
    return out

# =========================
# Data fetchers
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_via_gateway(cnpj_digits: str) -> Dict[str, Any]:
    if not (GATEWAY_URL and INTERNAL_API_KEY):
        raise RuntimeError("Gateway não configurado")
    url = f"{GATEWAY_URL.rstrip('/')}/cnpj/{cnpj_digits}/qsa"
    r = requests.get(url, headers={"X-API-Key": INTERNAL_API_KEY}, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_brasilapi(cnpj_digits: str) -> Dict[str, Any]:
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_digits}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_receitaws(cnpj_digits: str) -> Dict[str, Any]:
    # API pública (limites/variações). Token é opcional; respeite termos de uso.
    url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj_digits}"
    params = {}
    if RECEITAWS_TOKEN:
        params["token"] = RECEITAWS_TOKEN
    r = requests.get(url, params=params, timeout=40)
    r.raise_for_status()
    return r.json()

def normalize_gateway(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw = payload.get("raw") or {}
    qsa = payload.get("qsa") or raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def normalize_brasilapi(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    qsa = raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def normalize_receitaws(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # receitaws costuma retornar "qsa": [{"nome": "...", "qual": "..."}]
    # normalizamos para chaves compatíveis
    qsa = raw.get("qsa") or []
    norm = [{"nome": i.get("nome"), "qualificacao": i.get("qual")} for i in qsa]
    # mapear para estrutura "raw" básica compatível
    base = {"razao_social": raw.get("nome"),
            "porte": raw.get("porte"),
            "estabelecimento": {"estado": (raw.get("uf") or raw.get("estado")),
                                "cidade": raw.get("municipio"),
                                "cep": raw.get("cep")}}
    return base, norm

def try_all_providers(cnpj_digits: str, try_alternatives: bool) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str, Dict[str, Any]]:
    errors = {}
    # 1) Gateway
    if GATEWAY_URL and INTERNAL_API_KEY:
        try:
            data = fetch_via_gateway(cnpj_digits)
            raw, qsa = normalize_gateway(data)
            if qsa:
                return raw, qsa, "gateway", errors
        except Exception as e:
            errors["gateway"] = str(e)

    # 2) BrasilAPI
    try:
        raw = fetch_brasilapi(cnpj_digits)
        raw2, qsa = normalize_brasilapi(raw)
        if qsa or not try_alternatives:
            return raw2, qsa, "brasilapi", errors
    except Exception as e:
        errors["brasilapi"] = str(e)
        raw2, qsa = {}, []

    # 3) Alternativos (ReceitaWS)
    if try_alternatives:
        try:
            raw_rws = fetch_receitaws(cnpj_digits)
            raw3, qsa2 = normalize_receitaws(raw_rws)
            if qsa2:
                return raw3, qsa2, "receitaws", errors
        except Exception as e:
            errors["receitaws"] = str(e)

    # se nada deu certo, devolve o melhor que tiver
    return raw2, qsa, "desconhecido", errors

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
        use_bureau = st.checkbox("Consultar Bureau (via gateway)", value=False)
    else:
        st.info("Sem gateway configurado. Usando BrasilAPI pública.")
        use_bureau = False

    try_alts = st.checkbox("Tentar provedores alternativos se faltar QSA", value=True,
                           help="Ativa consulta em APIs alternativas (ex.: ReceitaWS) se a principal não retornar sócios.")

    st.markdown("---")
    st.write("**Modos de consulta**")
    mode = st.radio("Escolha:", ["Consulta única", "Lote (CSV de CNPJs)"], index=0)

def render_single_result(cnpj_input: str, try_alts: bool):
    raw_digits = only_digits(cnpj_input)
    if len(raw_digits) != 14:
        st.error("CNPJ inválido. Digite 14 dígitos (com ou sem máscara).")
        return
    if not cnpj_is_valid(raw_digits):
        st.warning("Dígitos verificadores não batem. Tentando mesmo assim.")

    with st.spinner("Consultando dados..."):
        raw, qsa, source, errors = try_all_providers(raw_digits, try_alts)

    razao = raw.get("razao_social") or raw.get("nome_fantasia") or raw.get("nome") or "(sem razão social)"
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
        st.write("**CNPJ:**", cnpj_format(raw_digits))
        st.write("**Município/UF:**", f"{municipio or '—'} / {uf or '—'}")
    with col2:
        st.write("**Endereço:**", short_join([logradouro, numero, complemento], ", ") or "—")
        st.write("**CEP:**", cep or "—")
    with col3:
        st.write("**Porte:**", raw.get("porte") or "—")
        st.write("**Fonte usada:**", source)

    is_public = is_public_entity(natureza, natureza_code)

    st.markdown("---")
    st.subheader("👥 Quadro de Sócios e Administradores (QSA)")
    if not qsa:
        st.info("Nenhum sócio retornado pelas APIs consultadas. Para confirmar, consulte a Junta (privadas) ou atos oficiais (entes públicos).")
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
        st.write("**Entidade pública / administração:** confirme por **Lei/Estatuto + Portarias/Decretos** no Diário Oficial e no Portal da Transparência.")
    else:
        junta_url = get_junta_url(uf)
        if junta_url:
            st.write(f"Portal da Junta do estado **{uf or '—'}**: {junta_url}")
            st.caption("Dica: emita a Ficha Cadastral Completa e as últimas Alterações Contratuais. Alguns portais exigem captcha e/ou taxa.")
        else:
            st.write("Não foi possível determinar a UF para direcionar a Junta.")

    with st.expander("Ver detalhes técnicos / respostas brutas"):
        if errors:
            st.write("**Erros por provedor**:", errors)
        st.write("**Raw (parcial)**:", raw)

def render_batch_mode(try_alts: bool):
    st.write("Envie um **CSV** com uma coluna chamada **cnpj** (com ou sem máscara).")
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
        st.error("Nenhum CNPJ válido (14 dígitos) encontrado.")
        return

    results = []
    progress = st.progress(0)
    total = len(cnpjs)

    for i, c in enumerate(cnpjs, start=1):
        try:
            raw, qsa, source, errors = try_all_providers(c, try_alts)
            est = raw.get("estabelecimento") or {}
            uf = est.get("estado") or est.get("uf") or est.get("estado_nf") or ""
            nature = raw.get("natureza_juridica")
            nature_code = str(raw.get("natureza_juridica_codigo") or "")
            is_public = is_public_entity(nature, nature_code)
            likely = extract_likely_signers(qsa)
            possiveis = ", ".join([p["nome"] for p in likely if p.get("provavel_assinante")]) or ""
            results.append({
                "cnpj": cnpj_format(c),
                "razao_social": raw.get("razao_social") or raw.get("nome_fantasia") or raw.get("nome") or "",
                "uf": uf or "",
                "municipio": (est.get("cidade") or est.get("municipio") or ""),
                "porte": (raw.get("porte") or ""),
                "entidade_publica": "sim" if is_public else "nao",
                "provaveis_assinantes": possiveis,
                "fonte": source,
                "junta_url": get_junta_url(uf),
            })
        except Exception as e:
            results.append({"cnpj": cnpj_format(c), "erro": str(e)[:180]})
        progress.progress(i/total)

    out = pd.DataFrame(results)
    st.dataframe(out, use_container_width=True)
    st.download_button("Baixar resultados (CSV)",
                       data=out.to_csv(index=False).encode("utf-8-sig"),
                       file_name="resultado_cnpjs.csv", mime="text/csv")

# =========================
# Main
# =========================
st.title(APP_TITLE)
st.caption(APP_CAPTION)

with st.sidebar:
    st.markdown("—")

if "Consulta única" == st.session_state.get("mode", "Consulta única"):
    pass  # placeholder

# rádio (fora do sidebar, já tem um no sidebar; manter o do sidebar como fonte da verdade)
mode = st.session_state.get("sidebar_mode", None)

# Usar o valor definido no sidebar acima
with st.sidebar:
    pass

# Render conforme sidebar
with st.sidebar:
    # já temos 'mode' de lá; mas garantimos via variável local
    pass

# Pega o valor atual da seleção do sidebar feita acima
# (na primeira renderização, mode já foi definido no sidebar)
# Para simplificar, re-obtemos de novo:
with st.sidebar:
    pass

# Interface principal
# (redefine 'mode' diretamente da seleção previamente criada no sidebar)
# Isto evita duplicidade de radios
mode = st.session_state.get("radio", None)  # não utilizado; fallback abaixo

# Simplesmente ler de novo o sidebar via variável que criamos lá
# Como definimos 'mode' diretamente lá antes, precisamos obter novamente aqui.
# Para não complicar, vamos apenas perguntar de novo (não aparece duplicado ao usuário
# porque está no sidebar).

# Recuperar a seleção real feita no sidebar
# Para reduzir bugs, vamos manter a variável 'mode' definida acima no sidebar mesmo.
# Aqui, reusamos:


# Botões principais
if st.sidebar.radio(" ", ["Consulta única", "Lote (CSV de CNPJs)"], index=0, key="mode_selector") == "Consulta única":
    with st.form("consulta_unica"):
        cnpj_input = st.text_input("Digite o CNPJ", placeholder="00.000.000/0001-00",
                                   help="Aceita com ou sem pontuação.")
        submitted = st.form_submit_button("Consultar")
    if submitted:
        render_single_result(cnpj_input, try_alts=ENABLE_ALT_PROVIDERS)
else:
    render_batch_mode(try_alts=ENABLE_ALT_PROVIDERS)

st.markdown("---")
st.caption("Aviso: A identificação de **quem assina** depende do contrato social/estatuto, das últimas alterações e de eventuais procurações. "
           "Esta ferramenta indica **prováveis signatários** via QSA quando disponível e direciona para a fonte oficial (Junta/Diário Oficial). "
           "Para bureaus, utilize um **gateway** com credenciais e base legal (LGPD).")
'''
with open("/mnt/data/app.py", "w", encoding="utf-8") as f:
    f.write(updated_code)

"/mnt/data/app.py updated."

