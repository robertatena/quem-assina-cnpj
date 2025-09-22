# app.py — versão limpa (sem escrever arquivos locais)
import os
import re
from typing import List, Dict, Any, Tuple, Optional

import requests
import pandas as pd
import streamlit as st

# ========= Config =========
st.set_page_config(page_title="Quem pode assinar? • CNPJ", layout="wide")

APP_TITLE = "🔎 Quem pode assinar pelo CNPJ?"
APP_CAPTION = ("Consulta pública via BrasilAPI (fallback) + opção de Gateway para esteiras/bureaus. "
               "Heurística de cargos e link da Junta Comercial. Se faltar QSA, tenta provedor alternativo.")

# Segredos/variáveis de ambiente (configure em Settings → Secrets do Streamlit)
GATEWAY_URL = os.getenv("GATEWAY_URL")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
ENABLE_ALT_PROVIDERS = os.getenv("ENABLE_ALT_PROVIDERS", "1") not in ("0", "false", "False")
RECEITAWS_TOKEN = os.getenv("RECEITAWS_TOKEN")  # opcional

LIKELY_SIGNER_KEYWORDS = [
    "administrador","administradora","sócio-administrador","sócio administrador",
    "diretor","diretora","presidente","presidenta","procurador","procuradora",
    "gerente","gerenta","representante","sócio gerente"
]

JUNTAS_BY_UF = {
    "AC":"https://www.juceac.ac.gov.br/","AL":"https://www.juceal.al.gov.br/","AM":"https://www.jucea.am.gov.br/",
    "AP":"https://www.jucap.ap.gov.br/","BA":"https://www.juceb.ba.gov.br/","CE":"https://www.jucec.ce.gov.br/",
    "DF":"https://www.jucis.df.gov.br/","ES":"https://www.jucees.es.gov.br/","GO":"https://www.juceg.go.gov.br/",
    "MA":"https://www.jucema.ma.gov.br/","MG":"https://www.jucemg.mg.gov.br/","MS":"https://www.jucems.ms.gov.br/",
    "MT":"https://www.jucemat.mt.gov.br/","PA":"https://www.jucepa.pa.gov.br/","PB":"https://www.jucep.pb.gov.br/",
    "PE":"https://www.jucepe.pe.gov.br/","PI":"https://www.jucepi.pi.gov.br/","PR":"https://www.juntacommercial.pr.gov.br/",
    "RJ":"https://www.jucerja.rj.gov.br/","RN":"https://www.jucern.rn.gov.br/","RO":"https://www.jucer.ro.gov.br/",
    "RR":"https://www.jucerr.rr.gov.br/","RS":"https://www.jucisrs.rs.gov.br/","SC":"https://www.jucesc.sc.gov.br/",
    "SE":"https://www.jucese.se.gov.br/","SP":"https://www.jucesp.sp.gov.br/","TO":"https://www.jucetins.to.gov.br/",
}
DIARIO_OFICIAL_HINTS = {
    "SP":{"municipal":"https://www.imprensaoficial.com.br/","estadual":"https://www.imprensaoficial.com.br/",
          "transparencia_municipio":"https://transparencia.prefeitura.sp.gov.br/"}
}

# ========= Utils =========
def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def cnpj_format(digits14: str) -> str:
    d = only_digits(digits14).zfill(14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

def cnpj_is_valid(cnpj: str) -> bool:
    d = only_digits(cnpj)
    if len(d) != 14 or d == d[0]*14: return False
    def calc(nums: str) -> str:
        w12=[5,4,3,2,9,8,7,6,5,4,3,2]; w13=[6,5,4,3,2,9,8,7,6,5,4,3,2]
        ws = w12 if len(nums)==12 else w13
        s = sum(int(n)*w for n,w in zip(nums,ws)); r=s%11
        return "0" if r<2 else str(11-r)
    d1=calc(d[:12]); d2=calc(d[:12]+d1)
    return d[-2:]==d1+d2

def short_join(vs: List[str], sep: str=", ")->str:
    return sep.join([v for v in vs if v])

def get_junta_url(uf: Optional[str]) -> str:
    return JUNTAS_BY_UF.get((uf or "").upper(),"") if uf else ""

def is_public_entity(natureza: Optional[str], code: Optional[str]) -> bool:
    if code and str(code).startswith("1"): return True
    if natureza and "administra" in natureza.lower(): return True
    return False

def extract_likely_signers(qsa: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out=[]
    for it in qsa or []:
        nome = it.get("nome_socio") or it.get("nome") or it.get("nome_rep_legal") or ""
        qual = it.get("qualificacao_socio") or it.get("qualificacao") or it.get("qual") or ""
        cargo = (qual or "").lower().strip()
        out.append({"nome": nome or "(sem nome)", "qualificacao": qual or "(sem qualificação)",
                    "provavel_assinante": any(k in cargo for k in LIKELY_SIGNER_KEYWORDS)})
    return out

# ========= Data fetchers =========
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_via_gateway(cnpj: str)->Dict[str,Any]:
    if not (GATEWAY_URL and INTERNAL_API_KEY):
        raise RuntimeError("Gateway não configurado")
    r = requests.get(f"{GATEWAY_URL.rstrip('/')}/cnpj/{cnpj}/qsa",
                     headers={"X-API-Key": INTERNAL_API_KEY}, timeout=30)
    r.raise_for_status(); return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_brasilapi(cnpj: str)->Dict[str,Any]:
    r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=30)
    r.raise_for_status(); return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_receitaws(cnpj: str)->Dict[str,Any]:
    url=f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
    params = {"token": RECEITAWS_TOKEN} if RECEITAWS_TOKEN else {}
    r = requests.get(url, params=params, timeout=40)
    r.raise_for_status(); return r.json()

def norm_gateway(payload: Dict[str,Any])->Tuple[Dict[str,Any],List[Dict[str,Any]]]:
    raw = payload.get("raw") or {}
    qsa = payload.get("qsa") or raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def norm_brasilapi(raw: Dict[str,Any])->Tuple[Dict[str,Any],List[Dict[str,Any]]]:
    qsa = raw.get("qsa") or raw.get("socios") or []
    return raw, qsa

def norm_receitaws(raw: Dict[str,Any])->Tuple[Dict[str,Any],List[Dict[str,Any]]]:
    qsa = raw.get("qsa") or []
    qsa_norm = [{"nome": i.get("nome"), "qualificacao": i.get("qual")} for i in qsa]
    base = {"razao_social": raw.get("nome"),
            "porte": raw.get("porte"),
            "estabelecimento": {"estado": raw.get("uf") or raw.get("estado"),
                                "cidade": raw.get("municipio"),
                                "cep": raw.get("cep")}}
    return base, qsa_norm

def try_all(cnpj: str, try_alts: bool)->Tuple[Dict[str,Any],List[Dict[str,Any]],str,Dict[str,str]]:
    errors={}
    if GATEWAY_URL and INTERNAL_API_KEY:
        try:
            raw,qsa = norm_gateway(fetch_via_gateway(cnpj))
            if qsa: return raw,qsa,"gateway",errors
        except Exception as e: errors["gateway"]=str(e)
    try:
        raw = fetch_brasilapi(cnpj); raw2,qsa = norm_brasilapi(raw)
        if qsa or not try_alts: return raw2,qsa,"brasilapi",errors
    except Exception as e:
        errors["brasilapi"]=str(e); raw2,qsa={},[]
    if try_alts:
        try:
            raw3,qsa2 = norm_receitaws(fetch_receitaws(cnpj))
            if qsa2: return raw3,qsa2,"receitaws",errors
        except Exception as e: errors["receitaws"]=str(e)
    return raw2,qsa,"desconhecido",errors

# ========= UI =========
st.title(APP_TITLE)
st.caption(APP_CAPTION)

with st.sidebar:
    st.subheader("⚙️ Configurações")
    if GATEWAY_URL and INTERNAL_API_KEY:
        st.success("Gateway configurado"); st.write(f"**Gateway:** {GATEWAY_URL}")
        use_bureau = st.checkbox("Consultar Bureau (via gateway)", value=False)
    else:
        st.info("Sem gateway configurado. Usando BrasilAPI pública."); use_bureau=False
    try_alts = st.checkbox("Tentar provedores alternativos se faltar QSA", value=ENABLE_ALT_PROVIDERS)
    st.markdown("---")
    mode = st.radio("Modos de consulta", ["Consulta única","Lote (CSV de CNPJs)"], index=0)

def render_single(cnpj_input: str, try_alts: bool):
    d = only_digits(cnpj_input)
    if len(d)!=14:
        st.error("CNPJ inválido. Digite 14 dígitos (com ou sem máscara)."); return
    if not cnpj_is_valid(d):
        st.warning("Dígitos verificadores não batem. Tentando mesmo assim.")
    with st.spinner("Consultando dados..."):
        raw,qsa,source,errors = try_all(d, try_alts)

    razao = raw.get("razao_social") or raw.get("nome_fantasia") or raw.get("nome") or "(sem razão social)"
    natureza = raw.get("natureza_juridica")
    natureza_code = str(raw.get("natureza_juridica_codigo") or "")
    est = raw.get("estabelecimento") or {}
    uf = est.get("estado") or est.get("uf") or est.get("estado_nf") or ""
    municipio = est.get("cidade") or est.get("municipio") or ""
    logradouro = " ".join([v for v in [est.get("tipo_logradouro",""), est.get("logradouro","")] if v]).strip()
    numero = est.get("numero",""); complemento = est.get("complemento",""); cep = est.get("cep","")

    st.subheader(f"📄 {razao}")
    c1,c2,c3 = st.columns(3)
    with c1:
        st.write("**CNPJ:**", cnpj_format(d))
        st.write("**Município/UF:**", f"{municipio or '—'} / {uf or '—'}")
    with c2:
        st.write("**Endereço:**", short_join([logradouro, numero, complemento], ", ") or "—")
        st.write("**CEP:**", cep or "—")
    with c3:
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
        names = [p["nome"] for p in likely if p.get("provavel_assinante")]
        if names:
            st.success(f"Prováveis signatários: {', '.join(names)}")
        else:
            st.warning("Nenhum cargo típico de assinante encontrado no QSA. Verifique contrato/alterações ou procurações.")

    st.markdown("---")
    st.subheader("🏛️ Onde confirmar quem assina")
    if is_public:
        st.write("**Entidade pública:** confirme por **Lei/Estatuto + Portarias/Decretos** no Diário Oficial e Portal da Transparência.")
    else:
        ju = get_junta_url(uf)
        if ju:
            st.write(f"Portal da Junta do estado **{uf or '—'}**: {ju}")
            st.caption("Dica: Ficha Cadastral Completa e últimas Alterações Contratuais.")
        else:
            st.write("Não foi possível determinar a UF para direcionar a Junta.")

    with st.expander("Ver detalhes técnicos / JSON bruto"):
        if errors: st.write("**Erros por provedor**:", errors)
        st.write("**Raw (parcial)**:", raw)

def render_batch(try_alts: bool):
    st.write("Envie um **CSV** com coluna **cnpj** (com ou sem máscara).")
    file = st.file_uploader("CSV com CNPJs", type=["csv"])
    if not file: return
    try:
        df = pd.read_csv(file, dtype=str)
    except Exception as e:
        st.error(f"Erro ao ler CSV: {e}"); return
    if "cnpj" not in df.columns:
        st.error("CSV deve conter coluna 'cnpj'."); return

    cnpjs = [only_digits(x) for x in df["cnpj"].fillna("").astype(str).tolist()]
    cnpjs = [c for c in cnpjs if len(c)==14]
    if not cnpjs:
        st.error("Nenhum CNPJ válido (14 dígitos) encontrado."); return

    rows=[]; prog=st.progress(0); total=len(cnpjs)
    for i,c in enumerate(cnpjs, start=1):
        try:
            raw,qsa,source,errors = try_all(c, try_alts)
            est = raw.get("estabelecimento") or {}
            uf = est.get("estado") or est.get("uf") or est.get("estado_nf") or ""
            nature = raw.get("natureza_juridica"); code = str(raw.get("natureza_juridica_codigo") or "")
            likely = extract_likely_signers(qsa)
            poss = ", ".join([p["nome"] for p in likely if p.get("provavel_assinante")]) or ""
            rows.append({
                "cnpj": cnpj_format(c),
                "razao_social": raw.get("razao_social") or raw.get("nome_fantasia") or raw.get("nome") or "",
                "uf": uf or "", "municipio": est.get("cidade") or est.get("municipio") or "",
                "porte": raw.get("porte") or "", "entidade_publica": "sim" if is_public_entity(nature,code) else "nao",
                "provaveis_assinantes": poss, "fonte": source, "junta_url": get_junta_url(uf),
            })
        except Exception as e:
            rows.append({"cnpj": cnpj_format(c), "erro": str(e)[:180]})
        prog.progress(i/total)
    out = pd.DataFrame(rows)
    st.dataframe(out, use_container_width=True)
    st.download_button("Baixar resultados (CSV)", out.to_csv(index=False).encode("utf-8-sig"),
                       file_name="resultado_cnpjs.csv", mime="text/csv")

# ========= Main =========
st.title(APP_TITLE)
st.caption(APP_CAPTION)

if 'mode' not in st.session_state:
    st.session_state.mode = mode  # pega do sidebar

if mode == "Consulta única":
    with st.form("consulta_unica"):
        cnpj_input = st.text_input("Digite o CNPJ", placeholder="00.000.000/0001-00",
                                   help="Aceita com ou sem pontuação.")
        submitted = st.form_submit_button("Consultar")
    if submitted:
        render_single(cnpj_input, try_alts=try_alts)
else:
    render_batch(try_alts=try_alts)

st.markdown("---")
st.caption("Aviso: A identificação de **quem assina** depende do contrato social/estatuto, das últimas alterações e de eventuais procurações. "
           "A ferramenta indica **prováveis signatários** via QSA quando disponível e direciona para a fonte oficial (Junta/Diário Oficial). "
           "Para bureaus, utilize um **gateway** com credenciais e base legal (LGPD).")


