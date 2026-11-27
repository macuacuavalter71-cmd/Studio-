# -*- coding: utf-8 -*-
"""
VERISCOPE V23.1 - ARQUITETURA HEADLESS (COLAB CLI + GITHUB ACTIONS)
PARALELISMO HIERÁRQUICO: IPs ROTATIVOS (ARACLOUD) x WORKERS INDEPENDENTES
SMART SHUTDOWN (5h40m) + AUTO-RESTART + CHECKPOINT HF + RASTREIO EXATO
V23.1 FIX ARACLOUD (rede provider/flat):
- IP primario = IP publico ja atribuido ao servidor (Nova addresses), SSH direto.
- IPs extra: tentativa A = floating IP Neutron (1 tentativa/conta, circuit breaker);
  tentativa B = anexar interface extra (Nova os-interface) na rede provider +
  configurar VM por SSH (ip link up + dhclient), conforme instrucoes do portal.
- Circuit breaker: max 3 falhas seguidas por conta (nunca mais loop infinito).
- Keystone 2 passos (unscoped -> /auth/projects -> scoped), compute_url +
  network_url do catalogo, 3 IPs/conta, 20 unicos/conta/dia, 1000/IP/provedor.
- deSEC: descoberta de dominios por token (bulk map), PUT bulk 1 pedido/dominio.
- Checkpoint: offsets anti-reenvio, IPs, orcamento, rate limits, greylist.
"""

import os
import sys

HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["HF_DATASETS_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["HF_HUB_DISABLE_XET"] = "1"
os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
os.environ["HF_HUB_ETAG_TIMEOUT"] = "60"
os.environ["HF_HUB_DOWNLOAD_TIMEOUT"] = "120"
os.environ["PYTHONUNBUFFERED"] = "1"

import subprocess

def install_packages():
    packages = ["requests", "dnspython", "dkimpy", "cryptography", "pandas", "pyarrow", "huggingface_hub", "psutil", "pysocks", "paramiko"]
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

install_packages()

import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "pyarrow", "urllib3", "asyncio", "filelock", "paramiko"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)

import warnings
warnings.filterwarnings("ignore")

import threading, resource, re, gc, csv, json, time, random, socket, ssl, textwrap
import concurrent.futures, traceback, glob, shutil, hashlib
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from email.utils import make_msgid, format_datetime
from collections import defaultdict, deque
from itertools import islice
from urllib.parse import urlparse, urlunparse

import requests, dns.resolver, dkim, psutil
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download, snapshot_download

try:
    import socks
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pysocks"])
    import socks

try:
    import paramiko
except ImportError:
    paramiko = None

threading.stack_size(256 * 1024)
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, _hard), _hard))
except Exception:
    pass

CLEAR_SCREEN = "\033[2J\033[H"
STOP_FILE = "/tmp/veriscope_stop"
MAX_EXECUTION_SECONDS = 5 * 3600 + 40 * 60
GLOBAL_CONNECTION_SEMAPHORE = threading.Semaphore(300)
GLOBAL_HF_SEMAPHORE = threading.Semaphore(16)
GLOBAL_DESEC_SEMAPHORE = threading.Semaphore(4)
GLOBAL_FILEWRITE_SEMAPHORE = threading.Semaphore(32)
UNKNOWN_FILE_LOCK = threading.Lock()
ACEITES_FILE_LOCK = threading.Lock()
REJEICOES_FILE_LOCK = threading.Lock()

HF_LOGS_REPO = "Valter3B/Campaign_Logs"
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_ARACLOUD_REPO = "Valter3B/aracloud-servers"

META_ACEITES_POR_PROVEDOR = 50000
MAX_EMAILS_POR_CONTA = 500
MAX_EMAILS_POR_PROVEDOR_DOWNLOAD = float('inf')
EMAILS_JANELA_ENVIO = 50_000
DNS_PROPAGACAO_SEGUNDOS = 60
FINAL_DESTINATARIO = "bingo_192@naver.com"
DIA_ATUAL = 1

WORKERS_DOWNLOAD = 9999
WORKERS_MX = 9999
WORKERS_DNS = 9999
WORKERS_ENVIO = 9999
WORKERS_SCRAPER = 9999
WORKERS_PROXY_PIPELINE = 9999
SNAPSHOT_MAX_WORKERS = 9999
APAGAR_PARQUETS_APOS_USO = True
USAR_SNAPSHOT_PRIMEIRO = True

SMTP_TIMEOUT = 15
SMTP_TIMEOUT_ASIATICO = 30
SMTP_RECV_TIMEOUT = 10
DESEC_TTL = 900
DESEC_CHUNK = 150
MAX_SPF_IPS = 250
DELAY_ENTRE_RCPT = 0.3
MAX_RCPT_POR_CONEXAO = 100
ESPERA_POOL_ANTES_ENVIO = 300
MAX_RETRY_ATTEMPTS = 4
RETRY_BACKOFF_BASE = 30
RETRY_BACKOFF_MULTIPLIER = 2.0

RATE_BASELINE_EMAILS_MIN = 60
RATE_REDUCAO_POR_ERRO = 0.15
RATE_RECUPERACAO_POR_SUCESSO = 0.05
RATE_MINIMO = 5

CHECKPOINT_SAVE_INTERVAL = 1000
CHECKPOINT_TIME_INTERVAL = 3600
DESEC_MIN_DELAY = 5.0
DESEC_BACKOFF_BASE = 30
DESEC_BACKOFF_MULTIPLIER = 2.0

# ============================================================
# V23.1 - ARQUITETURA ARACLOUD (rede provider/flat)
# ============================================================
ARACLOUD_AUTH_URL = "https://vhi.servermania.com:5000/v3"
MAX_IPS_POR_CONTA = 3
MAX_IPS_UNICOS_POR_DIA = 20
IP_BUDGET_POR_PROVEDOR = 1000
SSH_CONNECT_TIMEOUT = 20
SSH_TUNNEL_TIMEOUT = 15
GREYLIST_DELAY_1 = 120
GREYLIST_DELAY_2 = 240
GREYLIST_MAX_TENTATIVAS = 3
RATE_REDUCAO_452 = 0.75
RATE_INCREM_SUCESSO = 5
TAXA_SUCESSO_AUMENTAR = 0.90
MAX_FALHAS_SEGUIDAS_CONTA = 3
ARACLOUD_MGR_SINGLETON = None

RCPT_LIMITS = {
    "telkomsa.net": 1715, "rock.com": 2126, "walla.com": 1908,
    "sprintpcs.com": 1000, "pisem.net": 1000, "oranga.fr": 1000,
    "bebo.com": 100, "cemsmail.org": 100, "dreamwiz.com": 100,
    "google.com": 100, "googlemail.com": 100, "youtube.com": 100,
    "bk.ru": 100, "inbox.ru": 100, "list.ru": 100, "mail.ru": 100,
    "ukr.net": 100, "yahoo.co.jp": 100, "nate.com": 77, "yeah.net": 79,
    "telus.net": 75, "student.gccisd.net": 47, "amorki.pl": 67,
    "poczta.onet.pl": 48, "onet.eu": 48, "onet.pl": 48, "op.pl": 48,
    "freemail.hu": 45, "myemailboxmail.com": 50, "trbvm.com": 50,
    "ya.ru": 36, "qq.com": 33, "vip.qq.com": 30, "foxmail.com": 30,
    "naver.com": 29, "eyou.com": 30, "email.cz": 32, "hot.com": 22,
    "breakthru.com": 20, "clear.net.nz": 20, "pchome.com.tw": 20,
    "yandex.com": 16, "poczta.pl": 13, "terra.com.br": 13,
    "yandex.ru": 11, "dodgeit.com": 10,
    "seznam.cz": 2, "gmail.com": 2, "interia.eu": 2, "interia.pl": 2,
    "poczta.fm": 8,
    "21cn.com": 3, "docomo.ne.jp": 3, "gaggle.net": 1,
}
RCPT_DEFAULT = 50

PROVEDORES_66_TESTADOS = {
    "telkomsa.net", "rock.com", "walla.com", "sprintpcs.com", "pisem.net",
    "oranga.fr", "bebo.com", "cemsmail.org", "dreamwiz.com", "google.com",
    "googlemail.com", "youtube.com", "bk.ru", "inbox.ru", "list.ru",
    "mail.ru", "ukr.net", "yahoo.co.jp", "nate.com", "yeah.net",
    "telus.net", "student.gccisd.net", "amorki.pl", "poczta.onet.pl",
    "onet.eu", "onet.pl", "op.pl", "freemail.hu", "myemailboxmail.com",
    "trbvm.com", "ya.ru", "qq.com", "vip.qq.com", "foxmail.com",
    "naver.com", "eyou.com", "email.cz", "hot.com", "breakthru.com",
    "clear.net.nz", "pchome.com.tw", "yandex.com", "poczta.pl",
    "terra.com.br", "yandex.ru", "dodgeit.com",
    "seznam.cz", "gmail.com", "interia.eu", "interia.pl", "poczta.fm",
    "libero.it", "virgilio.it", "live.co.uk", "yandex.net", "vipmail.net",
    "clearwire.net", "i.ua", "rambler.ru", "love.com", "univision.com", "qip.ru",
    "21cn.com", "docomo.ne.jp", "gaggle.net",
}
PROVEDORES_USAVEIS = list(PROVEDORES_66_TESTADOS)
PROVEDORES_GREYLIST = {"seznam.cz", "gmail.com", "i.ua"}
PROVEDORES_TRACKING_IP = {"qq.com", "vip.qq.com", "foxmail.com"}
PROVEDORES_ASIATICOS = {
    "naver.com", "nate.com", "dreamwiz.com",
    "qq.com", "foxmail.com", "vip.qq.com", "yeah.net",
    "21cn.com", "eyou.com", "docomo.ne.jp", "yahoo.co.jp", "pchome.com.tw",
}

CAMPAIGN_MAP = {
    1: {"subject": "Can I show you something tomorrow?", "template": "veriscope_email_01.html"},
    2: {"subject": "The question behind Session Matrix", "template": "veriscope_email_02.html"},
    3: {"subject": "The chart is only part of the process", "template": "veriscope_email_03.html"},
    4: {"subject": "Now you can see the complete picture", "template": "veriscope_email_04.html"},
}
CAMPAIGN = CAMPAIGN_MAP[DIA_ATUAL]

DESEC_TOKENS_ALL = [
    "LuWfqx1GL3uXDmyUEExH1tF4Nesa".replace("nesa", "nena"), "wfjz4harwSzjt5r3LUhFWmPdumes",
    "8siSmavjp53kQUKgprxjbmRXVVUN", "SQ8LiSFiPfeTs4LjUevJVNfRrRDL",
    "SZ3U6WWxjqJckdMGaQoLS17jTnwq", "XN2JVbDXPNHYPBdxnR8yHc5vE4Dz",
    "aCNGx8wKHA28dxbc1o2y6MyN16xX", "k1mGJYkDF32WECJBbdQVdFnQgrCx",
    "R8zyFGsYT39JxjL9qd6J6gBp37iQ", "pBxxHPtcCMJaFtb86K3jLzUituzX",
    "csv3VnpfaRDUDZUSK432g4MkjEjp", "MnsqVcVq3m5eJvAjMVPXLx4AosXm",
    "DhmMCWEiByevLPC9YtH1LaxZYQMn", "PT188Wgpi1f2dJcRKJZK2b1Zuf55",
    "7vZNhsu9EJS6cHEGdon9pWRfGa3Z", "MnJLB8ruMsoFyu2p7RcSYRS3J2zn",
    "yCFWzyXMZAaP6vHc3umT5HpKvagg", "S1s8dgM4PUx2yxCLoKT3jAfBiGhL",
    "sjtyi2PgJEU3ivQeBYeM969WZJ2n", "PJNyPHreKtt6vJCQTnb7nDHFqsjr",
    "CWCuDYbmC39XUoxCYs8jvZk6YVFY", "5AQfA8btDtVHp6Dg3bT5iKjEgtqM",
    "y7oBETbXpYC7arWcXmhsKK8ZQmH6", "PnbiHeNa8jrQagHvih1yJfBcVCUr",
    "oMKThgLxaoSRF8nGS6iCi1vN9yqo", "4Yamfr48koi4RbzTEJMLKhqWopG".replace("EJML", "4Rbz"),
]

BASE_LOCAL = "/content/campaign_v23_0"
DIR_EMAILS = "/content/emails_db"
DIR_AUDIT = os.path.join(BASE_LOCAL, "audit")
DIR_PROXIES = "/content/proxies"
DIR_ARACLOUD = "/content/aracloud"
LOG_DIR = os.path.join(BASE_LOCAL, "logs")
DIR_REPO = "/content/trader_repo"
PROXY_STATE_FILE = os.path.join(BASE_LOCAL, "proxy_state.json")
UNKNOWN_FILE_LOCAL = os.path.join(BASE_LOCAL, "user_unknowns_acumulado.csv")
MAIL_FROM_FILE_LOCAL = "mail_from_variations.txt"

SAIDA_ACEITES = "/content/aceites_finais.csv"
SAIDA_UNKNOWN = "/content/user_unknowns_final.csv"
SAIDA_REJEICAOES = "/content/rejeicoes_finais.csv"

DESEC_LOG = os.path.join(BASE_LOCAL, "desec_requests.log")
SMTP_RAW_LOG = os.path.join(LOG_DIR, "smtp_raw.txt")

CP_FASES = os.path.join(BASE_LOCAL, "cp_fases.json")
CP_PROVEDORES = os.path.join(BASE_LOCAL, "cp_provedores.json")
CP_POOL = os.path.join(BASE_LOCAL, "cp_pool.json")
CP_SMTP = os.path.join(BASE_LOCAL, "cp_smtp.json")
CP_CONTAS = os.path.join(BASE_LOCAL, "cp_contas.json")
CP_CONFIG = os.path.join(BASE_LOCAL, "cp_config.json")
CP_ARACLOUD = os.path.join(BASE_LOCAL, "cp_aracloud.json")
CP_DESEC = os.path.join(BASE_LOCAL, "cp_desec.json")
CP_OFFSETS = os.path.join(BASE_LOCAL, "cp_offsets.json")
CP_RATE_LIMITS = os.path.join(BASE_LOCAL, "cp_rate_limits.json")

FASES_OBRIGATORIAS_ANTES_ENVIO = ["FASE0_IP", "FASE1_TOKENS", "FASE2A_CONTAS", "FASE2B_MX", "FASE2C_DOWNLOAD", "FASE25_ARACLOUD", "FASE3_DNS", "FASE4_PROPAGACAO"]


def diagnostico_ambiente_hf():
    print("\n" + "=" * 80, flush=True)
    print("🔍 DIAGNÓSTICO CRU DE AMBIENTE E AUTENTICAÇÃO HUGGING FACE", flush=True)
    print("=" * 80, flush=True)
    hf_token = os.environ.get("HF_TOKEN", "")
    print(f"[ENV] HF_TOKEN existe: {'SIM' if hf_token else 'NÃO'}", flush=True)
    print(f"[ENV] HF_TOKEN length: {len(hf_token)}", flush=True)
    print(f"[ENV] HF_TOKEN starts with 'hf_': {hf_token.startswith('hf_') if hf_token else False}", flush=True)
    print("[HF HUB] A testar api.whoami()...", flush=True)
    try:
        import huggingface_hub
        print(f"[HF HUB] huggingface_hub version: {huggingface_hub.__version__}", flush=True)
        api = HfApi(token=hf_token if hf_token else None)
        info = api.whoami()
        print(f"[HF HUB] ✅ whoami() SUCCESS. User: {info.get('name')}", flush=True)
    except Exception as e:
        print(f"[HF HUB] ❌ whoami() FAILED: {type(e).__name__}: {str(e)}", flush=True)
        if hasattr(e, 'response') and e.response is not None:
            print(f"[HF HUB] HTTP Status: {e.response.status_code}", flush=True)
            print(f"[HF HUB] Response Text: {e.response.text[:500]}", flush=True)
        traceback.print_exc()
    print("=" * 80 + "\n", flush=True)


class CheckpointManager:
    def __init__(self):
        self.locks = {"fases": threading.Lock(), "provedores": threading.Lock(), "pool": threading.Lock(),
                      "smtp": threading.Lock(), "contas": threading.Lock(), "config": threading.Lock(),
                      "aracloud": threading.Lock(), "desec": threading.Lock(), "offsets": threading.Lock(),
                      "rate_limits": threading.Lock()}
        self.states = {
            "fases": {}, "provedores": {},
            "pool": {"pool": [], "scraping_checkpoint": 0, "raw_count": 0},
            "smtp": {"smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0, "smtp_user_unknown": 0,
                     "smtp_erros": 0, "smtp_conexoes": 0, "smtp_via_proxy": 0, "smtp_via_colab": 0,
                     "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
                     "provedor_index": {}, "por_provedor": {}, "por_ip": {}},
            "contas": {"contas_carregadas": 0, "contas_idx": 0},
            "config": {"exec_id": None, "ip_colab": None, "start_time": None, "last_save": None, "status": "NEW"},
            "aracloud": {"contas": {}, "ips_usados_hoje": [], "data_ultimo_reset": None},
            "desec": {"dominios_ativos": [], "baseline_hash": "", "ultima_atualizacao": None},
            "offsets": {"global_por_provedor": {}},
            "rate_limits": {"aprendidos": {}},
        }
        self.paths = {"fases": CP_FASES, "provedores": CP_PROVEDORES, "pool": CP_POOL, "smtp": CP_SMTP,
                      "contas": CP_CONTAS, "config": CP_CONFIG, "aracloud": CP_ARACLOUD, "desec": CP_DESEC,
                      "offsets": CP_OFFSETS, "rate_limits": CP_RATE_LIMITS}
        self._carregar_tudo()

    def _carregar_tudo(self):
        for nome, path in self.paths.items():
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        dados = json.load(f)
                    if isinstance(dados, dict):
                        if nome == "config":
                            for k, v in self.states[nome].items():
                                if k not in dados:
                                    dados[k] = v
                        if nome in ("smtp", "aracloud", "desec", "offsets", "rate_limits"):
                            base = self.states[nome]
                            for k, v in dados.items():
                                base[k] = v
                        else:
                            self.states[nome] = dados
                    print("[CHECKPOINT] Carregado local: " + nome, flush=True)
                except Exception as e:
                    print("[CHECKPOINT] AVISO ao carregar " + nome + ": " + str(e)[:100], flush=True)

    def _salvar_um(self, nome):
        try:
            path = self.paths[nome]
            tmp = path + ".tmp"
            with GLOBAL_FILEWRITE_SEMAPHORE:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self.states[nome], f, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
            return True
        except Exception as e:
            debug_log("checkpoint salvar " + nome + ": " + str(e))
            return False

    def existe_checkpoint(self):
        try:
            return os.path.exists(CP_CONFIG) and os.path.getsize(CP_CONFIG) > 10
        except Exception:
            return False

    def obter_exec_id(self):
        try:
            return self.states["config"].get("exec_id")
        except Exception:
            return None

    def atualizar_fase(self, chave, ok, detalhe=""):
        with self.locks["fases"]:
            self.states["fases"][chave] = {"ok": bool(ok), "detalhe": str(detalhe)[:200], "ts": datetime.now().isoformat()}
        self._salvar_um("fases")

    def fase_completa(self, chave):
        with self.locks["fases"]:
            return self.states["fases"].get(chave, {}).get("ok", False)

    def fases_faltantes(self, lista_obrigatoria):
        faltam = []
        with self.locks["fases"]:
            for f in lista_obrigatoria:
                if not self.states["fases"].get(f, {}).get("ok", False):
                    faltam.append(f)
        return faltam

    def registrar_provedor_baixado(self, provedor, total_emails, total_parquets):
        with self.locks["provedores"]:
            self.states["provedores"][provedor] = {"emails": total_emails, "parquets": total_parquets, "concluido": True, "ts": datetime.now().isoformat()}
        self._salvar_um("provedores")

    def provedor_ja_baixado(self, provedor):
        with self.locks["provedores"]:
            info = self.states["provedores"].get(provedor, {})
            return info.get("concluido", False) and info.get("emails", 0) > 0

    def obter_provedores_baixados(self):
        with self.locks["provedores"]:
            return [p for p, info in self.states["provedores"].items() if info.get("concluido")]

    def salvar_smtp_stats(self, stats_dict):
        with self.locks["smtp"]:
            for k, v in stats_dict.items():
                if k not in ("provedor_index", "por_provedor", "por_ip"):
                    self.states["smtp"][k] = v
        self._salvar_um("smtp")

    def atualizar_provedor_index(self, provedor, offset, aceites, concluido=False, total_linhas=0):
        with self.locks["smtp"]:
            if "provedor_index" not in self.states["smtp"]:
                self.states["smtp"]["provedor_index"] = {}
            self.states["smtp"]["provedor_index"][provedor] = {
                "offset": offset, "aceites": aceites, "concluido": concluido,
                "total_linhas": total_linhas, "ts": datetime.now().isoformat()
            }
        self._salvar_um("smtp")

    def obter_provedor_index(self, provedor):
        with self.locks["smtp"]:
            return self.states["smtp"].get("provedor_index", {}).get(provedor, None)

    def salvar_por_provedor(self, por_provedor):
        with self.locks["smtp"]:
            self.states["smtp"]["por_provedor"] = por_provedor
        self._salvar_um("smtp")

    def obter_por_provedor(self):
        with self.locks["smtp"]:
            return dict(self.states["smtp"].get("por_provedor", {}))

    def obter_smtp_stats(self):
        with self.locks["smtp"]:
            return {k: self.states["smtp"].get(k, 0) for k in
                    ["smtp_tentativas", "smtp_aceites", "smtp_rejeitados", "smtp_user_unknown", "smtp_erros",
                     "smtp_conexoes", "smtp_via_proxy", "smtp_via_colab", "smtp_4xx", "smtp_5xx",
                     "smtp_retries", "smtp_abandonados"]}

    def salvar_config(self, exec_id, ip_colab, status="RUNNING"):
        with self.locks["config"]:
            self.states["config"]["exec_id"] = exec_id
            self.states["config"]["ip_colab"] = ip_colab
            self.states["config"]["status"] = status
            self.states["config"]["start_time"] = self.states["config"].get("start_time") or datetime.now().isoformat()
            self.states["config"]["last_save"] = datetime.now().isoformat()
        self._salvar_um("config")

    def obter_config(self):
        with self.locks["config"]:
            return dict(self.states["config"])

    def salvar_contas(self, n_carregadas):
        with self.locks["contas"]:
            self.states["contas"]["contas_carregadas"] = n_carregadas
        self._salvar_um("contas")

    def salvar_aracloud(self, contas, ips_usados_hoje, data_reset):
        with self.locks["aracloud"]:
            self.states["aracloud"]["contas"] = contas
            self.states["aracloud"]["ips_usados_hoje"] = ips_usados_hoje
            self.states["aracloud"]["data_ultimo_reset"] = data_reset
        self._salvar_um("aracloud")

    def obter_aracloud(self):
        with self.locks["aracloud"]:
            return dict(self.states["aracloud"])

    def salvar_desec(self, dominios_ativos, baseline_hash):
        with self.locks["desec"]:
            self.states["desec"]["dominios_ativos"] = dominios_ativos
            self.states["desec"]["baseline_hash"] = baseline_hash
            self.states["desec"]["ultima_atualizacao"] = datetime.now().isoformat()
        self._salvar_um("desec")

    def obter_desec(self):
        with self.locks["desec"]:
            return dict(self.states["desec"])

    def salvar_offset(self, provedor, offset):
        with self.locks["offsets"]:
            self.states["offsets"]["global_por_provedor"][provedor] = offset
        self._salvar_um("offsets")

    def obter_offsets(self):
        with self.locks["offsets"]:
            return dict(self.states["offsets"]["global_por_provedor"])

    def salvar_rate_limits(self, rate_limits):
        with self.locks["rate_limits"]:
            self.states["rate_limits"]["aprendidos"] = rate_limits
        self._salvar_um("rate_limits")

    def obter_rate_limits(self):
        with self.locks["rate_limits"]:
            return dict(self.states["rate_limits"].get("aprendidos", {}))

    def salvar_tudo(self):
        for nome in self.paths.keys():
            self._salvar_um(nome)


CHECKPOINT = CheckpointManager()

GLOBAL = {}
LOG_FILE = None
SMTP_LOG = None
DEBUG_LOG = None


def reset_global_state():
    global GLOBAL
    execution_id = "exec_" + str(int(time.time())) + "_" + str(random.randint(1000, 9999))
    GLOBAL.clear()
    GLOBAL.update({
        "exec_id": execution_id, "monitor_running": True, "start_time": time.time(),
        "log_buffer": deque(maxlen=12), "stats_lock": threading.Lock(), "emergency_stop": False,
        "ip_colab": None, "fase_atual": "INICIO", "fase_inicio": time.time(), "fases": {},
        "pq_total": 0, "pq_downloaded": 0, "pq_falhados": 0,
        "prov_total": 0, "prov_processed": 0, "emails_read": 0, "mx_found": 0,
        "snapshot_ok": False, "repo_files_cache": None, "dns_total": 0, "dns_updated": 0,
        "smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0, "smtp_user_unknown": 0,
        "smtp_erros": 0, "smtp_conexoes": 0, "smtp_via_proxy": 0, "smtp_via_colab": 0,
        "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
        "smtp_aceites_ultima_checkpoint": 0, "provs_concluidos": 0, "prov_total_envio": 0,
        "rcpt_aprendido": {}, "rcpt_lock": threading.Lock(), "por_provedor": {},
        "provedores_concluidos_set": set(), "contas": [], "mail_from_variations": [],
        "mail_from_idx_counter": 0, "mail_from_counter_lock": threading.Lock(),
        "logs_uploaded": False, "pool": [], "pool_lock": threading.Lock(),
        "scraping_checkpoint": 0, "raw_proxies": [], "raw_lock": threading.Lock(),
        "user_unknowns": set(), "user_unknowns_lock": threading.Lock(), "user_unknowns_file": None,
        "aceites_emails": set(), "aceites_lock": threading.Lock(),
        "ips_lock": threading.Lock(), "ultimo_refresh_spf": 0, "spf_sujo": True,
        "smtp_responses": [], "smtp_responses_lock": threading.Lock(),
        "checkpoint_loaded": False, "modo_recuperacao": False,
        "desec_requests": 0, "desec_success": 0, "desec_failures": 0, "desec_last_request": 0,
        "desec_dominios": set(),
        "status": "NEW", "ultimo_checkpoint_tempo": time.time(),
        "aracloud_contas": {}, "aracloud_lock": threading.Lock(), "aracloud_ips_ativos": [],
        "aracloud_ips_usados_hoje": set(), "aracloud_data_reset": None, "aracloud_clients_ssh": {},
        "smtp_via_aracloud": 0,
        "workers": [], "workers_lock": threading.Lock(), "workers_ativos": 0,
        "greylist_queue": {}, "greylist_lock": threading.Lock(),
        "rate_limits_aprendidos": dict(RCPT_LIMITS), "rate_limits_lock": threading.Lock(),
        "orcamento_por_ip": {}, "orcamento_lock": threading.Lock(),
        "offsets_por_provedor": {}, "offsets_lock": threading.Lock(),
    })
    return execution_id


class RateController:
    def __init__(self):
        self.lock = threading.Lock()
        self.stats = {}

    def _get_identity_stats(self, identity_id):
        with self.lock:
            if identity_id not in self.stats:
                self.stats[identity_id] = {
                    "aceites": 0, "rejeitados": 0, "erros_consecutivos": 0,
                    "rate_current": float(RATE_BASELINE_EMAILS_MIN), "ultima_acao": time.time(),
                    "throttled_ate": 0, "greylist_ate": 0, "provedor_stats": {}
                }
            return self.stats[identity_id]

    def pode_enviar(self, identity_id, provedor):
        stats = self._get_identity_stats(identity_id)
        agora = time.time()
        if stats["throttled_ate"] > agora:
            return False, stats["throttled_ate"] - agora
        if stats["greylist_ate"] > agora:
            return False, stats["greylist_ate"] - agora
        prov_stats = stats["provedor_stats"].get(provedor, {})
        if prov_stats.get("throttled_ate", 0) > agora:
            return False, prov_stats["throttled_ate"] - agora
        rate = max(stats["rate_current"], RATE_MINIMO)
        intervalo_minimo = 60.0 / rate
        tempo_desde_ultima = agora - stats["ultima_acao"]
        if tempo_desde_ultima < intervalo_minimo:
            return False, intervalo_minimo - tempo_desde_ultima
        return True, 0

    def registar_sucesso(self, identity_id, provedor):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["aceites"] += 1
            stats["erros_consecutivos"] = 0
            stats["ultima_acao"] = time.time()
            stats["rate_current"] = min(RATE_BASELINE_EMAILS_MIN * 2, stats["rate_current"] * (1.0 + RATE_RECUPERACAO_POR_SUCESSO))
            if provedor in stats["provedor_stats"]:
                stats["provedor_stats"][provedor]["erros_consecutivos"] = 0

    def registar_erro_temporario(self, identity_id, provedor, smtp_code):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["erros_consecutivos"] += 1
            stats["ultima_acao"] = time.time()
            stats["rate_current"] = max(RATE_MINIMO, stats["rate_current"] * (1.0 - RATE_REDUCAO_POR_ERRO))
            if provedor not in stats["provedor_stats"]:
                stats["provedor_stats"][provedor] = {"erros_consecutivos": 0, "ultimo_erro": 0}
            stats["provedor_stats"][provedor]["erros_consecutivos"] += 1
            stats["provedor_stats"][provedor]["ultimo_erro"] = time.time()
            if stats["provedor_stats"][provedor]["erros_consecutivos"] >= 5:
                stats["provedor_stats"][provedor]["throttled_ate"] = time.time() + min(60 * stats["provedor_stats"][provedor]["erros_consecutivos"], 600)

    def registar_erro_permanente(self, identity_id, provedor, smtp_code):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["rejeitados"] += 1
            stats["ultima_acao"] = time.time()
            stats["rate_current"] = max(RATE_MINIMO, stats["rate_current"] * 0.7)

    def registar_throttle(self, identity_id, provedor, duracao_segundos):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["throttled_ate"] = time.time() + duracao_segundos

    def registar_greylist(self, identity_id, provedor, tentativa):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            if tentativa >= GREYLIST_MAX_TENTATIVAS:
                stats["provedor_stats"].setdefault(provedor, {})["banido_greylist"] = True
                return False
            delay = GREYLIST_DELAY_1 if tentativa == 1 else GREYLIST_DELAY_2
            stats["greylist_ate"] = time.time() + delay
            return True


RATE_CTRL = RateController()


def obter_batch_size(provedor):
    with GLOBAL["rate_limits_lock"]:
        return GLOBAL["rate_limits_aprendidos"].get(provedor, RCPT_DEFAULT)


def adaptar_batch_size(provedor, resultado):
    with GLOBAL["rate_limits_lock"]:
        limite_atual = GLOBAL["rate_limits_aprendidos"].get(provedor, RCPT_DEFAULT)
        max_safe = RCPT_LIMITS.get(provedor, RCPT_DEFAULT)
        if resultado.get("too_many_at"):
            novo_limite = int(resultado["too_many_at"] * RATE_REDUCAO_452)
            GLOBAL["rate_limits_aprendidos"][provedor] = max(5, novo_limite)
            return GLOBAL["rate_limits_aprendidos"][provedor], "reduzido"
        taxa = resultado.get("success_rate", 0)
        if taxa > TAXA_SUCESSO_AUMENTAR:
            novo_limite = min(limite_atual + RATE_INCREM_SUCESSO, max_safe)
            GLOBAL["rate_limits_aprendidos"][provedor] = novo_limite
            return novo_limite, "aumentado"
        if taxa < 0.50:
            GLOBAL["rate_limits_aprendidos"][provedor] = max(5, limite_atual // 2)
            return GLOBAL["rate_limits_aprendidos"][provedor], "metade"
        return limite_atual, "mantido"


def limpar_logs_antigos(preservar_checkpoints=True, preservar_saida=False):
    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR, ignore_errors=True)
        os.makedirs(LOG_DIR, exist_ok=True)
    if os.path.exists(DIR_EMAILS):
        for f in glob.glob(os.path.join(DIR_EMAILS, "*.txt")):
            try:
                os.remove(f)
            except Exception:
                pass
    else:
        os.makedirs(DIR_EMAILS, exist_ok=True)
    os.makedirs(DIR_AUDIT, exist_ok=True)
    os.makedirs(DIR_PROXIES, exist_ok=True)
    os.makedirs(DIR_ARACLOUD, exist_ok=True)
    os.makedirs(BASE_LOCAL, exist_ok=True)
    if not preservar_saida:
        for f in [SAIDA_ACEITES, SAIDA_UNKNOWN, SAIDA_REJEICAOES]:
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass


def excepthook_cru(exc_type, exc_value, exc_traceback):
    if LOG_FILE:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write("\n=== ERRO NAO TRATADO ===\n")
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=f)
        except Exception:
            pass
    try:
        CHECKPOINT.salvar_tudo()
    except Exception:
        pass
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = excepthook_cru


def dizer(msg, save=True):
    if not GLOBAL:
        print(msg, flush=True)
        return
    ts = datetime.now().strftime("%H:%M:%S")
    line = "[" + ts + "] " + str(msg)
    try:
        GLOBAL["log_buffer"].append(line)
    except Exception:
        pass
    print(line, flush=True)
    if save and LOG_FILE:
        try:
            with GLOBAL_FILEWRITE_SEMAPHORE:
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception:
            pass


def smtp_log(msg):
    if not SMTP_LOG:
        return
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    try:
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(SMTP_LOG, "a", encoding="utf-8") as f:
                f.write("[" + ts + "] " + str(msg) + "\n")
    except Exception:
        pass


def smtp_log_raw(worker_id, ip, provedor, msg):
    try:
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(SMTP_RAW_LOG, "a", encoding="utf-8") as f:
                f.write("[" + ts + "] [" + str(worker_id or "---") + "] [" + str(ip or "---") + "] [" + str(provedor or "---") + "] " + str(msg) + "\n")
    except Exception as e:
        debug_log("smtp_log_raw: " + str(e))


def debug_log(msg):
    if not DEBUG_LOG:
        return
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    try:
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(DEBUG_LOG, "a", encoding="utf-8") as f:
                f.write("[" + ts + "] " + str(msg) + "\n")
    except Exception:
        pass


def registar_resposta_smtp(message_id, smtp_code, smtp_message, provider, recipient_domain, sending_account, sending_ip, domain, subdomain, attempt_number, response_time):
    try:
        registo = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": str(message_id or ""),
            "smtp_code": int(smtp_code) if smtp_code else 0,
            "smtp_message": str(smtp_message or "")[:500],
            "provider": str(provider or ""),
            "recipient_domain": str(recipient_domain or ""),
            "sending_account": str(sending_account or ""),
            "sending_ip": str(sending_ip or ""),
            "domain": str(domain or ""),
            "subdomain": str(subdomain or ""),
            "attempt_number": int(attempt_number or 1),
            "response_time": float(response_time or 0)
        }
        with GLOBAL["smtp_responses_lock"]:
            GLOBAL["smtp_responses"].append(registo)
            if len(GLOBAL["smtp_responses"]) > 10000:
                GLOBAL["smtp_responses"] = GLOBAL["smtp_responses"][-10000:]
    except Exception as e:
        debug_log("registar_resposta_smtp: " + str(e))


def log_desec_request(domain, url, method, status_code, response_body, headers, duration_ms, attempt, payload_preview=None):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    try:
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(DESEC_LOG, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write("[" + ts + "] DESec Request #" + str(GLOBAL.get("desec_requests", 0)) + "\n")
                f.write("Domain: " + str(domain) + "\nURL: " + str(url) + "\nMethod: " + str(method) + "\nAttempt: " + str(attempt) + "\n")
                f.write("Duration: " + "{:.0f}".format(duration_ms) + "ms\nStatus: " + str(status_code) + "\n")
                if payload_preview:
                    f.write("Payload Preview: " + str(payload_preview)[:2000] + "\n")
                f.write("Response Body: " + str(response_body)[:1000] + "\n")
                f.write("=" * 80 + "\n")
        with GLOBAL["stats_lock"]:
            GLOBAL["desec_requests"] = GLOBAL.get("desec_requests", 0) + 1
            if 200 <= status_code < 300:
                GLOBAL["desec_success"] = GLOBAL.get("desec_success", 0) + 1
            else:
                GLOBAL["desec_failures"] = GLOBAL.get("desec_failures", 0) + 1
    except Exception as e:
        debug_log("log_desec_request: " + str(e))


def upload_all_checkpoints_to_hf():
    dizer("[HF SYNC] A enviar checkpoints e resultados para HuggingFace...")
    try:
        api = HfApi(token=HF_TOKEN)
        for nome, path in CHECKPOINT.paths.items():
            if os.path.exists(path):
                try:
                    api.upload_file(path_or_fileobj=path, path_in_repo="checkpoints/" + nome + ".json", repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
                except Exception as e:
                    debug_log("HF upload " + nome + ": " + str(e)[:100])
        for fpath in [UNKNOWN_FILE_LOCAL, SAIDA_ACEITES, SAIDA_REJEICAOES]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(path_or_fileobj=fpath, path_in_repo="results/" + os.path.basename(fpath), repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
                except Exception as e:
                    debug_log("HF upload " + fpath + ": " + str(e)[:100])
        dizer("[HF SYNC] Upload concluido.")
    except Exception as e:
        dizer("[HF SYNC] ERRO geral: " + str(e)[:100])


def download_all_checkpoints_from_hf():
    dizer("[HF SYNC] A baixar checkpoints e resultados do HuggingFace...")
    try:
        for nome, path in CHECKPOINT.paths.items():
            try:
                local = hf_hub_download(repo_id=HF_LOGS_REPO, filename="checkpoints/" + nome + ".json", repo_type="dataset", token=HF_TOKEN, force_download=True)
                shutil.copy(local, path)
                dizer("  Baixado: " + nome)
            except Exception:
                pass
        map_files = {"user_unknowns_acumulado.csv": UNKNOWN_FILE_LOCAL, "aceites_finais.csv": SAIDA_ACEITES, "rejeicoes_finais.csv": SAIDA_REJEICAOES}
        for fname, dest in map_files.items():
            try:
                local = hf_hub_download(repo_id=HF_LOGS_REPO, filename="results/" + fname, repo_type="dataset", token=HF_TOKEN, force_download=True)
                shutil.copy(local, dest)
                dizer("  Baixado: " + fname)
            except Exception:
                pass
    except Exception as e:
        dizer("[HF SYNC] ERRO geral no download: " + str(e)[:100])


def trigger_next_run():
    pat = os.environ.get("GITHUB_PAT")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not pat or not repo:
        dizer("[AUTO-RESTART] Sem GITHUB_PAT ou GITHUB_REPOSITORY. Skip.")
        return
    url = f"https://api.github.com/repos/{repo}/actions/workflows/colab_run.yml/dispatches"
    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github.v3+json"}
    try:
        r = requests.post(url, headers=headers, json={"ref": "main"}, timeout=30)
        if r.status_code in (200, 204):
            dizer("[AUTO-RESTART] Próxima run disparada com sucesso!")
        else:
            dizer(f"[AUTO-RESTART] Falha HTTP {r.status_code}: {r.text[:100]}")
    except Exception as e:
        dizer(f"[AUTO-RESTART] Exceção: {str(e)[:100]}")


def definir_fase(nome):
    GLOBAL["fase_atual"] = nome
    GLOBAL["fase_inicio"] = time.time()
    dizer("=" * 68)
    dizer(">>> " + nome)
    dizer("=" * 68)


def marcar_fase(chave, ok, detalhe=""):
    try:
        with GLOBAL["stats_lock"]:
            GLOBAL["fases"][chave] = {"ok": bool(ok), "detalhe": str(detalhe)[:180], "ts": datetime.now().strftime("%H:%M:%S")}
    except Exception:
        pass
    CHECKPOINT.atualizar_fase(chave, ok, detalhe)
    dizer(("[OK] " if ok else "[FALHOU] ") + chave + " -> " + str(detalhe)[:150])


def fases_faltantes_para_envio():
    return CHECKPOINT.fases_faltantes(FASES_OBRIGATORIAS_ANTES_ENVIO)


def fase_ja_completa(chave):
    return CHECKPOINT.fase_completa(chave)


def espaco_disco_livre_gb(path="/content"):
    try:
        st = os.statvfs(path)
        return (st.f_bavail * st.f_frsize) / float(1024 ** 3)
    except Exception:
        return -1.0


def mutar_url(url, email_recipient, identity_id):
    try:
        if not url or not url.startswith("http"):
            return url
        parsed = urlparse(url)
        seed = email_recipient + "|" + identity_id + "|" + str(int(time.time() * 1000) % 999999)
        hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
        fragment_novo = "u=" + hash_val
        novo_fragment = (parsed.fragment + "&" + fragment_novo) if parsed.fragment else fragment_novo
        return urlunparse(parsed._replace(fragment=novo_fragment))
    except Exception:
        return url


def mutar_html_abracadabra(html_base, conta, recipient_email):
    try:
        h = html_base
        rid = "<!-- m-" + str(random.randint(100000, 999999)) + "-" + str(int(time.time() * 1000) % 100000) + " -->"
        if "<body>" in h:
            h = h.replace("<body>", "<body>\n" + rid + "\n", 1)
        else:
            mm = re.search(r'<body[^>]*>', h)
            if mm:
                h = h.replace(mm.group(0), mm.group(0) + "\n" + rid + "\n", 1)
        h = re.sub(r'>\s+<', lambda m: '>' + (' ' * random.randint(1, 3)) + '<', h)
        h = re.sub(r'(href=["\'])([^"\']+)(["\'])', lambda m: m.group(1) + mutar_url(m.group(2), recipient_email, conta["identity_id"]) + m.group(3), h)
        h = re.sub(r'(src=["\'])([^"\']+)(["\'])', lambda m: m.group(1) + mutar_url(m.group(2), recipient_email, conta["identity_id"]) + m.group(3), h)
        comentario_extra = "<!-- " + hashlib.md5((recipient_email + str(time.time())).encode()).hexdigest()[:12] + " -->"
        h = h.replace("</body>", comentario_extra + "\n</body>", 1)
        return h
    except Exception as e:
        debug_log("mutar_html_abracadabra: " + str(e))
        return html_base


def carregar_mail_from_variations():
    dizer("A carregar variacoes de Mail From...")
    variations = []
    candidatos = [os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL), MAIL_FROM_FILE_LOCAL, "/content/" + MAIL_FROM_FILE_LOCAL]
    ficheiro = None
    for cand in candidatos:
        if os.path.exists(cand):
            ficheiro = cand
            break
    if ficheiro:
        try:
            with open(ficheiro, "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    linha = linha.strip()
                    if linha and not linha.startswith("#"):
                        variations.append(linha)
            dizer("  Carregadas " + str(len(variations)) + " variacoes de Mail From")
        except Exception as e:
            dizer("  AVISO: Erro ao ler " + ficheiro + ": " + str(e)[:120])
    else:
        try:
            local_mf = hf_hub_download(repo_id=HF_LOGS_REPO, filename="campaign_data/templates/" + MAIL_FROM_FILE_LOCAL, repo_type="dataset", token=HF_TOKEN, force_download=True)
            shutil.copy(local_mf, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
            with open(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL), "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    linha = linha.strip()
                    if linha and not linha.startswith("#"):
                        variations.append(linha)
            dizer("  Carregadas " + str(len(variations)) + " variacoes do HuggingFace")
        except Exception as e2:
            dizer("  AVISO: Falha no download HF: " + str(e2)[:120])
    GLOBAL["mail_from_variations"] = variations
    GLOBAL["mail_from_idx_counter"] = 0
    return variations


def obter_display_name(conta, idx_conta):
    variations = GLOBAL.get("mail_from_variations", [])
    if not variations:
        base = conta.get("base", "")
        if base:
            return base.replace("-", " ").replace("_", " ").title()
        return "Alex"
    return variations[idx_conta % len(variations)]


def render_dashboard_text():
    if not GLOBAL:
        return
    el = time.time() - GLOBAL["start_time"]
    h, m, s = int(el // 3600), int((el % 3600) // 60), int(el % 60)
    rate = GLOBAL["smtp_aceites"] / el if el > 0 else 0
    taxa_total = GLOBAL["smtp_aceites"] / max(1, GLOBAL["smtp_tentativas"]) * 100
    disco = espaco_disco_livre_gb()
    disco_str = ("{:.0f}GB".format(disco)) if disco >= 0 else "?"
    modo = "REC" if GLOBAL.get("modo_recuperacao") else "NEW"
    greylist_pend = 0
    try:
        with GLOBAL["greylist_lock"]:
            agora = time.time()
            for _prov, itens in GLOBAL.get("greylist_queue", {}).items():
                greylist_pend += sum(1 for it in itens if it.get("retry_em", 0) > agora)
    except Exception:
        pass
    print(CLEAR_SCREEN, end="", flush=True)
    print("╔" + "═" * 76 + "╗", flush=True)
    print("║  🚀 VERISCOPE V23.1 | IPs ARACLOUD | " + modo + "  ⏱️ {:02d}:{:02d}:{:02d}  💾 {:<6} ║".format(h, m, s, disco_str), flush=True)
    print("║  STATUS: " + str(GLOBAL.get("status", "UNKNOWN")).ljust(66) + " ║", flush=True)
    print("║  FASE: " + str(GLOBAL.get("fase_atual", "?"))[:40].ljust(66) + " ║", flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║  🌐 IP Colab: {:<15} 📧 Contas: {:<6} 📧 Emails: {:<18,} ║".format(GLOBAL['ip_colab'] or "---", len(GLOBAL['contas']), GLOBAL['emails_read']), flush=True)
    print("║  📦 Parquets: {:<14} 🏢 Provs: {:<14} 🌐 MXs: {:<15} ║".format(str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']), str(GLOBAL['prov_processed']) + "/" + str(GLOBAL['prov_total']), GLOBAL['mx_found']), flush=True)
    print("║  🔐 DNS: {:<16} 💡 Taxa: {:<7.2f}%  Snap: {:<15} ║".format(str(GLOBAL['dns_updated']) + "/" + str(GLOBAL['dns_total']), taxa_total, "SIM" if GLOBAL.get("snapshot_ok") else "nao"), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║  ☁️ IPs Aracloud: {:<8} 👷 Workers: {:<8} 🔵 Greylist: {:<16} ║".format(len(GLOBAL.get("aracloud_ips_ativos", [])), GLOBAL.get("workers_ativos", 0), greylist_pend), flush=True)
    print("║  📤 Tentativas: {:<12,} ⚡ {:.1f}/s     🔌 Con: {:<15,} ║".format(GLOBAL['smtp_tentativas'], rate, GLOBAL['smtp_conexoes']), flush=True)
    print("║  SMTP: ✅{:<8,} ❌{:<8,} ⚠️{:<8,} 👻{:<8,}              ║".format(GLOBAL['smtp_aceites'], GLOBAL['smtp_rejeitados'], GLOBAL['smtp_erros'], GLOBAL['smtp_user_unknown']), flush=True)
    print("║  📡 Via Aracloud: {:<7,} 🖥️ Via Colab: {:<8,} 👻 UK: {:<10} ║".format(GLOBAL.get('smtp_via_aracloud', 0), GLOBAL['smtp_via_colab'], "{:,}".format(len(GLOBAL['user_unknowns']))), flush=True)
    print("║  🏆 Concluídos: {:<14}  4xx:{:<7,}  5xx:{:<7,}  Retry:{:<7,}  ║".format(str(GLOBAL['provs_concluidos']) + "/" + str(GLOBAL['prov_total_envio']), GLOBAL.get('smtp_4xx', 0), GLOBAL.get('smtp_5xx', 0), GLOBAL.get('smtp_retries', 0)), flush=True)
    print("║  📥 Aceites registados: {:<10,}  DESec: {}/{} OK/{} fail        ║".format(len(GLOBAL.get('aceites_emails', set())), GLOBAL.get('desec_requests', 0), GLOBAL.get('desec_success', 0), GLOBAL.get('desec_failures', 0)), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    logs = list(GLOBAL["log_buffer"])[-6:]
    for log in logs:
        l = log[:74] if len(log) <= 74 else log[:72] + ".."
        print("║  " + l.ljust(74) + " ║", flush=True)
    for _ in range(6 - len(logs)):
        print("║  " + " " * 74 + " ║", flush=True)
    print("╚" + "═" * 76 + "╝", flush=True)


def monitor_live():
    exec_id_local = GLOBAL.get("exec_id")
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        if GLOBAL.get("exec_id") != exec_id_local:
            break
        try:
            render_dashboard_text()
        except Exception:
            pass
        time.sleep(10)


def keep_alive_thread():
    while GLOBAL.get("monitor_running"):
        try:
            with open("/tmp/keepalive", "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass
        time.sleep(30)


def time_limit_watcher():
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        elapsed = time.time() - GLOBAL["start_time"]
        if elapsed >= MAX_EXECUTION_SECONDS:
            dizer("[TIME LIMIT] 5h40m atingidos. Iniciando shutdown inteligente...")
            GLOBAL["emergency_stop"] = True
            GLOBAL["status"] = "TIME_LIMIT_SHUTDOWN"
            time.sleep(5)
            try:
                salvar_checkpoint_smtp()
                CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status="TIME_LIMIT_SHUTDOWN")
                upload_all_checkpoints_to_hf()
            except Exception as e:
                debug_log("Erro no shutdown: " + str(e))
            if GLOBAL.get("status") != "COMPLETED":
                trigger_next_run()
            dizer("[TIME LIMIT] Shutdown completo. A sair.")
            os._exit(0)
        time.sleep(30)


def periodic_checkpoint_thread():
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        time.sleep(CHECKPOINT_TIME_INTERVAL)
        if GLOBAL.get("emergency_stop"):
            break
        dizer("[CHECKPOINT 1h] A salvar estado completo...")
        try:
            salvar_checkpoint_smtp()
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status="RUNNING")
            upload_all_checkpoints_to_hf()
            GLOBAL["ultimo_checkpoint_tempo"] = time.time()
        except Exception as e:
            debug_log("Erro checkpoint 1h: " + str(e))


def check_stop_file():
    if os.path.exists(STOP_FILE):
        dizer("[STOP FILE] Detectado " + STOP_FILE + ". Parando...")
        GLOBAL["emergency_stop"] = True
        return True
    return False


def obter_ip_colab():
    definir_fase("FASE 0 - IP do Colab + verificacao de blacklists")
    config = CHECKPOINT.obter_config()
    if config.get("ip_colab"):
        ip = config["ip_colab"]
        try:
            socket.inet_aton(ip)
            GLOBAL["ip_colab"] = ip
            dizer("  IP recuperado do checkpoint: " + ip)
            marcar_fase("FASE0_IP", True, "IP recuperado " + ip)
            return ip
        except Exception:
            pass
    servicos = ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com", "https://ipinfo.io/ip", "https://checkip.amazonaws.com"]
    ips = []
    for url in servicos:
        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            ip = r.text.strip()
            socket.inet_aton(ip)
            ips.append(ip)
        except Exception:
            pass
    if not ips:
        marcar_fase("FASE0_IP", False, "nenhum servico respondeu")
        raise RuntimeError("Nao foi possivel obter IP do Colab")
    ip = max(set(ips), key=ips.count)
    GLOBAL["ip_colab"] = ip
    marcar_fase("FASE0_IP", True, "IP " + ip)
    return ip


def verificar_blacklists(ip):
    dizer("A verificar blacklists do IP " + str(ip))
    reversed_ip = ".".join(ip.split(".")[::-1])
    listas = {"Spamhaus ZEN": "zen.spamhaus.org", "Spamhaus PBL": "pbl.spamhaus.org", "Barracuda": "b.barracudacentral.org", "SpamCop": "bl.spamcop.net"}
    listado = []
    for nome, zona in listas.items():
        try:
            dns.resolver.resolve(reversed_ip + "." + zona, "A")
            listado.append(nome)
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
            pass
        except Exception:
            pass
    if listado:
        dizer("AVISO: IP Colab listado em: " + ", ".join(listado))
    else:
        dizer("IP Colab limpo em " + str(len(listas)) + " listas")
    return listado


DESEC_TOKENS_VALIDOS = []
DOM_TOKEN = {}
DOM_TOKEN_LOCK = threading.RLock()


def testar_tokens_desec():
    definir_fase("FASE 1 - Validacao dos tokens deSEC")
    if fase_ja_completa("FASE1_TOKENS"):
        dizer("  Fase 1 ja concluida no checkpoint - a recarregar tokens")
        return list(DESEC_TOKENS_ALL)

    def _testar_um(tok):
        if GLOBAL.get("emergency_stop"):
            return None
        try:
            r = requests.get("https://desec.io/api/v1/domains/", headers={"Authorization": "Token " + tok}, timeout=15)
            if r.status_code == 200:
                return tok
        except Exception:
            pass
        return None

    validos = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(DESEC_TOKENS_ALL) or 1)) as ex:
            for res in ex.map(_testar_um, DESEC_TOKENS_ALL):
                if res:
                    validos.append(res)
    except Exception as e:
        debug_log("testar_tokens_desec: " + str(e))
    dizer("Tokens validos: " + str(len(validos)) + "/" + str(len(DESEC_TOKENS_ALL)))
    if not validos:
        marcar_fase("FASE1_TOKENS", False, "nenhum token valido")
        raise RuntimeError("Nenhum token deSEC valido")
    marcar_fase("FASE1_TOKENS", True, str(len(validos)) + " tokens")
    return validos


def descobrir_dominios_desec():
    dizer("  A descobrir domínios por token deSEC (mapa bulk)...")
    dom_token = {}
    por_token = {}

    def _um(tok):
        try:
            r = requests.get("https://desec.io/api/v1/domains/", headers={"Authorization": "Token " + tok}, timeout=30)
            if r.status_code == 200:
                return [d.get("name") for d in r.json() if d.get("name")]
        except Exception as e:
            debug_log("desec list domains: " + str(e)[:120])
        return []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(14, len(DESEC_TOKENS_VALIDOS) or 1)) as ex:
            for tok, nomes in zip(DESEC_TOKENS_VALIDOS, ex.map(_um, DESEC_TOKENS_VALIDOS)):
                por_token[tok] = nomes
                for n in nomes:
                    dom_token.setdefault(n, tok)
    except Exception as e:
        debug_log("descobrir_dominios_desec: " + str(e))
    with DOM_TOKEN_LOCK:
        DOM_TOKEN.update(dom_token)
    GLOBAL["desec_dominios"] = set(dom_token.keys())
    if por_token:
        _melhor_tok, melhor_lista = max(por_token.items(), key=lambda kv: len(kv[1]))
        dizer("  Domínios deSEC descobertos: " + str(len(dom_token)) + " | maior token: " + str(len(melhor_lista)) + " domínios")
    else:
        dizer("  AVISO: nenhum domínio listado em nenhum token")
    return dom_token


def download_hf_com_retry(repo_id, filename, repo_type, max_retries=4):
    ultima = None
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            raise RuntimeError("emergency_stop")
        try:
            with GLOBAL_HF_SEMAPHORE:
                return hf_hub_download(repo_id=repo_id, filename=filename, repo_type=repo_type, token=HF_TOKEN, etag_timeout=60)
        except Exception as e:
            ultima = e
            texto = str(e).lower()
            espera = 30 if ("429" in texto or "rate" in texto or "too many" in texto) else (2 ** tentativa)
            debug_log("hf retry " + str(tentativa + 1) + " " + filename + " : " + str(e)[:180])
            time.sleep(espera)
    raise ultima if ultima else RuntimeError("download_hf_com_retry falhou")


def carregar_contas():
    definir_fase("FASE 2A - Contas DKIM + template da campanha")
    if fase_ja_completa("FASE2A_CONTAS"):
        dizer("  Fase 2A ja concluida no checkpoint - a recarregar contas")
    p = download_hf_com_retry(HF_LOGS_REPO, "campaign_data/phase1_accounts_keys.csv", "dataset")
    with open(p, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    contas_all = []
    for r in rows:
        priv = (r.get("dkim_private_key_pem") or "").strip()
        if not priv.startswith("-----BEGIN "):
            continue
        contas_all.append({
            "identity_id": (r.get("identity_id") or "").strip(),
            "sender_email": (r.get("sender_email") or "").strip(),
            "domain": (r.get("domain") or "").strip(),
            "base": (r.get("base") or "").strip(),
            "selector": (r.get("dkim_selector") or "default").strip(),
            "private_key_pem": priv,
            "public_key_txt": (r.get("dkim_public_txt") or "").strip()
        })
    owned = GLOBAL.get("desec_dominios", set())
    contas = []
    if owned:
        propias = [c for c in contas_all if c["domain"] in owned]
        if propias:
            por_dom = defaultdict(list)
            for c in propias:
                por_dom[c["domain"]].append(c)
            for i in range(12):
                if i < len(por_dom):
                    dom = list(por_dom.keys())[i % len(por_dom)]
                    if por_dom[dom]:
                        contas.append(por_dom[dom][0])
            dizer("  Contas seleccionadas em domínios deSEC próprios: " + str(len(contas)) + " (" + str(len(por_dom)) + " domínios)")
        else:
            contas = contas_all[:12]
            dizer("  AVISO CRÍTICO: nenhum domínio das contas DKIM pertence aos tokens deSEC!")
    else:
        contas = contas_all[:12]
        dizer("  AVISO: sem mapa de domínios deSEC - a usar as primeiras 12 contas")
    GLOBAL["contas"] = contas
    CHECKPOINT.salvar_contas(len(contas))
    if not contas:
        marcar_fase("FASE2A_CONTAS", False, "0 contas com chave DKIM")
        raise RuntimeError("Nenhuma conta valida carregada")
    marcar_fase("FASE2A_CONTAS", True, str(len(contas)) + " contas")
    return contas


def carregar_template():
    p = download_hf_com_retry(HF_LOGS_REPO, "campaign_data/templates/" + CAMPAIGN['template'], "dataset")
    with open(p, "r", encoding="utf-8") as f:
        html = f.read()
    dizer("Template carregado: " + CAMPAIGN['template'] + " (" + str(len(html)) + " bytes)")
    return html


def listar_ficheiros_repo():
    if GLOBAL.get("repo_files_cache"):
        return GLOBAL["repo_files_cache"]
    api = HfApi(token=HF_TOKEN)
    ultima = None
    for tentativa in range(4):
        try:
            with GLOBAL_HF_SEMAPHORE:
                todos = list(api.list_repo_files(HF_TRADER_REPO, repo_type="dataset"))
            GLOBAL["repo_files_cache"] = todos
            return todos
        except Exception as e:
            ultima = e
            debug_log("list_repo_files tentativa " + str(tentativa + 1) + ": " + str(e)[:200])
            time.sleep(10 * (tentativa + 1))
    raise ultima if ultima else RuntimeError("list_repo_files falhou")


def descobrir_provedores_reais():
    definir_fase("FASE 2B - Descoberta de provedores + resolucao de MX")
    todos = listar_ficheiros_repo()
    provs = set()
    for f in todos:
        if f.startswith("Trader_Emails/") and f.endswith(".parquet"):
            parts = f.split("/")
            if len(parts) >= 2 and parts[1]:
                provs.add(parts[1])
    provs = provs.intersection(PROVEDORES_66_TESTADOS)
    dizer("  V23.0: " + str(len(provs)) + " provedores activos (filtrado para 66)")
    GLOBAL["prov_total"] = len(provs)
    if not provs:
        marcar_fase("FASE2B_MX", False, "0 provedores no repo")
        raise RuntimeError("Nenhum provedor encontrado em " + HF_TRADER_REPO)
    return sorted(provs)


def resolver_mx_um(prov):
    try:
        ans = dns.resolver.resolve(prov, 'MX')
        mxs = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in ans])
        if mxs:
            return prov, [mm[1] for mm in mxs]
        return prov, None
    except Exception:
        fb = {
            "naver.com": ["mx1.mail.naver.com", "mx2.mail.naver.com"],
            "gmail.com": ["gmail-smtp-in.l.google.com"],
            "googlemail.com": ["gmail-smtp-in.l.google.com"],
            "mail.ru": ["mxs.mail.ru"], "bk.ru": ["mxs.mail.ru"],
            "list.ru": ["mxs.mail.ru"], "inbox.ru": ["mxs.mail.ru"]
        }
        return (prov, fb.get(prov, None))


def resolver_mxs_paralelo(provedores):
    dizer("A resolver MX de " + str(len(provedores)) + " provedores...")
    out = {}
    trabalhadores = max(8, min(WORKERS_MX, len(provedores) or 1, 300))
    with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
        for prov, mxs in ex.map(resolver_mx_um, provedores):
            if mxs:
                out[prov] = mxs
                with GLOBAL["stats_lock"]:
                    GLOBAL["mx_found"] += len(mxs)
    dizer("Provedores com MX valido: " + str(len(out)) + " | MXs totais: " + str(GLOBAL["mx_found"]))
    marcar_fase("FASE2B_MX", len(out) > 0, str(len(out)) + "/" + str(len(provedores)) + " com MX")
    return out


def _heartbeat_snapshot(evento_parar):
    ultimo = -1
    while not evento_parar.is_set():
        try:
            n = 0
            if os.path.isdir(DIR_REPO):
                for _r, _d, fs in os.walk(DIR_REPO):
                    for f in fs:
                        if f.endswith(".parquet"):
                            n += 1
            if n != ultimo:
                dizer("  ... snapshot em curso: " + str(n) + " parquets | livre " + "{:.1f}GB".format(espaco_disco_livre_gb()))
            ultimo = n
        except Exception:
            pass
        evento_parar.wait(30)


def baixar_snapshot_repo():
    if not USAR_SNAPSHOT_PRIMEIRO:
        return None
    padroes = ["Trader_Emails/", "Trader_Emails/*/", "Trader_Emails/*/*/", "Trader_Emails/*.parquet", "Trader_Emails/**/*.parquet"]
    os.makedirs(DIR_REPO, exist_ok=True)
    evento = threading.Event()
    hb = threading.Thread(target=_heartbeat_snapshot, args=(evento,), daemon=True)
    hb.start()
    resultado = None
    ultima = None
    try:
        for tentativa in range(3):
            if GLOBAL.get("emergency_stop"):
                break
            try:
                dizer("  snapshot_download tentativa " + str(tentativa + 1) + "/3 ...")
                inicio = time.time()
                local = snapshot_download(
                    repo_id=HF_TRADER_REPO, repo_type="dataset", token=HF_TOKEN,
                    allow_patterns=padroes, local_dir=DIR_REPO,
                    max_workers=max(8, min(SNAPSHOT_MAX_WORKERS, 64)), etag_timeout=60
                )
                decorrido = time.time() - inicio
                contar = sum(1 for _r, _d, fs in os.walk(local or DIR_REPO) for f in fs if f.endswith(".parquet"))
                dizer("  Snapshot OK em " + "{:.0f}".format(decorrido) + "s (" + str(contar) + " parquets)")
                resultado = local or DIR_REPO
                GLOBAL["snapshot_ok"] = True
                break
            except Exception as e:
                ultima = e
                dizer("  snapshot_download falhou: " + str(e)[:200])
                time.sleep(20 * (tentativa + 1))
    except Exception as e:
        debug_log("baixar_snapshot_repo outer: " + str(e))
    finally:
        evento.set()
    return resultado


def baixar_todos_emails_paralelo(provedores):
    definir_fase("FASE 2C - Download HuggingFace (snapshot) + extraccao de emails")
    livres = espaco_disco_livre_gb()
    dizer("  Espaco livre em /content: " + "{:.1f}".format(livres) + " GB")
    ja_baixados = CHECKPOINT.obter_provedores_baixados()
    if ja_baixados:
        dizer("  Provedores ja baixados no checkpoint: " + str(len(ja_baixados)))
    todos = listar_ficheiros_repo()
    por_prov = defaultdict(list)
    for f in todos:
        if f.endswith(".parquet") and f.startswith("Trader_Emails/"):
            parts = f.split("/")
            if len(parts) >= 2 and parts[1] in provedores:
                por_prov[parts[1]].append(f)
    args = [(p, por_prov[p]) for p in provedores if p in por_prov]
    GLOBAL["pq_total"] = sum(len(v) for v in por_prov.values())
    dizer("  Parquets identificados: " + str(GLOBAL['pq_total']) + " em " + str(len(args)) + " provedores")
    emails_recuperados = 0
    for p in ja_baixados:
        path_p = os.path.join(DIR_EMAILS, p + ".txt")
        if os.path.exists(path_p):
            try:
                with open(path_p, "r", encoding="utf-8", errors="ignore") as f:
                    n = sum(1 for _ in f)
                with GLOBAL["stats_lock"]:
                    GLOBAL["emails_read"] += n
                    GLOBAL["prov_processed"] += 1
                    GLOBAL["pq_downloaded"] += len(por_prov.get(p, []))
                emails_recuperados += n
            except Exception:
                pass
    if emails_recuperados > 0:
        dizer("  Emails recuperados do checkpoint: " + "{:,}".format(emails_recuperados))
    raiz_local = baixar_snapshot_repo()
    if raiz_local:
        itens_args = [(p, [(rel, os.path.join(raiz_local, rel) if os.path.exists(os.path.join(raiz_local, rel)) else None) for rel in rels]) for p, rels in args]
    else:
        itens_args = [(p, [(rel, None) for rel in rels]) for p, rels in args]
    if GLOBAL.get("emergency_stop"):
        marcar_fase("FASE2C_DOWNLOAD", False, "paragem de emergencia")
        return
    trabalhadores = max(8, min(WORKERS_DOWNLOAD, len(itens_args) or 1, 200))
    with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
        futs = [ex.submit(processar_provedor_emails, a) for a in itens_args]
        for fu in concurrent.futures.as_completed(futs):
            try:
                fu.result()
            except Exception as e:
                debug_log("processar_provedor_emails: " + str(e))
            if GLOBAL.get("emergency_stop"):
                break
    dizer("  Emails extraidos: " + "{:,}".format(GLOBAL['emails_read']))
    dizer("  Parquets processados: " + str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']) + " | falhas: " + str(GLOBAL['pq_falhados']))
    completos = (GLOBAL['pq_downloaded'] + GLOBAL['pq_falhados']) >= GLOBAL['pq_total']
    marcar_fase("FASE2C_DOWNLOAD", completos and GLOBAL['pq_downloaded'] > 0, str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']) + " parquets")


def processar_provedor_emails(args):
    prov, itens = args
    if GLOBAL.get("emergency_stop"):
        return prov, 0
    if CHECKPOINT.provedor_ja_baixado(prov):
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    n = sum(1 for _ in f)
                with GLOBAL["stats_lock"]:
                    GLOBAL["pq_downloaded"] += len(itens)
                    GLOBAL["prov_processed"] += 1
                    GLOBAL["emails_read"] += n
                return prov, n
            except Exception:
                pass
    path = os.path.join(DIR_EMAILS, prov + ".txt")
    seen = set()
    total = 0
    ok_pq = 0
    falha_pq = 0
    try:
        with open(path, "w", encoding="utf-8") as fout:
            for rel, local in itens:
                if GLOBAL.get("emergency_stop"):
                    break
                origem = None
                veio_do_snapshot = False
                pf = None
                try:
                    if local and os.path.exists(local):
                        origem = local
                        veio_do_snapshot = True
                    else:
                        origem = download_hf_com_retry(HF_TRADER_REPO, rel, "dataset")
                    pf = pq.ParquetFile(origem)
                    nomes = pf.schema_arrow.names
                    col = None
                    for c in nomes:
                        if str(c).lower() == "email":
                            col = c
                            break
                    if col is None:
                        for c in nomes:
                            try:
                                if str(pf.schema_arrow.field(c).type) == "string":
                                    col = c
                                    break
                            except Exception:
                                continue
                    if col is None:
                        falha_pq += 1
                        continue
                    buf = []
                    for batch in pf.iter_batches(batch_size=50000, columns=[col]):
                        if GLOBAL.get("emergency_stop"):
                            break
                        try:
                            valores = batch.column(0).to_pylist()
                        except Exception:
                            continue
                        for v in valores:
                            if v is None:
                                continue
                            e = str(v).strip().lower()
                            if "@" not in e or e in seen or e.startswith("@") or e.endswith("@"):
                                continue
                            seen.add(e)
                            buf.append(e)
                            total += 1
                        if buf:
                            fout.write("\n".join(buf) + "\n")
                            buf = []
                    ok_pq += 1
                except Exception as e:
                    falha_pq += 1
                    debug_log("parquet " + str(rel) + " (" + str(prov) + "): " + str(e)[:200])
                finally:
                    pf = None
                    if APAGAR_PARQUETS_APOS_USO and veio_do_snapshot and origem:
                        try:
                            if os.path.exists(origem):
                                os.remove(origem)
                        except Exception:
                            pass
                fout.flush()
    except Exception as e:
        debug_log("processar " + str(prov) + ": " + str(e)[:200])
    try:
        gc.collect()
    except Exception:
        pass
    if total > 0:
        CHECKPOINT.registrar_provedor_baixado(prov, total, ok_pq)
    with GLOBAL["stats_lock"]:
        GLOBAL["pq_downloaded"] += ok_pq
        GLOBAL["pq_falhados"] += falha_pq
        GLOBAL["prov_processed"] += 1
        GLOBAL["emails_read"] += total
    return prov, total


def baixar_unknowns_acumulado():
    definir_fase("FASE 2D - Blacklist persistente de USER_UNKNOWN")
    dizer("A carregar blacklist de USER_UNKNOWN...")
    try:
        local = download_hf_com_retry(HF_LOGS_REPO, "blacklists/user_unknowns_acumulado.csv", "dataset")
        if local and os.path.exists(local):
            shutil.copy(local, UNKNOWN_FILE_LOCAL)
        dizer("  Ficheiro de blacklist descarregado do HuggingFace")
    except Exception as e:
        dizer("  Ficheiro ainda nao existe no HuggingFace (normal na 1a execucao)")
    unknowns = set()
    if os.path.exists(UNKNOWN_FILE_LOCAL):
        try:
            with open(UNKNOWN_FILE_LOCAL, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and "email" in row and row["email"]:
                        unknowns.add(row["email"].lower().strip())
        except Exception:
            unknowns = set()
    GLOBAL["user_unknowns"] = unknowns
    dizer("  USER_UNKNOWNs activos: " + "{:,}".format(len(unknowns)))
    return unknowns


def salvar_unknown(email, provedor):
    try:
        email = email.lower().strip()
    except Exception:
        return
    if not email or "@" not in email:
        return
    with GLOBAL["user_unknowns_lock"]:
        if email in GLOBAL["user_unknowns"]:
            return
        GLOBAL["user_unknowns"].add(email)
    try:
        with UNKNOWN_FILE_LOCK:
            file_exists = os.path.exists(UNKNOWN_FILE_LOCAL)
            with open(UNKNOWN_FILE_LOCAL, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["email", "provedor", "primeiro_visto", "vezes_rejeitado"])
                writer.writerow([email, provedor, datetime.now().isoformat(), 1])
    except Exception as e:
        debug_log("salvar_unknown: " + str(e))


def registrar_aceite(email, provedor, conta_id, mx_host):
    try:
        email_lower = email.lower().strip()
        with GLOBAL["aceites_lock"]:
            if email_lower in GLOBAL["aceites_emails"]:
                return False
            GLOBAL["aceites_emails"].add(email_lower)
        with ACEITES_FILE_LOCK:
            file_exists = os.path.exists(SAIDA_ACEITES)
            with open(SAIDA_ACEITES, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                writer.writerow([email_lower, provedor, conta_id, mx_host, datetime.now().isoformat()])
        return True
    except Exception as e:
        debug_log("registrar_aceite: " + str(e))
        return False


def registrar_rejeicao(email, provedor, smtp_code, smtp_message):
    try:
        with REJEICOES_FILE_LOCK:
            file_exists = os.path.exists(SAIDA_REJEICAOES)
            with open(SAIDA_REJEICAOES, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["email", "provedor", "smtp_code", "smtp_message", "timestamp"])
                writer.writerow([email.lower(), provedor, smtp_code, smtp_message[:200], datetime.now().isoformat()])
    except Exception as e:
        debug_log("registrar_rejeicao: " + str(e))


def upload_unknowns_hf():
    if not os.path.exists(UNKNOWN_FILE_LOCAL):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        api.upload_file(path_or_fileobj=UNKNOWN_FILE_LOCAL, path_in_repo="blacklists/user_unknowns_acumulado.csv", repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
        dizer("  USER_UNKNOWNs enviados para HuggingFace (" + "{:,}".format(len(GLOBAL['user_unknowns'])) + ")")
    except Exception as e:
        dizer("  AVISO: upload de blacklist falhou (" + str(e)[:100] + ")")


def gerar_ficheiros_finais():
    definir_fase("GERACAO DE FICHEIROS FINAIS")
    try:
        with GLOBAL["aceites_lock"]:
            aceites_set = set(GLOBAL.get("aceites_emails", set()))
        if not os.path.exists(SAIDA_ACEITES):
            with open(SAIDA_ACEITES, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
        ts = datetime.now().isoformat()
        for email in aceites_set:
            with ACEITES_FILE_LOCK:
                with open(SAIDA_ACEITES, "a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([email, "", "", "", ts])
        dizer("  Aceites: " + "{:,}".format(len(aceites_set)))
    except Exception as e:
        dizer("  ERRO ao gerar ficheiro de aceites: " + str(e))
    try:
        if os.path.exists(UNKNOWN_FILE_LOCAL):
            shutil.copy(UNKNOWN_FILE_LOCAL, SAIDA_UNKNOWN)
    except Exception as e:
        dizer("  ERRO ao copiar user_unknowns: " + str(e))
    try:
        api = HfApi(token=HF_TOKEN)
        ts_upload = datetime.now().strftime('%Y%m%d_%H%M%S')
        for fpath, nome in [(SAIDA_ACEITES, "aceites_250ok_"), (SAIDA_UNKNOWN, "user_unknowns_"), (SAIDA_REJEICAOES, "rejeicoes_5xx_")]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(path_or_fileobj=fpath, path_in_repo="results/" + nome + ts_upload + ".csv", repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
                except Exception as e:
                    debug_log("upload " + nome + ": " + str(e)[:120])
    except Exception as e:
        dizer("  ERRO geral no upload: " + str(e))


# ====================================================================
# V23.1 - ARACLOUD MANAGER (rede provider/flat + circuit breaker)
# ====================================================================
class AracloudManager:
    """Gerencia contas Aracloud x N IPs.
    Estrategia V23.1 (fix associacao em rede provider/flat):
      1) IP primario = IP publico ja atribuido ao servidor (Nova addresses) - SSH direto.
      2) IPs extra tentativa A: floating IP Neutron (1 tentativa/conta; se a
         associacao falhar -> fips_suportados=False e passa a estrategia B).
      3) IPs extra tentativa B: anexar interface de rede extra (Nova os-interface)
         na mesma rede provider + configurar a VM por SSH (ip link up + dhclient),
         exatamente como o portal Aracloud instrui no dialogo 'Attach Networks'.
      4) Circuit breaker: max 3 falhas seguidas por conta -> nunca mais loop infinito.
    """

    def __init__(self):
        self.auth_url = ARACLOUD_AUTH_URL
        self.tokens = {}
        self.compute_urls = {}
        self.network_urls = {}
        self.ext_net_ids = {}
        self.fips_suportados = {}
        self.provider_net_ids = {}

    def _carregar_credenciais_hf(self):
        os.makedirs(DIR_ARACLOUD, exist_ok=True)
        credenciais = {}
        try:
            api = HfApi(token=HF_TOKEN)
            fs = list(api.list_repo_files(repo_id=HF_ARACLOUD_REPO, repo_type="dataset"))
        except Exception as e:
            dizer("  AVISO: falha ao listar repo " + HF_ARACLOUD_REPO + ": " + str(e)[:120])
            return credenciais
        dizer("  Repo " + HF_ARACLOUD_REPO + ": " + str(len(fs)) + " ficheiros")
        pastas = sorted({f.split("/")[0] for f in fs if f.startswith("servidor-") and "/" in f})
        dizer("  Pastas de servidor: " + str(len(pastas)) + " -> " + ", ".join(pastas))
        for pasta in pastas:
            try:
                fcred = pasta + "/credenciais.json"
                fpem = pasta + "/k.pem"
                if fcred not in fs:
                    dizer("  AVISO: " + pasta + " sem credenciais.json")
                    continue
                fcontas = None
                for cand in (pasta + "/Contas.json", pasta + "/contas.json", pasta + "/conta.json"):
                    if cand in fs:
                        fcontas = cand
                        break
                local_cred = hf_hub_download(repo_id=HF_ARACLOUD_REPO, filename=fcred, repo_type="dataset", token=HF_TOKEN, force_download=True)
                with open(local_cred, "r", encoding="utf-8") as f:
                    dados = json.load(f)
                if "api" not in dados:
                    dados["api"] = {}
                if fcontas:
                    local_contas = hf_hub_download(repo_id=HF_ARACLOUD_REPO, filename=fcontas, repo_type="dataset", token=HF_TOKEN, force_download=True)
                    with open(local_contas, "r", encoding="utf-8") as f:
                        cd = json.load(f)
                    for k in ("domain_id", "project_id", "identity_url"):
                        if k in cd:
                            dados["api"][k] = cd[k]
                    if "api_users" in cd:
                        dados["api"]["api_users"] = cd["api_users"]
                    dizer("  ✅ " + pasta + ": " + fcontas.split("/")[-1] + " OK (domain=" + str(cd.get("domain_id", "?"))[:12] + "...)")
                else:
                    dizer("  ⚠️ " + pasta + ": sem Contas.json - auth descobrirá o projeto")
                pem_path = ""
                if fpem in fs:
                    pem_path = hf_hub_download(repo_id=HF_ARACLOUD_REPO, filename=fpem, repo_type="dataset", token=HF_TOKEN, force_download=True)
                    try:
                        os.chmod(pem_path, 0o600)
                    except Exception:
                        pass
                else:
                    dizer("  ⚠️ " + pasta + ": sem k.pem")
                credenciais[pasta] = {"cred": dados, "pem_path": pem_path}
            except Exception as e:
                dizer("  ⚠️ ERRO em " + pasta + ": " + str(e)[:120])
                debug_log("aracloud cred " + pasta + ": " + traceback.format_exc())
        return credenciais

    def _autenticar(self, nome_conta, cred):
        api = cred.get("api", {})
        user = api.get("utilizador") or api.get("user", "")
        password = api.get("password") or api.get("pass", "")
        domain_id = api.get("domain_id", "default")
        project_id = api.get("project_id", "")
        auth_url = (api.get("identity_url") or self.auth_url).rstrip("/")
        if not user or not password:
            dizer("  AVISO: " + nome_conta + " sem utilizador/password")
            return None
        identity = {"methods": ["password"], "password": {"user": {"name": user, "domain": {"id": domain_id}, "password": password}}}
        try:
            token = None
            catalog = []
            if project_id:
                r = requests.post(auth_url + "/auth/tokens", json={"auth": {"identity": identity, "scope": {"project": {"id": project_id}}}}, timeout=30, verify=False)
                if r.status_code in (200, 201):
                    token = r.headers.get("X-Subject-Token")
                    try:
                        catalog = r.json().get("token", {}).get("catalog", [])
                    except Exception:
                        pass
            if not token:
                r1 = requests.post(auth_url + "/auth/tokens", json={"auth": {"identity": identity}}, timeout=30, verify=False)
                if r1.status_code not in (200, 201):
                    debug_log("aracloud auth unscoped " + nome_conta + ": HTTP " + str(r1.status_code) + " " + r1.text[:200])
                    return None
                t1 = r1.headers.get("X-Subject-Token")
                projects = []
                try:
                    rp = requests.get(auth_url + "/auth/projects", headers={"X-Auth-Token": t1}, timeout=30, verify=False)
                    if rp.status_code == 200:
                        projects = rp.json().get("projects", [])
                except Exception:
                    pass
                if not projects:
                    debug_log("aracloud auth " + nome_conta + ": sem projetos")
                    return None
                pid = project_id or projects[0].get("id")
                r2 = requests.post(auth_url + "/auth/tokens", json={"auth": {"identity": identity, "scope": {"project": {"id": pid}}}}, timeout=30, verify=False)
                if r2.status_code not in (200, 201):
                    debug_log("aracloud auth scoped2 " + nome_conta + ": HTTP " + str(r2.status_code))
                    return None
                token = r2.headers.get("X-Subject-Token")
                try:
                    catalog = r2.json().get("token", {}).get("catalog", [])
                except Exception:
                    pass
            if token:
                self.tokens[nome_conta] = token
                cu = nu = None
                for service in catalog:
                    if service.get("type") == "compute":
                        for ep in service.get("endpoints", []):
                            if ep.get("interface") == "public":
                                cu = ep.get("url", "").rstrip("/")
                                break
                    if service.get("type") == "network":
                        for ep in service.get("endpoints", []):
                            if ep.get("interface") == "public":
                                nu = ep.get("url", "").rstrip("/")
                                break
                if cu:
                    self.compute_urls[nome_conta] = cu
                    dizer("  ✅ " + nome_conta + ": compute_url = " + cu[:60])
                else:
                    dizer("  ⚠️ " + nome_conta + ": sem endpoint compute")
                if nu:
                    self.network_urls[nome_conta] = nu
                    debug_log("aracloud " + nome_conta + " network_url = " + nu)
                else:
                    dizer("  ⚠️ " + nome_conta + ": sem endpoint network")
                return token
        except Exception as e:
            debug_log("aracloud auth " + nome_conta + ": " + str(e)[:160])
            return None

    def _listar_servidor(self, nome_conta, cred):
        token = self.tokens.get(nome_conta)
        compute_url = self.compute_urls.get(nome_conta)
        if not token or not compute_url:
            return None
        hostname = cred.get("hostname", "") or cred.get("name", "") or cred.get("api", {}).get("name", "")
        try:
            r = requests.get(compute_url + "/servers/detail", headers={"X-Auth-Token": token}, timeout=30, verify=False)
            if r.status_code == 200:
                servers = r.json().get("servers", [])
                for s in servers:
                    if hostname and (s.get("name") == hostname or hostname in s.get("name", "")):
                        return s["id"]
                if servers:
                    return servers[0]["id"]
        except Exception as e:
            debug_log("aracloud listar " + nome_conta + ": " + str(e)[:160])
        return None

    def _obter_ports_servidor(self, nome_conta, server_id):
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url:
            return []
        try:
            r = requests.get(network_url + "/v2.0/ports", headers={"X-Auth-Token": token}, params={"device_id": server_id}, timeout=30, verify=False)
            if r.status_code == 200:
                return r.json().get("ports", [])
            debug_log("aracloud ports " + nome_conta + ": HTTP " + str(r.status_code) + " " + r.text[:200])
        except Exception as e:
            debug_log("aracloud ports " + nome_conta + ": " + str(e)[:160])
        return []

    def _obter_port_id_servidor(self, nome_conta, server_id):
        ports = self._obter_ports_servidor(nome_conta, server_id)
        if ports:
            return ports[0].get("id")
        return None

    def _ips_publicos_servidor(self, nome_conta, server_id):
        token = self.tokens.get(nome_conta)
        compute_url = self.compute_urls.get(nome_conta)
        if not token or not compute_url:
            return []
        try:
            r = requests.get(compute_url + "/servers/" + str(server_id), headers={"X-Auth-Token": token}, timeout=30, verify=False)
            if r.status_code == 200:
                sv = r.json().get("server", {})
                ips = [a["addr"] for lst in (sv.get("addresses") or {}).values() for a in lst if a.get("version") == 4]
                return [i for i in ips if ip_publico_valido(i)]
        except Exception as e:
            debug_log("aracloud ips_servidor " + nome_conta + ": " + str(e)[:160])
        return []

    def _descobrir_rede_externa(self, nome_conta):
        if nome_conta in self.ext_net_ids:
            return self.ext_net_ids[nome_conta]
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url:
            return None
        try:
            r = requests.get(network_url + "/v2.0/networks", headers={"X-Auth-Token": token}, timeout=30, verify=False)
            if r.status_code == 200:
                networks = r.json().get("networks", [])
                ext_nets = [n for n in networks if n.get("router:external") is True]
                if ext_nets:
                    ext_id = ext_nets[0]["id"]
                    self.ext_net_ids[nome_conta] = ext_id
                    return ext_id
        except Exception as e:
            debug_log("aracloud redes_externas " + nome_conta + ": " + str(e)[:160])
        return None

    def _listar_ips_flutuantes(self, nome_conta):
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url:
            return []
        try:
            r = requests.get(network_url + "/v2.0/floatingips", headers={"X-Auth-Token": token}, timeout=30, verify=False)
            if r.status_code == 200:
                fips = r.json().get("floatingips", [])
                return [{"id": fi.get("id"), "ip": fi.get("floating_ip_address"), "port_id": fi.get("port_id")} for fi in fips if fi.get("floating_ip_address")]
        except Exception as e:
            debug_log("aracloud listar_ips " + nome_conta + ": " + str(e)[:160])
        return []

    def _criar_ip_flutuante(self, nome_conta):
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url:
            return None
        ext_net_id = self._descobrir_rede_externa(nome_conta)
        if not ext_net_id:
            return None
        try:
            payload = {"floatingip": {"floating_network_id": ext_net_id}}
            r = requests.post(network_url + "/v2.0/floatingips", headers={"X-Auth-Token": token, "Content-Type": "application/json"}, json=payload, timeout=30, verify=False)
            if r.status_code in (200, 201):
                fip = r.json().get("floatingip", {})
                return {"ip": fip.get("floating_ip_address"), "id": fip.get("id")}
            debug_log("aracloud criar_ip " + nome_conta + ": HTTP " + str(r.status_code) + " " + r.text[:200])
        except Exception as e:
            debug_log("aracloud criar_ip " + nome_conta + ": " + str(e)[:160])
        return None

    def _associar_ip(self, nome_conta, fip_id, port_id):
        """Retorna (ok, detalhe). detalhe contem HTTP + corpo para diagnostico."""
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url or not fip_id or not port_id:
            return False, "sem token/network/port"
        try:
            payload = {"floatingip": {"port_id": port_id}}
            r = requests.put(network_url + "/v2.0/floatingips/" + str(fip_id), headers={"X-Auth-Token": token, "Content-Type": "application/json"}, json=payload, timeout=30, verify=False)
            if r.status_code in (200, 202):
                return True, "ok"
            detalhe = "HTTP " + str(r.status_code) + ": " + r.text[:150]
            debug_log("aracloud associar " + nome_conta + " fip=" + str(fip_id) + ": " + detalhe)
            return False, detalhe
        except Exception as e:
            debug_log("aracloud associar " + nome_conta + ": " + str(e)[:160])
            return False, str(e)[:150]

    def _desassociar_ip(self, nome_conta, fip_id):
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url or not fip_id:
            return False
        try:
            payload = {"floatingip": {"port_id": None}}
            r = requests.put(network_url + "/v2.0/floatingips/" + str(fip_id), headers={"X-Auth-Token": token, "Content-Type": "application/json"}, json=payload, timeout=30, verify=False)
            return r.status_code in (200, 202)
        except Exception as e:
            debug_log("aracloud desassociar " + nome_conta + ": " + str(e)[:160])
            return False

    def _remover_ip(self, nome_conta, fip_id):
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url or not fip_id:
            return False
        try:
            r = requests.delete(network_url + "/v2.0/floatingips/" + str(fip_id), headers={"X-Auth-Token": token}, timeout=30, verify=False)
            return r.status_code in (200, 204)
        except Exception as e:
            debug_log("aracloud remover_ip " + nome_conta + ": " + str(e)[:160])
            return False

    def _anexar_interface(self, nome_conta, server_id, net_id):
        """Anexa interface de rede extra (Nova os-interface). Retorna port_id ou None."""
        token = self.tokens.get(nome_conta)
        compute_url = self.compute_urls.get(nome_conta)
        if not token or not compute_url or not net_id:
            return None
        try:
            payload = {"interfaceAttachment": {"net_id": net_id}}
            r = requests.post(compute_url + "/servers/" + str(server_id) + "/os-interface",
                              headers={"X-Auth-Token": token, "Content-Type": "application/json"},
                              json=payload, timeout=45, verify=False)
            if r.status_code in (200, 202):
                att = r.json().get("interfaceAttachment", {}) or {}
                pid = att.get("port_id")
                if pid:
                    return pid
            debug_log("aracloud anexar_iface " + nome_conta + ": HTTP " + str(r.status_code) + " " + r.text[:200])
        except Exception as e:
            debug_log("aracloud anexar_iface " + nome_conta + ": " + str(e)[:160])
        return None

    def _desanexar_interface(self, nome_conta, server_id, port_id):
        token = self.tokens.get(nome_conta)
        compute_url = self.compute_urls.get(nome_conta)
        if not token or not compute_url or not port_id:
            return False
        try:
            r = requests.delete(compute_url + "/servers/" + str(server_id) + "/os-interface/" + str(port_id),
                                headers={"X-Auth-Token": token}, timeout=45, verify=False)
            return r.status_code in (200, 202, 204)
        except Exception as e:
            debug_log("aracloud desanexar_iface " + nome_conta + ": " + str(e)[:160])
            return False

    def _esperar_port_com_ip(self, nome_conta, port_id, timeout_seg=60):
        fim = time.time() + timeout_seg
        token = self.tokens.get(nome_conta)
        network_url = self.network_urls.get(nome_conta)
        if not token or not network_url:
            return None
        while time.time() < fim:
            try:
                r = requests.get(network_url + "/v2.0/ports/" + str(port_id), headers={"X-Auth-Token": token}, timeout=30, verify=False)
                if r.status_code == 200:
                    port = r.json().get("port", {})
                    fixed = [fx.get("ip_address") for fx in port.get("fixed_ips", []) if fx.get("ip_address")]
                    if fixed:
                        return fixed[0]
            except Exception as e:
                debug_log("aracloud esperar_port " + nome_conta + ": " + str(e)[:120])
            time.sleep(5)
        return None

    def _configurar_redes_vm(self, ssh_client):
        """Sobe interfaces novas + dhclient (passos do portal) e devolve IPs publicos da VM."""
        if ssh_client is None:
            return []
        cmd = ("ip a | awk '/^[0-9]+:/ {print $2}' | sed 's/://' | grep -v lo | xargs -r -I {} sudo ip link set {} up; "
               "ip a | awk '/^[0-9]+:/ && !/inet / {print $2}' | sed 's/://' | grep -v lo | xargs -r -I {} sudo dhclient -nw {} >/dev/null 2>&1; "
               "sleep 2; ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1")
        try:
            _i, o, _e = ssh_client.exec_command(cmd, timeout=90)
            out = o.read().decode(errors="ignore")
            ips = [l.strip() for l in out.splitlines() if l.strip()]
            return [i for i in ips if ip_publico_valido(i)]
        except Exception as e:
            debug_log("configurar_redes_vm: " + str(e)[:160])
            return []

    def _abrir_ssh(self, nome_conta, ip, pem_path):
        if paramiko is None:
            debug_log("paramiko indisponivel - sem tunel SSH")
            return None
        if not pem_path or not os.path.exists(pem_path):
            return None
        for usuario in ("ubuntu", "root"):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                try:
                    client.connect(hostname=ip, username=usuario, key_filename=pem_path, timeout=SSH_CONNECT_TIMEOUT, allow_agent=False, look_for_keys=False)
                except Exception:
                    pkey = paramiko.RSAKey.from_private_key_file(pem_path)
                    client.connect(hostname=ip, username=usuario, pkey=pkey, timeout=SSH_CONNECT_TIMEOUT, allow_agent=False, look_for_keys=False)
                return client
            except Exception as e:
                debug_log("aracloud ssh " + nome_conta + " " + ip + " (" + usuario + "): " + str(e)[:120])
        return None

    def _localizar_ip(self, ip):
        for nome, estado in GLOBAL.get("aracloud_contas", {}).items():
            for item in estado.get("ips_ativos", []):
                if isinstance(item, dict) and item.get("ip") == ip:
                    return nome, estado, item
        return None, None, None

    def _reset_diario_se_necessario(self):
        hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if GLOBAL.get("aracloud_data_reset") != hoje:
            dizer("  Aracloud: novo dia UTC - a resetar IPs usados hoje")
            GLOBAL["aracloud_ips_usados_hoje"] = set()
            GLOBAL["aracloud_data_reset"] = hoje

    def _registar_ip_activo(self, nome_conta, conta_estado, ip, item_id, tipo, ssh_client, total_ref):
        conta_estado["ips_ativos"].append({"ip": ip, "id": item_id, "tipo": tipo})
        if tipo != "primario":
            conta_estado["total_ips_unicos"] = conta_estado.get("total_ips_unicos", 0) + 1
            GLOBAL["aracloud_ips_usados_hoje"].add(ip)
        if ip not in GLOBAL["aracloud_ips_ativos"]:
            GLOBAL["aracloud_ips_ativos"].append(ip)
        if ssh_client is not None:
            GLOBAL["aracloud_clients_ssh"][ip] = ssh_client
        with GLOBAL["orcamento_lock"]:
            GLOBAL["orcamento_por_ip"].setdefault(ip, {})
        return total_ref + 1

    def inicializar(self):
        """FASE 2.5: autentica contas, IP primario + IPs extra (fip ou interface)."""
        self._reset_diario_se_necessario()
        credenciais = self._carregar_credenciais_hf()
        dizer("  Aracloud: " + str(len(credenciais)) + " contas com credenciais")
        hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        total_ips = 0
        contas_funcionais = 0
        for nome_conta, dados in credenciais.items():
            if GLOBAL.get("emergency_stop"):
                break
            cred = dados["cred"]
            pem_path = dados["pem_path"]
            if not pem_path:
                dizer("  AVISO: " + nome_conta + " sem k.pem - saltado")
                continue
            token = self._autenticar(nome_conta, cred)
            if not token:
                dizer("  AVISO: falha de autenticacao em " + nome_conta + " (credenciais inválidas)")
                continue
            server_id = self._listar_servidor(nome_conta, cred)
            if not server_id:
                dizer("  AVISO: servidor nao encontrado em " + nome_conta)
                continue
            port_id = self._obter_port_id_servidor(nome_conta, server_id)
            contas_funcionais += 1
            conta_estado = GLOBAL["aracloud_contas"].get(nome_conta, {}) or {}
            conta_estado["server_id"] = server_id
            conta_estado["port_id"] = port_id
            conta_estado["pem_path"] = pem_path
            if conta_estado.get("data") != hoje:
                conta_estado["total_ips_unicos"] = 0
                conta_estado["data"] = hoje
            conta_estado["ips_ativos"] = []
            # rede provider da port primaria (usada para anexar interfaces extra)
            for p in self._obter_ports_servidor(nome_conta, server_id):
                if p.get("id") == port_id:
                    self.provider_net_ids[nome_conta] = p.get("network_id")
                    break
            # ---- 1) IP PRIMARIO: IP publico ja atribuido ao servidor ----
            ips_base = self._ips_publicos_servidor(nome_conta, server_id)
            primary_ip = None
            ssh_prim = None
            for ipb in ips_base:
                cli = self._abrir_ssh(nome_conta, ipb, pem_path)
                if cli is not None:
                    primary_ip = ipb
                    ssh_prim = cli
                    break
            if not primary_ip:
                dizer("  AVISO: " + nome_conta + " sem IP publico acessivel por SSH - saltado")
                GLOBAL["aracloud_contas"][nome_conta] = conta_estado
                continue
            total_ips = self._registar_ip_activo(nome_conta, conta_estado, primary_ip, None, "primario", ssh_prim, total_ips)
            dizer("  Aracloud: IP primario " + primary_ip + " activo em " + nome_conta)
            # ---- 2) Reaproveitar floating IPs ja associados a esta port ----
            for fip_info in self._listar_ips_flutuantes(nome_conta):
                if len(conta_estado["ips_ativos"]) >= MAX_IPS_POR_CONTA:
                    break
                if fip_info.get("port_id") != port_id:
                    continue
                ip_addr = fip_info.get("ip")
                fip_id = fip_info.get("id")
                if not ip_addr or not fip_id or not ip_publico_valido(ip_addr):
                    continue
                cli = self._abrir_ssh(nome_conta, ip_addr, pem_path)
                if cli is None:
                    self._desassociar_ip(nome_conta, fip_id)
                    self._remover_ip(nome_conta, fip_id)
                    continue
                total_ips = self._registar_ip_activo(nome_conta, conta_estado, ip_addr, fip_id, "fip", cli, total_ips)
                dizer("  Aracloud: IP existente " + ip_addr + " reaproveitado em " + nome_conta)
            # ---- 3) Reaproveitar interfaces extra ja anexadas ----
            for p in self._obter_ports_servidor(nome_conta, server_id):
                if len(conta_estado["ips_ativos"]) >= MAX_IPS_POR_CONTA:
                    break
                if p.get("id") == port_id:
                    continue
                fixed = [fx.get("ip_address") for fx in p.get("fixed_ips", []) if fx.get("ip_address")]
                if not fixed:
                    continue
                ip_addr = fixed[0]
                if not ip_publico_valido(ip_addr):
                    continue
                cli = self._abrir_ssh(nome_conta, ip_addr, pem_path)
                if cli is None:
                    self._configurar_redes_vm(ssh_prim)
                    cli = self._abrir_ssh(nome_conta, ip_addr, pem_path)
                if cli is None:
                    continue
                total_ips = self._registar_ip_activo(nome_conta, conta_estado, ip_addr, p.get("id"), "iface", cli, total_ips)
                dizer("  Aracloud: interface existente " + ip_addr + " reaproveitada em " + nome_conta)
            # ---- 4) Criar IPs extra com circuit breaker ----
            falhas_seguidas = 0
            tentativas = 0
            while (len(conta_estado["ips_ativos"]) < MAX_IPS_POR_CONTA
                   and falhas_seguidas < MAX_FALHAS_SEGUIDAS_CONTA
                   and tentativas < MAX_IPS_POR_CONTA * 4
                   and conta_estado.get("total_ips_unicos", 0) < MAX_IPS_UNICOS_POR_DIA
                   and not GLOBAL.get("emergency_stop")):
                tentativas += 1
                novo_ip = None
                novo_id = None
                novo_tipo = None
                novo_cli = None
                # 4a) floating IP - UMA tentativa por conta
                if self.fips_suportados.get(nome_conta, True):
                    fip = self._criar_ip_flutuante(nome_conta)
                    if fip and fip.get("ip") and fip.get("id"):
                        ok_assoc, detalhe = self._associar_ip(nome_conta, fip["id"], port_id)
                        if ok_assoc:
                            cli = self._abrir_ssh(nome_conta, fip["ip"], pem_path)
                            if cli is not None:
                                novo_ip, novo_id, novo_tipo, novo_cli = fip["ip"], fip["id"], "fip", cli
                            else:
                                self._desassociar_ip(nome_conta, fip["id"])
                                self._remover_ip(nome_conta, fip["id"])
                                falhas_seguidas += 1
                        else:
                            dizer("  AVISO: " + nome_conta + " floating IP nao associavel (" + detalhe[:90] + ") - a usar interfaces extra")
                            self._remover_ip(nome_conta, fip["id"])
                            self.fips_suportados[nome_conta] = False
                            falhas_seguidas += 1
                    else:
                        self.fips_suportados[nome_conta] = False
                        falhas_seguidas += 1
                # 4b) interface extra na rede provider + config VM por SSH
                if novo_ip is None:
                    net_id = self.provider_net_ids.get(nome_conta)
                    port_novo = self._anexar_interface(nome_conta, server_id, net_id) if net_id else None
                    if port_novo:
                        ip_novo = self._esperar_port_com_ip(nome_conta, port_novo, timeout_seg=60)
                        if ip_novo and ip_publico_valido(ip_novo):
                            ips_vm = self._configurar_redes_vm(ssh_prim)
                            cli = None
                            if ip_novo in ips_vm:
                                cli = self._abrir_ssh(nome_conta, ip_novo, pem_path)
                            if cli is None:
                                cli = self._abrir_ssh(nome_conta, ip_novo, pem_path)
                            if cli is not None:
                                novo_ip, novo_id, novo_tipo, novo_cli = ip_novo, port_novo, "iface", cli
                            else:
                                self._desanexar_interface(nome_conta, server_id, port_novo)
                                falhas_seguidas += 1
                        else:
                            self._desanexar_interface(nome_conta, server_id, port_novo)
                            falhas_seguidas += 1
                    else:
                        dizer("  AVISO: " + nome_conta + " nao consegue anexar mais interfaces (quota/rede)")
                        break
                if novo_ip:
                    falhas_seguidas = 0
                    total_ips = self._registar_ip_activo(nome_conta, conta_estado, novo_ip, novo_id, novo_tipo, novo_cli, total_ips)
                    dizer("  Aracloud: IP " + novo_ip + " activo em " + nome_conta + " (" + novo_tipo + ")")
                else:
                    falhas_seguidas += 1
                    time.sleep(2)
            GLOBAL["aracloud_contas"][nome_conta] = conta_estado
        CHECKPOINT.salvar_aracloud(GLOBAL["aracloud_contas"], list(GLOBAL["aracloud_ips_usados_hoje"]), GLOBAL.get("aracloud_data_reset"))
        dizer("  Aracloud: " + str(contas_funcionais) + " contas funcionais, " + str(total_ips) + " IPs activos")
        dizer("  Arquitectura real: " + str(total_ips) + " workers (primarios + extra)")
        marcar_fase("FASE25_ARACLOUD", total_ips > 0, str(total_ips) + " IPs activos (" + str(contas_funcionais) + " contas)")
        return total_ips

    def obter_lista_workers(self):
        workers = []
        contas_dkim = GLOBAL.get("contas", [])
        dominios_dkim = list({c["domain"] for c in contas_dkim}) if contas_dkim else []
        for i, ip in enumerate(GLOBAL.get("aracloud_ips_ativos", [])):
            dominio = dominios_dkim[i % len(dominios_dkim)] if dominios_dkim else "fallback.local"
            conta_dkim = next((c for c in contas_dkim if c["domain"] == dominio), None)
            workers.append({
                "worker_id": "W" + str(i + 1).zfill(2),
                "ip": ip, "domain": dominio, "conta_dkim": conta_dkim,
                "ssh_client": GLOBAL.get("aracloud_clients_ssh", {}).get(ip),
            })
        return workers

    def rodar_ip(self, ip):
        conta_nome, estado, item = self._localizar_ip(ip)
        if not conta_nome or not item:
            return None
        tipo = item.get("tipo", "fip")
        item_id = item.get("id")
        if tipo == "primario":
            debug_log("rodar_ip: " + ip + " e IP primario (fixo) - nao rotativo")
            return None
        server_id = estado.get("server_id")
        port_id = estado.get("port_id")
        pem = estado.get("pem_path", "")
        # remover antigo
        ssh_old = GLOBAL.get("aracloud_clients_ssh", {}).pop(ip, None)
        if ssh_old:
            try:
                ssh_old.close()
            except Exception:
                pass
        if ip in GLOBAL["aracloud_ips_ativos"]:
            GLOBAL["aracloud_ips_ativos"].remove(ip)
        estado["ips_ativos"] = [x for x in estado.get("ips_ativos", []) if x.get("ip") != ip]
        if tipo == "fip" and item_id:
            self._desassociar_ip(conta_nome, item_id)
            self._remover_ip(conta_nome, item_id)
        elif tipo == "iface" and item_id:
            self._desanexar_interface(conta_nome, server_id, item_id)
        if estado.get("total_ips_unicos", 0) >= MAX_IPS_UNICOS_POR_DIA:
            dizer("  " + conta_nome + ": nao e possivel rodar IP (limite diario 20)")
            return None
        # substituto: fip se suportado, senao interface
        novo_ip = None
        novo_id = None
        novo_tipo = None
        if self.fips_suportados.get(conta_nome, True):
            fip = self._criar_ip_flutuante(conta_nome)
            if fip:
                ok, det = self._associar_ip(conta_nome, fip["id"], port_id)
                if ok:
                    cli = self._abrir_ssh(conta_nome, fip["ip"], pem)
                    if cli is not None:
                        GLOBAL["aracloud_clients_ssh"][fip["ip"]] = cli
                        novo_ip, novo_id, novo_tipo = fip["ip"], fip["id"], "fip"
                    else:
                        self._desassociar_ip(conta_nome, fip["id"])
                        self._remover_ip(conta_nome, fip["id"])
                        self.fips_suportados[conta_nome] = False
                else:
                    self._remover_ip(conta_nome, fip["id"])
                    self.fips_suportados[conta_nome] = False
        if novo_ip is None:
            net_id = self.provider_net_ids.get(conta_nome)
            port_novo = self._anexar_interface(conta_nome, server_id, net_id) if net_id else None
            if port_novo:
                ip_n = self._esperar_port_com_ip(conta_nome, port_novo, 60)
                if ip_n and ip_publico_valido(ip_n):
                    prim = next((x["ip"] for x in estado["ips_ativos"] if x.get("tipo") == "primario"), None)
                    ssh_prim = GLOBAL["aracloud_clients_ssh"].get(prim) if prim else None
                    if ssh_prim:
                        self._configurar_redes_vm(ssh_prim)
                    cli = self._abrir_ssh(conta_nome, ip_n, pem)
                    if cli is not None:
                        GLOBAL["aracloud_clients_ssh"][ip_n] = cli
                        novo_ip, novo_id, novo_tipo = ip_n, port_novo, "iface"
                    else:
                        self._desanexar_interface(conta_nome, server_id, port_novo)
        if novo_ip is None:
            return None
        estado["ips_ativos"].append({"ip": novo_ip, "id": novo_id, "tipo": novo_tipo})
        estado["total_ips_unicos"] = estado.get("total_ips_unicos", 0) + 1
        GLOBAL["aracloud_ips_usados_hoje"].add(novo_ip)
        GLOBAL["aracloud_ips_ativos"].append(novo_ip)
        with GLOBAL["orcamento_lock"]:
            GLOBAL["orcamento_por_ip"].setdefault(novo_ip, {})
        CHECKPOINT.salvar_aracloud(GLOBAL["aracloud_contas"], list(GLOBAL["aracloud_ips_usados_hoje"]), GLOBAL.get("aracloud_data_reset"))
        dizer("  Aracloud: IP " + ip + " substituido por " + novo_ip + " em " + conta_nome)
        GLOBAL["spf_sujo"] = True
        return novo_ip


def abrir_canal_smtp_aracloud(ssh_client, mx_host, porta=25, timeout=None):
    if ssh_client is None:
        return None
    if timeout is None:
        timeout = SSH_TUNNEL_TIMEOUT
    try:
        transport = ssh_client.get_transport()
        if transport is None or not transport.is_active():
            return None
        canal = transport.open_channel("direct-tcpip", (mx_host, porta), ("127.0.0.1", 0))
        canal.settimeout(timeout)
        return canal
    except Exception as e:
        debug_log("abrir_canal_smtp_aracloud " + str(mx_host) + ": " + str(e)[:160])
        return None


def _canal_suporta_tls(sock):
    try:
        import paramiko as _pk
        if isinstance(sock, _pk.Channel):
            return False
    except Exception:
        pass
    try:
        sock.fileno()
        return True
    except Exception:
        return False


def tentar_conexao_via_aracloud(worker_ctx, mx_host, timeout, provedor):
    ip = worker_ctx.get("ip") if worker_ctx else None
    ssh_client = worker_ctx.get("ssh_client") if worker_ctx else None
    worker_id = worker_ctx.get("worker_id") if worker_ctx else "---"
    if not ssh_client:
        smtp_log_raw(worker_id, ip, provedor, "🛑 sem ssh_client - nao e possivel ligar")
        return None, None, "sem_ssh"
    canal = None
    try:
        smtp_log_raw(worker_id, ip, provedor, "    ARACLOUD " + str(ip) + " -> " + mx_host + ":25")
        canal = abrir_canal_smtp_aracloud(ssh_client, mx_host, 25, timeout)
        if canal is None:
            return None, None, "canal_falhou"
        banner = canal.recv(4096).decode(errors="ignore").strip()
        smtp_log_raw(worker_id, ip, provedor, "    Banner: " + banner[:90])
        if not banner.startswith("220"):
            try:
                canal.close()
            except Exception:
                pass
            return None, None, "banner_invalido"
        return canal, banner, "ok"
    except socket.timeout:
        smtp_log_raw(worker_id, ip, provedor, "    TIMEOUT em " + mx_host + " via " + str(ip))
        if canal is not None:
            try:
                canal.close()
            except Exception:
                pass
        return None, None, "timeout"
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        smtp_log_raw(worker_id, ip, provedor, "    RESET/ERRO " + str(ip) + ": " + str(e)[:110])
        if canal is not None:
            try:
                canal.close()
            except Exception:
                pass
        return None, None, "reset"
    except Exception as e:
        smtp_log_raw(worker_id, ip, provedor, "    ERRO " + str(ip) + ": " + str(e)[:110])
        if canal is not None:
            try:
                canal.close()
            except Exception:
                pass
        return None, None, "erro"


def obter_socket_para_mx(mx_list, provedor, worker_ctx):
    if not mx_list:
        return None, None, None
    timeout = get_timeout(provedor)
    for mx_host in mx_list:
        if GLOBAL.get("emergency_stop"):
            break
        sock, banner, status = tentar_conexao_via_aracloud(worker_ctx, mx_host, timeout, provedor)
        if sock is not None:
            return sock, mx_host, "aracloud"
    return None, None, None


def ip_publico_valido(ip):
    if not ip:
        return False
    ip_str = str(ip).strip()
    if not re.match(r'^[\d.]+$', ip_str):
        return False
    try:
        partes = [int(p) for p in ip_str.split(".")]
    except Exception:
        return False
    if len(partes) != 4:
        return False
    for p in partes:
        if p < 0 or p > 255:
            return False
    a, b = partes[0], partes[1]
    if a in (0, 10, 127, 255):
        return False
    if a == 172 and 16 <= b <= 31:
        return False
    if a == 192 and b == 168:
        return False
    if a == 169 and b == 254:
        return False
    if a >= 224:
        return False
    if a == 100 and 64 <= b <= 127:
        return False
    if a == 198 and b in (18, 19):
        return False
    return True


def token_para_dominio(domain):
    with DOM_TOKEN_LOCK:
        if domain in DOM_TOKEN:
            return DOM_TOKEN[domain]
    for tok in DESEC_TOKENS_VALIDOS:
        if GLOBAL.get("emergency_stop"):
            return None
        try:
            r = requests.get("https://desec.io/api/v1/domains/" + domain + "/", headers={"Authorization": "Token " + tok}, timeout=15)
            if r.status_code == 200:
                with DOM_TOKEN_LOCK:
                    DOM_TOKEN[domain] = tok
                return tok
        except Exception:
            continue
    return None


def desec_put_com_retry(url, headers, payload, domain, max_retries=5):
    payload_preview = None
    try:
        if isinstance(payload, list) and payload:
            for rrset in payload:
                if rrset.get("type") == "TXT":
                    payload_preview = json.dumps(rrset, indent=2)
                    break
            if not payload_preview:
                payload_preview = json.dumps(payload[0], indent=2)
    except Exception as e:
        payload_preview = "erro ao serializar: " + str(e)
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            return False, "STOP"
        inicio_req = time.time()
        agora = time.time()
        tempo_desde_ultima = agora - GLOBAL.get("desec_last_request", 0)
        if tempo_desde_ultima < DESEC_MIN_DELAY:
            time.sleep(DESEC_MIN_DELAY - tempo_desde_ultima)
        try:
            with GLOBAL_DESEC_SEMAPHORE:
                r = requests.put(url, headers=headers, json=payload, timeout=180)
            duracao_ms = (time.time() - inicio_req) * 1000
            GLOBAL["desec_last_request"] = time.time()
            log_desec_request(domain=domain, url=url, method="PUT", status_code=r.status_code, response_body=r.text, headers=dict(r.headers), duration_ms=duracao_ms, attempt=tentativa + 1, payload_preview=payload_preview)
            if r.status_code in (200, 201, 204):
                return True, r.status_code
            if r.status_code == 429:
                try:
                    wait_hdr = int(r.headers.get("Retry-After", DESEC_BACKOFF_BASE))
                except Exception:
                    wait_hdr = DESEC_BACKOFF_BASE
                wait = max(wait_hdr, int(DESEC_BACKOFF_BASE * (DESEC_BACKOFF_MULTIPLIER ** tentativa)))
                dizer("  Rate limit deSEC (tent " + str(tentativa + 1) + "). A esperar " + str(wait) + "s...")
                time.sleep(wait + 1)
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(5 * (tentativa + 1))
                continue
            if r.status_code == 400:
                dizer("  Erro 400 deSEC: " + r.text[:200])
                return False, "400: " + r.text[:200]
            return False, str(r.status_code) + ": " + r.text[:150]
        except Exception as e:
            duracao_ms = (time.time() - inicio_req) * 1000
            log_desec_request(domain=domain, url=url, method="PUT", status_code=-1, response_body=str(e), headers={}, duration_ms=duracao_ms, attempt=tentativa + 1, payload_preview=payload_preview)
            if tentativa < max_retries - 1:
                time.sleep(3 * (tentativa + 1))
                continue
            return False, "MAX_RETRIES"
    return False, "MAX_RETRIES"


def _dividir_em_chunks_com_aspas(texto, tamanho_max=255):
    if not texto:
        return '""'
    if len(texto) <= tamanho_max:
        return '"' + texto + '"'
    chunks = []
    restante = texto
    while restante:
        if len(restante) <= tamanho_max:
            chunks.append(restante)
            break
        split_idx = restante.rfind(' ', 0, tamanho_max)
        if split_idx == -1 or split_idx == 0:
            split_idx = tamanho_max
        chunks.append(restante[:split_idx])
        restante = restante[split_idx:].lstrip()
    return " ".join(['"' + c + '"' for c in chunks])


def montar_spf_record(ips):
    if not ips:
        return '"v=spf1 ~all"'
    spf_parts = ["v=spf1"]
    for ip in ips:
        spf_parts.append("ip4:" + str(ip))
    spf_parts.append("~all")
    return _dividir_em_chunks_com_aspas(" ".join(spf_parts), 255)


def obter_ips_para_spf():
    ips = set()
    with GLOBAL.get("ips_lock", threading.Lock()):
        for ip in GLOBAL.get("aracloud_ips_ativos", []):
            if ip and ip_publico_valido(ip):
                ips.add(ip)
    return [i for i in ips if i][:MAX_SPF_IPS]


def construir_rrsets_completos(contas_do_dominio, spf_record, ip_aracloud=None):
    rrsets = []
    dmarc_record = '"v=DMARC1; p=none; sp=none;"'
    ip_alvo = ip_aracloud or (obter_ips_para_spf() or ["127.0.0.1"])[0]
    for c in contas_do_dominio:
        base = c["base"]
        domain = c["domain"]
        selector = c["selector"]
        pub = (c.get("public_key_txt") or "").replace('"', "").strip()
        if not pub:
            continue
        if not pub.startswith("v=DKIM1"):
            pub = ("v=DKIM1; k=rsa; p=" + pub) if "p=" in pub else ("v=DKIM1; k=rsa; " + pub)
        rrsets.append({"subname": base, "type": "TXT", "ttl": DESEC_TTL, "records": [spf_record]})
        rrsets.append({"subname": selector + "._domainkey." + base, "type": "TXT", "ttl": DESEC_TTL, "records": [_dividir_em_chunks_com_aspas(pub, 255)]})
        rrsets.append({"subname": "_dmarc." + base, "type": "TXT", "ttl": DESEC_TTL, "records": [dmarc_record]})
        rrsets.append({"subname": "*." + base, "type": "A", "ttl": DESEC_TTL, "records": [ip_alvo]})
        rrsets.append({"subname": base, "type": "A", "ttl": DESEC_TTL, "records": [ip_alvo]})
        rrsets.append({"subname": base, "type": "MX", "ttl": DESEC_TTL, "records": ["10 " + base + "." + domain + "."]})
    return rrsets


def atualizar_dominio_completo(domain, contas_dom, spf_record, ip_aracloud=None):
    tok = token_para_dominio(domain)
    if not tok:
        dizer("  AVISO: Token nao encontrado para " + domain)
        return domain, 0, 0
    rrsets = construir_rrsets_completos(contas_dom, spf_record, ip_aracloud)
    total_rrsets = len(rrsets)
    if total_rrsets == 0:
        return domain, 0, 0
    ok_count = 0
    url = "https://desec.io/api/v1/domains/" + domain + "/rrsets/"
    headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
    dizer("  BULK PUT " + str(total_rrsets) + " rrsets para " + domain + "...")
    for i in range(0, total_rrsets, DESEC_CHUNK):
        if GLOBAL.get("emergency_stop"):
            break
        chunk = rrsets[i:i + DESEC_CHUNK]
        ok, status = desec_put_com_retry(url, headers, chunk, domain)
        if ok:
            ok_count += len(chunk)
            dizer("    Chunk " + str(i // DESEC_CHUNK + 1) + " OK (" + str(status) + ")")
        else:
            dizer("    Chunk " + str(i // DESEC_CHUNK + 1) + " FALHOU: " + str(status)[:100])
        time.sleep(1.0)
    return domain, ok_count, total_rrsets


def fase3_dns_completo(contas):
    definir_fase("FASE 3 - DNS completo BULK (SPF+DKIM+DMARC+A+WX+MX) - 1ª vez")
    if fase_ja_completa("FASE3_DNS"):
        dizer("  Fase 3 ja concluida no checkpoint - a saltar")
        return True
    domain_ip_map = {}
    for w in GLOBAL.get("workers", []):
        if w.get("domain") and w.get("ip"):
            domain_ip_map[w["domain"]] = w["ip"]
    ips_spf = obter_ips_para_spf()
    spf_record = montar_spf_record(ips_spf)
    dizer("  SPF com " + str(len(ips_spf)) + " IPs Aracloud")
    por_dom = defaultdict(list)
    for c in contas:
        por_dom[c["domain"]].append(c)
    total_bases = sum(len(v) for v in por_dom.values())
    GLOBAL["dns_total"] = total_bases
    dizer("  Dominios: " + str(len(por_dom)) + " | bases: " + str(total_bases))
    ok_bases = 0
    falhados = 0
    for idx, domain in enumerate(sorted(por_dom.keys()), 1):
        if GLOBAL.get("emergency_stop"):
            break
        contas_dom = por_dom[domain]
        dizer("  [" + str(idx) + "/" + str(len(por_dom)) + "] " + domain + " (" + str(len(contas_dom)) + " contas)...")
        dom, ok, total = atualizar_dominio_completo(domain, contas_dom, spf_record, domain_ip_map.get(domain))
        bases_sucesso = (ok // 6) if ok > 0 else 0
        ok_bases += bases_sucesso
        if ok == 0:
            falhados += 1
            dizer("     FALHOU (" + str(ok) + "/" + str(total) + " rrsets)")
        else:
            dizer("     OK (" + str(ok) + "/" + str(total) + " rrsets, " + str(bases_sucesso) + " bases)")
        GLOBAL["dns_updated"] = ok_bases
        time.sleep(2.0)
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    CHECKPOINT.salvar_desec(sorted(por_dom.keys()), hashlib.md5(spf_record.encode()).hexdigest())
    dizer("  DNS: " + str(ok_bases) + "/" + str(total_bases) + " bases | dominios falhados: " + str(falhados))
    marcar_fase("FASE3_DNS", ok_bases > 0, str(ok_bases) + "/" + str(total_bases) + " bases")
    return ok_bases > 0


def refresh_spf_bulk(motivo=""):
    ips_spf = obter_ips_para_spf()
    if not ips_spf:
        return
    spf_record = montar_spf_record(ips_spf)
    dizer("  Refresh SPF-ONLY bulk (" + str(motivo) + ") com " + str(len(ips_spf)) + " IPs")
    por_dom = defaultdict(list)
    for c in GLOBAL.get("contas", []):
        por_dom[c["domain"]].append(c)
    ok_dom = 0
    for domain, contas_dom in por_dom.items():
        if GLOBAL.get("emergency_stop"):
            break
        tok = token_para_dominio(domain)
        if not tok:
            continue
        rrsets = [{"subname": c["base"], "type": "TXT", "ttl": DESEC_TTL, "records": [spf_record]} for c in contas_dom]
        url = "https://desec.io/api/v1/domains/" + domain + "/rrsets/"
        headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
        ok = False
        for i in range(0, len(rrsets), DESEC_CHUNK):
            ok, status = desec_put_com_retry(url, headers, rrsets[i:i + DESEC_CHUNK], domain)
            if not ok:
                debug_log("refresh_spf " + domain + ": " + str(status))
                break
        if ok:
            ok_dom += 1
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    dizer("  SPF actualizado (bulk 1 req/dom) em " + str(ok_dom) + "/" + str(len(por_dom)) + " dominios")


def fase4_esperar_propagacao():
    definir_fase("FASE 4 - Propagacao DNS (" + str(DNS_PROPAGACAO_SEGUNDOS) + "s)")
    if fase_ja_completa("FASE4_PROPAGACAO"):
        dizer("  Fase 4 ja concluida no checkpoint - a saltar")
        return
    restante = DNS_PROPAGACAO_SEGUNDOS
    while restante > 0:
        if GLOBAL.get("emergency_stop"):
            break
        passo = min(10, restante)
        time.sleep(passo)
        restante -= passo
    dizer("  Propagacao concluida")


def verificar_propagacao_amostra(contas, ip_ref=None):
    dizer("  Verificacao de propagacao (amostra de 5)")
    ips_spf = obter_ips_para_spf()
    if not contas:
        marcar_fase("FASE4_PROPAGACAO", True, "sem contas para amostrar")
        return True
    amostra = random.sample(contas, min(5, len(contas)))
    ok = 0
    res = dns.resolver.Resolver()
    res.nameservers = ["8.8.8.8", "1.1.1.1"]
    res.timeout = 8
    res.lifetime = 10
    for c in amostra:
        fqdn = c['base'] + "." + c['domain']
        try:
            answers = res.resolve(fqdn, "TXT")
            for rdata in answers:
                txt = b"".join(rdata.strings).decode(errors="ignore")
                if "v=spf1" in txt:
                    for ip in ips_spf[:8]:
                        if ("ip4:" + str(ip)) in txt:
                            ok += 1
                            break
                    break
        except Exception:
            pass
    dizer("  Propagacao confirmada: " + str(ok) + "/" + str(len(amostra)))
    marcar_fase("FASE4_PROPAGACAO", True, str(ok) + "/" + str(len(amostra)) + " propagados")
    return ok >= max(1, len(amostra) // 2)


def aplicar_mutacao_abracadabra(html_base, conta, target_emails, subject=None):
    if subject is None:
        subject = CAMPAIGN["subject"]
    email_ref = target_emails[0] if isinstance(target_emails, list) and target_emails else str(target_emails)
    h = mutar_html_abracadabra(html_base, conta, email_ref)
    msg = EmailMessage()
    msg['Subject'] = subject
    idx_conta = 0
    for i, c in enumerate(GLOBAL.get("contas", [])):
        if c.get("identity_id") == conta.get("identity_id"):
            idx_conta = i
            break
    msg['From'] = obter_display_name(conta, idx_conta) + " <" + conta['sender_email'] + ">"
    msg['To'] = target_emails[0] if isinstance(target_emails, list) and target_emails else str(target_emails)
    msg['Message-ID'] = make_msgid(domain=conta['domain'])
    msg['Date'] = format_datetime(datetime.now(timezone.utc))
    msg.set_content(h, subtype='html')
    b = msg.as_bytes()
    try:
        sig = dkim.sign(message=b, selector=conta['selector'].encode(), domain=conta['domain'].encode(), privkey=conta['private_key_pem'].encode(), include_headers=[b"from", b"to", b"subject", b"date", b"message-id"])
        b = sig + b
    except Exception as e:
        smtp_log("DKIM falhou em " + str(conta.get('identity_id')) + ": " + str(e))
    return b


def recv_smtp(sock, timeout=SMTP_RECV_TIMEOUT):
    sock.settimeout(timeout)
    buf, resp, atual = b"", [], []
    while True:
        try:
            data = sock.recv(131072)
        except socket.timeout:
            break
        except Exception:
            break
        if not data:
            break
        buf += data
        while b"\r\n" in buf:
            ln, buf = buf.split(b"\r\n", 1)
            s = ln.decode(errors="ignore")
            atual.append(s)
            if re.match(r"^\d{3}\s", s) or re.match(r"^\d{3}$", s):
                resp.append(" ".join(atual))
                atual = []
        if len(buf) > 8 * 1024 * 1024:
            break
    if atual:
        resp.append(" ".join(atual))
    return resp


def criar_contexto_tls():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES256-SHA256:!aNULL:!eNULL:!MD5:!RC4:!DES:!3DES")
    except Exception:
        try:
            ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        except Exception:
            pass
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_3
    except Exception:
        pass
    return ctx


def get_timeout(provedor):
    if provedor in PROVEDORES_ASIATICOS:
        return SMTP_TIMEOUT_ASIATICO
    return SMTP_TIMEOUT


def classificar_resposta_smtp(smtp_code, smtp_message, provedor):
    code = int(smtp_code) if smtp_code else 0
    msg_low = str(smtp_message or "").lower()
    if 200 <= code < 300:
        return "ACEITE", "SMTP acceptance"
    if code == 421:
        return "CONEXAO_FECHADA", "421 connection closed"
    if code == 450:
        return "MAILBOX_BUSY", "450 mailbox busy"
    if code == 451:
        if "greylist" in msg_low or provedor in PROVEDORES_GREYLIST:
            return "GREYLIST", "451 greylist"
        return "TEMPORARIA", "451 falha temporaria"
    if code == 452:
        return "TOO_MANY", "452 too many recipients"
    if 400 <= code < 500:
        throttle_keywords = ["too many", "rate limit", "try again later", "temporarily", "throttl", "limit exceeded", "too fast", "slow down", "please wait", "retry later", "greylist", "temporarily rejected", "please try again", "quota exceeded", "connection limit", "message limit", "recipient limit"]
        if any(kw in msg_low for kw in throttle_keywords):
            return "THROTTLE", "Throttling detected"
        return "TEMPORARIA", "Falha temporaria"
    if code == 550:
        return "USER_UNKNOWN", "550 user unknown"
    if code == 552:
        return "QUOTA_EXCEEDED", "552 quota exceeded"
    if code == 553:
        return "INVALID_DOMAIN", "553 invalid domain"
    if code == 554:
        return "BLACKLIST", "554 blocked/blacklist"
    if 500 <= code < 600:
        if any(kw in msg_low for kw in ["user unknown", "no such user", "does not exist", "invalid recipient", "mailbox not found", "unknown user", "recipient rejected"]):
            return "USER_UNKNOWN", "Destinatario inexistente"
        if any(kw in msg_low for kw in ["spam", "blocked", "blacklist", "rbl", "rejected for policy", "not allowed", "reputation"]):
            return "REPUTACAO", "Problema de reputacao"
        return "PERMANENTE", "Rejeicao permanente generica"
    return "DESCONHECIDA", "Codigo " + str(code)


class RetryQueue:
    def __init__(self):
        self.items = deque()
        self.lock = threading.Lock()

    def adicionar(self, email, provedor, attempts=1):
        if attempts > MAX_RETRY_ATTEMPTS:
            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_abandonados"] += 1
            smtp_log("ABANDONADO (max retries): " + email + " para " + provedor)
            return False
        delay = RETRY_BACKOFF_BASE * (RETRY_BACKOFF_MULTIPLIER ** (attempts - 1))
        with self.lock:
            self.items.append((email, provedor, attempts, time.time() + delay))
        with GLOBAL["stats_lock"]:
            GLOBAL["smtp_retries"] += 1
        return True

    def obter_prontos(self, max_items=100):
        agora = time.time()
        prontos = []
        with self.lock:
            remaining = deque()
            for item in self.items:
                if item[3] <= agora and len(prontos) < max_items:
                    prontos.append((item[0], item[1], item[2]))
                else:
                    remaining.append(item)
            self.items = remaining
        return prontos


RETRY_QUEUE = RetryQueue()


def adicionar_greylist(provedor, emails_batch, tentativa):
    delay = GREYLIST_DELAY_1 if tentativa == 1 else GREYLIST_DELAY_2
    with GLOBAL["greylist_lock"]:
        if provedor not in GLOBAL["greylist_queue"]:
            GLOBAL["greylist_queue"][provedor] = []
        for em in emails_batch:
            GLOBAL["greylist_queue"][provedor].append({"email": em, "tentativa": tentativa, "retry_em": time.time() + delay})
    smtp_log("  GREYLIST " + provedor + ": " + str(len(emails_batch)) + " emails retry em " + str(delay) + "s (tent " + str(tentativa) + ")")


def obter_greylist_prontos(provedor, max_items=200):
    agora = time.time()
    prontos = []
    with GLOBAL["greylist_lock"]:
        fila = GLOBAL["greylist_queue"].get(provedor, [])
        restantes = []
        for item in fila:
            if item["retry_em"] <= agora and len(prontos) < max_items:
                prontos.append(item)
            else:
                restantes.append(item)
        GLOBAL["greylist_queue"][provedor] = restantes
    return prontos


def salvar_checkpoint_smtp():
    try:
        stats = {k: GLOBAL.get(k, 0) for k in
                 ["smtp_tentativas", "smtp_aceites", "smtp_rejeitados", "smtp_user_unknown", "smtp_erros",
                  "smtp_conexoes", "smtp_via_proxy", "smtp_via_colab", "smtp_4xx", "smtp_5xx",
                  "smtp_retries", "smtp_abandonados"]}
        CHECKPOINT.salvar_smtp_stats(stats)
        CHECKPOINT.salvar_por_provedor(GLOBAL.get("por_provedor", {}))
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]
    except Exception as e:
        debug_log("salvar_checkpoint_smtp: " + str(e))


def talvez_salvar_checkpoint():
    diff = GLOBAL["smtp_aceites"] - GLOBAL.get("smtp_aceites_ultima_checkpoint", 0)
    if diff >= CHECKPOINT_SAVE_INTERVAL:
        salvar_checkpoint_smtp()


def handshake_smtp(sock, conta, mx_host):
    try:
        sock.sendall(("EHLO " + str(conta['identity_id']) + "\r\n").encode())
        ehlo_resp = recv_smtp(sock)
        ehlo_text = "\n".join(ehlo_resp) if ehlo_resp else ""
        smtp_log("   <- EHLO: " + ehlo_text[:120])
        if not ehlo_resp:
            return sock, False
        if "STARTTLS" in ehlo_text.upper() and _canal_suporta_tls(sock):
            sock.sendall(b"STARTTLS\r\n")
            tls_resp = recv_smtp(sock)
            if tls_resp and tls_resp[0].startswith("220"):
                try:
                    ctx = criar_contexto_tls()
                    sock = ctx.wrap_socket(sock, server_hostname=mx_host)
                    sock.sendall(("EHLO " + str(conta['identity_id']) + "\r\n").encode())
                    recv_smtp(sock)
                    smtp_log("  TLS estabelecido")
                except Exception as e:
                    smtp_log("  TLS handshake falhou: " + str(e)[:120])
                    raise
        elif "STARTTLS" in ehlo_text.upper():
            smtp_log("  STARTTLS saltado (túnel SSH já encriptado)")
        return sock, True
    except Exception as e:
        smtp_log("  Handshake falhou: " + str(e)[:140])
        return sock, False


def aguardar_ips_aracloud(minimo, timeout_seg=300):
    definir_fase("FASE 5 - Aguardar IPs Aracloud minimo (" + str(minimo) + ")")
    inicio = time.time()
    while time.time() - inicio < timeout_seg:
        if GLOBAL.get("emergency_stop"):
            return False
        n = len(GLOBAL.get("aracloud_ips_ativos", []))
        if n >= minimo:
            dizer("  IPs Aracloud prontos: " + str(n))
            return True
        dizer("  IPs Aracloud " + str(n) + "/" + str(minimo) + " - a aguardar... (" + "{:.0f}".format(time.time() - inicio) + "s)")
        time.sleep(20)
    n = len(GLOBAL.get("aracloud_ips_ativos", []))
    dizer("  AVISO: timeout a aguardar IPs Aracloud (tem " + str(n) + ").")
    return n > 0


def enviar_para_provedor(task):
    if len(task) == 5:
        prov, mx_list, contas_pool, html_base, worker_ctx = task
    else:
        prov, mx_list, contas_pool, html_base = task
        worker_ctx = None
    worker_id = worker_ctx.get("worker_id", "---") if worker_ctx else "---"
    worker_ip = worker_ctx.get("ip", "---") if worker_ctx else "---"
    path = os.path.join(DIR_EMAILS, prov + ".txt")
    res = {"provedor": prov, "aceites": 0, "tentados": 0, "rejeitados": 0, "erros": 0, "user_unknown": 0, "via_aracloud": 0, "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0}
    if not os.path.exists(path) or os.path.getsize(path) == 0 or not contas_pool:
        GLOBAL["por_provedor"][prov] = res
        return res
    if worker_ctx:
        with GLOBAL["orcamento_lock"]:
            orc_ip = GLOBAL["orcamento_por_ip"].get(worker_ip, {})
            if orc_ip.get(prov, 0) >= IP_BUDGET_POR_PROVEDOR:
                smtp_log_raw(worker_id, worker_ip, prov, "ORCAMENTO ESGOTADO (" + str(IP_BUDGET_POR_PROVEDOR) + ") - a saltar")
                GLOBAL["por_provedor"][prov] = res
                return res
    total_linhas = 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                total_linhas += 1
    except Exception:
        total_linhas = 0
    cp_info = CHECKPOINT.obter_provedor_index(prov)
    offset_inicial = 0
    aceites_iniciais = 0
    if cp_info:
        offset_inicial = cp_info.get("offset", 0)
        aceites_iniciais = cp_info.get("aceites", 0)
    if cp_info and (cp_info.get("concluido", False) or (total_linhas > 0 and offset_inicial >= total_linhas)):
        dizer("  " + prov + ": Ja totalmente tentado. Saltando.")
        res["aceites"] = aceites_iniciais
        res["concluido"] = True
        GLOBAL["por_provedor"][prov] = res
        return res
    res["aceites"] = aceites_iniciais
    dizer("  CHECKPOINT " + prov + " [" + worker_id + "]: offset=" + str(offset_inicial) + "/" + str(total_linhas) + " aceites=" + str(aceites_iniciais))
    rcpt_limit = obter_batch_size(prov)
    smtp_log_raw(worker_id, worker_ip, prov, "=== INICIO ENVIO: " + prov + " | batch=" + str(rcpt_limit) + " ===")
    try:
        fh = open(path, "r", encoding="utf-8", errors="ignore")
    except Exception as e:
        debug_log("abrir " + path + ": " + str(e))
        GLOBAL["por_provedor"][prov] = res
        return res
    linhas_consumidas = offset_inicial
    emails_saltados = 0
    try:
        while emails_saltados < offset_inicial:
            linha = fh.readline()
            if not linha:
                break
            emails_saltados += 1
        if emails_saltados > 0:
            dizer("  " + prov + ": saltados " + str(emails_saltados) + " emails do checkpoint")
    except Exception as e:
        debug_log("saltar offset " + prov + ": " + str(e))
    conta_idx = random.randint(0, max(0, len(contas_pool) - 1))
    try:
        while res["aceites"] < META_ACEITES_POR_PROVEDOR and not GLOBAL.get("emergency_stop"):
            if check_stop_file():
                break
            if worker_ctx:
                with GLOBAL["orcamento_lock"]:
                    orc_ip = GLOBAL["orcamento_por_ip"].get(worker_ip, {})
                    if orc_ip.get(prov, 0) >= IP_BUDGET_POR_PROVEDOR:
                        smtp_log_raw(worker_id, worker_ip, prov, "ORCAMENTO ESGOTADO - a parar janela")
                        break
            janela_bruta = [l.strip() for l in islice(fh, EMAILS_JANELA_ENVIO)]
            linhas_consumidas += len(janela_bruta)
            janela = []
            for email in janela_bruta:
                if not email or "@" not in email:
                    continue
                email = email.lower().strip()
                if email.startswith("@") or email.endswith("@"):
                    continue
                with GLOBAL["user_unknowns_lock"]:
                    if email in GLOBAL["user_unknowns"]:
                        continue
                janela.append(email)
            for retry_email, retry_prov, retry_attempts in RETRY_QUEUE.obter_prontos(max_items=200):
                if retry_prov == prov and retry_email not in janela:
                    janela.append(retry_email)
            for g_item in obter_greylist_prontos(prov, max_items=200):
                if g_item["email"] not in janela:
                    janela.append(g_item["email"])
            if not janela:
                break
            random.shuffle(janela)
            fila = deque(janela)
            while fila and res["aceites"] < META_ACEITES_POR_PROVEDOR and not GLOBAL.get("emergency_stop"):
                if check_stop_file():
                    break
                if len(GLOBAL.get("aracloud_ips_ativos", [])) == 0:
                    aguardar_ips_aracloud(1, timeout_seg=120)
                sock, mx_host, via = obter_socket_para_mx(mx_list, prov, worker_ctx)
                if sock is None:
                    res["erros"] += 1
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_erros"] += 1
                    time.sleep(1.0)
                    continue
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_conexoes"] += 1
                conta = contas_pool[conta_idx % len(contas_pool)]
                conta_idx += 1
                conta["_worker_id"] = worker_id
                conta["_worker_ip"] = worker_ip
                pode, espera = RATE_CTRL.pode_enviar(conta["identity_id"], prov)
                if not pode and espera > 0:
                    time.sleep(min(espera, 5.0))
                sock, hs_ok = handshake_smtp(sock, conta, mx_host)
                if not hs_ok:
                    res["erros"] += 1
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_erros"] += 1
                    try:
                        sock.close()
                    except Exception:
                        pass
                    continue
                enviados_conta = 0
                while (enviados_conta < MAX_EMAILS_POR_CONTA and fila and res["aceites"] < META_ACEITES_POR_PROVEDOR and not GLOBAL.get("emergency_stop")):
                    if check_stop_file():
                        break
                    with GLOBAL["rcpt_lock"]:
                        lr = GLOBAL["rcpt_aprendido"].get(prov)
                        bs = min(lr or rcpt_limit, len(fila), MAX_RCPT_POR_CONEXAO)
                        if bs <= 0:
                            bs = min(1, len(fila))
                    batch = [fila.popleft() for _ in range(bs)]
                    if not batch:
                        break
                    res["tentados"] += len(batch)
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_tentativas"] += len(batch)
                    try:
                        sock.sendall(("MAIL FROM:<" + conta['sender_email'] + ">\r\n").encode())
                        smtp_log_raw(worker_id, worker_ip, prov, "  -> MAIL FROM:<" + conta['sender_email'] + ">")
                        mr = recv_smtp(sock)
                        smtp_log_raw(worker_id, worker_ip, prov, "  <- MAIL FROM: " + str(mr)[:140])
                        if not mr or not mr[0].startswith("250"):
                            try:
                                sock.close()
                            except Exception:
                                pass
                            res["erros"] += len(batch)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_erros"] += len(batch)
                            fila.extendleft(reversed(batch))
                            break
                    except Exception as e:
                        smtp_log_raw(worker_id, worker_ip, prov, "  MAIL FROM erro: " + str(e)[:140])
                        try:
                            sock.close()
                        except Exception:
                            pass
                        res["erros"] += len(batch)
                        with GLOBAL["stats_lock"]:
                            GLOBAL["smtp_erros"] += len(batch)
                        fila.extendleft(reversed(batch))
                        break
                    aceitos = []
                    rcpt_erro_fatal = False
                    greylist_batch = []
                    too_many_detectado = False
                    for em in batch:
                        if GLOBAL.get("emergency_stop"):
                            break
                        inicio_req = time.time()
                        try:
                            sock.sendall(("RCPT TO:<" + em + ">\r\n").encode())
                            smtp_log_raw(worker_id, worker_ip, prov, "  -> RCPT TO:<" + em + ">")
                            rr = recv_smtp(sock)
                            response_time = time.time() - inicio_req
                            smtp_log_raw(worker_id, worker_ip, prov, "  <- RCPT: " + str(rr)[:140] + " (" + "{:.2f}".format(response_time) + "s)")
                            if rr:
                                smtp_code = 0
                                try:
                                    smtp_code = int(rr[0][:3])
                                except Exception:
                                    pass
                                smtp_message = " ".join(rr)
                                registar_resposta_smtp(None, smtp_code, smtp_message, prov, em.split("@")[-1] if "@" in em else "", conta.get("sender_email", ""), worker_ip, conta.get("domain", ""), conta.get("base", ""), 1, response_time)
                                if rr[0].startswith("250") or rr[0].startswith("251"):
                                    aceitos.append(em)
                                    RATE_CTRL.registar_sucesso(conta["identity_id"], prov)
                                else:
                                    classe, motivo = classificar_resposta_smtp(smtp_code, smtp_message, prov)
                                    if classe == "USER_UNKNOWN":
                                        res["user_unknown"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_user_unknown"] += 1
                                        salvar_unknown(em, prov)
                                    elif classe == "GREYLIST":
                                        res["smtp_4xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_4xx"] += 1
                                        greylist_batch.append(em)
                                    elif classe == "TOO_MANY":
                                        res["smtp_4xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_4xx"] += 1
                                        too_many_detectado = True
                                        RATE_CTRL.registar_erro_temporario(conta["identity_id"], prov, smtp_code)
                                        RETRY_QUEUE.adicionar(em, prov, attempts=1)
                                    elif classe in ("CONEXAO_FECHADA", "MAILBOX_BUSY", "TEMPORARIA", "THROTTLE"):
                                        res["smtp_4xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_4xx"] += 1
                                        RATE_CTRL.registar_erro_temporario(conta["identity_id"], prov, smtp_code)
                                        RETRY_QUEUE.adicionar(em, prov, attempts=1)
                                    elif classe == "BLACKLIST":
                                        res["smtp_5xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_5xx"] += 1
                                        smtp_log_raw(worker_id, worker_ip, prov, "🛑 BLACKLIST (554) - IP " + str(worker_ip) + " marcado para rotação")
                                        GLOBAL["spf_sujo"] = True
                                        registrar_rejeicao(em, prov, smtp_code, smtp_message)
                                        res["rejeitados"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_rejeitados"] += 1
                                    elif classe == "INVALID_DOMAIN":
                                        res["smtp_5xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_5xx"] += 1
                                        registrar_rejeicao(em, prov, smtp_code, smtp_message)
                                        res["rejeitados"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_rejeitados"] += 1
                                        break
                                    else:
                                        res["smtp_5xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_5xx"] += 1
                                        RATE_CTRL.registar_erro_permanente(conta["identity_id"], prov, smtp_code)
                                        registrar_rejeicao(em, prov, smtp_code, smtp_message)
                                        res["rejeitados"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_rejeitados"] += 1
                                    if classe == "THROTTLE":
                                        RATE_CTRL.registar_throttle(conta["identity_id"], prov, 60)
                                        break
                            time.sleep(DELAY_ENTRE_RCPT)
                        except Exception as e:
                            smtp_log_raw(worker_id, worker_ip, prov, "  RCPT erro: " + str(e)[:140])
                            res["erros"] += 1
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_erros"] += 1
                            rcpt_erro_fatal = True
                            break
                    if greylist_batch:
                        adicionar_greylist(prov, greylist_batch, tentativa=1)
                    if too_many_detectado:
                        novo_lim, acao = adaptar_batch_size(prov, {"too_many_at": len(batch)})
                        smtp_log_raw(worker_id, worker_ip, prov, "  452 TOO_MANY - batch " + acao + " para " + str(novo_lim))
                        rcpt_limit = novo_lim
                    if rcpt_erro_fatal:
                        nao_processados = [b for b in batch if b not in set(aceitos)]
                        if nao_processados:
                            fila.extendleft(reversed(nao_processados))
                        try:
                            sock.close()
                        except Exception:
                            pass
                        break
                    if not aceitos:
                        try:
                            sock.sendall(b"RSET\r\n")
                            recv_smtp(sock)
                        except Exception:
                            pass
                        enviados_conta += bs
                        continue
                    try:
                        sock.sendall(b"DATA\r\n")
                        dr = recv_smtp(sock)
                        if not dr or not dr[0].startswith("354"):
                            res["rejeitados"] += len(aceitos)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_rejeitados"] += len(aceitos)
                            enviados_conta += bs
                            try:
                                sock.sendall(b"RSET\r\n")
                                recv_smtp(sock)
                            except Exception:
                                pass
                            continue
                    except Exception as e:
                        smtp_log_raw(worker_id, worker_ip, prov, "  DATA erro: " + str(e)[:140])
                        try:
                            sock.close()
                        except Exception:
                            pass
                        res["erros"] += len(aceitos)
                        with GLOBAL["stats_lock"]:
                            GLOBAL["smtp_erros"] += len(aceitos)
                        fila.extendleft(reversed(aceitos))
                        break
                    try:
                        payload = aplicar_mutacao_abracadabra(html_base, conta, aceitos)
                        if not payload.endswith(b"\r\n"):
                            payload += b"\r\n"
                        payload = payload.replace(b"\r\n.", b"\r\n..")
                        sock.sendall(payload)
                        sock.sendall(b"\r\n.\r\n")
                        smtp_log_raw(worker_id, worker_ip, prov, "  -> PAYLOAD (" + str(len(aceitos)) + " RCPTs, " + str(len(payload)) + " bytes)")
                        fr = recv_smtp(sock, timeout=30)
                        smtp_log_raw(worker_id, worker_ip, prov, "  <- FINAL: " + str(fr)[:160])
                        if fr and fr[0].startswith("250"):
                            res["aceites"] += len(aceitos)
                            res["via_aracloud"] += len(aceitos)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_aceites"] += len(aceitos)
                                GLOBAL["smtp_via_aracloud"] = GLOBAL.get("smtp_via_aracloud", 0) + len(aceitos)
                            if worker_ctx:
                                with GLOBAL["orcamento_lock"]:
                                    GLOBAL["orcamento_por_ip"].setdefault(worker_ip, {})
                                    GLOBAL["orcamento_por_ip"][worker_ip][prov] = GLOBAL["orcamento_por_ip"][worker_ip].get(prov, 0) + len(aceitos)
                            for em_aceite in aceitos:
                                registrar_aceite(em_aceite, prov, conta.get("identity_id", ""), mx_host)
                            talvez_salvar_checkpoint()
                        else:
                            res["rejeitados"] += len(aceitos)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_rejeitados"] += len(aceitos)
                    except Exception as e:
                        smtp_log_raw(worker_id, worker_ip, prov, "  PAYLOAD erro: " + str(e)[:140])
                        res["erros"] += len(aceitos)
                        with GLOBAL["stats_lock"]:
                            GLOBAL["smtp_erros"] += len(aceitos)
                        fila.extendleft(reversed(aceitos))
                        try:
                            sock.close()
                        except Exception:
                            pass
                        break
                    enviados_conta += bs
                    time.sleep(0.1)
                try:
                    sock.sendall(b"QUIT\r\n")
                    recv_smtp(sock, timeout=3)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
            if res["aceites"] >= META_ACEITES_POR_PROVEDOR:
                break
    except Exception:
        debug_log("enviar_para_provedor " + prov + ": " + traceback.format_exc())
    finally:
        try:
            fh.close()
        except Exception:
            pass
    try:
        concluido = bool(total_linhas > 0 and linhas_consumidas >= total_linhas)
        CHECKPOINT.atualizar_provedor_index(prov, linhas_consumidas, res["aceites"], concluido=concluido, total_linhas=total_linhas)
        CHECKPOINT.salvar_offset(prov, linhas_consumidas)
    except Exception as e:
        debug_log("atualizar_provedor_index " + prov + ": " + str(e))
    if res["aceites"] >= META_ACEITES_POR_PROVEDOR:
        with GLOBAL["stats_lock"]:
            GLOBAL["provedores_concluidos_set"].add(prov)
            GLOBAL["provs_concluidos"] = len(GLOBAL["provedores_concluidos_set"])
    smtp_log_raw(worker_id, worker_ip, prov, "=== FIM " + prov + ": " + str(res['aceites']) + " aceites / " + str(res['tentados']) + " tentados ===")
    GLOBAL["por_provedor"][prov] = res
    CHECKPOINT.salvar_por_provedor(GLOBAL["por_provedor"])
    return res


def worker_loop(worker_ctx, mxs, contas, html_base):
    worker_id = worker_ctx.get("worker_id", "---")
    worker_ip = worker_ctx.get("ip", "---")
    providers_queue = deque(PROVEDORES_USAVEIS)
    ciclos_sem_envio = 0
    smtp_log_raw(worker_id, worker_ip, "---", "WORKER INICIADO com " + str(len(providers_queue)) + " provedores")
    while providers_queue and not GLOBAL.get("emergency_stop"):
        if check_stop_file():
            break
        prov = providers_queue.popleft()
        if prov not in mxs or not mxs[prov]:
            continue
        with GLOBAL["orcamento_lock"]:
            orc_ip = GLOBAL["orcamento_por_ip"].get(worker_ip, {})
            if orc_ip.get(prov, 0) >= IP_BUDGET_POR_PROVEDOR:
                continue
        task = (prov, mxs[prov], contas, html_base, worker_ctx)
        try:
            resultado = enviar_para_provedor(task)
            if resultado.get("tentados", 0) > 0:
                ciclos_sem_envio = 0
            else:
                ciclos_sem_envio += 1
        except Exception as e:
            debug_log("worker_loop " + worker_id + " " + prov + ": " + str(e)[:160])
            ciclos_sem_envio += 1
        with GLOBAL["orcamento_lock"]:
            orc_ip = GLOBAL["orcamento_por_ip"].get(worker_ip, {})
            if orc_ip.get(prov, 0) < IP_BUDGET_POR_PROVEDOR:
                providers_queue.append(prov)
        if ciclos_sem_envio > len(PROVEDORES_USAVEIS) * 2:
            time.sleep(60)
            ciclos_sem_envio = 0
        time.sleep(random.uniform(0.5, 2.0))
    smtp_log_raw(worker_id, worker_ip, "---", "WORKER TERMINADO")
    with GLOBAL["workers_lock"]:
        GLOBAL["workers_ativos"] = max(0, GLOBAL.get("workers_ativos", 0) - 1)


def fase_envio_massivo(mxs, contas, html_base):
    global ARACLOUD_MGR_SINGLETON
    definir_fase("FASE 6 - Envio massivo PARALELO HIERÁRQUICO (workers Aracloud)")
    faltam = fases_faltantes_para_envio()
    if faltam:
        dizer("  FASE 6 BLOQUEADA - fases obrigatorias incompletas: " + ", ".join(faltam))
        marcar_fase("FASE6_ENVIO", False, "bloqueada por: " + ", ".join(faltam))
        return
    if not contas:
        marcar_fase("FASE6_ENVIO", False, "sem contas")
        return
    aracloud_mgr = ARACLOUD_MGR_SINGLETON or AracloudManager()
    workers_config = aracloud_mgr.obter_lista_workers()
    if not workers_config:
        marcar_fase("FASE6_ENVIO", False, "0 IPs Aracloud")
        return
    provedores_prontos = []
    for prov, mx_list in mxs.items():
        if not mx_list:
            continue
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            cp_info = CHECKPOINT.obter_provedor_index(prov)
            if cp_info and cp_info.get("concluido", False):
                continue
            provedores_prontos.append(prov)
    GLOBAL["prov_total_envio"] = len(provedores_prontos)
    dizer("  Provedores prontos: " + str(len(provedores_prontos)) + " | Workers: " + str(len(workers_config)))
    if not provedores_prontos:
        marcar_fase("FASE6_ENVIO", False, "0 provedores pendentes")
        return
    marcar_fase("FASE6_ENVIO_INICIO", True, str(len(provedores_prontos)) + " provedores x " + str(len(workers_config)) + " workers")
    GLOBAL["workers"] = workers_config
    GLOBAL["workers_ativos"] = len(workers_config)
    threads = []
    for wc in workers_config:
        t = threading.Thread(target=worker_loop, args=(wc, mxs, contas, html_base), daemon=True, name="worker-" + wc.get("worker_id", "w"))
        threads.append(t)
        t.start()
        dizer("  Worker " + wc.get("worker_id") + " iniciado (IP " + wc.get("ip") + ")")
    try:
        while any(t.is_alive() for t in threads) and not GLOBAL.get("emergency_stop"):
            time.sleep(5)
            if check_stop_file():
                break
    except Exception as e:
        debug_log("fase_envio_massivo espera: " + str(e))
    salvar_checkpoint_smtp()
    all_done = all((CHECKPOINT.obter_provedor_index(p) or {}).get("concluido", False) for p in provedores_prontos)
    if all_done and provedores_prontos:
        dizer("[STATUS] TODOS OS PROVEDORES PROCESSADOS. CAMPANHA CONCLUÍDA.")
        GLOBAL["status"] = "COMPLETED"
        CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status="COMPLETED")
        upload_all_checkpoints_to_hf()
    marcar_fase("FASE6_ENVIO", True, str(GLOBAL['smtp_aceites']) + " aceites")


def monitor_ips_continuo():
    global ARACLOUD_MGR_SINGLETON
    dizer("[MONITOR-IP] Thread de monitoramento de IPs Aracloud iniciada")
    aracloud_mgr = ARACLOUD_MGR_SINGLETON or AracloudManager()
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        time.sleep(60)
        for ip in list(GLOBAL.get("aracloud_ips_ativos", [])):
            if GLOBAL.get("emergency_stop"):
                break
            _cn, _es, item = aracloud_mgr._localizar_ip(ip)
            if item is None or item.get("tipo") == "primario":
                continue
            with GLOBAL["orcamento_lock"]:
                orc_ip = GLOBAL["orcamento_por_ip"].get(ip, {})
                restantes = [p for p in PROVEDORES_USAVEIS if orc_ip.get(p, 0) < IP_BUDGET_POR_PROVEDOR]
            if not restantes:
                dizer("[MONITOR-IP] IP " + ip + " esgotou 1000 envios em TODOS os provedores - a rodar")
                novo_ip = aracloud_mgr.rodar_ip(ip)
                if novo_ip:
                    GLOBAL["spf_sujo"] = True
                    dizer("[MONITOR-IP] IP " + ip + " substituido por " + novo_ip)
        if GLOBAL.get("spf_sujo") or (time.time() - GLOBAL.get("ultimo_refresh_spf", 0) > 900):
            try:
                refresh_spf_bulk("monitor-ips")
                GLOBAL["workers"] = aracloud_mgr.obter_lista_workers()
            except Exception as e:
                debug_log("monitor_ips refresh SPF: " + str(e))


def enviar_email_unico(dest, assunto, html, conta):
    worker_ctx = GLOBAL["workers"][0] if GLOBAL.get("workers") else None
    try:
        payload = aplicar_mutacao_abracadabra(html, conta, dest, subject=assunto)
        dominio = dest.split("@")[-1]
        try:
            answers = dns.resolver.resolve(dominio, 'MX')
            mxs_sorted = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
        except Exception as e:
            smtp_log("FINAL: sem MX para " + dominio + ": " + str(e)[:100])
            return False
        for _pref, mxh in mxs_sorted:
            try:
                if not worker_ctx:
                    return False
                sock, banner, status = tentar_conexao_via_aracloud(worker_ctx, mxh, 30, dominio)
                if sock is None:
                    continue
                sock.sendall(("EHLO " + str(conta['identity_id']) + "\r\n").encode())
                recv_smtp(sock)
                sock.sendall(("MAIL FROM:<" + conta['sender_email'] + ">\r\n").encode())
                mr = recv_smtp(sock)
                if not mr or not mr[0].startswith("250"):
                    sock.close()
                    continue
                sock.sendall(("RCPT TO:<" + dest + ">\r\n").encode())
                rr = recv_smtp(sock)
                if not rr or not rr[0].startswith("250"):
                    sock.close()
                    continue
                sock.sendall(b"DATA\r\n")
                dr = recv_smtp(sock)
                if not dr or not dr[0].startswith("354"):
                    sock.close()
                    continue
                corpo = payload
                if not corpo.endswith(b"\r\n"):
                    corpo += b"\r\n"
                corpo = corpo.replace(b"\r\n.", b"\r\n..")
                sock.sendall(corpo)
                sock.sendall(b"\r\n.\r\n")
                fr = recv_smtp(sock, timeout=30)
                try:
                    sock.sendall(b"QUIT\r\n")
                    sock.close()
                except Exception:
                    pass
                if fr and fr[0].startswith("250"):
                    return True
            except Exception as e:
                smtp_log("FINAL erro em " + mxh + ": " + str(e)[:140])
                continue
        return False
    except Exception as e:
        debug_log("enviar_email_unico: " + traceback.format_exc())
        return False


def gerar_relatorio(rt):
    assunto = "VERISCOPE V23.1 - " + datetime.now().strftime('%Y-%m-%d %H:%M')
    linhas_fases = ""
    try:
        with GLOBAL["stats_lock"]:
            for chave in sorted(GLOBAL.get("fases", {}).keys()):
                info = GLOBAL["fases"][chave]
                linhas_fases += ("<tr><td>" + str(chave) + "</td><td>" + ("OK" if info.get("ok") else "FALHOU") + "</td><td>" + str(info.get("ts", "")) + "</td><td>" + str(info.get("detalhe", "")) + "</td></tr>")
    except Exception:
        linhas_fases = "<tr><td colspan='4'>sem dados</td></tr>"
    linhas_prov = ""
    try:
        for p, r in sorted(rt['por_provedor'].items()):
            taxa = r['aceites'] / max(1, r['tentados']) * 100
            linhas_prov += ("<tr><td>" + str(p) + "</td><td class='ok'>" + "{:,}".format(r['aceites']) + "</td><td>" + "{:,}".format(r['tentados']) + "</td><td>" + "{:,}".format(r['rejeitados']) + "</td><td>" + "{:,}".format(r.get('user_unknown', 0)) + "</td><td>" + "{:,}".format(r['erros']) + "</td><td>" + "{:.1f}".format(taxa) + "%</td></tr>")
    except Exception:
        linhas_prov = "<tr><td colspan='7'>sem dados</td></tr>"
    html = """<html><head><meta charset="utf-8"><style>
body{font-family:Arial;margin:20px;color:#222}h1{color:#2c5f2d}h2{color:#3a7d44}
table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:7px;font-size:13px}
th{background:#3a7d44;color:#fff;text-align:left}.ok{color:#2c5f2d;font-weight:bold}ul{line-height:1.7}
</style></head><body>
<h1>VERISCOPE V23.1 - IPs ARACLOUD (rede provider/flat)</h1>
<h2>Resumo</h2><ul>
<li>IPs Aracloud activos: <b>""" + str(len(GLOBAL.get('aracloud_ips_ativos', []))) + """</b></li>
<li>IPs únicos usados hoje: <b>""" + str(len(GLOBAL.get('aracloud_ips_usados_hoje', set()))) + """</b></li>
<li>Emails base local: <b>""" + "{:,}".format(GLOBAL.get('emails_read', 0)) + """</b> | Parquets: """ + str(GLOBAL.get('pq_downloaded')) + "/" + str(GLOBAL.get('pq_total')) + """</li>
<li>DNS bases: <b>""" + str(GLOBAL.get('dns_updated')) + "/" + str(GLOBAL.get('dns_total')) + """</b> | DESec: """ + str(GLOBAL.get('desec_requests', 0)) + " req / " + str(GLOBAL.get('desec_success', 0)) + " OK / " + str(GLOBAL.get('desec_failures', 0)) + """ fail</li>
<li>MODO RECUPERACAO: <b>""" + ("SIM" if GLOBAL.get("modo_recuperacao") else "NAO") + """</b></li></ul>
<h2>Envio</h2><ul>
<li>Aceites 250: <b>""" + "{:,}".format(rt['aceites']) + """</b> | Tentados: """ + "{:,}".format(rt['tentados']) + """ | Taxa: """ + "{:.2f}".format(rt['aceites'] / max(1, rt['tentados']) * 100) + """%</li>
<li>Rejeitados: """ + "{:,}".format(rt['rejeitados']) + """ | Erros: """ + "{:,}".format(rt['erros']) + """ | 4xx: """ + "{:,}".format(GLOBAL.get('smtp_4xx', 0)) + """ | 5xx: """ + "{:,}".format(GLOBAL.get('smtp_5xx', 0)) + """</li>
<li>USER_UNKNOWN: """ + "{:,}".format(GLOBAL.get('smtp_user_unknown', 0)) + """ | Retries: """ + "{:,}".format(GLOBAL.get('smtp_retries', 0)) + """ | Abandonados: """ + "{:,}".format(GLOBAL.get('smtp_abandonados', 0)) + """</li>
<li>Via Aracloud: """ + "{:,}".format(GLOBAL.get('smtp_via_aracloud', 0)) + """</li></ul>
<h2>Fases</h2><table><tr><th>Fase</th><th>Estado</th><th>Hora</th><th>Detalhe</th></tr>""" + linhas_fases + """</table>
<h2>Por provedor</h2><table><tr><th>Provedor</th><th>Aceites</th><th>Tentados</th><th>Rejeitados</th><th>UK</th><th>Erros</th><th>Taxa</th></tr>""" + linhas_prov + """</table>
</body></html>"""
    return assunto, html


def salvar_logs_hf():
    if GLOBAL.get("logs_uploaded"):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        enviados = 0
        for fname, fpath in [("log", LOG_FILE), ("smtp", SMTP_LOG), ("debug", DEBUG_LOG), ("desec", DESEC_LOG), ("smtp_raw", SMTP_RAW_LOG)]:
            if fpath and os.path.exists(fpath):
                try:
                    api.upload_file(path_or_fileobj=fpath, path_in_repo="logs/" + fname + "_" + str(GLOBAL['exec_id']) + ".txt", repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
                    enviados += 1
                except Exception as e:
                    debug_log("upload log " + fname + ": " + str(e)[:160])
        dizer("  Logs enviados para HuggingFace: " + str(enviados) + "/5")
        GLOBAL["logs_uploaded"] = True
    except Exception as e:
        dizer("  AVISO: upload de logs falhou (" + str(e)[:100] + ")")


def baixar_ficheiros_campanha():
    dizer("A baixar ficheiros de campanha do HuggingFace...")
    try:
        local_mailfrom = hf_hub_download(repo_id=HF_LOGS_REPO, filename="campaign_data/templates/" + MAIL_FROM_FILE_LOCAL, repo_type="dataset", token=HF_TOKEN, force_download=True)
        shutil.copy(local_mailfrom, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
        dizer("  mail_from_variations descarregado")
    except Exception as e:
        dizer("  AVISO: Falha ao baixar mail_from_variations: " + str(e)[:100])


# ====================================================================
# MAIN V23.1
# ====================================================================
def main():
    global DESEC_TOKENS_VALIDOS, LOG_FILE, SMTP_LOG, DEBUG_LOG, ARACLOUD_MGR_SINGLETON

    diagnostico_ambiente_hf()
    execution_id = reset_global_state()
    download_all_checkpoints_from_hf()
    CHECKPOINT._carregar_tudo()
    config = CHECKPOINT.obter_config()

    if config.get("status") == "COMPLETED":
        print("[FIM] Campanha já concluída em execução anterior. A sair.", flush=True)
        sys.exit(0)

    tem_checkpoint = CHECKPOINT.existe_checkpoint()
    if tem_checkpoint:
        print("[RECUPERACAO] Checkpoint detectado - a retomar execucao anterior", flush=True)
        GLOBAL["modo_recuperacao"] = True
        GLOBAL["checkpoint_loaded"] = True
        GLOBAL["status"] = "RUNNING"
        if config.get("exec_id"):
            execution_id = config["exec_id"]
            GLOBAL["exec_id"] = execution_id
        if config.get("ip_colab"):
            GLOBAL["ip_colab"] = config["ip_colab"]
        if config.get("start_time"):
            try:
                GLOBAL["start_time"] = datetime.fromisoformat(config["start_time"]).timestamp()
            except Exception:
                pass
        for k, v in CHECKPOINT.obter_smtp_stats().items():
            if k in GLOBAL:
                GLOBAL[k] = v
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]
        GLOBAL["por_provedor"] = CHECKPOINT.obter_por_provedor()
        for prov_nome, prov_info in GLOBAL["por_provedor"].items():
            if isinstance(prov_info, dict) and prov_info.get("concluido", False):
                GLOBAL["provedores_concluidos_set"].add(prov_nome)
        GLOBAL["provs_concluidos"] = len(GLOBAL["provedores_concluidos_set"])
        with GLOBAL["stats_lock"]:
            for chave, info in CHECKPOINT.states["fases"].items():
                GLOBAL["fases"][chave] = info
        aracloud_state = CHECKPOINT.obter_aracloud()
        if aracloud_state.get("contas"):
            GLOBAL["aracloud_contas"] = aracloud_state["contas"]
            GLOBAL["aracloud_ips_usados_hoje"] = set(aracloud_state.get("ips_usados_hoje", []))
            GLOBAL["aracloud_data_reset"] = aracloud_state.get("data_ultimo_reset")
        dizer("[RECUPERACAO] Estado Aracloud carregado: " + str(len(GLOBAL["aracloud_ips_usados_hoje"])) + " IPs usados hoje")
        try:
            _mgr = AracloudManager()
            _cred = _mgr._carregar_credenciais_hf()
            reconectados = 0
            for _nome, _estado in GLOBAL["aracloud_contas"].items():
                _cd = _cred.get(_nome) or {}
                _pem = _cd.get("pem_path") or _estado.get("pem_path", "")
                if not _pem:
                    continue
                _norm = []
                for _it in _estado.get("ips_ativos", []):
                    if isinstance(_it, str):
                        _norm.append({"ip": _it, "id": None, "tipo": "primario"})
                    elif isinstance(_it, dict):
                        _it.setdefault("tipo", "fip" if _it.get("id") else "primario")
                        _norm.append(_it)
                _estado["ips_ativos"] = _norm
                if _mgr._autenticar(_nome, _cd.get("cred", {})):
                    _vivos = []
                    for ip_info in list(_estado.get("ips_ativos", [])):
                        _ip = ip_info.get("ip")
                        _cli = _mgr._abrir_ssh(_nome, _ip, _pem)
                        if _cli is not None:
                            _vivos.append(ip_info)
                            if _ip not in GLOBAL["aracloud_ips_ativos"]:
                                GLOBAL["aracloud_ips_ativos"].append(_ip)
                            GLOBAL["aracloud_clients_ssh"][_ip] = _cli
                            GLOBAL.setdefault("orcamento_por_ip", {}).setdefault(_ip, {})
                            reconectados += 1
                        else:
                            dizer("  [RECUPERACAO] SSH falhou para " + str(_ip) + " - descartado")
                    _estado["ips_ativos"] = _vivos
            dizer("[RECUPERACAO] " + str(reconectados) + " IPs reconectados via SSH")
            ARACLOUD_MGR_SINGLETON = _mgr
        except Exception as e_rec:
            dizer("[RECUPERACAO] Erro ao reconectar IPs: " + str(e_rec)[:120])
        rate_limits_cp = CHECKPOINT.obter_rate_limits()
        if rate_limits_cp:
            GLOBAL["rate_limits_aprendidos"].update(rate_limits_cp)
            dizer("[RECUPERACAO] Rate limits aprendidos: " + str(len(rate_limits_cp)))
        offsets_cp = CHECKPOINT.obter_offsets()
        if offsets_cp:
            GLOBAL["offsets_por_provedor"] = offsets_cp
        if os.path.exists(SAIDA_ACEITES):
            try:
                with open(SAIDA_ACEITES, "r", encoding="utf-8", errors="ignore") as f:
                    for row in csv.DictReader(f):
                        if row and row.get("email"):
                            GLOBAL["aceites_emails"].add(row["email"].lower().strip())
                dizer("[RECUPERACAO] Aceites anteriores: " + "{:,}".format(len(GLOBAL["aceites_emails"])))
            except Exception as e:
                debug_log("recuperar aceites: " + str(e))
        limpar_logs_antigos(preservar_checkpoints=True, preservar_saida=True)
    else:
        limpar_logs_antigos(preservar_checkpoints=False, preservar_saida=False)

    LOG_FILE = os.path.join(LOG_DIR, "log_" + execution_id + ".txt")
    SMTP_LOG = os.path.join(LOG_DIR, "smtp_" + execution_id + ".txt")
    DEBUG_LOG = os.path.join(LOG_DIR, "debug_" + execution_id + ".txt")
    try:
        for fp in (LOG_FILE, SMTP_LOG, DEBUG_LOG, DESEC_LOG, SMTP_RAW_LOG):
            open(fp, "w", encoding="utf-8").close()
    except Exception:
        pass
    GLOBAL["user_unknowns_file"] = UNKNOWN_FILE_LOCAL

    threading.Thread(target=monitor_live, daemon=True).start()
    threading.Thread(target=keep_alive_thread, daemon=True).start()
    threading.Thread(target=time_limit_watcher, daemon=True).start()
    threading.Thread(target=periodic_checkpoint_thread, daemon=True).start()
    time.sleep(1)

    dizer("VERISCOPE V23.1 HEADLESS [" + execution_id + "]")
    dizer("ARQUITETURA: IP primario + extra (fip/iface) | " + str(MAX_IPS_UNICOS_POR_DIA) + " unicos/conta/dia")
    dizer("CHECKPOINT: " + ("RECUPERAR" if tem_checkpoint else "NOVO"))
    baixar_ficheiros_campanha()

    contas = []
    html_base = ""
    mxs = {}
    rt = None

    try:
        ip_colab = obter_ip_colab()
        CHECKPOINT.salvar_config(execution_id, ip_colab, status="RUNNING")
        GLOBAL["status"] = "RUNNING"
        verificar_blacklists(ip_colab)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        DESEC_TOKENS_VALIDOS = testar_tokens_desec()
        descobrir_dominios_desec()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        contas = carregar_contas()
        html_base = carregar_template()
        carregar_mail_from_variations()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        provedores = descobrir_provedores_reais()
        mxs = resolver_mxs_paralelo(provedores)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        baixar_todos_emails_paralelo(sorted(mxs.keys()))
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        baixar_unknowns_acumulado()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        definir_fase("FASE 2.5 - Aracloud: autenticacao + IPs (primario/iface/fip) + SSH")
        aracloud_mgr = AracloudManager()
        ARACLOUD_MGR_SINGLETON = aracloud_mgr
        total_ips = aracloud_mgr.inicializar()
        if total_ips == 0:
            dizer("  AVISO CRÍTICO: nenhum IP Aracloud activo. Envio impossível.")
            marcar_fase("FASE25_ARACLOUD", False, "0 IPs Aracloud")
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        fase3_dns_completo(contas)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        fase4_esperar_propagacao()
        verificar_propagacao_amostra(contas)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        if len(GLOBAL.get("aracloud_ips_ativos", [])) == 0 and not fase_ja_completa("FASE25_ARACLOUD"):
            dizer("  FASE 5 SALTADA (fail-fast): FASE25 falhou com 0 IPs - nada para aguardar")
        else:
            aguardar_ips_aracloud(4, timeout_seg=ESPERA_POOL_ANTES_ENVIO)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        threading.Thread(target=monitor_ips_continuo, daemon=True).start()
        dizer("  Monitor de IPs Aracloud activado (rotação + SPF refresh)")
        fase_envio_massivo(mxs, contas, html_base)

    except KeyboardInterrupt:
        GLOBAL["emergency_stop"] = True
        dizer("Interrompido pelo utilizador / paragem de emergencia")
    except Exception as e:
        dizer("ERRO GLOBAL: " + str(e))
        debug_log("main:\n" + traceback.format_exc())
        try:
            traceback.print_exc()
        except Exception:
            pass
    finally:
        GLOBAL["monitor_running"] = False
        try:
            dizer("[CHECKPOINT] Salvamento final do estado...")
            salvar_checkpoint_smtp()
            CHECKPOINT.salvar_aracloud(GLOBAL.get("aracloud_contas", {}), list(GLOBAL.get("aracloud_ips_usados_hoje", set())), GLOBAL.get("aracloud_data_reset"))
            CHECKPOINT.salvar_rate_limits(GLOBAL.get("rate_limits_aprendidos", {}))
            final_status = GLOBAL.get("status", "RUNNING")
            if final_status not in ("COMPLETED", "TIME_LIMIT_SHUTDOWN"):
                final_status = "RUNNING"
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status=final_status)
            CHECKPOINT.salvar_tudo()
            upload_all_checkpoints_to_hf()
            dizer("[CHECKPOINT] Estado salvo e enviado para HF")
        except Exception as e:
            dizer("ERRO ao salvar checkpoint final: " + str(e))
        try:
            gerar_ficheiros_finais()
        except Exception as e:
            dizer("ERRO ao gerar ficheiros finais: " + str(e))

    rt = {"aceites": 0, "tentados": 0, "rejeitados": 0, "erros": 0, "via_aracloud": 0, "por_provedor": GLOBAL["por_provedor"]}
    for r in GLOBAL["por_provedor"].values():
        for k in ("aceites", "tentados", "rejeitados", "erros", "via_aracloud"):
            if k in r:
                rt[k] += r[k]

    if GLOBAL.get("emergency_stop"):
        dizer("Paragem de emergencia - estado guardado no checkpoint")
    else:
        dizer("FASE 7 - Relatorio final para " + FINAL_DESTINATARIO)
        cf = random.choice(contas) if contas else None
        if cf and html_base:
            ok1 = enviar_email_unico(FINAL_DESTINATARIO, CAMPAIGN["subject"], html_base, cf)
            dizer("  Email da campanha: " + ("OK" if ok1 else "FALHOU"))
            time.sleep(5)
            ar, cr = gerar_relatorio(rt)
            ok2 = enviar_email_unico(FINAL_DESTINATARIO, ar, cr, cf)
            dizer("  Relatorio HTML: " + ("OK" if ok2 else "FALHOU"))
            try:
                rp = os.path.join(DIR_AUDIT, "relatorio_final.html")
                with open(rp, "w", encoding="utf-8") as f:
                    f.write(cr)
                api = HfApi(token=HF_TOKEN)
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                api.upload_file(path_or_fileobj=rp, path_in_repo="reports/v23_1_" + ts + ".html", repo_id=HF_LOGS_REPO, repo_type="dataset", token=HF_TOKEN)
                dizer("  Relatorio enviado para HuggingFace")
            except Exception as e:
                dizer("  AVISO: upload do relatorio falhou (" + str(e)[:110] + ")")
        else:
            dizer("  Sem conta/template para enviar o relatorio final")

    try:
        upload_unknowns_hf()
    except Exception as e:
        debug_log("upload_unknowns_hf: " + str(e))
    try:
        salvar_logs_hf()
    except Exception as e:
        debug_log("salvar_logs_hf: " + str(e))

    taxa = rt['aceites'] / max(1, rt['tentados']) * 100
    print("\n" + "=" * 70, flush=True)
    print("EXECUCAO TERMINADA - " + str(GLOBAL.get('exec_id')), flush=True)
    print("  Aceites (250 OK) : " + "{:,}".format(rt['aceites']), flush=True)
    print("  Tentativas       : " + "{:,}".format(rt['tentados']), flush=True)
    print("  Taxa             : " + "{:.2f}".format(taxa) + "%", flush=True)
    print("  Via Aracloud     : " + "{:,}".format(rt['via_aracloud']), flush=True)
    print("  IPs Aracloud     : " + str(len(GLOBAL.get('aracloud_ips_ativos', []))), flush=True)
    print("  IPs usados hoje  : " + str(len(GLOBAL.get('aracloud_ips_usados_hoje', set()))), flush=True)
    print("  USER_UNKNOWN     : " + "{:,}".format(len(GLOBAL['user_unknowns'])), flush=True)
    print("  4xx / 5xx        : " + "{:,}".format(GLOBAL.get('smtp_4xx', 0)) + " / " + "{:,}".format(GLOBAL.get('smtp_5xx', 0)), flush=True)
    print("  Retries / Aband. : " + "{:,}".format(GLOBAL.get('smtp_retries', 0)) + " / " + "{:,}".format(GLOBAL.get('smtp_abandonados', 0)), flush=True)
    print("  DESec            : " + str(GLOBAL.get('desec_requests', 0)) + " req | " + str(GLOBAL.get('desec_success', 0)) + " OK | " + str(GLOBAL.get('desec_failures', 0)) + " fail", flush=True)
    print("  STATUS FINAL     : " + str(GLOBAL.get("status", "UNKNOWN")), flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
