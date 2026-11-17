# -*- coding: utf-8 -*-
"""
VERISCOPE V21.0.0 - ARQUITETURA ASSÍNCRONA + 501 WORKERS FIXOS
================================================================
CORREÇÕES IMPLEMENTADAS:
1. asyncio em Fase 6 com 501 workers fixos (500 proxies + 1 Colab)
2. Colab como worker #501 paralelo (não fallback)
3. 4 regras de parada: morto, timeout, 421, 500 emails
4. RCPT dinâmico aprendido por provedor
5. Checkpoint robusto com campaign_started_at vs run_started_at
6. Dashboard 3D HTML profissional
7. Upload HF apenas no final (não periódico)
8. Descoberta inteligente do domínio com 501+ contas
9. SPF bulk com chunks por tamanho
10. Preservação de todas as 18 correções da V20.0.2
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
    packages = [
        "requests", "dnspython", "dkimpy", "cryptography", "pandas",
        "pyarrow", "huggingface_hub", "psutil", "pysocks",
        "aiohttp", "aiosmtplib", "nest_asyncio"
    ]
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

install_packages()

import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "pyarrow", "urllib3", "asyncio", "filelock"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)

import warnings
warnings.filterwarnings("ignore")

import asyncio
import aiohttp
import aiosmtplib
import nest_asyncio

nest_asyncio.apply()

import threading, resource, re, gc, csv, json, time, random, socket, ssl, textwrap
import concurrent.futures, traceback, glob, shutil, hashlib
from datetime import datetime, timezone
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
GLOBAL_DESEC_SEMAPHORE = threading.Semaphore(2)
GLOBAL_FILEWRITE_SEMAPHORE = threading.Semaphore(32)
UNKNOWN_FILE_LOCK = threading.Lock()
ACEITES_FILE_LOCK = threading.Lock()
REJEICOES_FILE_LOCK = threading.Lock()

HF_LOGS_REPO = "Valter3B/Campaign_Logs"
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_CHECKPOINT_REPO = "Valter3B/Veriscope_Checkpoints"

META_ACEITES_POR_PROVEDOR = 50000
MAX_EMAILS_POR_CONTA = 500
MAX_EMAILS_POR_PROVEDOR_DOWNLOAD = float('inf')
EMAILS_JANELA_ENVIO = 50_000
DNS_PROPAGACAO_SEGUNDOS = 300
FINAL_DESTINATARIO = "bingo_192@naver.com"
DIA_ATUAL = 1

WORKERS_DOWNLOAD = 9999
WORKERS_MX = 9999
WORKERS_DNS = 9999
WORKERS_ENVIO = 501
WORKERS_SCRAPER = 9999
WORKERS_PROXY_PIPELINE = 9999
SNAPSHOT_MAX_WORKERS = 9999

APAGAR_PARQUETS_APOS_USO = True
USAR_SNAPSHOT_PRIMEIRO = True

SMTP_TIMEOUT = 15
SMTP_TIMEOUT_ASIATICO = 30
SMTP_RECV_TIMEOUT = 10

DESEC_TTL = 900
DESEC_CHUNK = 400
EXIT_IP_TIMEOUT = 8
PORT25_TIMEOUT = 5
TARGET_VALID_PROXIES = 500
MIN_PROXIES_REFILL = 10
PROXY_COOLDOWN_SEGUNDOS = 120
MAX_TIMEOUTS_BAN = 3
RESCRAPE_INTERVALO_MINIMO = 300
JANELA_VALIDACAO_MAX = 400
MAX_SPF_IPS = 501
DELAY_ENTRE_RCPT = 0.3
MAX_RCPT_POR_CONEXAO = 50
MX_TESTE_VALIDACAO = "mxs.mail.ru"
ESPERA_POOL_ANTES_ENVIO = 900
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 30
RETRY_BACKOFF_MULTIPLIER = 2.0
RATE_BASELINE_EMAILS_MIN = 60
RATE_REDUCAO_POR_ERRO = 0.15
RATE_RECUPERACAO_POR_SUCESSO = 0.05
RATE_MINIMO = 5
CHECKPOINT_SAVE_INTERVAL = 1000
CHECKPOINT_TIME_INTERVAL = 3600
MAX_EMAILS_IP_PROVEDOR = 500
MAX_TIMEOUTS_CONSECUTIVOS = 3
MAX_421_COUNT = 5

DESEC_MIN_DELAY = 1.5
DESEC_PER_DOMAIN_WRITE_INTERVAL = 4.5
DESEC_MAX_BODY_BYTES = 400000
DESEC_MAX_RRSETS_PER_REQ = 150
DESEC_MAX_WAIT_429 = 900
DESEC_BACKOFF_BASE = 30
DESEC_BACKOFF_MULTIPLIER = 2.0

SPF_REFRESH_INTERVALO_MINIMO = 900
DIRECT_FAIL_BLOCK_THRESHOLD = 5
PROXY_GLOBAL_FAIL_REMOVE = 8
CONTAS_PARALELAS_POR_PROVEDOR = 10
PROVEDORES_PARALELOS = 407

RCPT_LIMITS = {
    "yahoo.com": 10, "ymail.com": 10, "aol.com": 10, "rocketmail.com": 10,
    "yahoo.co.uk": 10, "yahoo.fr": 10, "yahoo.de": 10, "yahoo.it": 10,
    "yahoo.es": 10, "yahoo.ca": 10, "yahoo.com.br": 10, "yahoo.com.mx": 10,
    "aim.com": 10, "netscape.net": 10, "mail.com": 10,
    "outlook.com": 25, "hotmail.com": 25, "live.com": 25, "msn.com": 25,
    "hotmail.co.uk": 25, "hotmail.fr": 25, "hotmail.de": 25,
    "icloud.com": 25, "me.com": 25, "mac.com": 25,
    "protonmail.com": 25, "protonmail.ch": 25,
    "mail.ru": 50, "yandex.ru": 50, "ya.ru": 50,
    "bk.ru": 50, "list.ru": 50, "inbox.ru": 50,
    "daum.net": 50, "naver.com": 100,
}
RCPT_DEFAULT = 50

PROVEDORES_ASIATICOS = {
    "naver.com", "hanmail.net", "daum.net", "korea.com", "lycos.co.kr",
    "nate.com", "empal.com", "dreamwiz.com",
    "qq.com", "foxmail.com", "vip.qq.com", "163.com", "126.com", "yeah.net",
    "sohu.com", "sina.com", "tom.com", "21cn.com", "eyou.com",
    "ezweb.ne.jp", "i.softbank.jp", "docomo.ne.jp", "softbank.ne.jp",
    "yahoo.co.jp", "hotmail.co.jp",
    "pchome.com.tw", "kimo.com", "yahoo.com.tw"
}

CAMPAIGN_MAP = {
    1: {"subject": "Can I show you something tomorrow?", "template": "veriscope_email_01.html"},
    2: {"subject": "The question behind Session Matrix", "template": "veriscope_email_02.html"},
    3: {"subject": "The chart is only part of the process", "template": "veriscope_email_03.html"},
    4: {"subject": "Now you can see the complete picture", "template": "veriscope_email_04.html"},
}
CAMPAIGN = CAMPAIGN_MAP[DIA_ATUAL]

DESEC_TOKENS_ALL = [
    "LuWfqx1GL3uXDmyUEExH1tF4Nena", "wfjz4harwSzjt5r3LUhFWmPdumes",
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
    "oMKThgLxaoSRF8nGS6iCi1vN9yqo", "4Yamfr48koi4RbzTEJjMLKhqWopG",
]

FALLBACK_PROXY_URLS = []
BASE_LOCAL = "/content/campaign_v19_0"
DIR_EMAILS = "/content/emails_db"
DIR_AUDIT = os.path.join(BASE_LOCAL, "audit")
DIR_PROXIES = "/content/proxies"
LOG_DIR = os.path.join(BASE_LOCAL, "logs")
DIR_REPO = "/content/trader_repo"

PROXY_STATE_FILE = os.path.join(BASE_LOCAL, "proxy_state.json")
UNKNOWN_FILE_LOCAL = os.path.join(BASE_LOCAL, "user_unknowns_acumulado.csv")
MAIL_FROM_FILE_LOCAL = "mail_from_alex_liquidity_alert_2115.txt"
SAIDA_ACEITES = "/content/aceites_finais.csv"
SAIDA_UNKNOWN = "/content/user_unknowns_final.csv"
SAIDA_REJEICAOES = "/content/rejeicoes_finais.csv"
DESEC_LOG = os.path.join(BASE_LOCAL, "desec_requests.log")
DASHBOARD_3D_FILE = os.path.join(BASE_LOCAL, "dashboard_3d.html")

CP_FASES = os.path.join(BASE_LOCAL, "cp_fases.json")
CP_PROVEDORES = os.path.join(BASE_LOCAL, "cp_provedores.json")
CP_POOL = os.path.join(BASE_LOCAL, "cp_pool.json")
CP_SMTP = os.path.join(BASE_LOCAL, "cp_smtp.json")
CP_CONTAS = os.path.join(BASE_LOCAL, "cp_contas.json")
CP_CONFIG = os.path.join(BASE_LOCAL, "cp_config.json")
CP_UNKNOWN = os.path.join(BASE_LOCAL, "cp_unknown.json")
CP_ACEITES = os.path.join(BASE_LOCAL, "cp_aceites.json")

PROXY_SEARCH_DIRS = [
    DIR_PROXIES, "/content/proxies", "/content", "/content/data",
    "/content/uploads", "./", "/content/drive/MyDrive",
    "/content/drive/MyDrive/proxies", "/content/drive/MyDrive/Proxies",
]

FASES_OBRIGATORIAS_ANTES_ENVIO = [
    "FASE0_IP", "FASE1_TOKENS", "FASE2A_CONTAS", "FASE2B_MX",
    "FASE2C_DOWNLOAD", "FASE25_PROXIES", "FASE3_DNS", "FASE4_PROPAGACAO",
]

def diagnostico_ambiente_hf():
    print("\n" + "=" * 80, flush=True)
    print("🔍 DIAGNÓSTICO CRU DE AMBIENTE E AUTENTICAÇÃO HUGGING FACE", flush=True)
    print("=" * 80, flush=True)
    hf_token = os.environ.get("HF_TOKEN", "")
    print("[ENV] HF_TOKEN existe: " + ("SIM" if hf_token else "NÃO"), flush=True)
    print("[ENV] HF_TOKEN length: " + str(len(hf_token)), flush=True)
    print("\n[HF HUB] A testar api.whoami()...", flush=True)
    try:
        api = HfApi(token=hf_token if hf_token else None)
        info = api.whoami()
        print("[HF HUB] ✅ whoami() SUCCESS. User: " + str(info.get("name")), flush=True)
    except Exception as e:
        print("[HF HUB] ❌ whoami() FAILED: " + str(e)[:200], flush=True)
    print("=" * 80 + "\n", flush=True)

class CheckpointManager:
    def __init__(self):
        self.locks = {
            "fases": threading.Lock(), "provedores": threading.Lock(),
            "pool": threading.Lock(), "smtp": threading.Lock(),
            "contas": threading.Lock(), "config": threading.Lock(),
            "unknown": threading.Lock(), "aceites": threading.Lock(),
        }
        self.states = {
            "fases": {}, "provedores": {},
            "pool": {"pool": [], "scraping_checkpoint": 0, "raw_count": 0},
            "smtp": {
                "smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0,
                "smtp_user_unknown": 0, "smtp_erros": 0, "smtp_conexoes": 0,
                "smtp_via_proxy": 0, "smtp_via_colab": 0,
                "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
                "provedor_index": {}, "por_provedor": {},
            },
            "contas": {"contas_carregadas": 0, "contas_idx": 0},
            "config": {
                "exec_id": None, "ip_colab": None,
                "campaign_started_at": None,
                "run_started_at": None,
                "last_save": None, "status": "NEW",
            },
            "unknown": {"emails": []},
            "aceites": {"emails": []},
        }
        self.paths = {
            "fases": CP_FASES, "provedores": CP_PROVEDORES, "pool": CP_POOL,
            "smtp": CP_SMTP, "contas": CP_CONTAS, "config": CP_CONFIG,
            "unknown": CP_UNKNOWN, "aceites": CP_ACEITES,
        }
        self._carregar_tudo()

    def _carregar_tudo(self):
        for nome, path in self.paths.items():
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        dados = json.load(f)
                    if isinstance(dados, dict):
                        for k, v in self.states[nome].items():
                            if k not in dados:
                                dados[k] = v
                        self.states[nome] = dados
                    print("[CHECKPOINT] Carregado local: " + nome, flush=True)
                except Exception as e:
                    print("[CHECKPOINT] AVISO ao carregar " + nome + ": " + str(e)[:100], flush=True)

    def _salvar_um(self, nome):
        try:
            path = self.paths[nome]
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
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
            self.states["fases"][chave] = {
                "ok": bool(ok), "detalhe": str(detalhe)[:200],
                "ts": datetime.now().isoformat(),
            }
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
            self.states["provedores"][provedor] = {
                "emails": total_emails, "parquets": total_parquets,
                "concluido": True, "ts": datetime.now().isoformat(),
            }
            self._salvar_um("provedores")

    def provedor_ja_baixado(self, provedor):
        with self.locks["provedores"]:
            info = self.states["provedores"].get(provedor, {})
            return info.get("concluido", False) and info.get("emails", 0) > 0

    def obter_provedores_baixados(self):
        with self.locks["provedores"]:
            return [p for p, info in self.states["provedores"].items() if info.get("concluido")]

    def salvar_pool(self, pool_lista, scraping_checkpoint, raw_count):
        with self.locks["pool"]:
            self.states["pool"]["pool"] = pool_lista
            self.states["pool"]["scraping_checkpoint"] = scraping_checkpoint
            self.states["pool"]["raw_count"] = raw_count
            self._salvar_um("pool")

    def carregar_pool(self):
        with self.locks["pool"]:
            return (
                list(self.states["pool"].get("pool", [])),
                int(self.states["pool"].get("scraping_checkpoint", 0)),
                int(self.states["pool"].get("raw_count", 0)),
            )

    def salvar_smtp_stats(self, stats_dict):
        with self.locks["smtp"]:
            for k, v in stats_dict.items():
                if k != "provedor_index" and k != "por_provedor":
                    self.states["smtp"][k] = v
            self._salvar_um("smtp")

    def atualizar_provedor_index(self, provedor, offset, aceites, concluido=False, total_linhas=0):
        with self.locks["smtp"]:
            if "provedor_index" not in self.states["smtp"]:
                self.states["smtp"]["provedor_index"] = {}
            self.states["smtp"]["provedor_index"][provedor] = {
                "offset": offset, "aceites": aceites, "concluido": concluido,
                "total_linhas": total_linhas, "ts": datetime.now().isoformat(),
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
            return {
                "smtp_tentativas": self.states["smtp"].get("smtp_tentativas", 0),
                "smtp_aceites": self.states["smtp"].get("smtp_aceites", 0),
                "smtp_rejeitados": self.states["smtp"].get("smtp_rejeitados", 0),
                "smtp_user_unknown": self.states["smtp"].get("smtp_user_unknown", 0),
                "smtp_erros": self.states["smtp"].get("smtp_erros", 0),
                "smtp_conexoes": self.states["smtp"].get("smtp_conexoes", 0),
                "smtp_via_proxy": self.states["smtp"].get("smtp_via_proxy", 0),
                "smtp_via_colab": self.states["smtp"].get("smtp_via_colab", 0),
                "smtp_4xx": self.states["smtp"].get("smtp_4xx", 0),
                "smtp_5xx": self.states["smtp"].get("smtp_5xx", 0),
                "smtp_retries": self.states["smtp"].get("smtp_retries", 0),
                "smtp_abandonados": self.states["smtp"].get("smtp_abandonados", 0),
            }

    def salvar_config(self, exec_id, ip_colab, status="RUNNING"):
        with self.locks["config"]:
            self.states["config"]["exec_id"] = exec_id
            self.states["config"]["ip_colab"] = ip_colab
            self.states["config"]["status"] = status
            if not self.states["config"].get("campaign_started_at"):
                self.states["config"]["campaign_started_at"] = datetime.now().isoformat()
            self.states["config"]["run_started_at"] = datetime.now().isoformat()
            self.states["config"]["last_save"] = datetime.now().isoformat()
            self._salvar_um("config")

    def obter_config(self):
        with self.locks["config"]:
            return dict(self.states["config"])

    def salvar_contas(self, n_carregadas):
        with self.locks["contas"]:
            self.states["contas"]["contas_carregadas"] = n_carregadas
            self._salvar_um("contas")

    def salvar_unknowns(self, unknowns_set):
        with self.locks["unknown"]:
            try:
                lista = list(unknowns_set)
            except Exception:
                lista = []
            self.states["unknown"]["emails"] = lista
            self._salvar_um("unknown")

    def carregar_unknowns(self):
        with self.locks["unknown"]:
            try:
                return set(self.states["unknown"].get("emails", []))
            except Exception:
                return set()

    def salvar_aceites(self, aceites_set):
        with self.locks["aceites"]:
            try:
                lista = list(aceites_set)
            except Exception:
                lista = []
            self.states["aceites"]["emails"] = lista
            self._salvar_um("aceites")

    def carregar_aceites(self):
        with self.locks["aceites"]:
            try:
                return set(self.states["aceites"].get("emails", []))
            except Exception:
                return set()

    def salvar_tudo(self):
        for nome in self.paths.keys():
            self._salvar_um(nome)

    def limpar_tudo(self):
        for path in self.paths.values():
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

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
        "exec_id": execution_id,
        "monitor_running": True,
        "start_time": time.time(),
        "log_buffer": deque(maxlen=12),
        "stats_lock": threading.Lock(),
        "emergency_stop": False,
        "ip_colab": None,
        "fase_atual": "INICIO",
        "fase_inicio": time.time(),
        "fases": {},
        "pq_total": 0, "pq_downloaded": 0, "pq_falhados": 0,
        "prov_total": 0, "prov_processed": 0, "emails_read": 0, "mx_found": 0,
        "snapshot_ok": False,
        "repo_files_cache": None,
        "dns_total": 0, "dns_updated": 0, "dns_escritos": 0,
        "smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0,
        "smtp_user_unknown": 0, "smtp_erros": 0, "smtp_conexoes": 0,
        "smtp_via_proxy": 0, "smtp_via_colab": 0,
        "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
        "smtp_aceites_ultima_checkpoint": 0,
        "provs_concluidos": 0, "prov_total_envio": 0,
        "rcpt_aprendido": {}, "rcpt_lock": threading.Lock(),
        "por_provedor": {}, "provedores_concluidos_set": set(),
        "contas": [],
        "mail_from_variations": [],
        "mail_from_idx_counter": 0,
        "mail_from_counter_lock": threading.Lock(),
        "logs_uploaded": False,
        "pool": [],
        "pool_lock": threading.Lock(),
        "scraping_checkpoint": 0,
        "raw_proxies": [],
        "raw_lock": threading.Lock(),
        "ips_vistos": set(),
        "proxy_state": {},
        "proxy_state_lock": threading.Lock(),
        "proxy_fail_counts": {},
        "proxies_mortos_count": 0,
        "proxies_banidos": 0,
        "scraping_inicial_feito": False,
        "ultimo_rescrape": 0,
        "scrape_lock": threading.Lock(),
        "urls_total": 0,
        "urls_scraped": 0,
        "urls_failed": 0,
        "proxies_extracted": 0,
        "ficheiros_proxy": 0,
        "user_unknowns": set(),
        "user_unknowns_lock": threading.Lock(),
        "user_unknowns_file": None,
        "aceites_emails": set(),
        "aceites_lock": threading.Lock(),
        "active_ips": [],
        "ips_lock": threading.Lock(),
        "ultimo_refresh_spf": 0,
        "spf_sujo": True,
        "spf_record_escrito": None,
        "spf_ips_escritos": [],
        "colab_port25_blocked": False,
        "colab_direct_fails": 0,
        "dominios_rate_limited": [],
        "rate_controller": {},
        "rate_lock": threading.Lock(),
        "smtp_responses": [],
        "smtp_responses_lock": threading.Lock(),
        "checkpoint_loaded": False,
        "modo_recuperacao": False,
        "desec_requests": 0,
        "desec_success": 0,
        "desec_failures": 0,
        "desec_last_request": 0,
        "status": "NEW",
        "ultimo_checkpoint_tempo": time.time(),
        "tls_warnings": 0,
        "dominio_principal": None,
        "colab_spf_atualizado": False,
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
                    "rate_current": float(RATE_BASELINE_EMAILS_MIN),
                    "ultima_acao": time.time(),
                    "ultima_resposta_4xx": 0, "ultima_resposta_5xx": 0,
                    "throttled_ate": 0, "provedor_stats": {},
                }
            return self.stats[identity_id]

    def pode_enviar(self, identity_id, provedor):
        stats = self._get_identity_stats(identity_id)
        agora = time.time()
        if stats["throttled_ate"] > agora:
            return False, stats["throttled_ate"] - agora
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
            stats["rate_current"] = min(
                RATE_BASELINE_EMAILS_MIN * 2,
                stats["rate_current"] * (1.0 + RATE_RECUPERACAO_POR_SUCESSO)
            )
            if provedor in stats["provedor_stats"]:
                stats["provedor_stats"][provedor]["erros_consecutivos"] = 0

    def registar_erro_temporario(self, identity_id, provedor, smtp_code):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["erros_consecutivos"] += 1
            stats["ultima_resposta_4xx"] = time.time()
            stats["ultima_acao"] = time.time()
            stats["rate_current"] = max(
                RATE_MINIMO,
                stats["rate_current"] * (1.0 - RATE_REDUCAO_POR_ERRO)
            )
            if provedor not in stats["provedor_stats"]:
                stats["provedor_stats"][provedor] = {"erros_consecutivos": 0, "ultimo_erro": 0}
            stats["provedor_stats"][provedor]["erros_consecutivos"] += 1
            stats["provedor_stats"][provedor]["ultimo_erro"] = time.time()
            prov_errors = stats["provedor_stats"][provedor]["erros_consecutivos"]
            if prov_errors >= 5:
                throttle_dur = min(60 * prov_errors, 600)
                stats["provedor_stats"][provedor]["throttled_ate"] = time.time() + throttle_dur

    def registar_erro_permanente(self, identity_id, provedor, smtp_code):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["rejeitados"] += 1
            stats["ultima_resposta_5xx"] = time.time()
            stats["ultima_acao"] = time.time()
            stats["rate_current"] = max(RATE_MINIMO, stats["rate_current"] * 0.7)

    def registar_throttle(self, identity_id, provedor, duracao_segundos):
        stats = self._get_identity_stats(identity_id)
        with self.lock:
            stats["throttled_ate"] = time.time() + duracao_segundos

RATE_CTRL = RateController()

def garantir_diretorios():
    for d in [BASE_LOCAL, LOG_DIR, DIR_EMAILS, DIR_AUDIT, DIR_PROXIES, DIR_REPO]:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass

def garantir_diretorio_para(caminho):
    try:
        d = os.path.dirname(caminho)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass

def limpar_logs_antigos(preservar_checkpoints=True, preservar_saida=False):
    garantir_diretorios()
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
    os.makedirs(BASE_LOCAL, exist_ok=True)
    if not preservar_checkpoints and os.path.exists(PROXY_STATE_FILE):
        try:
            os.remove(PROXY_STATE_FILE)
        except Exception:
            pass
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

def registar_resposta_smtp(message_id, smtp_code, smtp_message, provider,
                            recipient_domain, sending_account, sending_ip,
                            domain, subdomain, attempt_number, response_time):
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
            "response_time": float(response_time or 0),
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
        garantir_diretorio_para(DESEC_LOG)
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(DESEC_LOG, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write("[" + ts + "] DESec Request #" + str(GLOBAL.get("desec_requests", 0)) + "\n")
                f.write("Domain: " + str(domain) + "\n")
                f.write("URL: " + str(url) + "\n")
                f.write("Method: " + str(method) + "\n")
                f.write("Attempt: " + str(attempt) + "\n")
                f.write("Duration: " + "{:.0f}".format(duration_ms) + "ms\n")
                f.write("Status: " + str(status_code) + "\n")
                f.write("Response Body: " + str(response_body)[:1000] + "\n")
                f.write("=" * 80 + "\n")
        with GLOBAL["stats_lock"]:
            GLOBAL["desec_requests"] = GLOBAL.get("desec_requests", 0) + 1
            if status_code is not None and 200 <= int(status_code) < 300:
                GLOBAL["desec_success"] = GLOBAL.get("desec_success", 0) + 1
            elif method != "GET":
                GLOBAL["desec_failures"] = GLOBAL.get("desec_failures", 0) + 1
    except Exception as e:
        debug_log("log_desec_request: " + str(e))

def upload_all_checkpoints_to_hf():
    dizer("[HF SYNC] A enviar checkpoints para HuggingFace...")
    try:
        api = HfApi(token=HF_TOKEN)
        enviados = 0
        for nome, path in CHECKPOINT.paths.items():
            if os.path.exists(path):
                try:
                    api.upload_file(
                        path_or_fileobj=path,
                        path_in_repo="checkpoints/" + nome + ".json",
                        repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN,
                    )
                    enviados += 1
                except Exception as e:
                    dizer("  [HF SYNC] AVISO upload " + nome + ": " + str(e)[:120])
        for fpath in [UNKNOWN_FILE_LOCAL, SAIDA_ACEITES, SAIDA_REJEICAOES]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(
                        path_or_fileobj=fpath,
                        path_in_repo="results/" + os.path.basename(fpath),
                        repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN,
                    )
                    enviados += 1
                except Exception as e:
                    dizer("  [HF SYNC] AVISO upload " + os.path.basename(fpath) + ": " + str(e)[:120])
        if os.path.exists(DASHBOARD_3D_FILE):
            try:
                api.upload_file(
                    path_or_fileobj=DASHBOARD_3D_FILE,
                    path_in_repo="dashboard/dashboard_3d.html",
                    repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN,
                )
                enviados += 1
            except Exception as e:
                dizer("  [HF SYNC] AVISO upload dashboard: " + str(e)[:120])
        dizer("[HF SYNC] Upload concluido: " + str(enviados) + " ficheiros enviados.")
    except Exception as e:
        dizer("[HF SYNC] ERRO geral: " + str(e)[:100])

def download_all_checkpoints_from_hf():
    dizer("[HF SYNC] A baixar checkpoints do HuggingFace...")
    garantir_diretorios()
    baixados = 0
    ausentes = 0
    falhados = 0
    for nome, path in CHECKPOINT.paths.items():
        try:
            local = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO,
                filename="checkpoints/" + nome + ".json",
                repo_type="dataset", token=HF_TOKEN, force_download=True
            )
            garantir_diretorio_para(path)
            shutil.copy(local, path)
            baixados += 1
            dizer("  [HF SYNC] Checkpoint baixado: " + nome + ".json")
        except Exception as e:
            texto = str(e)
            if "404" in texto or "EntryNotFound" in texto or "does not exist" in texto.lower():
                ausentes += 1
                dizer("  [HF SYNC] Checkpoint inexistente na HF: " + nome + ".json (normal na 1a execucao)")
            else:
                falhados += 1
                dizer("  [HF SYNC] FALHA ao baixar " + nome + ".json: " + texto[:140])
    dizer("[HF SYNC] Downloads: " + str(baixados) + " ok | " + str(ausentes) + " ausentes | " + str(falhados) + " falhados")
    return baixados

def trigger_next_run():
    pat = os.environ.get("GITHUB_PAT")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not pat or not repo:
        dizer("[AUTO-RESTART] Sem GITHUB_PAT ou GITHUB_REPOSITORY. Skip.")
        return
    workflow_file = "colab_run.yml"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    headers = {
        "Authorization": f"token {pat}",
        "Accept": "application/vnd.github.v3+json"
    }
    data = {"ref": "main"}
    try:
        r = requests.post(url, headers=headers, json=data, timeout=30)
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
            GLOBAL["fases"][chave] = {
                "ok": bool(ok),
                "detalhe": str(detalhe)[:180],
                "ts": datetime.now().strftime("%H:%M:%S"),
            }
    except Exception:
        pass
    CHECKPOINT.atualizar_fase(chave, ok, detalhe)
    simbolo = "[OK] " if ok else "[FALHOU] "
    dizer(simbolo + chave + " -> " + str(detalhe)[:150])

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
        if parsed.fragment:
            novo_fragment = parsed.fragment + "&" + fragment_novo
        else:
            novo_fragment = fragment_novo
        parsed_novo = parsed._replace(fragment=novo_fragment)
        return urlunparse(parsed_novo)
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
        def substituir_href(match):
            prefixo = match.group(1)
            url_original = match.group(2)
            sufixo = match.group(3)
            url_mutada = mutar_url(url_original, recipient_email, conta["identity_id"])
            return prefixo + url_mutada + sufixo
        h = re.sub(r'(href=["\'])([^"\']+)(["\'])', substituir_href, h)
        def substituir_src(match):
            prefixo = match.group(1)
            url_original = match.group(2)
            sufixo = match.group(3)
            url_mutada = mutar_url(url_original, recipient_email, conta["identity_id"])
            return prefixo + url_mutada + sufixo
        h = re.sub(r'(src=["\'])([^"\']+)(["\'])', substituir_src, h)
        comentario_extra = "<!-- " + hashlib.md5(
            (recipient_email + str(time.time())).encode()
        ).hexdigest()[:12] + " -->"
        h = h.replace("</body>", comentario_extra + "\n</body>", 1)
        return h
    except Exception as e:
        debug_log("mutar_html_abracadabra: " + str(e))
        return html_base

def carregar_mail_from_variations():
    dizer("A carregar variacoes de Mail From...")
    variations = []
    candidatos = [
        os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL),
        MAIL_FROM_FILE_LOCAL,
        "/content/" + MAIL_FROM_FILE_LOCAL,
        "/content/data/" + MAIL_FROM_FILE_LOCAL,
    ]
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
            dizer("  AVISO: Erro ao ler ficheiro: " + str(e)[:120])
    else:
        try:
            local_mf = hf_hub_download(
                repo_id=HF_LOGS_REPO,
                filename="campaign_data/templates/" + MAIL_FROM_FILE_LOCAL,
                repo_type="dataset", token=HF_TOKEN, force_download=True
            )
            garantir_diretorio_para(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
            shutil.copy(local_mf, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
            with open(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL), "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    linha = linha.strip()
                    if linha and not linha.startswith("#"):
                        variations.append(linha)
            dizer("  Carregadas " + str(len(variations)) + " variacoes do HuggingFace")
        except Exception as e:
            dizer("  AVISO: Falha no download HF: " + str(e)[:120])
    GLOBAL["mail_from_variations"] = variations
    GLOBAL["mail_from_idx_counter"] = 0
    return variations

def obter_display_name(conta, idx_conta):
    variations = GLOBAL.get("mail_from_variations", [])
    if not variations:
        base = conta.get("base", "")
        if base:
            nome_limpo = base.replace("-", " ").replace("_", " ").title()
            return nome_limpo
        return "Alex"
    idx_var = idx_conta % len(variations)
    return variations[idx_var]

def render_dashboard_text():
    if not GLOBAL:
        return
    el = time.time() - GLOBAL["start_time"]
    h, m, s = int(el // 3600), int((el % 3600) // 60), int(el % 60)
    rate = GLOBAL["smtp_aceites"] / el if el > 0 else 0
    taxa_total = GLOBAL["smtp_aceites"] / max(1, GLOBAL["smtp_tentativas"]) * 100
    pq_str = str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total'])
    prov_str = str(GLOBAL['prov_processed']) + "/" + str(GLOBAL['prov_total'])
    dns_str = str(GLOBAL['dns_updated']) + "/" + str(GLOBAL['dns_total'])
    ip_str = GLOBAL['ip_colab'] or "---"
    prov_conc = str(GLOBAL['provs_concluidos']) + "/" + str(GLOBAL['prov_total_envio'])
    pool_str = str(len(GLOBAL['pool'])) + "/" + str(TARGET_VALID_PROXIES)
    unk_str = "{:,}".format(len(GLOBAL['user_unknowns']))
    scrape_str = str(GLOBAL['urls_scraped']) + "/" + str(GLOBAL['urls_total'])
    disco = espaco_disco_livre_gb()
    disco_str = ("{:.0f}GB".format(disco)) if disco >= 0 else "?"
    snap = "SIM" if GLOBAL.get("snapshot_ok") else "nao"
    fase_atual = str(GLOBAL.get("fase_atual", "?"))[:40]
    modo = "REC" if GLOBAL.get("modo_recuperacao") else "NEW"
    aceites_set = len(GLOBAL.get('aceites_emails', set()))
    desec_req = GLOBAL.get("desec_requests", 0)
    desec_ok = GLOBAL.get("desec_success", 0)
    desec_fail = GLOBAL.get("desec_failures", 0)
    status = GLOBAL.get("status", "UNKNOWN")
    print(CLEAR_SCREEN, end="", flush=True)
    print("╔" + "═" * 76 + "╗", flush=True)
    print("║  🚀 VERISCOPE V21.0.0 | ASYNC 501 WORKERS | " + modo + "  ⏱️ {:02d}:{:02d}:{:02d}  💾 {:<6} ║".format(h, m, s, disco_str), flush=True)
    print("║  STATUS: " + status.ljust(66) + " ║", flush=True)
    print("║  FASE: " + fase_atual.ljust(66) + " ║", flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║  🌐 IP Colab: {:<15} 📧 Contas: {:<6} 📧 Emails: {:<18,} ║".format(ip_str, len(GLOBAL['contas']), GLOBAL['emails_read']), flush=True)
    print("║  📦 Parquets: {:<14} 🏢 Provs: {:<14} 🌐 MXs: {:<15} ║".format(pq_str, prov_str, GLOBAL['mx_found']), flush=True)
    print("║  🔐 DNS: {:<16} 💡 Taxa: {:<7.2f}%  Snap: {:<15} ║".format(dns_str, taxa_total, snap), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║  🔌 Proxies: {:<11} 💀 Mortos: {:<6} 🚫 Banidos: {:<15} ║".format(pool_str, GLOBAL['proxies_mortos_count'], GLOBAL['proxies_banidos']), flush=True)
    print("║  📤 Tentativas: {:<12,} ⚡ {:.1f}/s     🔌 Con: {:<15,} ║".format(GLOBAL['smtp_tentativas'], rate, GLOBAL['smtp_conexoes']), flush=True)
    print("║  SMTP: ✅{:<8,} ❌{:<8,} ⚠️{:<8,} 👻{:<8,}              ║".format(
        GLOBAL['smtp_aceites'], GLOBAL['smtp_rejeitados'], GLOBAL['smtp_erros'], GLOBAL['smtp_user_unknown']), flush=True)
    print("║  📡 Via Proxy: {:<8,} 🖥️  Via Colab: {:<8,} 👻 UK: {:<10} ║".format(
        GLOBAL['smtp_via_proxy'], GLOBAL['smtp_via_colab'], unk_str), flush=True)
    print("║  🏆 Concluídos: {:<14}  4xx:{:<7,}  5xx:{:<7,}  Retry:{:<7,}  ║".format(
        prov_conc, GLOBAL.get('smtp_4xx', 0), GLOBAL.get('smtp_5xx', 0), GLOBAL.get('smtp_retries', 0)), flush=True)
    print("║  📥 Aceites registados: {:<10,}  URLs scrapadas: {:<20} ║".format(aceites_set, scrape_str), flush=True)
    print("║  🔒 DESec: {} req | {} OK | {} fail | RL:{}                          ║".format(
        desec_req, desec_ok, desec_fail, len(GLOBAL.get('dominios_rate_limited', []))), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║  🛑 Para parar remotamente: touch /tmp/veriscope_stop                  ║", flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    logs = list(GLOBAL["log_buffer"])[-4:]
    for log in logs:
        l = log[:74] if len(log) <= 74 else log[:72] + ".."
        print("║  " + l.ljust(74) + " ║", flush=True)
    for _ in range(4 - len(logs)):
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
                CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
                CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status="TIME_LIMIT_SHUTDOWN")
                CHECKPOINT.salvar_unknowns(GLOBAL["user_unknowns"])
                CHECKPOINT.salvar_aceites(GLOBAL["aceites_emails"])
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
            CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status=GLOBAL.get("status", "RUNNING"))
            CHECKPOINT.salvar_unknowns(GLOBAL.get("user_unknowns", set()))
            CHECKPOINT.salvar_aceites(GLOBAL.get("aceites_emails", set()))
            GLOBAL["ultimo_checkpoint_tempo"] = time.time()
            dizer("[CHECKPOINT 1h] Estado salvo localmente (upload no final).")
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
    servicos = [
        "https://api.ipify.org", "https://ifconfig.me/ip",
        "https://icanhazip.com", "https://ipinfo.io/ip",
        "https://checkip.amazonaws.com",
    ]
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
    listas = {
        "Spamhaus ZEN": "zen.spamhaus.org",
        "Spamhaus PBL": "pbl.spamhaus.org",
        "Barracuda": "b.barracudacentral.org",
        "SpamCop": "bl.spamcop.net",
    }
    listado = []
    limpo = []
    for nome, zona in listas.items():
        try:
            dns.resolver.resolve(reversed_ip + "." + zona, "A")
            listado.append(nome)
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
            limpo.append(nome)
        except Exception:
            pass
    if listado:
        dizer("AVISO: IP Colab listado em: " + ", ".join(listado))
    else:
        dizer("IP Colab limpo em " + str(len(limpo)) + " listas")
    return listado

DESEC_TOKENS_VALIDOS = []
DOM_TOKEN = {}
DOM_MIN_TTL = {}
DOM_TOKEN_LOCK = threading.RLock()
DOM_ULTIMA_ESCRITA = {}
DOM_ESCRITA_LOCK = threading.Lock()

def testar_tokens_desec():
    definir_fase("FASE 1 - Validacao dos tokens deSEC + mapa de dominios")
    def _testar_um(tok):
        if GLOBAL.get("emergency_stop"):
            return tok, None
        try:
            r = requests.get(
                "https://desec.io/api/v1/domains/",
                headers={"Authorization": "Token " + tok},
                timeout=20
            )
            time.sleep(0.4)
            if r.status_code == 200:
                dados = r.json()
                doms = []
                if isinstance(dados, list):
                    for d in dados:
                        if isinstance(d, dict) and d.get("name"):
                            try:
                                min_ttl = int(d.get("minimum_ttl") or DESEC_TTL)
                            except Exception:
                                min_ttl = DESEC_TTL
                            doms.append((str(d["name"]), min_ttl))
                return tok, doms
            debug_log("token " + tok[:6] + "... status " + str(r.status_code))
        except Exception as e:
            debug_log("token test " + tok[:6] + "...: " + str(e)[:120])
        return tok, None
    validos = []
    mapa_feito = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            for tok, doms in ex.map(_testar_um, DESEC_TOKENS_ALL):
                if doms is None:
                    continue
                validos.append(tok)
                with DOM_TOKEN_LOCK:
                    for nome_dom, min_ttl in doms:
                        DOM_TOKEN[nome_dom] = tok
                        DOM_MIN_TTL[nome_dom] = min_ttl
                mapa_feito += 1
    except Exception as e:
        debug_log("testar_tokens_desec: " + str(e))
    DESEC_TOKENS_VALIDOS.clear()
    DESEC_TOKENS_VALIDOS.extend(validos)
    dizer("Tokens validos: " + str(len(validos)) + "/" + str(len(DESEC_TOKENS_ALL)) +
          " | dominios mapeados: " + str(mapa_feito))
    if not validos:
        marcar_fase("FASE1_TOKENS", False, "nenhum token valido")
        raise RuntimeError("Nenhum token deSEC valido")
    marcar_fase("FASE1_TOKENS", True, str(len(validos)) + " tokens, " + str(mapa_feito) + " dominios")
    return validos

def download_hf_com_retry(repo_id, filename, repo_type, max_retries=4):
    ultima = None
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            raise RuntimeError("emergency_stop")
        try:
            with GLOBAL_HF_SEMAPHORE:
                return hf_hub_download(
                    repo_id=repo_id, filename=filename, repo_type=repo_type,
                    token=HF_TOKEN, etag_timeout=60,
                )
        except Exception as e:
            ultima = e
            texto = str(e).lower()
            espera = 30 if ("429" in texto or "rate" in texto or "too many" in texto) else (2 ** tentativa)
            debug_log("hf_retry " + str(tentativa + 1) + " " + filename + " : " + str(e)[:180])
            time.sleep(espera)
    raise ultima if ultima else RuntimeError("download_hf_com_retry falhou")

def carregar_contas():
    definir_fase("FASE 2A - Contas DKIM + template da campanha")
    if fase_ja_completa("FASE2A_CONTAS"):
        dizer("  Fase 2A ja concluida no checkpoint - a recarregar contas")
    p = download_hf_com_retry(HF_LOGS_REPO, "campaign_data/phase1_accounts_keys.csv", "dataset")
    with open(p, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    contas = []
    for r in rows:
        priv = (r.get("dkim_private_key_pem") or "").strip().replace("\n", "\n")
        if not priv.startswith("-----BEGIN "):
            continue
        contas.append({
            "identity_id": (r.get("identity_id") or "").strip(),
            "sender_email": (r.get("sender_email") or "").strip(),
            "domain": (r.get("domain") or "").strip(),
            "base": (r.get("base") or "").strip(),
            "selector": (r.get("dkim_selector") or "default").strip(),
            "private_key_pem": priv,
            "public_key_txt": (r.get("dkim_public_txt") or "").strip(),
        })
    
    # Descoberta inteligente do domínio principal com 501+ contas
    por_dominio = defaultdict(list)
    for conta in contas:
        por_dominio[conta['domain']].append(conta)
    
    dominio_principal = None
    for domain, contas_dom in por_dominio.items():
        if len(contas_dom) >= 501:
            dominio_principal = domain
            break
    
    if not dominio_principal:
        dominio_principal = max(por_dominio.keys(), key=lambda d: len(por_dominio[d]))
        dizer("AVISO: Nenhum domínio com 501+ contas. Usando " + dominio_principal + 
              " com " + str(len(por_dominio[dominio_principal])) + " contas")
    
    GLOBAL['dominio_principal'] = dominio_principal
    contas_selecionadas = por_dominio[dominio_principal][:501]
    GLOBAL["contas"] = contas_selecionadas
    CHECKPOINT.salvar_contas(len(contas_selecionadas))
    
    if not contas_selecionadas:
        marcar_fase("FASE2A_CONTAS", False, "0 contas com chave DKIM")
        raise RuntimeError("Nenhuma conta valida carregada")
    
    dizer("  Domínio principal: " + dominio_principal + " com " + str(len(contas_selecionadas)) + " contas selecionadas")
    marcar_fase("FASE2A_CONTAS", True, str(len(contas_selecionadas)) + " contas do domínio " + dominio_principal)
    return contas_selecionadas

def carregar_template():
    p = download_hf_com_retry(
        HF_LOGS_REPO,
        "campaign_data/templates/" + CAMPAIGN['template'],
        "dataset"
    )
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
    GLOBAL["prov_total"] = len(provs)
    dizer("Provedores descobertos: " + str(len(provs)))
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
            "mail.ru": ["mxs.mail.ru"],
            "bk.ru": ["mxs.mail.ru"],
            "list.ru": ["mxs.mail.ru"],
            "inbox.ru": ["mxs.mail.ru"],
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
                dizer("  ... snapshot em curso: " + str(n) + " parquets no disco | livre " +
                      "{:.1f}GB".format(espaco_disco_livre_gb()))
                ultimo = n
        except Exception:
            pass
        evento_parar.wait(30)

def baixar_snapshot_repo():
    if not USAR_SNAPSHOT_PRIMEIRO:
        return None
    padroes = [
        "Trader_Emails/", "Trader_Emails/*/", "Trader_Emails/*/*/",
        "Trader_Emails/*.parquet", "Trader_Emails/**/*.parquet",
    ]
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
                    max_workers=max(8, min(SNAPSHOT_MAX_WORKERS, 64)),
                    etag_timeout=60,
                )
                decorrido = time.time() - inicio
                contar = 0
                for _r, _d, fs in os.walk(local or DIR_REPO):
                    for f in fs:
                        if f.endswith(".parquet"):
                            contar += 1
                dizer("  Snapshot OK em " + "{:.0f}".format(decorrido) + "s -> " + str(local) +
                      " (" + str(contar) + " parquets)")
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
    if resultado is None and ultima is not None:
        dizer("  AVISO: snapshot impossivel - recurso a downloads individuais (mais lento)")
    return resultado

def baixar_todos_emails_paralelo(provedores):
    definir_fase("FASE 2C - Download HuggingFace (snapshot) + extraccao de emails")
    livres = espaco_disco_livre_gb()
    dizer("  Espaco livre em /content: " + "{:.1f}".format(livres) + " GB")
    if 0 <= livres < 10:
        dizer("  AVISO: pouco espaco em disco - activada limpeza agressiva de parquets")
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
    args = []
    for p in provedores:
        if p in por_prov:
            args.append((p, por_prov[p]))
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
        itens_args = []
        for p, rels in args:
            pares = []
            for rel in rels:
                cand = os.path.join(raiz_local, rel)
                pares.append((rel, cand if os.path.exists(cand) else None))
            itens_args.append((p, pares))
    else:
        itens_args = [(p, [(rel, None) for rel in rels]) for p, rels in args]
    if GLOBAL.get("emergency_stop"):
        marcar_fase("FASE2C_DOWNLOAD", False, "paragem de emergencia")
        return
    dizer("  A extrair emails para " + DIR_EMAILS + " ...")
    trabalhadores = max(8, min(WORKERS_DOWNLOAD, len(itens_args) or 1, 200))
    dizer("  Threads de extraccao: " + str(trabalhadores))
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
    dizer("  Parquets processados: " + str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']) +
          " | falhas: " + str(GLOBAL['pq_falhados']))
    dizer("  Provedores com ficheiro: " + str(GLOBAL['prov_processed']) + "/" + str(GLOBAL['prov_total']))
    completos = (GLOBAL['pq_downloaded'] + GLOBAL['pq_falhados']) >= GLOBAL['pq_total']
    marcar_fase("FASE2C_DOWNLOAD", completos and GLOBAL['pq_downloaded'] > 0,
                str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']) + " parquets")

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
                            if "@" not in e or e in seen:
                                continue
                            if e.startswith("@") or e.endswith("@"):
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
                if total >= MAX_EMAILS_POR_PROVEDOR_DOWNLOAD:
                    break
            fout.flush()
            try:
                os.fsync(fout.fileno())
            except Exception:
                pass
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
    baixou = False
    try:
        local = download_hf_com_retry(HF_LOGS_REPO, "blacklists/user_unknowns_acumulado.csv", "dataset")
        if local and os.path.exists(local):
            garantir_diretorio_para(UNKNOWN_FILE_LOCAL)
            shutil.copy(local, UNKNOWN_FILE_LOCAL)
            baixou = True
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
        except Exception as e:
            dizer("  AVISO: blacklist local ilegivel (" + str(e)[:80] + ") - a comecar vazia")
        unknowns = set()
    try:
        cp_unk = CHECKPOINT.carregar_unknowns()
    except Exception:
        cp_unk = set()
    atual = GLOBAL.get("user_unknowns") or set()
    GLOBAL["user_unknowns"] = unknowns | cp_unk | atual
    dizer("  USER_UNKNOWNs activos: " + "{:,}".format(len(GLOBAL["user_unknowns"])))
    return GLOBAL["user_unknowns"]

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
        garantir_diretorio_para(UNKNOWN_FILE_LOCAL)
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
        try:
            with ACEITES_FILE_LOCK:
                file_exists = os.path.exists(SAIDA_ACEITES)
                with open(SAIDA_ACEITES, "a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                    writer.writerow([email_lower, provedor, conta_id, mx_host, datetime.now().isoformat()])
        except Exception as e:
            debug_log("registrar_aceite ficheiro: " + str(e))
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
        dizer("  (sem blacklist para enviar)")
        return
    try:
        api = HfApi(token=HF_TOKEN)
        api.upload_file(
            path_or_fileobj=UNKNOWN_FILE_LOCAL,
            path_in_repo="blacklists/user_unknowns_acumulado.csv",
            repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN
        )
        dizer("  USER_UNKNOWNs enviados para HuggingFace (" + "{:,}".format(len(GLOBAL['user_unknowns'])) + ")")
    except Exception as e:
        dizer("  AVISO: upload de blacklist falhou (" + str(e)[:100] + ")")

def gerar_ficheiros_finais():
    definir_fase("GERACAO DE FICHEIROS FINAIS")
    dizer("A gerar ficheiro de aceites 250 OK...")
    try:
        aceites_set = set()
        with GLOBAL["aceites_lock"]:
            aceites_set = set(GLOBAL.get("aceites_emails", set()))
        if os.path.exists(SAIDA_ACEITES) and os.path.getsize(SAIDA_ACEITES) > 60:
            dizer("  Ficheiro " + SAIDA_ACEITES + " ja existe com " + "{:,}".format(len(aceites_set)) + " aceites")
        else:
            with open(SAIDA_ACEITES, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                ts = datetime.now().isoformat()
                for email in aceites_set:
                    writer.writerow([email, "", "", "", ts])
            dizer("  Ficheiro " + SAIDA_ACEITES + " criado com " + "{:,}".format(len(aceites_set)) + " aceites")
    except Exception as e:
        dizer("  ERRO ao gerar ficheiro de aceites: " + str(e))
    
    dizer("A gerar ficheiro final de user_unknowns...")
    try:
        if os.path.exists(UNKNOWN_FILE_LOCAL):
            shutil.copy(UNKNOWN_FILE_LOCAL, SAIDA_UNKNOWN)
            with GLOBAL["user_unknowns_lock"]:
                n_unk = len(GLOBAL["user_unknowns"])
            dizer("  Ficheiro " + SAIDA_UNKNOWN + " criado com " + "{:,}".format(n_unk) + " user_unknowns")
    except Exception as e:
        dizer("  ERRO ao copiar user_unknowns: " + str(e))

REGEX_PROXY = re.compile(
    r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})["\'\s\)\]]*[:|\t,;/]*["\'\s\(\[]*(\d{2,5})'
)
REGEX_PROXY_JSON_IP = re.compile(
    r'"(?:ip|host|address|ip_address)"\s*:\s*"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"\s*,\s*'
    r'"(?:port|p)"\s*:\s*"?(\d{2,5})"?',
    re.I
)
REGEX_PROXY_JSON_PORT = re.compile(
    r'"(?:port|p)"\s*:\s*"?(\d{2,5})"?\s*,\s*'
    r'"(?:ip|host|address|ip_address)"\s*:\s*"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"',
    re.I
)

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

def inferir_protocolo_url(u):
    baixo = u.lower()
    if "socks5" in baixo:
        return "socks5"
    if "socks4" in baixo:
        return "socks4"
    if "socks" in baixo:
        return "socks5"
    if "http" in baixo:
        return "http"
    return "socks5"

def descobrir_ficheiros_proxy():
    candidatos = []
    dirs_vistos = set()
    bloqueados = {
        "emails_db", "logs", "audit", ".git", ".cache", ".ipynb_checkpoints",
        "campaign_v19_0", "sample_data", "trainer_repo", "__pycache__",
    }
    for base in PROXY_SEARCH_DIRS:
        if not base or not os.path.isdir(base):
            continue
        real = os.path.realpath(base)
        if real in dirs_vistos:
            continue
        dirs_vistos.add(real)
        dir_principal = (real == os.path.realpath(DIR_PROXIES)) or (real == os.path.realpath("./"))
        for raiz, dirs, ficheiros in os.walk(real):
            try:
                sufixo = raiz[len(real):]
            except Exception:
                sufixo = ""
            profundidade = sufixo.count(os.sep) if sufixo else 0
            if profundidade >= 2:
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if d not in bloqueados and not d.startswith(".")]
            for fn in ficheiros:
                baixo = fn.lower()
                if not baixo.endswith((".txt", ".list", ".csv", ".json", ".lst")):
                    continue
                if dir_principal:
                    candidatos.append(os.path.join(raiz, fn))
                    continue
                if any(k in baixo for k in ("proxy", "proxies", "socks", "prx", "proxylist")):
                    candidatos.append(os.path.join(raiz, fn))
    unicos = []
    vistos = set()
    for c in candidatos:
        try:
            r = os.path.realpath(c)
        except Exception:
            continue
        if r in vistos:
            continue
        vistos.add(r)
        unicos.append(c)
    return unicos

def carregar_urls_proxies():
    urls = []
    diretos = []
    urls_vistos = set()
    ips_vistos = set()
    try:
        os.makedirs(DIR_PROXIES, exist_ok=True)
    except Exception:
        pass
    ficheiros = descobrir_ficheiros_proxy()
    GLOBAL["ficheiros_proxy"] = len(ficheiros)
    if not ficheiros:
        dizer("  AVISO: nenhum ficheiro de proxies encontrado no disco.")
    if FALLBACK_PROXY_URLS:
        for u in FALLBACK_PROXY_URLS:
            if u not in urls_vistos:
                urls_vistos.add(u)
                urls.append(u)
    if ficheiros:
        dizer("  Ficheiros de proxies encontrados: " + str(len(ficheiros)))
        for fp in ficheiros[:25]:
            try:
                tam = os.path.getsize(fp)
            except Exception:
                tam = 0
            dizer("     - " + fp + " (" + "{:,}".format(tam) + " bytes)")
    linhas_lidas = 0
    for fp in ficheiros:
        try:
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                for ln in f:
                    linhas_lidas += 1
                    ln = ln.strip().strip('"').strip("'").strip()
                    if not ln or ln.startswith("#"):
                        continue
                    if ln.startswith("http://") or ln.startswith("https://"):
                        if ln not in urls_vistos:
                            urls_vistos.add(ln)
                            urls.append(ln)
                        continue
                    encontrados = extrair_proxies_do_texto(ln, "socks5")
                    for item, _p in encontrados:
                        if item not in ips_vistos:
                            ips_vistos.add(item)
                            diretos.append(item)
        except Exception as e:
            debug_log("ler " + fp + ": " + str(e))
    dizer("  Linhas lidas: " + "{:,}".format(linhas_lidas))
    dizer("  URLs para scraping: " + str(len(urls)))
    dizer("  IPs diretos no ficheiro: " + str(len(diretos)))
    if diretos:
        adicionados = 0
        with GLOBAL["raw_lock"]:
            vistos = GLOBAL["ips_vistos"]
            for item in diretos:
                if item in vistos:
                    continue
                vistos.add(item)
                GLOBAL["raw_proxies"].append((item, "socks5"))
                adicionados += 1
        GLOBAL["proxies_extracted"] = len(GLOBAL["raw_proxies"])
        dizer("  IPs diretos adicionados a fila de validacao: " + str(adicionados))
    return urls, diretos

def scrape_url(args):
    u, proto = args
    if GLOBAL.get("emergency_stop"):
        return "", proto, u
    ultima_exc = None
    for tentativa in range(3):
        try:
            with GLOBAL_CONNECTION_SEMAPHORE:
                r = requests.get(
                    u, timeout=20,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/122.0 Safari/537.36",
                        "Accept": "*/*",
                    },
                    allow_redirects=True,
                )
                texto = r.text or ""
                if r.status_code == 200 or len(texto) > 60:
                    with GLOBAL["stats_lock"]:
                        GLOBAL["urls_scraped"] += 1
                    return texto, proto, u
                with GLOBAL["stats_lock"]:
                    GLOBAL["urls_failed"] += 1
                return "", proto, u
        except Exception as e:
            ultima_exc = e
            time.sleep(1 + tentativa)
            with GLOBAL["stats_lock"]:
                GLOBAL["urls_failed"] += 1
    return "", proto, u

def extrair_proxies_do_texto(texto, proto_sugerido="socks5"):
    resultados = []
    if not texto:
        return resultados
    vistos_locais = set()
    for regex, ordem in (
        (REGEX_PROXY, (1, 2)),
        (REGEX_PROXY_JSON_IP, (1, 2)),
        (REGEX_PROXY_JSON_PORT, (2, 1)),
    ):
        try:
            for match in regex.finditer(texto):
                g_ip = match.group(ordem[0])
                g_port = match.group(ordem[1])
                try:
                    porta = int(g_port)
                except Exception:
                    continue
                if not (1 <= porta <= 65535):
                    continue
                if not ip_publico_valido(g_ip):
                    continue
                chave = (g_ip, porta)
                if chave in vistos_locais:
                    continue
                vistos_locais.add(chave)
                resultados.append((chave, proto_sugerido))
        except Exception:
            continue
    return resultados

def carregar_e_scrapear_proxies(motivo="inicial"):
    dizer("FASE 2.5 - Scraping de proxies (" + motivo + ")")
    urls, diretos = carregar_urls_proxies()
    with GLOBAL["stats_lock"]:
        GLOBAL["urls_total"] = len(urls)
        GLOBAL["urls_scraped"] = 0
        GLOBAL["urls_failed"] = 0
    if not urls:
        dizer("  Sem URLs para scraping - a usar apenas IPs diretos do ficheiro")
        with GLOBAL["raw_lock"]:
            GLOBAL["proxies_extracted"] = len(GLOBAL["raw_proxies"])
        return GLOBAL["raw_proxies"]
    dizer("  A descarregar " + str(len(urls)) + " listas em paralelo...")
    pares = [(u, inferir_protocolo_url(u)) for u in urls]
    trabalhadores = max(8, min(WORKERS_SCRAPER, len(pares), 250))
    dizer("  Threads de scraping: " + str(trabalhadores))
    novos = 0
    inicio = time.time()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
            for texto, proto, url in ex.map(scrape_url, pares, chunksize=1):
                if GLOBAL.get("emergency_stop"):
                    break
                if not texto:
                    continue
                extraidos = extrair_proxies_do_texto(texto, proto)
                if not extraidos:
                    continue
                adicionados = 0
                with GLOBAL["raw_lock"]:
                    vistos = GLOBAL["ips_vistos"]
                    lista = GLOBAL["raw_proxies"]
                    for item, p in extraidos:
                        if item in vistos:
                            continue
                        vistos.add(item)
                        lista.append((item, p))
                        adicionados += 1
                novos += adicionados
    except Exception as e:
        debug_log("scraping paralelo: " + traceback.format_exc())
        dizer("  AVISO: scraping interrompido (" + str(e)[:120] + ")")
    with GLOBAL["raw_lock"]:
        total_raw = len(GLOBAL["raw_proxies"])
    with GLOBAL["stats_lock"]:
        GLOBAL["proxies_extracted"] = total_raw
    dizer("  Scraping concluido em " + "{:.0f}".format(time.time() - inicio) + "s")
    dizer("  Listas OK: " + str(GLOBAL['urls_scraped']) + "/" + str(GLOBAL['urls_total']) +
          " | falharam: " + str(GLOBAL['urls_failed']))
    dizer("  Novos proxies desta ronda: " + "{:,}".format(novos))
    dizer("  Candidatos acumulados: " + "{:,}".format(total_raw))
    return GLOBAL["raw_proxies"]

def _socket_proxy(proxy_ip, proxy_port, proto, dest_host, dest_port, timeout):
    if proto in ('socks5', 'socks4'):
        s = socks.socksocket()
        if proto == 'socks5':
            s.set_proxy(socks.SOCKS5, proxy_ip, proxy_port)
        else:
            s.set_proxy(socks.SOCKS4, proxy_ip, proxy_port)
        s.settimeout(timeout)
        s.connect((dest_host, dest_port))
        return s
    else:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect((proxy_ip, proxy_port))
        s.sendall(("CONNECT " + dest_host + ":" + str(dest_port) +
                   " HTTP/1.1\r\nHost: " + dest_host + "\r\n\r\n").encode())
        r = s.recv(4096)
        if b"200" not in r:
            s.close()
            raise ConnectionError("CONNECT fail")
        return s

def _obter_exit_ip(proxy_ip, proxy_port, proto):
    servicos = [
        ("api.ipify.org", 80, b"GET /?format=json HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n", "ip"),
        ("ifconfig.me", 80, b"GET /ip HTTP/1.0\r\nHost: ifconfig.me\r\n\r\n", None),
        ("icanhazip.com", 80, b"GET / HTTP/1.0\r\nHost: icanhazip.com\r\n\r\n", None),
        ("ipinfo.io", 80, b"GET /json HTTP/1.0\r\nHost: ipinfo.io\r\n\r\n", "ip"),
    ]
    for host, port, request, json_key in servicos:
        s = None
        try:
            with GLOBAL_CONNECTION_SEMAPHORE:
                s = _socket_proxy(proxy_ip, proxy_port, proto, host, port, EXIT_IP_TIMEOUT)
                s.sendall(request)
                data = s.recv(8192)
                try:
                    s.close()
                except Exception:
                    pass
                s = None
                body = data.split(b"\r\n\r\n", 1)[-1].strip()
                if json_key:
                    try:
                        result = json.loads(body)
                        if isinstance(result, dict) and json_key in result:
                            val = str(result[json_key]).strip()
                            if ip_publico_valido(val):
                                return val
                    except Exception:
                        pass
                text = body.decode(errors="ignore").strip()
                mm = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', text)
                if mm:
                    ip_encontrado = mm.group(1)
                    if ip_publico_valido(ip_encontrado):
                        return ip_encontrado
                if text and ip_publico_valido(text):
                    return text
        except Exception:
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass
            continue
    return proxy_ip

def validar_proxy_rapido(item, proto_sugerido):
    ip, port = item
    proto = proto_sugerido if proto_sugerido else 'socks5'
    if proto == 'socks5':
        ordem = ['socks5', 'socks4', 'http']
    elif proto == 'socks4':
        ordem = ['socks4', 'socks5', 'http']
    else:
        ordem = ['http', 'socks5', 'socks4']
    proto_funcional = None
    for p in ordem:
        if GLOBAL.get("emergency_stop"):
            return None
        s = None
        try:
            with GLOBAL_CONNECTION_SEMAPHORE:
                s = _socket_proxy(ip, port, p, MX_TESTE_VALIDACAO, 25, PORT25_TIMEOUT)
                banner = s.recv(4096).decode(errors="ignore").strip()
                try:
                    s.close()
                except Exception:
                    pass
                s = None
                if banner.startswith("220"):
                    proto_funcional = p
                    break
        except Exception:
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass
            continue
    if proto_funcional is None:
        with GLOBAL["stats_lock"]:
            GLOBAL["proxies_mortos_count"] += 1
        return None
    exit_ip = _obter_exit_ip(ip, port, proto_funcional)
    if not ip_publico_valido(exit_ip):
        exit_ip = ip
    info = {
        "ip": ip, "port": port, "proto": proto_funcional,
        "exit_ip": exit_ip, "added_at": time.time(),
    }
    with GLOBAL["pool_lock"]:
        ja = False
        for existente in GLOBAL["pool"]:
            if existente["ip"] == ip and existente["port"] == port:
                ja = True
                break
        if not ja:
            GLOBAL["pool"].append(info)
        else:
            return None
    GLOBAL["spf_sujo"] = True
    CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
    return info

def validar_proxies_ate_target(target_count):
    start_idx = GLOBAL["scraping_checkpoint"]
    with GLOBAL["raw_lock"]:
        raw = list(GLOBAL["raw_proxies"])
    total = len(raw)
    if total == 0:
        dizer("  AVISO: fila de validacao vazia - nada para testar")
        return 0
    if start_idx >= total:
        dizer("  Todos os " + "{:,}".format(total) + " candidatos ja foram testados")
        return 0
    validos_iniciais = len(GLOBAL["pool"])
    if validos_iniciais >= target_count:
        dizer("  Pool ja tem " + str(validos_iniciais) + " >= alvo " + str(target_count))
        return 0
    dizer("  A validar: " + "{:,}".format(total - start_idx) + " candidatos a partir do indice " +
          str(start_idx) + " (pool " + str(validos_iniciais) + "/" + str(target_count) + ")")
    validos_novos = 0
    next_i = start_idx
    pend = {}
    window = max(16, min(WORKERS_PROXY_PIPELINE, JANELA_VALIDACAO_MAX))
    trabalhadores = max(8, min(WORKERS_PROXY_PIPELINE, window, 250))
    dizer("  Threads de validacao: " + str(trabalhadores))
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
            while next_i < total and len(pend) < window:
                item, proto = raw[next_i]
                pend[ex.submit(validar_proxy_rapido, item, proto)] = next_i
                next_i += 1
            while pend:
                if GLOBAL.get("emergency_stop"):
                    for f in pend:
                        f.cancel()
                    break
                done, _nao = concurrent.futures.wait(
                    pend, return_when=concurrent.futures.FIRST_COMPLETED, timeout=60
                )
                if not done:
                    continue
                for fut in done:
                    pend.pop(fut, None)
                    try:
                        result = fut.result()
                        if result:
                            validos_novos += 1
                    except Exception:
                        pass
                GLOBAL["scraping_checkpoint"] = next_i
                if validos_iniciais + validos_novos >= target_count:
                    for f in pend:
                        f.cancel()
                    break
                while next_i < total and len(pend) < window:
                    item, proto = raw[next_i]
                    pend[ex.submit(validar_proxy_rapido, item, proto)] = next_i
                    next_i += 1
    except Exception as e:
        debug_log("validar_proxies_ate_target: " + traceback.format_exc())
        dizer("  AVISO: validacao interrompida (" + str(e)[:120] + ")")
    GLOBAL["scraping_checkpoint"] = next_i
    com_pool = len(GLOBAL["pool"])
    CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
    dizer("  Validacao terminada: + " + str(validos_novos) + " | pool agora " +
          str(com_pool) + "/" + str(target_count))
    if com_pool > 0:
        GLOBAL["spf_sujo"] = True
    return validos_novos

def revalidar_pool_recuperado():
    with GLOBAL["pool_lock"]:
        pool = list(GLOBAL["pool"])
    if not pool:
        return 0
    dizer("  Revalidando " + str(len(pool)) + " proxies recuperados do checkpoint...")
    def _testar(p):
        if GLOBAL.get("emergency_stop"):
            return False
        sock = None
        try:
            with GLOBAL_CONNECTION_SEMAPHORE:
                sock = _socket_proxy(p["ip"], p["port"], p.get("proto", "socks5"),
                                     MX_TESTE_VALIDACAO, 25, PORT25_TIMEOUT)
                banner = sock.recv(4096).decode(errors="ignore").strip()
                try:
                    sock.close()
                except Exception:
                    pass
                sock = None
                return banner.startswith("220")
        except Exception:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
            return False
    vivos = []
    try:
        trabalhadores = max(4, min(32, len(pool)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
            resultados = list(ex.map(_testar, pool))
            for p, vivo in zip(pool, resultados):
                if vivo:
                    vivos.append(p)
    except Exception as e:
        debug_log("revalidar_pool_recuperado: " + traceback.format_exc())
        vivos = pool
    mortos = len(pool) - len(vivos)
    with GLOBAL["pool_lock"]:
        GLOBAL["pool"] = vivos
    if mortos > 0:
        GLOBAL["spf_sujo"] = True
        with GLOBAL["stats_lock"]:
            GLOBAL["proxies_mortos_count"] += mortos
    dizer("  Pool recuperado: " + str(len(vivos)) + " vivos, " + str(mortos) + " mortos removidos")
    CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
    return len(vivos)

def carregar_proxy_state():
    if os.path.exists(PROXY_STATE_FILE):
        try:
            with open(PROXY_STATE_FILE, "r", encoding="utf-8") as f:
                dados = json.load(f)
            if isinstance(dados, dict):
                return dados
        except Exception:
            pass
    return {}

def contar_banidos_no_state(estado):
    n = 0
    try:
        for _pk, provedores in estado.items():
            if isinstance(provedores, dict):
                for _pv, st in provedores.items():
                    if isinstance(st, dict) and st.get("banido"):
                        n += 1
    except Exception:
        pass
    return n

def salvar_proxy_state(estado):
    try:
        garantir_diretorio_para(PROXY_STATE_FILE)
        with GLOBAL_FILEWRITE_SEMAPHORE:
            tmp = PROXY_STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(estado, f, indent=2)
            os.replace(tmp, PROXY_STATE_FILE)
    except Exception as e:
        debug_log("salvar_proxy_state: " + str(e))

def proxy_pode_usar(proxy_key, provedor):
    with GLOBAL["proxy_state_lock"]:
        estado = GLOBAL["proxy_state"]
        if proxy_key not in estado:
            return True
        if provedor not in estado[proxy_key]:
            return True
        return not estado[proxy_key][provedor].get("banido", False)

def registrar_timeout(proxy_key, provedor):
    if not proxy_key or proxy_key == "colab":
        return
    with GLOBAL["proxy_state_lock"]:
        estado = GLOBAL["proxy_state"]
        if proxy_key not in estado:
            estado[proxy_key] = {}
        if provedor not in estado[proxy_key]:
            estado[proxy_key][provedor] = {"timeouts_consecutivos": 0, "banido": False}
        estado[proxy_key][provedor]["timeouts_consecutivos"] += 1
        if estado[proxy_key][provedor]["timeouts_consecutivos"] >= MAX_TIMEOUTS_BAN:
            if not estado[proxy_key][provedor].get("banido"):
                estado[proxy_key][provedor]["banido"] = True
                estado[proxy_key][provedor]["banido_em"] = datetime.now().isoformat()
                with GLOBAL["stats_lock"]:
                    GLOBAL["proxies_banidos"] += 1
        salvar_proxy_state(estado)

def registrar_sucesso(proxy_key, provedor):
    if not proxy_key or proxy_key == "colab":
        return
    with GLOBAL["proxy_state_lock"]:
        estado = GLOBAL["proxy_state"]
        if proxy_key in estado and provedor in estado[proxy_key]:
            if estado[proxy_key][provedor].get("timeouts_consecutivos", 0) != 0:
                estado[proxy_key][provedor]["timeouts_consecutivos"] = 0
                salvar_proxy_state(estado)

def registrar_falha_global_proxy(proxy_key):
    if not proxy_key or proxy_key == "colab":
        return
    remover = False
    with GLOBAL["proxy_state_lock"]:
        contagens = GLOBAL.setdefault("proxy_fail_counts", {})
        contagens[proxy_key] = contagens.get(proxy_key, 0) + 1
        if contagens[proxy_key] >= PROXY_GLOBAL_FAIL_REMOVE:
            remover = True
            contagens.pop(proxy_key, None)
    if remover:
        with GLOBAL["pool_lock"]:
            antes = len(GLOBAL["pool"])
            GLOBAL["pool"] = [p for p in GLOBAL["pool"]
                              if (str(p.get("ip")) + ":" + str(p.get("port"))) != proxy_key]
            removidos = antes - len(GLOBAL["pool"])
            if removidos > 0:
                with GLOBAL["stats_lock"]:
                    GLOBAL["proxies_mortos_count"] += removidos
                GLOBAL["spf_sujo"] = True
                smtp_log("PROXY " + str(proxy_key) + " REMOVIDO DO POOL")
                dizer("  [PROXY] Removido proxy morto " + proxy_key + " do pool")

def registrar_sucesso_global_proxy(proxy_key):
    if not proxy_key or proxy_key == "colab":
        return
    with GLOBAL["proxy_state_lock"]:
        contagens = GLOBAL.setdefault("proxy_fail_counts", {})
        if proxy_key in contagens:
            contagens[proxy_key] = max(0, contagens.get(proxy_key, 0) - 2)
            if contagens[proxy_key] == 0:
                contagens.pop(proxy_key, None)

def registrar_falha_direta():
    with GLOBAL["stats_lock"]:
        GLOBAL["colab_direct_fails"] = GLOBAL.get("colab_direct_fails", 0) + 1
        if (not GLOBAL.get("colab_port25_blocked") and
                GLOBAL["colab_direct_fails"] >= DIRECT_FAIL_BLOCK_THRESHOLD):
            GLOBAL["colab_port25_blocked"] = True
            dizer("  [REDE] Porta 25 de saida do Colab confirmada como BLOQUEADA")

def token_para_dominio(domain):
    with DOM_TOKEN_LOCK:
        if domain in DOM_TOKEN:
            return DOM_TOKEN[domain]
    for tok in DESEC_TOKENS_VALIDOS:
        if GLOBAL.get("emergency_stop"):
            return None
        try:
            r = requests.get(
                "https://desec.io/api/v1/domains/" + domain + "/",
                headers={"Authorization": "Token " + tok},
                timeout=15
            )
            if r.status_code == 200:
                try:
                    dados = r.json()
                    min_ttl = int((dados or {}).get("minimum_ttl") or DESEC_TTL)
                except Exception:
                    min_ttl = DESEC_TTL
                with DOM_TOKEN_LOCK:
                    DOM_TOKEN[domain] = tok
                    DOM_MIN_TTL[domain] = min_ttl
                return tok
        except Exception:
            continue
    return None

def desec_throttle_dominio(domain):
    with DOM_ESCRITA_LOCK:
        ultima = DOM_ULTIMA_ESCRITA.get(domain, 0.0)
        agora = time.time()
        espera = DESEC_PER_DOMAIN_WRITE_INTERVAL - (agora - ultima)
        if espera > 0:
            time.sleep(espera)
        DOM_ULTIMA_ESCRITA[domain] = time.time()

def desec_get_com_retry(url, tok, domain, max_retries=4):
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            return None, "STOP"
        agora = time.time()
        espera_min = DESEC_MIN_DELAY - (agora - GLOBAL.get("desec_last_request", 0))
        if espera_min > 0:
            time.sleep(espera_min)
        inicio_req = time.time()
        try:
            with GLOBAL_DESEC_SEMAPHORE:
                r = requests.get(url, headers={"Authorization": "Token " + tok}, timeout=60)
                duracao_ms = (time.time() - inicio_req) * 1000
                GLOBAL["desec_last_request"] = time.time()
                if r.status_code == 200:
                    log_desec_request(domain=domain, url=url, method="GET",
                                      status_code=200, response_body="(lista de rrsets omitida)",
                                      headers={}, duration_ms=duracao_ms, attempt=tentativa + 1)
                    return r, None
                if r.status_code == 429:
                    try:
                        wait = int(r.headers.get("Retry-After", 30))
                    except Exception:
                        wait = 30
                    wait = min(wait, DESEC_MAX_WAIT_429)
                    dizer("  Rate limit deSEC em GET " + domain + ". A esperar " + str(wait) + "s...")
                    time.sleep(wait + 1)
                    continue
                if r.status_code in (500, 502, 503, 504):
                    time.sleep(5 * (tentativa + 1))
                    continue
                log_desec_request(domain=domain, url=url, method="GET",
                                  status_code=r.status_code, response_body=r.text,
                                  headers=dict(r.headers), duration_ms=duracao_ms, attempt=tentativa + 1)
                return None, str(r.status_code) + ": " + r.text[:150]
        except Exception as e:
            debug_log("desec_get " + url + " tentativa " + str(tentativa + 1) + ": " + str(e)[:140])
            time.sleep(3 * (tentativa + 1))
    return None, "MAX_RETRIES"

def _parse_next_link(headers):
    try:
        link = headers.get("Link", "") or ""
        for parte in link.split(","):
            if 'rel="next"' in parte:
                m = re.search(r'<([^>]+)>', parte)
                if m:
                    return m.group(1)
    except Exception:
        pass
    return None

def obter_rrsets_atuais(domain, tok):
    atuais = {}
    url = "https://desec.io/api/v1/domains/" + domain + "/rrsets/?cursor="
    paginas = 0
    while url and paginas < 60:
        if GLOBAL.get("emergency_stop"):
            return None
        r, erro = desec_get_com_retry(url, tok, domain)
        if r is None:
            return None
        try:
            dados = r.json()
        except Exception:
            return None
        if not isinstance(dados, list):
            return None
        for rs in dados:
            if isinstance(rs, dict):
                chave = (str(rs.get("subname", "")), str(rs.get("type", "")))
                atuais[chave] = rs
        paginas += 1
        url = _parse_next_link(r.headers)
    return atuais

def _normalizar_record(tipo, s):
    try:
        s = str(s).strip()
        if tipo == "TXT":
            s = re.sub(r'"\s*"', '', s)
            s = s.strip().strip('"')
            s = s.replace(" ", "").replace("\t", "")
            return s.lower()
        if tipo == "MX":
            s = s.lower().rstrip(".")
            s = re.sub(r"\s+", " ", s.strip())
            return s
        return s.lower().rstrip(".")
    except Exception:
        return str(s)

def records_iguais(tipo, atuais_recs, desejados_recs):
    try:
        a = sorted(_normalizar_record(tipo, x) for x in (atuais_recs or []))
        d = sorted(_normalizar_record(tipo, x) for x in (desejados_recs or []))
        return a == d
    except Exception:
        return False

def dividir_rrsets_por_tamanho(rrsets):
    chunks = []
    atual = []
    tamanho = 2
    for rs in rrsets:
        try:
            tam = len(json.dumps(rs, ensure_ascii=False).encode("utf-8")) + 1
        except Exception:
            tam = 3000
        if atual and (tamanho + tam > DESEC_MAX_BODY_BYTES or len(atual) >= DESEC_MAX_RRSETS_PER_REQ):
            chunks.append(atual)
            atual = []
            tamanho = 2
        atual.append(rs)
        tamanho += tam
    if atual:
        chunks.append(atual)
    return chunks

def desec_put_com_retry(url, headers, payload, domain, max_retries=5):
    payload_preview = None
    try:
        if isinstance(payload, list) and len(payload) > 0:
            amostra = payload[0]
            for rrset in payload:
                if isinstance(rrset, dict) and rrset.get("type") == "TXT":
                    amostra = rrset
                    break
            payload_preview = json.dumps(amostra, indent=2)[:2000]
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
                log_desec_request(
                    domain=domain, url=url, method="PUT",
                    status_code=r.status_code, response_body=r.text,
                    headers=dict(r.headers), duration_ms=duracao_ms,
                    attempt=tentativa + 1, payload_preview=payload_preview
                )
                if r.status_code in (200, 201, 204):
                    return True, r.status_code
                if r.status_code == 429:
                    try:
                        wait_hdr = int(r.headers.get("Retry-After", DESEC_BACKOFF_BASE))
                    except Exception:
                        wait_hdr = DESEC_BACKOFF_BASE
                    if wait_hdr > DESEC_MAX_WAIT_429:
                        dizer("  Rate limit LONGO em " + domain + ". Dominio rate-limited; a saltar.")
                        return False, "RATE_LIMIT_LONGO"
                    wait_calc = int(DESEC_BACKOFF_BASE * (DESEC_BACKOFF_MULTIPLIER ** tentativa))
                    wait = min(max(wait_hdr, wait_calc), DESEC_MAX_WAIT_429)
                    dizer("  Rate limit deSEC em " + domain + ". A esperar " + str(wait) + "s...")
                    time.sleep(wait + 1)
                    continue
                if r.status_code in (500, 502, 503, 504):
                    wait = 5 * (tentativa + 1)
                    dizer("  Erro de servidor deSEC em " + domain + ". A esperar " + str(wait) + "s...")
                    time.sleep(wait)
                    continue
                if r.status_code == 400:
                    dizer("  Erro 400 deSEC em " + domain + ": " + r.text[:250])
                    return False, "400: " + r.text[:250]
                if r.status_code == 404:
                    dizer("  Erro 404 deSEC em " + domain)
                    return False, "404"
                dizer("  Erro deSEC " + domain + ": " + str(r.status_code) + " - " + r.text[:150])
                return False, str(r.status_code)
        except Exception as e:
            debug_log("desec_put tentativa " + str(tentativa + 1) + " " + domain + ": " + str(e)[:160])
            if tentativa < max_retries - 1:
                wait = 3 * (tentativa + 1)
                time.sleep(wait)
                continue
            return False, "MAX_RETRIES"
    return False, "MAX_RETRIES"

def escrever_chunk_com_isolamento(url, headers, chunk, domain, profundidade=0):
    if not chunk:
        return 0, []
    if domain in GLOBAL.get("dominios_rate_limited", []):
        falhados = [(rs.get("subname"), rs.get("type"), "DOMAIN_RATE_LIMITED") for rs in chunk]
        return 0, falhados
    desec_throttle_dominio(domain)
    ok, status = desec_put_com_retry(url, headers, chunk, domain, max_retries=4)
    if ok:
        return len(chunk), []
    if isinstance(status, str) and status == "RATE_LIMIT_LONGO":
        with GLOBAL["stats_lock"]:
            rl = GLOBAL.setdefault("dominios_rate_limited", [])
            if domain not in rl:
                rl.append(domain)
        falhados = [(rs.get("subname"), rs.get("type"), "RATE_LIMIT_LONGO") for rs in chunk]
        return 0, falhados
    if isinstance(status, str) and status.startswith("400") and len(chunk) > 1 and profundidade < 6:
        meio = len(chunk) // 2
        dizer("    Chunk com 400 em " + domain + " - a isolar por bisseccao")
        ok1, falhas1 = escrever_chunk_com_isolamento(url, headers, chunk[:meio], domain, profundidade + 1)
        ok2, falhas2 = escrever_chunk_com_isolamento(url, headers, chunk[meio:], domain, profundidade + 1)
        return ok1 + ok2, falhas1 + falhas2
    falhados = [(rs.get("subname"), rs.get("type"), str(status)[:80]) for rs in chunk]
    return 0, falhados

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
    full_spf = " ".join(spf_parts)
    return _dividir_em_chunks_com_aspas(full_spf, 255)

def obter_ips_para_spf():
    ips = set()
    if GLOBAL.get("ip_colab"):
        if not GLOBAL.get("colab_spf_atualizado"):
            ips.add(GLOBAL["ip_colab"])
    with GLOBAL["pool_lock"]:
        for p in GLOBAL["pool"]:
            try:
                exit_ip = p.get("exit_ip")
                if exit_ip and ip_publico_valido(exit_ip):
                    ips.add(exit_ip)
            except Exception:
                continue
    lista = [i for i in ips if i and ip_publico_valido(i)]
    return lista[:MAX_SPF_IPS]

def construir_rrsets_completos(contas_do_dominio, spf_record, ttl=None):
    if ttl is None:
        ttl = DESEC_TTL
    rrsets = []
    vistos = set()
    dmarc_record = '"v=DMARC1; p=none; sp=none;"'
    ip_colab = GLOBAL.get("ip_colab") or "127.0.0.1"
    for c in contas_do_dominio:
        base = c["base"]
        domain = c["domain"]
        selector = c["selector"]
        pub = (c.get("public_key_txt") or "").replace('"', "").strip()
        if not pub:
            continue
        if not pub.startswith("v=DKIM1"):
            pub = ("v=DKIM1; k=rsa; p=" + pub) if "p=" in pub else ("v=DKIM1; k=rsa; " + pub)
        dkim_wrapped = _dividir_em_chunks_com_aspas(pub, 255)
        candidatos = [
            (base, "TXT", [spf_record]),
            (selector + "._domainkey." + base, "TXT", [dkim_wrapped]),
            ("_dmarc." + base, "TXT", [dmarc_record]),
            ("*." + base, "A", [ip_colab]),
            (base, "A", [ip_colab]),
            (base, "MX", ["10 " + base + "." + domain + "."]),
        ]
        for subname, tipo, records in candidatos:
            chave = (subname, tipo)
            if chave in vistos:
                continue
            vistos.add(chave)
            rrsets.append({
                "subname": subname, "type": tipo,
                "ttl": int(ttl), "records": records,
            })
    return rrsets

def fase3_dns_completo(contas):
    definir_fase("FASE 3 - DNS bulk por dominio com diff (SPF+DKIM+DMARC+A+MX)")
    ips_spf = obter_ips_para_spf()
    spf_record = montar_spf_record(ips_spf)
    dizer("  SPF com " + str(len(ips_spf)) + " IPs (proxies + Colab)")
    
    # V21.0.0: Bulk no domínio principal
    dominio_principal = GLOBAL.get('dominio_principal')
    if not dominio_principal:
        marcar_fase("FASE3_DNS", False, "sem domínio principal")
        return False
    
    tok = token_para_dominio(dominio_principal)
    if not tok:
        marcar_fase("FASE3_DNS", False, "sem token para " + dominio_principal)
        return False
    
    min_ttl = DOM_MIN_TTL.get(dominio_principal, DESEC_TTL)
    ttl_eff = max(DESEC_TTL, min_ttl)
    
    desejados = construir_rrsets_completos(contas, spf_record, ttl=ttl_eff)
    total_desejados = len(desejados)
    
    if total_desejados == 0:
        marcar_fase("FASE3_DNS", False, "sem rrsets")
        return False
    
    atuais = obter_rrsets_atuais(dominio_principal, tok)
    para_escrever = []
    confirmados = 0
    
    if atuais is None:
        para_escrever = desejados
    else:
        for rs in desejados:
            chave = (rs.get("subname", ""), rs.get("type", ""))
            atual = atuais.get(chave)
            if atual is not None and records_iguais(rs["type"], atual.get("records"), rs.get("records")):
                confirmados += 1
            else:
                para_escrever.append(rs)
    
    if not para_escrever:
        dizer("    " + dominio_principal + ": " + str(confirmados) + "/" + str(total_desejados) +
              " rrsets ja corretos - NADA a escrever")
        GLOBAL["dns_updated"] = len(contas)
        GLOBAL["ultimo_refresh_spf"] = time.time()
        GLOBAL["spf_sujo"] = False
        GLOBAL["spf_record_escrito"] = spf_record
        GLOBAL["spf_ips_escritos"] = list(ips_spf)
        GLOBAL["colab_spf_atualizado"] = True
        marcar_fase("FASE3_DNS", True, str(confirmados) + " rrsets confirmados")
        return True
    
    chunks = dividir_rrsets_por_tamanho(para_escrever)
    dizer("    " + dominio_principal + ": " + str(confirmados) + " ok, " + str(len(para_escrever)) +
          " rrsets a escrever em " + str(len(chunks)) + " chunk(s) bulk")
    
    url = "https://desec.io/api/v1/domains/" + dominio_principal + "/rrsets/"
    headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
    escritos_total = 0
    falhas = []
    
    for i, chunk in enumerate(chunks, 1):
        if GLOBAL.get("emergency_stop"):
            break
        ok_n, falhas_chunk = escrever_chunk_com_isolamento(url, headers, chunk, dominio_principal)
        escritos_total += ok_n
        falhas.extend(falhas_chunk)
        if ok_n > 0:
            dizer("    Chunk " + str(i) + "/" + str(len(chunks)) + ": " + str(ok_n) + " rrsets OK")
    
    confirmados_final = confirmados + escritos_total
    GLOBAL["dns_updated"] = len(contas) if escritos_total > 0 else confirmados // 6
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    GLOBAL["spf_record_escrito"] = spf_record
    GLOBAL["spf_ips_escritos"] = list(ips_spf)
    GLOBAL["colab_spf_atualizado"] = True
    
    dizer("  DNS: " + str(GLOBAL["dns_updated"]) + "/" + str(len(contas)) + " bases confirmadas | " +
          str(escritos_total) + " rrsets escritos | " + str(len(falhas)) + " falhas")
    
    marcar_fase("FASE3_DNS", confirmados_final > 0,
                str(confirmados_final) + "/" + str(total_desejados) + " rrsets OK")
    return confirmados_final > 0

def refresh_spf_bulk(motivo=""):
    ips_spf = obter_ips_para_spf()
    if not ips_spf:
        return
    spf_record = montar_spf_record(ips_spf)
    if spf_record == GLOBAL.get("spf_record_escrito"):
        GLOBAL["spf_sujo"] = False
        GLOBAL["ultimo_refresh_spf"] = time.time()
        dizer("  Refresh SPF (" + str(motivo) + "): SPF inalterado - 0 writes necessarios")
        return
    dizer("  Refresh SPF (" + str(motivo) + ") com " + str(len(ips_spf)) + " IPs")
    
    dominio_principal = GLOBAL.get('dominio_principal')
    if not dominio_principal:
        return
    
    tok = token_para_dominio(dominio_principal)
    if not tok:
        return
    
    min_ttl = DOM_MIN_TTL.get(dominio_principal, DESEC_TTL)
    ttl_eff = max(DESEC_TTL, min_ttl)
    
    rrsets = []
    vistos = set()
    for c in GLOBAL["contas"]:
        base = c["base"]
        if (base, "TXT") in vistos:
            continue
        vistos.add((base, "TXT"))
        rrsets.append({
            "subname": base, "type": "TXT",
            "ttl": int(ttl_eff), "records": [spf_record],
        })
    
    chunks = dividir_rrsets_por_tamanho(rrsets)
    url = "https://desec.io/api/v1/domains/" + dominio_principal + "/rrsets/"
    headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
    
    escritos_total = 0
    for chunk in chunks:
        ok_n, falhas = escrever_chunk_com_isolamento(url, headers, chunk, dominio_principal)
        escritos_total += ok_n
    
    GLOBAL["spf_record_escrito"] = spf_record
    GLOBAL["spf_ips_escritos"] = list(ips_spf)
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    dizer("  SPF actualizado: " + str(escritos_total) + " rrsets")

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
        if restante % 60 == 0 and restante > 0:
            dizer("  ... faltam " + str(restante) + "s")
    dizer("  Propagacao concluida")

def verificar_propagacao_amostra(contas, ip_colab):
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
    if isinstance(target_emails, list):
        email_ref = target_emails[0] if target_emails else ""
    else:
        email_ref = str(target_emails)
    h = mutar_html_abracadabra(html_base, conta, email_ref)
    msg = EmailMessage()
    msg['Subject'] = subject
    idx_conta = 0
    for i, c in enumerate(GLOBAL.get("contas", [])):
        if c.get("identity_id") == conta.get("identity_id"):
            idx_conta = i
            break
    display_name = obter_display_name(conta, idx_conta)
    msg['From'] = display_name + " <" + conta['sender_email'] + ">"
    if isinstance(target_emails, list):
        msg['To'] = target_emails[0] if target_emails else ""
    else:
        msg['To'] = str(target_emails)
    msg['Message-ID'] = make_msgid(domain=conta['domain'])
    msg['Date'] = format_datetime(datetime.now(timezone.utc))
    msg.set_content(h, subtype='html')
    b = msg.as_bytes()
    try:
        sig = dkim.sign(
            message=b,
            selector=conta['selector'].encode(),
            domain=conta['domain'].encode(),
            privkey=conta['private_key_pem'].encode(),
            include_headers=[b"from", b"to", b"subject", b"date", b"message-id"]
        )
        b = sig + b
    except Exception as e:
        raise RuntimeError("DKIM falhou para conta " + str(conta.get('identity_id')) + ": " + str(e))
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
        ctx.set_ciphers(
            "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:"
            "AES128-GCM-SHA256:AES256-GCM-SHA384:"
            "AES128-SHA256:AES256-SHA256:"
            "!aNULL:!eNULL:!MD5:!RC4:!DES:!3DES:!EXPORT:!PSK:!SRP:!DSS"
        )
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
    try:
        ctx.options |= ssl.OP_NO_RENEGOTIATION
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
    if 400 <= code < 500:
        throttle_keywords = [
            "too many", "rate limit", "try again later", "temporarily",
            "throttl", "limit exceeded", "too fast", "slow down",
            "please wait", "retry later", "greylist",
            "temporarily rejected", "please try again", "quota exceeded",
            "connection limit", "message limit", "recipient limit"
        ]
        if any(kw in msg_low for kw in throttle_keywords):
            return "THROTTLE", "Throttling detected"
        return "TEMPORARIA", "Falha temporaria"
    if 500 <= code < 600:
        if any(kw in msg_low for kw in ["user unknown", "no such user", "does not exist",
                                        "invalid recipient", "mailbox not found",
                                        "unknown user", "recipient rejected"]):
            return "USER_UNKNOWN", "Destinatario inexistente"
        if any(kw in msg_low for kw in ["spam", "blocked", "blacklist", "rbl",
                                        "rejected for policy", "not allowed", "reputation"]):
            return "REPUTACAO", "Problema de reputacao/politica"
        if any(kw in msg_low for kw in ["invalid message", "malformed", "syntax"]):
            return "INVALIDA", "Mensagem invalida"
        return "PERMANENTE", "Rejeicao permanente generica"
    return "DESCONHECIDA", "Codigo " + str(code)

class RetryQueue:
    def __init__(self):
        self.items = deque()
        self.tentativas = {}
        self.lock = threading.Lock()

    def adicionar(self, email, provedor, attempts=None):
        chave = (email, provedor)
        abandonar = False
        with self.lock:
            total = self.tentativas.get(chave, 0) + 1
            if total > MAX_RETRY_ATTEMPTS:
                self.tentativas.pop(chave, None)
                abandonar = True
            else:
                self.tentativas[chave] = total
                delay = RETRY_BACKOFF_BASE * (RETRY_BACKOFF_MULTIPLIER ** (total - 1))
                next_try = time.time() + delay
                self.items.append((email, provedor, total, next_try))
        if abandonar:
            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_abandonados"] += 1
            return False
        with GLOBAL["stats_lock"]:
            GLOBAL["smtp_retries"] += 1
        return True

    def obter_prontos_para_provedor(self, provedor, max_items=200):
        agora = time.time()
        prontos = []
        with self.lock:
            remaining = deque()
            for item in self.items:
                email, item_prov, attempts, next_try = item
                if item_prov == provedor and next_try <= agora and len(prontos) < max_items:
                    prontos.append((email, item_prov, attempts))
                else:
                    remaining.append(item)
            self.items = remaining
        return prontos

    def obter_prontos(self, max_items=100):
        agora = time.time()
        prontos = []
        with self.lock:
            remaining = deque()
            for item in self.items:
                email, provedor, attempts, next_try = item
                if next_try <= agora and len(prontos) < max_items:
                    prontos.append((email, provedor, attempts))
                else:
                    remaining.append(item)
            self.items = remaining
        return prontos

    def limpar(self, email, provedor):
        chave = (email, provedor)
        with self.lock:
            self.tentativas.pop(chave, None)

    def count(self):
        with self.lock:
            return len(self.items)

RETRY_QUEUE = RetryQueue()

def salvar_checkpoint_smtp():
    try:
        stats = {
            "smtp_tentativas": GLOBAL["smtp_tentativas"],
            "smtp_aceites": GLOBAL["smtp_aceites"],
            "smtp_rejeitados": GLOBAL["smtp_rejeitados"],
            "smtp_user_unknown": GLOBAL["smtp_user_unknown"],
            "smtp_erros": GLOBAL["smtp_erros"],
            "smtp_conexoes": GLOBAL["smtp_conexoes"],
            "smtp_via_proxy": GLOBAL["smtp_via_proxy"],
            "smtp_via_colab": GLOBAL["smtp_via_colab"],
            "smtp_4xx": GLOBAL.get("smtp_4xx", 0),
            "smtp_5xx": GLOBAL.get("smtp_5xx", 0),
            "smtp_retries": GLOBAL.get("smtp_retries", 0),
            "smtp_abandonados": GLOBAL.get("smtp_abandonados", 0),
        }
        CHECKPOINT.salvar_smtp_stats(stats)
        CHECKPOINT.salvar_por_provedor(GLOBAL.get("por_provedor", {}))
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]
    except Exception as e:
        debug_log("salvar_checkpoint_smtp: " + str(e))

def talvez_salvar_checkpoint():
    diff = GLOBAL["smtp_aceites"] - GLOBAL.get("smtp_aceites_ultima_checkpoint", 0)
    if diff >= CHECKPOINT_SAVE_INTERVAL:
        salvar_checkpoint_smtp()
        try:
            CHECKPOINT.salvar_unknowns(GLOBAL.get("user_unknowns", set()))
            CHECKPOINT.salvar_aceites(GLOBAL.get("aceites_emails", set()))
        except Exception as e:
            debug_log("talvez_salvar_checkpoint uk/ac: " + str(e))

# ==================== V21.0.0: ARQUITETURA ASSÍNCRONA ====================

class AsyncSMTPWorker:
    """Worker assíncrono para envio SMTP com regras de hibernação."""
    
    def __init__(self, worker_id, proxy_info, conta):
        self.worker_id = worker_id
        self.proxy_info = proxy_info  # None para worker Colab
        self.conta = conta
        self.enviados = 0
        self.timeouts_consecutivos = 0
        self.count_421 = 0
        self.ativo = True
        self.orcamento = {}  # {provedor: {"enviados": X, "limite": 500}}
        
    async def enviar_email(self, session, email, provedor, mx_list):
        """Envia um email via proxy/Colab com todas as validações."""
        if not self.ativo:
            return False, "hibernado"
        
        # Verificar orçamento por provedor
        if provedor not in self.orcamento:
            self.orcamento[provedor] = {"enviados": 0, "limite": MAX_EMAILS_IP_PROVEDOR}
        
        if self.orcamento[provedor]["enviados"] >= self.orcamento[provedor]["limite"]:
            return False, "orcamento_esgotado"
        
        # Selecionar MX
        mx_host = mx_list[0] if mx_list else None
        if not mx_host:
            return False, "sem_mx"
        
        # Conectar via proxy ou direto
        try:
            if self.proxy_info:
                # Via proxy - usar socket síncrono em thread
                sock = await asyncio.to_thread(
                    self._conectar_via_proxy, self.proxy_info, mx_host
                )
            else:
                # Via Colab direto
                sock = await asyncio.to_thread(
                    self._conectar_direto, mx_host
                )
            
            if not sock:
                self.timeouts_consecutivos += 1
                if self.timeouts_consecutivos >= MAX_TIMEOUTS_CONSECUTIVOS:
                    self.ativo = False
                    return False, "timeout_consecutivo"
                return False, "timeout"
            
            # Handshake SMTP
            hs_ok = await asyncio.to_thread(self._handshake_smtp, sock, mx_host)
            if not hs_ok:
                sock.close()
                self.timeouts_consecutivos += 1
                return False, "handshake_falhou"
            
            # MAIL FROM
            sock.sendall(("MAIL FROM:<" + self.conta['sender_email'] + ">\r\n").encode())
            mr = await asyncio.to_thread(recv_smtp, sock)
            if not mr or not mr[0].startswith("250"):
                sock.close()
                return False, "mail_from_rejeitado"
            
            # RCPT TO com aprendizado dinâmico
            rcpt_inicial = RCPT_LIMITS.get(provedor, RCPT_DEFAULT)
            with GLOBAL["rcpt_lock"]:
                rcpt_atual = GLOBAL["rcpt_aprendido"].get(provedor, rcpt_inicial)
            
            sock.sendall(("RCPT TO:<" + email + ">\r\n").encode())
            rr = await asyncio.to_thread(recv_smtp, sock)
            
            if not rr:
                sock.close()
                return False, "rcpt_sem_resposta"
            
            smtp_code = 0
            try:
                smtp_code = int(rr[0][:3])
            except Exception:
                pass
            
            smtp_message = " ".join(rr)
            classe, motivo = classificar_resposta_smtp(smtp_code, smtp_message, provedor)
            
            if classe == "THROTTLE" or "too many" in smtp_message.lower():
                # Reduzir RCPT
                with GLOBAL["rcpt_lock"]:
                    rcpt_atual = max(rcpt_atual // 2, 1)
                    GLOBAL["rcpt_aprendido"][provedor] = rcpt_atual
                
                if "421" in str(smtp_code) or "too many conn" in smtp_message.lower():
                    self.count_421 += 1
                    if self.count_421 >= MAX_421_COUNT:
                        self.ativo = False
                        sock.close()
                        return False, "421_excessivo"
                    await asyncio.sleep(60)
                
                sock.close()
                return False, "throttle"
            
            if not rr[0].startswith("250"):
                sock.close()
                if classe == "USER_UNKNOWN":
                    salvar_unknown(email, provedor)
                return False, classe
            
            # DATA
            sock.sendall(b"DATA\r\n")
            dr = await asyncio.to_thread(recv_smtp, sock)
            if not dr or not dr[0].startswith("354"):
                sock.close()
                return False, "data_rejeitado"
            
            # Payload com DKIM
            try:
                payload = aplicar_mutacao_abracadabra(None, self.conta, [email], CAMPAIGN["subject"])
            except RuntimeError as e:
                sock.close()
                return False, "dkim_falhou"
            
            if not payload.endswith(b"\r\n"):
                payload += b"\r\n"
            payload = payload.replace(b"\r\n.", b"\r\n..")
            
            sock.sendall(payload)
            sock.sendall(b"\r\n.\r\n")
            
            fr = await asyncio.to_thread(recv_smtp, sock, 30)
            
            try:
                sock.sendall(b"QUIT\r\n")
            except Exception:
                pass
            sock.close()
            
            if fr and fr[0].startswith("250"):
                # Sucesso!
                self.enviados += 1
                self.timeouts_consecutivos = 0
                self.orcamento[provedor]["enviados"] += 1
                
                # Aumentar RCPT se sucesso consecutivo
                with GLOBAL["rcpt_lock"]:
                    rcpt_atual = min(rcpt_atual + 5, rcpt_inicial * 2)
                    GLOBAL["rcpt_aprendido"][provedor] = rcpt_atual
                
                return True, "250 OK"
            else:
                return False, "final_rejeitado"
                
        except Exception as e:
            debug_log("worker " + str(self.worker_id) + " erro: " + str(e))
            self.timeouts_consecutivos += 1
            return False, "excecao"
    
    def _conectar_via_proxy(self, proxy_info, mx_host):
        """Conecta via proxy SOCKS."""
        try:
            sock = _socket_proxy(
                proxy_info["ip"], proxy_info["port"], proxy_info["proto"],
                mx_host, 25, get_timeout(mx_host.split(".")[-2] if "." in mx_host else "")
            )
            banner = sock.recv(4096).decode(errors="ignore").strip()
            if banner.startswith("220"):
                return sock
            sock.close()
        except Exception:
            pass
        return None
    
    def _conectar_direto(self, mx_host):
        """Conecta diretamente via Colab."""
        try:
            sock = socket.create_connection((mx_host, 25), timeout=SMTP_TIMEOUT)
            banner = sock.recv(4096).decode(errors="ignore").strip()
            if banner.startswith("220"):
                return sock
            sock.close()
        except Exception:
            pass
        return None
    
    def _handshake_smtp(self, sock, mx_host):
        """Faz EHLO + STARTTLS."""
        try:
            sock.sendall(("EHLO " + self.conta['identity_id'] + "\r\n").encode())
            ehlo_resp = recv_smtp(sock)
            ehlo_text = "\n".join(ehlo_resp) if ehlo_resp else ""
            
            if "STARTTLS" in ehlo_text.upper():
                sock.sendall(b"STARTTLS\r\n")
                tls_resp = recv_smtp(sock)
                if tls_resp and tls_resp[0].startswith("220"):
                    ctx = criar_contexto_tls()
                    sock = ctx.wrap_socket(sock, server_hostname=mx_host)
                    sock.sendall(("EHLO " + self.conta['identity_id'] + "\r\n").encode())
                    recv_smtp(sock)
            return True
        except Exception:
            return False

async def fase6_envio_massivo_async(mxs, contas, html_base):
    """Fase 6 assíncrona com 501 workers fixos."""
    definir_fase("FASE 6 - Envio massivo ASSÍNCRONO (501 workers fixos)")
    
    faltam = fases_faltantes_para_envio()
    if faltam:
        dizer("  FASE 6 BLOQUEADA - fases obrigatorias incompletas: " + ", ".join(faltam))
        marcar_fase("FASE6_ENVIO", False, "bloqueada por: " + ", ".join(faltam))
        return
    
    if not contas:
        dizer("  FASE 6 BLOQUEADA - sem contas DKIM")
        marcar_fase("FASE6_ENVIO", False, "sem contas")
        return
    
    # Criar workers
    workers = []
    pool = list(GLOBAL["pool"])
    
    # Workers 1-500: proxies
    for i, proxy in enumerate(pool[:500]):
        if i < len(contas):
            workers.append(AsyncSMTPWorker(i + 1, proxy, contas[i]))
    
    # Worker 501: Colab (paralelo, não fallback)
    if len(contas) > 500:
        workers.append(AsyncSMTPWorker(501, None, contas[500]))
    elif len(contas) > len(workers):
        workers.append(AsyncSMTPWorker(len(workers) + 1, None, contas[len(workers)]))
    
    dizer("  Workers criados: " + str(len(workers)) + " (" + str(len(pool)) + " proxies + 1 Colab)")
    
    # Carregar emails na queue
    email_queue = asyncio.Queue()
    tarefas = []
    for prov, mx_list in mxs.items():
        if not mx_list:
            continue
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            cp_info = CHECKPOINT.obter_provedor_index(prov)
            if cp_info and cp_info.get("concluido", False):
                continue
            tarefas.append((prov, mx_list, path))
    
    # Carregar emails na queue
    emails_carregados = 0
    for prov, mx_list, path in tarefas:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    email = linha.strip().lower()
                    if email and "@" in email:
                        with GLOBAL["user_unknowns_lock"]:
                            if email not in GLOBAL["user_unknowns"]:
                                await email_queue.put((email, prov, mx_list))
                                emails_carregados += 1
        except Exception as e:
            debug_log("carregar emails " + prov + ": " + str(e))
    
    dizer("  Emails na queue: " + "{:,}".format(emails_carregados))
    
    if emails_carregados == 0:
        marcar_fase("FASE6_ENVIO", False, "0 emails pendentes")
        return
    
    marcar_fase("FASE6_ENVIO_INICIO", True, str(len(tarefas)) + " provedores, " + str(len(workers)) + " workers")
    
    # Loop de envio
    async with aiohttp.ClientSession() as session:
        tasks_worker = []
        for worker in workers:
            tasks_worker.append(asyncio.create_task(
                _worker_loop(worker, email_queue, session)
            ))
        
        # Aguardar todos os workers
        await asyncio.gather(*tasks_worker, return_exceptions=True)
    
    dizer("  Fase 6 concluida")
    marcar_fase("FASE6_ENVIO", True, str(GLOBAL['smtp_aceites']) + " aceites")

async def _worker_loop(worker, email_queue, session):
    """Loop individual de cada worker."""
    while not GLOBAL.get("emergency_stop"):
        if not worker.ativo:
            await asyncio.sleep(10)
            continue
        
        try:
            email, provedor, mx_list = await asyncio.wait_for(email_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break
        
        # Rate control
        pode, espera = RATE_CTRL.pode_enviar(worker.conta["identity_id"], provedor)
        if not pode and espera > 0:
            await asyncio.sleep(min(espera, 30.0))
        
        # Enviar
        sucesso, motivo = await worker.enviar_email(session, email, provedor, mx_list)
        
        if sucesso:
            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_tentativas"] += 1
                GLOBAL["smtp_aceites"] += 1
                GLOBAL["smtp_conexoes"] += 1
                if worker.proxy_info:
                    GLOBAL["smtp_via_proxy"] += 1
                else:
                    GLOBAL["smtp_via_colab"] += 1
            
            RATE_CTRL.registar_sucesso(worker.conta["identity_id"], provedor)
            registrar_aceite(email, provedor, worker.conta.get("identity_id", ""), mx_list[0] if mx_list else "")
            talvez_salvar_checkpoint()
            
            if worker.proxy_info:
                proxy_key = str(worker.proxy_info['ip']) + ":" + str(worker.proxy_info['port'])
                registrar_sucesso(proxy_key, provedor)
                registrar_sucesso_global_proxy(proxy_key)
        else:
            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_tentativas"] += 1
                if motivo == "USER_UNKNOWN":
                    GLOBAL["smtp_user_unknown"] += 1
                elif motivo in ("THROTTLE", "TEMPORARIA"):
                    GLOBAL["smtp_4xx"] += 1
                elif motivo in ("PERMANENTE", "REPUTACAO"):
                    GLOBAL["smtp_5xx"] += 1
                else:
                    GLOBAL["smtp_erros"] += 1
            
            if motivo in ("THROTTLE", "TEMPORARIA"):
                RETRY_QUEUE.adicionar(email, provedor)
                RATE_CTRL.registar_erro_temporario(worker.conta["identity_id"], provedor, 400)
            elif motivo in ("PERMANENTE", "REPUTACAO"):
                RATE_CTRL.registar_erro_permanente(worker.conta["identity_id"], provedor, 500)
                registrar_rejeicao(email, provedor, 500, motivo)
            
            if worker.proxy_info:
                proxy_key = str(worker.proxy_info['ip']) + ":" + str(worker.proxy_info['port'])
                if motivo in ("timeout", "timeout_consecutivo"):
                    registrar_timeout(proxy_key, provedor)
                registrar_falha_global_proxy(proxy_key)

def gerar_dashboard_3d_html(rt):
    """Gera dashboard 3D HTML profissional."""
    el = time.time() - GLOBAL["start_time"]
    h, m, s = int(el // 3600), int((el % 3600) // 60), int(el % 60)
    taxa = rt['aceites'] / max(1, rt['tentados']) * 100
    
    linhas_prov = ""
    try:
        for p, r in sorted(rt['por_provedor'].items())[:20]:
            taxa_p = r['aceites'] / max(1, r['tentados']) * 100
            linhas_prov += "<tr><td>" + str(p) + "</td>"
            linhas_prov += "<td class='ok'>" + "{:,}".format(r['aceites']) + "</td>"
            linhas_prov += "<td>" + "{:,}".format(r['tentados']) + "</td>"
            linhas_prov += "<td>" + "{:,}".format(r['rejeitados']) + "</td>"
            linhas_prov += "<td>" + "{:.1f}%</td></tr>".format(taxa_p)
    except Exception:
        linhas_prov = "<tr><td colspan='5'>sem dados</td></tr>"
    
    html = """<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VERISCOPE V21.0.0 Dashboard</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: #fff;
    min-height: 100vh;
    padding: 20px;
}
.container { max-width: 1400px; margin: 0 auto; }
h1 {
    text-align: center;
    font-size: 2.5em;
    margin-bottom: 30px;
    background: linear-gradient(90deg, #00d4ff, #00ff88);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-shadow: 0 0 30px rgba(0, 212, 255, 0.5);
}
.status-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: rgba(255,255,255,0.1);
    border-radius: 15px;
    padding: 15px 25px;
    margin-bottom: 30px;
    backdrop-filter: blur(10px);
}
.status-badge {
    padding: 8px 20px;
    border-radius: 20px;
    font-weight: bold;
    font-size: 1.1em;
}
.status-running { background: #00ff88; color: #000; }
.status-completed { background: #00d4ff; color: #000; }
.status-error { background: #ff4444; color: #fff; }
.grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
    margin-bottom: 30px;
}
.card {
    background: rgba(255,255,255,0.08);
    border-radius: 20px;
    padding: 25px;
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,0.1);
    transition: transform 0.3s, box-shadow 0.3s;
}
.card:hover {
    transform: translateY(-5px);
    box-shadow: 0 10px 40px rgba(0, 212, 255, 0.3);
}
.card-title {
    font-size: 0.9em;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 10px;
}
.card-value {
    font-size: 2.5em;
    font-weight: bold;
    background: linear-gradient(90deg, #00d4ff, #00ff88);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.card-subtitle {
    font-size: 0.85em;
    color: #aaa;
    margin-top: 5px;
}
.cube-container {
    perspective: 1000px;
    width: 200px;
    height: 200px;
    margin: 30px auto;
}
.cube {
    width: 100%;
    height: 100%;
    position: relative;
    transform-style: preserve-3d;
    animation: rotate 20s infinite linear;
}
@keyframes rotate {
    from { transform: rotateX(0) rotateY(0); }
    to { transform: rotateX(360deg) rotateY(360deg); }
}
.face {
    position: absolute;
    width: 200px;
    height: 200px;
    background: rgba(0, 212, 255, 0.2);
    border: 2px solid #00d4ff;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    font-size: 1.2em;
    font-weight: bold;
    backdrop-filter: blur(5px);
}
.face-label { font-size: 0.7em; color: #888; margin-bottom: 5px; }
.face-value { color: #00ff88; }
.front { transform: translateZ(100px); }
.back { transform: rotateY(180deg) translateZ(100px); }
.right { transform: rotateY(90deg) translateZ(100px); }
.left { transform: rotateY(-90deg) translateZ(100px); }
.top { transform: rotateX(90deg) translateZ(100px); }
.bottom { transform: rotateX(-90deg) translateZ(100px); }
table {
    width: 100%;
    border-collapse: collapse;
    background: rgba(255,255,255,0.05);
    border-radius: 15px;
    overflow: hidden;
}
th, td {
    padding: 12px 15px;
    text-align: left;
    border-bottom: 1px solid rgba(255,255,255,0.1);
}
th {
    background: rgba(0, 212, 255, 0.2);
    font-weight: 600;
}
tr:hover { background: rgba(255,255,255,0.05); }
.ok { color: #00ff88; font-weight: bold; }
.error { color: #ff4444; }
.section-title {
    font-size: 1.5em;
    margin: 30px 0 20px;
    padding-left: 15px;
    border-left: 4px solid #00d4ff;
}
.progress-bar {
    width: 100%;
    height: 10px;
    background: rgba(255,255,255,0.1);
    border-radius: 5px;
    overflow: hidden;
    margin-top: 10px;
}
.progress-fill {
    height: 100%;
    background: linear-gradient(90deg, #00d4ff, #00ff88);
    border-radius: 5px;
    transition: width 0.5s;
}
.footer {
    text-align: center;
    margin-top: 40px;
    padding: 20px;
    color: #666;
    font-size: 0.9em;
}
</style>
</head>
<body>
<div class="container">
    <h1>🚀 VERISCOPE V21.0.0</h1>
    
    <div class="status-bar">
        <div>
            <strong>Campaign ID:</strong> """ + str(GLOBAL.get('exec_id', 'N/A')) + """
        </div>
        <div>
            <strong>Tempo:</strong> """ + "{:02d}:{:02d}:{:02d}".format(h, m, s) + """
        </div>
        <div class="status-badge """ + ("status-running" if GLOBAL.get('status') == 'RUNNING' else "status-completed") + """">
            """ + str(GLOBAL.get('status', 'UNKNOWN')) + """
        </div>
    </div>
    
    <div class="cube-container">
        <div class="cube">
            <div class="face front">
                <span class="face-label">Aceites</span>
                <span class="face-value">""" + "{:,}".format(rt['aceites']) + """</span>
            </div>
            <div class="face back">
                <span class="face-label">Tentativas</span>
                <span class="face-value">""" + "{:,}".format(rt['tentados']) + """</span>
            </div>
            <div class="face right">
                <span class="face-label">Taxa</span>
                <span class="face-value">""" + "{:.1f}%".format(taxa) + """</span>
            </div>
            <div class="face left">
                <span class="face-label">Proxies</span>
                <span class="face-value">""" + str(len(GLOBAL.get('pool', []))) + """</span>
            </div>
            <div class="face top">
                <span class="face-label">Unknown</span>
                <span class="face-value">""" + "{:,}".format(len(GLOBAL.get('user_unknowns', set()))) + """</span>
            </div>
            <div class="face bottom">
                <span class="face-label">Workers</span>
                <span class="face-value">501</span>
            </div>
        </div>
    </div>
    
    <div class="grid">
        <div class="card">
            <div class="card-title">Aceites (250 OK)</div>
            <div class="card-value">""" + "{:,}".format(rt['aceites']) + """</div>
            <div class="card-subtitle">""" + "{:,}".format(rt.get('via_proxy', 0)) + """ via proxy, """ + "{:,}".format(rt.get('via_colab', 0)) + """ via Colab</div>
        </div>
        <div class="card">
            <div class="card-title">Tentativas</div>
            <div class="card-value">""" + "{:,}".format(rt['tentados']) + """</div>
            <div class="card-subtitle">""" + "{:,}".format(GLOBAL.get('smtp_conexoes', 0)) + """ conexões SMTP</div>
        </div>
        <div class="card">
            <div class="card-title">Taxa de Sucesso</div>
            <div class="card-value">""" + "{:.2f}%".format(taxa) + """</div>
            <div class="progress-bar"><div class="progress-fill" style="width: """ + str(min(taxa, 100)) + """%"></div></div>
        </div>
        <div class="card">
            <div class="card-title">Rejeitados</div>
            <div class="card-value">""" + "{:,}".format(rt['rejeitados']) + """</div>
            <div class="card-subtitle">4xx: """ + "{:,}".format(GLOBAL.get('smtp_4xx', 0)) + """ | 5xx: """ + "{:,}".format(GLOBAL.get('smtp_5xx', 0)) + """</div>
        </div>
        <div class="card">
            <div class="card-title">USER_UNKNOWN</div>
            <div class="card-value">""" + "{:,}".format(len(GLOBAL.get('user_unknowns', set()))) + """</div>
            <div class="card-subtitle">""" + "{:,}".format(GLOBAL.get('smtp_user_unknown', 0)) + """ nesta run</div>
        </div>
        <div class="card">
            <div class="card-title">Retries</div>
            <div class="card-value">""" + "{:,}".format(GLOBAL.get('smtp_retries', 0)) + """</div>
            <div class="card-subtitle">""" + "{:,}".format(GLOBAL.get('smtp_abandonados', 0)) + """ abandonados</div>
        </div>
    </div>
    
    <h2 class="section-title">📊 Por Provedor (Top 20)</h2>
    <table>
        <thead>
            <tr>
                <th>Provedor</th>
                <th>Aceites</th>
                <th>Tentados</th>
                <th>Rejeitados</th>
                <th>Taxa</th>
            </tr>
        </thead>
        <tbody>
            """ + linhas_prov + """
        </tbody>
    </table>
    
    <div class="footer">
        <p>VERISCOPE V21.0.0 | Assíncrono 501 Workers | Gerado em """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
        <p>IP Colab: """ + str(GLOBAL.get('ip_colab', 'N/A')) + """ | Domínio: """ + str(GLOBAL.get('dominio_principal', 'N/A')) + """</p>
    </div>
</div>
</body>
</html>"""
    
    try:
        with open(DASHBOARD_3D_FILE, "w", encoding="utf-8") as f:
            f.write(html)
        dizer("  Dashboard 3D gerado: " + DASHBOARD_3D_FILE)
    except Exception as e:
        debug_log("gerar_dashboard_3d: " + str(e))

def salvar_logs_hf():
    if GLOBAL.get("logs_uploaded"):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        enviados = 0
        for fname, fpath in [("log", LOG_FILE), ("smtp", SMTP_LOG), ("debug", DEBUG_LOG), ("desec", DESEC_LOG)]:
            if not fpath or not os.path.exists(fpath):
                continue
            try:
                api.upload_file(
                    path_or_fileobj=fpath,
                    path_in_repo="logs/" + fname + "_" + str(GLOBAL['exec_id']) + ".txt",
                    repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN,
                )
                enviados += 1
            except Exception as e:
                debug_log("upload log " + fname + ": " + str(e)[:160])
        GLOBAL["logs_uploaded"] = True
    except Exception as e:
        debug_log("salvar_logs_hf: " + str(e))

def baixar_ficheiros_campanha():
    try:
        local_proxies = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/proxies.txt",
            repo_type="dataset", token=HF_TOKEN, force_download=True,
        )
        os.makedirs(DIR_PROXIES, exist_ok=True)
        shutil.copy(local_proxies, os.path.join(DIR_PROXIES, "proxies.txt"))
        local_mailfrom = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/" + MAIL_FROM_FILE_LOCAL,
            repo_type="dataset", token=HF_TOKEN, force_download=True,
        )
        garantir_diretorio_para(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
        shutil.copy(local_mailfrom, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
    except Exception as e:
        debug_log("baixar_ficheiros_campanha: " + str(e)[:100])

def monitor_proxies_continuo():
    dizer("[MONITOR] Thread de monitoramento de proxies iniciada")
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        if GLOBAL.get("scraping_inicial_feito"):
            break
        time.sleep(10)
    dizer("[MONITOR] Scraping inicial confirmado - monitor activo")
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        time.sleep(60)
        with GLOBAL["pool_lock"]:
            pool_atual = len(GLOBAL["pool"])
        if pool_atual >= MIN_PROXIES_REFILL:
            decorrido_refresh = time.time() - GLOBAL.get("ultimo_refresh_spf", 0)
            if (GLOBAL.get("spf_sujo") and
                    decorrido_refresh >= SPF_REFRESH_INTERVALO_MINIMO and
                    fase_ja_completa("FASE3_DNS")):
                try:
                    refresh_spf_bulk("agendado")
                except Exception as e:
                    debug_log("refresh SPF agendado: " + str(e))
            continue
        if not GLOBAL["scrape_lock"].acquire(blocking=False):
            continue
        try:
            with GLOBAL["pool_lock"]:
                pool_atual = len(GLOBAL["pool"])
            if pool_atual >= MIN_PROXIES_REFILL:
                continue
            agora = time.time()
            if (agora - GLOBAL.get("ultimo_rescrape", 0)) >= RESCRAPE_INTERVALO_MINIMO:
                GLOBAL["ultimo_rescrape"] = agora
                try:
                    carregar_e_scrapear_proxies(motivo="refill-monitor")
                except Exception as e:
                    debug_log("rescrape monitor: " + str(e))
            try:
                novos = validar_proxies_ate_target(TARGET_VALID_PROXIES)
                if novos > 0 and fase_ja_completa("FASE3_DNS"):
                    GLOBAL["spf_sujo"] = True
            except Exception as e:
                debug_log("refill monitor: " + str(e))
        except Exception as e:
            debug_log("monitor_proxies_continuo: " + str(e))
        finally:
            try:
                GLOBAL["scrape_lock"].release()
            except Exception:
                pass

def aguardar_pool_minimo(minimo, timeout_seg=900):
    definir_fase("FASE 5 - Aguardar pool minimo de proxies (" + str(minimo) + ")")
    inicio = time.time()
    while time.time() - inicio < timeout_seg:
        if GLOBAL.get("emergency_stop"):
            return False
        with GLOBAL["pool_lock"]:
            n = len(GLOBAL["pool"])
        if n >= minimo:
            dizer("  Pool pronto: " + str(n) + " proxies validos")
            return True
        dizer("  Pool " + str(n) + "/" + str(minimo) + " - a aguardar...")
        time.sleep(20)
    with GLOBAL["pool_lock"]:
        n = len(GLOBAL["pool"])
    dizer("  AVISO: timeout a aguardar pool (tem " + str(n) + ").")
    return n > 0

async def main():
    global DESEC_TOKENS_VALIDOS, LOG_FILE, SMTP_LOG, DEBUG_LOG
    diagnostico_ambiente_hf()
    execution_id = reset_global_state()
    garantir_diretorios()
    n_baixados = download_all_checkpoints_from_hf()
    CHECKPOINT._carregar_tudo()
    config = CHECKPOINT.obter_config()
    
    if config.get("status") == "COMPLETED":
        print("[FIM] A campanha já foi totalmente concluída em execução anterior.", flush=True)
        sys.exit(0)
    
    tem_checkpoint = CHECKPOINT.existe_checkpoint()
    if tem_checkpoint:
        GLOBAL["modo_recuperacao"] = True
        GLOBAL["checkpoint_loaded"] = True
        GLOBAL["status"] = "RUNNING"
        if config.get("exec_id"):
            execution_id = config["exec_id"]
            GLOBAL["exec_id"] = execution_id
        if config.get("ip_colab"):
            GLOBAL["ip_colab"] = config["ip_colab"]
        # V21.0.0: NÃO recuperar start_time da campanha - cada run tem o seu relógio
        # campaign_started_at é preservado, mas start_time é o início desta run
        stats_antigos = CHECKPOINT.obter_smtp_stats()
        for k, v in stats_antigos.items():
            if k in GLOBAL:
                GLOBAL[k] = v
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]
        pool_antigo, scraping_cp, raw_count = CHECKPOINT.carregar_pool()
        GLOBAL["pool"] = pool_antigo
        GLOBAL["scraping_checkpoint"] = scraping_cp
        GLOBAL["por_provedor"] = CHECKPOINT.obter_por_provedor()
        por_prov_cp = CHECKPOINT.obter_por_provedor()
        for prov_nome, prov_info in por_prov_cp.items():
            if isinstance(prov_info, dict) and prov_info.get("concluido", False):
                GLOBAL["provedores_concluidos_set"].add(prov_nome)
        GLOBAL["provs_concluidos"] = len(GLOBAL["provedores_concluidos_set"])
        with GLOBAL["stats_lock"]:
            for chave, info in CHECKPOINT.states["fases"].items():
                GLOBAL["fases"][chave] = info
        try:
            unknowns_cp = CHECKPOINT.carregar_unknowns()
            if unknowns_cp:
                GLOBAL["user_unknowns"] = set(GLOBAL.get("user_unknowns", set())) | unknowns_cp
            aceites_cp = CHECKPOINT.carregar_aceites()
            if aceites_cp:
                GLOBAL["aceites_emails"] = set(GLOBAL.get("aceites_emails", set())) | aceites_cp
        except Exception as e:
            debug_log("recuperar unknowns/aceites do CP: " + str(e))
        if os.path.exists(SAIDA_ACEITES):
            try:
                with open(SAIDA_ACEITES, "r", encoding="utf-8", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row and row.get("email"):
                            GLOBAL["aceites_emails"].add(row["email"].lower().strip())
            except Exception as e:
                debug_log("recuperar aceites do ficheiro: " + str(e))
        limpar_logs_antigos(preservar_checkpoints=True, preservar_saida=True)
    else:
        limpar_logs_antigos(preservar_checkpoints=False, preservar_saida=False)
    
    LOG_FILE = os.path.join(LOG_DIR, "log_" + execution_id + ".txt")
    SMTP_LOG = os.path.join(LOG_DIR, "smtp_" + execution_id + ".txt")
    DEBUG_LOG = os.path.join(LOG_DIR, "debug_" + execution_id + ".txt")
    try:
        open(LOG_FILE, "w", encoding="utf-8").close()
        open(SMTP_LOG, "w", encoding="utf-8").close()
        open(DEBUG_LOG, "w", encoding="utf-8").close()
        open(DESEC_LOG, "w", encoding="utf-8").close()
    except Exception:
        pass
    
    GLOBAL["user_unknowns_file"] = UNKNOWN_FILE_LOCAL
    GLOBAL["proxy_state"] = carregar_proxy_state()
    GLOBAL["proxies_banidos"] = contar_banidos_no_state(GLOBAL["proxy_state"])
    
    threading.Thread(target=monitor_live, daemon=True).start()
    threading.Thread(target=keep_alive_thread, daemon=True).start()
    threading.Thread(target=time_limit_watcher, daemon=True).start()
    threading.Thread(target=periodic_checkpoint_thread, daemon=True).start()
    
    time.sleep(1)
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
        
        definir_fase("FASE 2D - Blacklist persistente de USER_UNKNOWN")
        baixar_unknowns_acumulado()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        
        definir_fase("FASE 2.5 - Proxies: leitura do disco + scraping + validacao")
        pool_ok = False
        if len(GLOBAL["pool"]) >= TARGET_VALID_PROXIES:
            vivos = revalidar_pool_recuperado()
            if vivos >= MIN_PROXIES_REFILL:
                pool_ok = True
                marcar_fase("FASE25_PROXIES", True, "pool recuperado e revalidado " + str(vivos))
        
        if not pool_ok:
            try:
                carregar_e_scrapear_proxies(motivo="inicial")
            except Exception as e:
                debug_log("scraping inicial: " + str(e))
            try:
                validar_proxies_ate_target(TARGET_VALID_PROXIES)
            except Exception as e:
                debug_log("validacao inicial: " + str(e))
        
        with GLOBAL["pool_lock"]:
            n_pool = len(GLOBAL["pool"])
        with GLOBAL["raw_lock"]:
            n_raw = len(GLOBAL["raw_proxies"])
        
        proxy_ok = (n_pool > 0) or (n_raw > 0)
        marcar_fase("FASE25_PROXIES", proxy_ok, "pool=" + str(n_pool) + " raw=" + "{:,}".format(n_raw))
        GLOBAL["scraping_inicial_feito"] = True
        GLOBAL["ultimo_rescrape"] = time.time()
        
        proxy_monitor_thread = threading.Thread(target=monitor_proxies_continuo, daemon=True)
        proxy_monitor_thread.start()
        
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        
        fase3_dns_completo(contas)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        
        fase4_esperar_propagacao()
        verificar_propagacao_amostra(contas, ip_colab)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        
        aguardar_pool_minimo(MIN_PROXIES_REFILL, timeout_seg=ESPERA_POOL_ANTES_ENVIO)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        
        # V21.0.0: Fase 6 assíncrona
        await fase6_envio_massivo_async(mxs, contas, html_base)
        
    except KeyboardInterrupt:
        GLOBAL["emergency_stop"] = True
    except Exception as e:
        debug_log("main:\n" + traceback.format_exc())
    finally:
        GLOBAL["monitor_running"] = False
        
        try:
            salvar_checkpoint_smtp()
            CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
            final_status = GLOBAL.get("status", "RUNNING")
            if final_status != "COMPLETED" and final_status != "TIME_LIMIT_SHUTDOWN":
                final_status = "RUNNING"
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status=final_status)
            CHECKPOINT.salvar_unknowns(GLOBAL["user_unknowns"])
            CHECKPOINT.salvar_aceites(GLOBAL["aceites_emails"])
            CHECKPOINT.salvar_tudo()
        except Exception as e:
            debug_log("checkpoint final: " + str(e))
        
        try:
            gerar_ficheiros_finais()
        except Exception as e:
            debug_log("gerar_ficheiros_finais: " + str(e))
        
        rt = {
            "aceites": 0, "tentados": 0, "rejeitados": 0, "erros": 0,
            "via_proxy": 0, "via_colab": 0,
            "por_provedor": GLOBAL["por_provedor"],
        }
        for r in GLOBAL["por_provedor"].values():
            for k in ("aceites", "tentados", "rejeitados", "erros", "via_proxy", "via_colab"):
                if k in r:
                    rt[k] += r[k]
        
        # Gerar dashboard 3D
        gerar_dashboard_3d_html(rt)
        
        # Upload final para HF
        try:
            upload_all_checkpoints_to_hf()
        except Exception as e:
            debug_log("upload final: " + str(e))
        
        try:
            upload_unknowns_hf()
        except Exception:
            pass
        
        try:
            salvar_logs_hf()
        except Exception:
            pass
        
        taxa = rt["aceites"] / max(1, rt["tentados"]) * 100
        print("EXECUCAO TERMINADA - " + str(GLOBAL.get("exec_id")), flush=True)
        print("  Aceites (250 OK) : " + "{:,}".format(rt["aceites"]), flush=True)
        print("  Tentativas         : " + "{:,}".format(rt["tentados"]), flush=True)
        print("  Taxa             : " + "{:.2f}".format(taxa) + "%", flush=True)
        print("  STATUS FINAL     : " + str(GLOBAL.get("status", "UNKNOWN")), flush=True)

if __name__ == "__main__":
    asyncio.run(main())
