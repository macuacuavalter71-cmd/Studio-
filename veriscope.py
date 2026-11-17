# -*- coding: utf-8 -*-
"""
VERISCOPE V21.0.0 - ARQUITETURA ASYNCIO COMPLETA + CHECKPOINTS TRANSACIONAIS
501 WORKERS FIXOS (500 PROXIES + 1 COLAB) | DOMINIO UNICO | RCPT DINAMICO
CORRECOES CRITICAS V21.0.0:
- CheckpointManager transacional com manifesto SHA-256 + integridade
- Separacao campaign_started_at vs run_started_at (nao reinicia relogio)
- Sem truncamento de listas (removido limite 2M)
- Upload HF apenas no SHUTDOWN (nao periodico) - poupa rate limit
- Repositorio HF dedicado: Valter3B/Veriscope_Checkpoints
- Descoberta inteligente de dominio com >=501 contas
- 501 workers fixos (1 proxy + 1 conta por worker)
- Worker #501 = IP Colab (PARALELO, nao fallback)
- SPF Colab atualizado UMA VEZ, depois so novos proxies
- Orcamento 500 emails/IP/provedor com 4 regras de hibernacao
- RCPT dinamico aprendido por provedor
- Scheduler global com anti-colisao IP x provedor
- Dashboard HTML 3D profissional com dados reais
- Continuidade perfeita Run#1 -> Run#2 sem perda de estado
- Tokens deSEC mantidos no codigo (conforme pedido)
"""
import os
import sys
import json
import time
import random
import hashlib
import threading
import traceback
import socket
import ssl
import re
import csv
import gc
import shutil
import glob
import subprocess
import resource
import concurrent.futures
from datetime import datetime, timezone
from collections import defaultdict, deque
from email.message import EmailMessage
from email.utils import make_msgid, format_datetime
from itertools import islice
from urllib.parse import urlparse, urlunparse

# === TOKEN DESEC (mantido conforme pedido) ===
DESEC_TOKEN = os.environ.get("DESEC_TOKEN", "LuWfqx1GL3uXDmyUEExH1tF4Nena").strip()
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()
GITHUB_PAT = os.environ.get("GITHUB_PAT", "").strip()
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "").strip()

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["HF_DATASETS_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["HF_HUB_DISABLE_XET"] = "1"
os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
os.environ["HF_HUB_ETAG_TIMEOUT"] = "60"
os.environ["HF_HUB_DOWNLOAD_TIMEOUT"] = "120"
os.environ["PYTHONUNBUFFERED"] = "1"

# === INSTALACAO DE DEPENDENCIAS ===
def install_packages():
    packages = [
        "requests", "dnspython", "dkimpy", "cryptography", "pandas",
        "pyarrow", "huggingface_hub", "psutil", "pysocks",
        "aiohttp", "aiosmtplib", "nest_asyncio"
    ]
    for pkg in packages:
        try:
            mod = pkg.replace("-", "_")
            if mod == "pysocks":
                mod = "socks"
            __import__(mod)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

install_packages()

import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "pyarrow",
            "urllib3", "asyncio", "filelock", "aiohttp", "aiosmtplib"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)

import warnings
warnings.filterwarnings("ignore")

import asyncio
import aiohttp
import aiosmtplib
import nest_asyncio
nest_asyncio.apply()

import requests, dns.resolver, dkim, psutil, socks
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download, snapshot_download

threading.stack_size(256 * 1024)
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, _hard), _hard))
except Exception:
    pass

# === CONSTANTES GLOBAIS ===
CLEAR_SCREEN = "\033[2J\033[H"
STOP_FILE = "/tmp/veriscope_stop"
MAX_EXECUTION_SECONDS = 5 * 3600 + 40 * 60  # 5h40m

GLOBAL_CONNECTION_SEMAPHORE = threading.Semaphore(300)
GLOBAL_HF_SEMAPHORE = threading.Semaphore(16)
GLOBAL_DESEC_SEMAPHORE = threading.Semaphore(2)
GLOBAL_FILEWRITE_SEMAPHORE = threading.Semaphore(32)
UNKNOWN_FILE_LOCK = threading.Lock()
ACEITES_FILE_LOCK = threading.Lock()
REJEICOES_FILE_LOCK = threading.Lock()

# Repositorios HF
HF_LOGS_REPO = "Valter3B/Campaign_Logs"
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_CHECKPOINT_REPO = "Valter3B/Veriscope_Checkpoints"  # NOVO repositorio dedicado

# Configuracoes de campanha
META_ACEITES_POR_PROVEDOR = 50000
MAX_EMAILS_POR_CONTA = 500
MAX_EMAILS_POR_PROVEDOR_DOWNLOAD = float('inf')
EMAILS_JANELA_ENVIO = 50_000
DNS_PROPAGACAO_SEGUNDOS = 300
FINAL_DESTINATARIO = "bingo_192@naver.com"
DIA_ATUAL = 1

# Paralelismo asyncio (substitui threads)
WORKERS_DOWNLOAD_ASYNC = 15000
WORKERS_MX_ASYNC = 9000
WORKERS_VALIDACAO_ASYNC = 20000
WORKERS_ENVIO_ASYNC = 501  # 500 proxies + 1 Colab
WORKERS_DNS_ASYNC = 500

# Configuracoes SMTP
SMTP_TIMEOUT = 15
SMTP_TIMEOUT_ASIATICO = 30
SMTP_RECV_TIMEOUT = 10
DELAY_ENTRE_RCPT = 0.3
MAX_RCPT_POR_CONEXAO = 50

# deSEC
DESEC_TTL = 900
DESEC_CHUNK_BYTES = 400000
DESEC_MAX_RRSETS_PER_REQ = 150
DESEC_MIN_DELAY = 1.5
DESEC_PER_DOMAIN_WRITE_INTERVAL = 4.5
DESEC_MAX_WAIT_429 = 900
DESEC_BACKOFF_BASE = 30
DESEC_BACKOFF_MULTIPLIER = 2.0

# Proxies
TARGET_VALID_PROXIES = 500  # Alvo: maximo possivel
MIN_PROXIES_REFILL = 5
PROXY_COOLDOWN_SEGUNDOS = 120
MAX_TIMEOUTS_BAN = 3
PROXY_GLOBAL_FAIL_REMOVE = 8
RESCRAPE_INTERVALO_MINIMO = 300

# SPF
MAX_SPF_IPS = 250
SPF_REFRESH_INTERVALO_MINIMO = 900

# Retry
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 30
RETRY_BACKOFF_MULTIPLIER = 2.0

# Rate controller
RATE_BASELINE_EMAILS_MIN = 60
RATE_REDUCAO_POR_ERRO = 0.15
RATE_RECUPERACAO_POR_SUCESSO = 0.05
RATE_MINIMO = 5

# Checkpoint
CHECKPOINT_SAVE_INTERVAL = 1000
CHECKPOINT_TIME_INTERVAL = 3600

# Orcamento IP (NOVO)
MAX_EMAILS_IP_PROVEDOR = 500
MIN_INTERVALO_IP_PROVEDOR = 5
WORKER_HIBERNACAO_421 = 60
WORKER_TIMEOUTS_CONSECUTIVOS_MAX = 3

RCPT_LIMITS = {
    "yahoo.com": 20, "ymail.com": 20, "aol.com": 20, "rocketmail.com": 20,
    "yahoo.co.uk": 20, "yahoo.fr": 20, "yahoo.de": 20, "yahoo.it": 20,
    "yahoo.es": 20, "yahoo.ca": 20, "yahoo.com.br": 20, "yahoo.com.mx": 20,
    "aim.com": 20, "netscape.net": 20, "mail.com": 20,
    "outlook.com": 50, "hotmail.com": 50, "live.com": 50, "msn.com": 50,
    "hotmail.co.uk": 50, "hotmail.fr": 50, "hotmail.de": 50,
    "icloud.com": 50, "me.com": 50, "mac.com": 50,
    "protonmail.com": 50, "protonmail.ch": 50,
    "mail.ru": 100, "yandex.ru": 100, "ya.ru": 100,
    "bk.ru": 100, "list.ru": 100, "inbox.ru": 100,
    "daum.net": 50, "naver.com": 100,
    "gmail.com": 50, "googlemail.com": 50,
    "qq.com": 50, "163.com": 50, "126.com": 50,
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

# Token deSEC unico (dominio principal com 600+ subdominios)
DESEC_TOKENS_ALL = [DESEC_TOKEN] if DESEC_TOKEN else []

FALLBACK_PROXY_URLS = []

# Diretorios
BASE_LOCAL = "/content/campaign_v21_0"
DIR_EMAILS = "/content/emails_db"
DIR_AUDIT = os.path.join(BASE_LOCAL, "audit")
DIR_PROXIES = "/content/proxies"
LOG_DIR = os.path.join(BASE_LOCAL, "logs")
DIR_REPO = "/content/trader_repo"

# Ficheiros de estado
PROXY_STATE_FILE = os.path.join(BASE_LOCAL, "proxy_state.json")
UNKNOWN_FILE_LOCAL = os.path.join(BASE_LOCAL, "user_unknowns_acumulado.csv")
MAIL_FROM_FILE_LOCAL = "mail_from_alex_liquidity_alert_2115.txt"
SAIDA_ACEITES = "/content/aceites_finais.csv"
SAIDA_UNKNOWN = "/content/user_unknowns_final.csv"
SAIDA_REJEICAOES = "/content/rejeicoes_finais.csv"
DESEC_LOG = os.path.join(BASE_LOCAL, "desec_requests.log")
MANIFEST_FILE = os.path.join(BASE_LOCAL, "checkpoint_manifest.json")

# Ficheiros de checkpoint (agora com versao e hash)
CP_DIR = os.path.join(BASE_LOCAL, "checkpoints")
CP_FASES = os.path.join(CP_DIR, "cp_fases.json")
CP_PROVEDORES = os.path.join(CP_DIR, "cp_provedores.json")
CP_POOL = os.path.join(CP_DIR, "cp_pool.json")
CP_SMTP = os.path.join(CP_DIR, "cp_smtp.json")
CP_CONTAS = os.path.join(CP_DIR, "cp_contas.json")
CP_CONFIG = os.path.join(CP_DIR, "cp_config.json")
CP_UNKNOWN = os.path.join(CP_DIR, "cp_unknown.json")
CP_ACEITES = os.path.join(CP_DIR, "cp_aceites.json")
CP_WORKERS = os.path.join(CP_DIR, "cp_workers.json")
CP_ORCAMENTO = os.path.join(CP_DIR, "cp_orcamento.json")
CP_RCPT = os.path.join(CP_DIR, "cp_rcpt.json")
CP_DASHBOARD = os.path.join(CP_DIR, "cp_dashboard.json")

PROXY_SEARCH_DIRS = [
    DIR_PROXIES, "/content/proxies", "/content", "/content/data",
    "/content/uploads", "./", "/content/drive/MyDrive",
    "/content/drive/MyDrive/proxies", "/content/drive/MyDrive/Proxies",
]

FASES_OBRIGATORIAS_ANTES_ENVIO = [
    "FASE0_IP", "FASE1_TOKENS", "FASE2A_CONTAS", "FASE2B_MX",
    "FASE2C_DOWNLOAD", "FASE25_PROXIES", "FASE3_DNS", "FASE4_PROPAGACAO",
]

# === DIAGNOSTICO DE AMBIENTE ===
def diagnostico_ambiente_hf():
    print("\n" + "=" * 80, flush=True)
    print("🔍 DIAGNÓSTICO DE AMBIENTE V21.0.0", flush=True)
    print("=" * 80, flush=True)
    print(f"[ENV] HF_TOKEN: {'SIM' if HF_TOKEN else 'NÃO'} ({len(HF_TOKEN)} chars)", flush=True)
    print(f"[ENV] DESEC_TOKEN: {'SIM' if DESEC_TOKEN else 'NÃO'}", flush=True)
    print(f"[ENV] GITHUB_PAT: {'SIM' if GITHUB_PAT else 'NÃO'}", flush=True)
    print(f"[REPOS] Checkpoints: {HF_CHECKPOINT_REPO}", flush=True)
    print(f"[REPOS] Logs: {HF_LOGS_REPO}", flush=True)
    print(f"[REPOS] Trader: {HF_TRADER_REPO}", flush=True)
    if HF_TOKEN:
        try:
            api = HfApi(token=HF_TOKEN)
            info = api.whoami()
            print(f"[HF] ✅ whoami OK: {info.get('name')}", flush=True)
        except Exception as e:
            print(f"[HF] ❌ whoami FAILED: {str(e)[:100]}", flush=True)
    print("=" * 80 + "\n", flush=True)

# === GARANTIR DIRETORIOS ===
def garantir_diretorios():
    for d in [BASE_LOCAL, LOG_DIR, DIR_EMAILS, DIR_AUDIT, DIR_PROXIES, DIR_REPO, CP_DIR]:
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

# === CHECKPOINT MANAGER TRANSACIONAL ===
class CheckpointManager:
    """Checkpoint transacional com manifesto SHA-256 e integridade."""
    VERSION = "21.0.0"

    def __init__(self):
        self.locks = {
            "fases": threading.Lock(),
            "provedores": threading.Lock(),
            "pool": threading.Lock(),
            "smtp": threading.Lock(),
            "contas": threading.Lock(),
            "config": threading.Lock(),
            "unknown": threading.Lock(),
            "aceites": threading.Lock(),
            "workers": threading.Lock(),
            "orcamento": threading.Lock(),
            "rcpt": threading.Lock(),
            "dashboard": threading.Lock(),
        }
        self.states = {
            "fases": {},
            "provedores": {},
            "pool": {"pool": [], "scraping_checkpoint": 0, "raw_count": 0},
            "smtp": {
                "smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0,
                "smtp_user_unknown": 0, "smtp_erros": 0, "smtp_conexoes": 0,
                "smtp_via_proxy": 0, "smtp_via_colab": 0,
                "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
                "provedor_index": {}, "por_provedor": {},
            },
            "contas": {"contas_carregadas": 0, "contas_idx": 0, "dominio_principal": None},
            "config": {
                "campaign_id": None,
                "exec_id": None,
                "ip_colab": None,
                "campaign_started_at": None,
                "run_started_at": None,
                "last_save": None,
                "status": "NEW",
                "run_history": [],
                "total_runs": 0,
            },
            "unknown": {"emails": []},
            "aceites": {"emails": []},
            "workers": {
                "workers_ativos": {},
                "hibernados": {},
                "mortos": [],
            },
            "orcamento": {"ip_provedor": {}},
            "rcpt": {"aprendido": {}},
            "dashboard": {
                "ultima_geracao": None,
                "historico_runs": [],
            },
        }
        self.paths = {
            "fases": CP_FASES,
            "provedores": CP_PROVEDORES,
            "pool": CP_POOL,
            "smtp": CP_SMTP,
            "contas": CP_CONTAS,
            "config": CP_CONFIG,
            "unknown": CP_UNKNOWN,
            "aceites": CP_ACEITES,
            "workers": CP_WORKERS,
            "orcamento": CP_ORCAMENTO,
            "rcpt": CP_RCPT,
            "dashboard": CP_DASHBOARD,
        }
        self.manifest_path = MANIFEST_FILE
        self._carregar_tudo()

    def _calcular_hash(self, path):
        """Calcula SHA-256 de um ficheiro."""
        if not os.path.exists(path):
            return None
        h = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return None

    def _carregar_tudo(self):
        """Carrega todos os checkpoints do disco."""
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
                        self.states[nome] = dados
                        print(f"[CHECKPOINT] Carregado local: {nome}", flush=True)
                except Exception as e:
                    print(f"[CHECKPOINT] AVISO ao carregar {nome}: {str(e)[:100]}", flush=True)

    def _salvar_um(self, nome):
        """Salva um checkpoint atomicamente."""
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
            debug_log(f"checkpoint salvar {nome}: {str(e)}")
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

    def obter_campaign_id(self):
        try:
            return self.states["config"].get("campaign_id")
        except Exception:
            return None

    def obter_campaign_started_at(self):
        try:
            return self.states["config"].get("campaign_started_at")
        except Exception:
            return None

    def atualizar_fase(self, chave, ok, detalhe=""):
        with self.locks["fases"]:
            self.states["fases"][chave] = {
                "ok": bool(ok),
                "detalhe": str(detalhe)[:200],
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
                "emails": total_emails,
                "parquets": total_parquets,
                "concluido": True,
                "ts": datetime.now().isoformat(),
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
                if k not in ("provedor_index", "por_provedor"):
                    self.states["smtp"][k] = v
            self._salvar_um("smtp")

    def atualizar_provedor_index(self, provedor, offset, aceites, concluido=False, total_linhas=0):
        with self.locks["smtp"]:
            if "provedor_index" not in self.states["smtp"]:
                self.states["smtp"]["provedor_index"] = {}
            self.states["smtp"]["provedor_index"][provedor] = {
                "offset": offset,
                "aceites": aceites,
                "concluido": concluido,
                "total_linhas": total_linhas,
                "ts": datetime.now().isoformat(),
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

    def salvar_config(self, exec_id, ip_colab, status="RUNNING", campaign_id=None):
        """Salva config com separacao campaign_started_at vs run_started_at."""
        with self.locks["config"]:
            cfg = self.states["config"]
            agora = datetime.now().isoformat()

            # campaign_id e campaign_started_at sao persistentes entre runs
            if campaign_id:
                cfg["campaign_id"] = campaign_id
            elif not cfg.get("campaign_id"):
                cfg["campaign_id"] = f"camp_{int(time.time())}"

            if not cfg.get("campaign_started_at"):
                cfg["campaign_started_at"] = agora

            # exec_id e run_started_at sao novos a cada run
            cfg["exec_id"] = exec_id
            cfg["ip_colab"] = ip_colab
            cfg["status"] = status
            cfg["run_started_at"] = agora
            cfg["last_save"] = agora

            # Incrementa contador de runs
            cfg["total_runs"] = cfg.get("total_runs", 0) + 1

            self._salvar_um("config")

    def registar_fim_run(self, motivo="NORMAL", status_final="COMPLETED"):
        """Regista o fim de uma run no historico."""
        with self.locks["config"]:
            cfg = self.states["config"]
            run_info = {
                "exec_id": cfg.get("exec_id"),
                "run_started_at": cfg.get("run_started_at"),
                "run_ended_at": datetime.now().isoformat(),
                "motivo": motivo,
                "status": status_final,
            }
            if "run_history" not in cfg:
                cfg["run_history"] = []
            cfg["run_history"].append(run_info)
            # Manter apenas ultimas 50 runs
            if len(cfg["run_history"]) > 50:
                cfg["run_history"] = cfg["run_history"][-50:]
            cfg["status"] = status_final
            self._salvar_um("config")

    def obter_config(self):
        with self.locks["config"]:
            return dict(self.states["config"])

    def salvar_contas(self, n_carregadas, dominio_principal=None):
        with self.locks["contas"]:
            self.states["contas"]["contas_carregadas"] = n_carregadas
            if dominio_principal:
                self.states["contas"]["dominio_principal"] = dominio_principal
            self._salvar_um("contas")

    def obter_dominio_principal(self):
        with self.locks["contas"]:
            return self.states["contas"].get("dominio_principal")

    def salvar_unknowns(self, unknowns_set):
        """Salva SEM truncamento - removeu limite 2M."""
        with self.locks["unknown"]:
            try:
                lista = list(unknowns_set)
            except Exception:
                lista = []
            # SEM truncamento - persiste tudo
            self.states["unknown"]["emails"] = lista
            self._salvar_um("unknown")

    def carregar_unknowns(self):
        with self.locks["unknown"]:
            try:
                return set(self.states["unknown"].get("emails", []))
            except Exception:
                return set()

    def salvar_aceites(self, aceites_set):
        """Salva SEM truncamento."""
        with self.locks["aceites"]:
            try:
                lista = list(aceites_set)
            except Exception:
                lista = []
            # SEM truncamento
            self.states["aceites"]["emails"] = lista
            self._salvar_um("aceites")

    def carregar_aceites(self):
        with self.locks["aceites"]:
            try:
                return set(self.states["aceites"].get("emails", []))
            except Exception:
                return set()

    def salvar_orcamento(self, orcamento_dict):
        with self.locks["orcamento"]:
            self.states["orcamento"]["ip_provedor"] = orcamento_dict
            self._salvar_um("orcamento")

    def obter_orcamento(self):
        with self.locks["orcamento"]:
            return dict(self.states["orcamento"].get("ip_provedor", {}))

    def salvar_rcpt_aprendido(self, rcpt_dict):
        with self.locks["rcpt"]:
            self.states["rcpt"]["aprendido"] = rcpt_dict
            self._salvar_um("rcpt")

    def obter_rcpt_aprendido(self):
        with self.locks["rcpt"]:
            return dict(self.states["rcpt"].get("aprendido", {}))

    def salvar_workers(self, workers_dict):
        with self.locks["workers"]:
            self.states["workers"] = workers_dict
            self._salvar_um("workers")

    def obter_workers(self):
        with self.locks["workers"]:
            return dict(self.states["workers"])

    def salvar_dashboard(self, dashboard_dict):
        with self.locks["dashboard"]:
            self.states["dashboard"] = dashboard_dict
            self._salvar_um("dashboard")

    def obter_dashboard(self):
        with self.locks["dashboard"]:
            return dict(self.states["dashboard"])

    def gerar_manifesto(self, generation_id):
        """Gera manifesto com hashes SHA-256 de todos os checkpoints."""
        manifest = {
            "version": self.VERSION,
            "generation_id": generation_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "files": {},
        }
        for nome, path in self.paths.items():
            if os.path.exists(path):
                manifest["files"][nome] = {
                    "path": path,
                    "hash_sha256": self._calcular_hash(path),
                    "size_bytes": os.path.getsize(path),
                }
        # Salva manifesto atomicamente
        try:
            tmp = self.manifest_path + ".tmp"
            with GLOBAL_FILEWRITE_SEMAPHORE:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(manifest, f, indent=2, ensure_ascii=False)
                os.replace(tmp, self.manifest_path)
            return manifest
        except Exception as e:
            debug_log(f"gerar_manifesto: {str(e)}")
            return None

    def validar_manifesto(self):
        """Valida integridade do manifesto e dos ficheiros."""
        if not os.path.exists(self.manifest_path):
            return False, "Sem manifesto"
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except Exception as e:
            return False, f"Manifesto corrompido: {e}"

        for nome, info in manifest.get("files", {}).items():
            path = info.get("path")
            expected_hash = info.get("hash_sha256")
            if not path or not os.path.exists(path):
                return False, f"Ficheiro ausente: {nome}"
            actual_hash = self._calcular_hash(path)
            if actual_hash != expected_hash:
                return False, f"Hash invalido: {nome}"
        return True, "OK"

    def salvar_tudo_com_manifesto(self):
        """Salva todos os checkpoints e gera manifesto."""
        generation_id = f"gen_{int(time.time())}_{random.randint(1000,9999)}"
        ok_count = 0
        for nome in self.paths.keys():
            if self._salvar_um(nome):
                ok_count += 1
        manifest = self.gerar_manifesto(generation_id)
        return ok_count, manifest

    def salvar_tudo(self):
        for nome in self.paths.keys():
            self._salvar_um(nome)

    def limpar_tudo(self):
        for path in list(self.paths.values()) + [self.manifest_path]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
        self.states = {
            "fases": {},
            "provedores": {},
            "pool": {"pool": [], "scraping_checkpoint": 0, "raw_count": 0},
            "smtp": {
                "smtp_tentativas": 0, "smtp_aceites": 0, "smtp_rejeitados": 0,
                "smtp_user_unknown": 0, "smtp_erros": 0, "smtp_conexoes": 0,
                "smtp_via_proxy": 0, "smtp_via_colab": 0,
                "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0, "smtp_abandonados": 0,
                "provedor_index": {}, "por_provedor": {},
            },
            "contas": {"contas_carregadas": 0, "contas_idx": 0, "dominio_principal": None},
            "config": {
                "campaign_id": None, "exec_id": None, "ip_colab": None,
                "campaign_started_at": None, "run_started_at": None,
                "last_save": None, "status": "NEW", "run_history": [], "total_runs": 0,
            },
            "unknown": {"emails": []},
            "aceites": {"emails": []},
            "workers": {"workers_ativos": {}, "hibernados": {}, "mortos": []},
            "orcamento": {"ip_provedor": {}},
            "rcpt": {"aprendido": {}},
            "dashboard": {"ultima_geracao": None, "historico_runs": []},
        }

CHECKPOINT = CheckpointManager()

# === ESTADO GLOBAL ===
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
        "campaign_id": None,
        "campaign_started_at": None,
        "run_started_at": time.time(),
        "monitor_running": True,
        "start_time": time.time(),
        "log_buffer": deque(maxlen=20),
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
        "dominio_principal": None,
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
        "colab_spf_atualizado": False,
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
        "orcamento_ip": {},
        "orcamento_lock": threading.Lock(),
        "workers_info": {},
        "workers_lock": threading.Lock(),
        "n_workers_ativos": 0,
        "dashboard_ultima_geracao": None,
    })
    return execution_id

# === LOGGING ===
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
        debug_log(f"registar_resposta_smtp: {str(e)}")

def log_desec_request(domain, url, method, status_code, response_body, headers, duration_ms, attempt, payload_preview=None):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    try:
        garantir_diretorio_para(DESEC_LOG)
        with GLOBAL_FILEWRITE_SEMAPHORE:
            with open(DESEC_LOG, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write(f"[{ts}] DESec Request #{GLOBAL.get('desec_requests', 0)}\n")
                f.write(f"Domain: {domain}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Method: {method}\n")
                f.write(f"Attempt: {attempt}\n")
                f.write(f"Duration: {duration_ms:.0f}ms\n")
                f.write(f"Status: {status_code}\n")
                f.write(f"Headers: {headers}\n")
                if payload_preview:
                    f.write(f"Payload Preview: {str(payload_preview)[:2000]}\n")
                f.write(f"Response Body: {str(response_body)[:1000]}\n")
                f.write("=" * 80 + "\n")
        with GLOBAL["stats_lock"]:
            GLOBAL["desec_requests"] = GLOBAL.get("desec_requests", 0) + 1
            if status_code is not None and 200 <= int(status_code) < 300:
                GLOBAL["desec_success"] = GLOBAL.get("desec_success", 0) + 1
            elif method != "GET":
                GLOBAL["desec_failures"] = GLOBAL.get("desec_failures", 0) + 1
    except Exception as e:
        try:
            print(f"[log_desec_request] erro: {str(e)[:100]}", flush=True)
        except Exception:
            pass

# === HUGGING FACE SYNC ===
def criar_repo_hf_se_nao_existe(repo_id, repo_type="dataset"):
    """Cria repositorio HF se nao existir."""
    if not HF_TOKEN:
        return False
    try:
        api = HfApi(token=HF_TOKEN)
        try:
            api.repo_info(repo_id=repo_id, repo_type=repo_type)
            dizer(f"[HF] Repositorio {repo_id} ja existe")
            return True
        except Exception:
            pass
        dizer(f"[HF] A criar repositorio {repo_id}...")
        api.create_repo(repo_id=repo_id, repo_type=repo_type, private=False, exist_ok=True)
        dizer(f"[HF] ✅ Repositorio {repo_id} criado")
        return True
    except Exception as e:
        dizer(f"[HF] ❌ Erro ao criar repositorio: {str(e)[:120]}")
        return False

def upload_all_checkpoints_to_hf():
    """Upload de checkpoints apenas no SHUTDOWN (nao periodico)."""
    dizer("[HF SYNC] A enviar checkpoints para HuggingFace...")
    try:
        criar_repo_hf_se_nao_existe(HF_CHECKPOINT_REPO, "dataset")
        api = HfApi(token=HF_TOKEN)
        enviados = 0
        for nome, path in CHECKPOINT.paths.items():
            if os.path.exists(path):
                try:
                    api.upload_file(
                        path_or_fileobj=path,
                        path_in_repo=f"checkpoints/{nome}.json",
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    enviados += 1
                except Exception as e:
                    dizer(f"  [HF SYNC] AVISO upload {nome}: {str(e)[:120]}")
        # Upload manifesto
        if os.path.exists(MANIFEST_FILE):
            try:
                api.upload_file(
                    path_or_fileobj=MANIFEST_FILE,
                    path_in_repo="checkpoints/manifest.json",
                    repo_id=HF_CHECKPOINT_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                enviados += 1
            except Exception as e:
                dizer(f"  [HF SYNC] AVISO upload manifesto: {str(e)[:120]}")
        # Upload ficheiros de resultados
        for fpath in [UNKNOWN_FILE_LOCAL, SAIDA_ACEITES, SAIDA_REJEICAOES]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(
                        path_or_fileobj=fpath,
                        path_in_repo=f"results/{os.path.basename(fpath)}",
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    enviados += 1
                except Exception as e:
                    dizer(f"  [HF SYNC] AVISO upload {os.path.basename(fpath)}: {str(e)[:120]}")
        dizer(f"[HF SYNC] Upload concluido: {enviados} ficheiros enviados.")
    except Exception as e:
        dizer(f"[HF SYNC] ERRO geral: {str(e)[:100]}")

def download_all_checkpoints_from_hf():
    """Download de checkpoints do repositorio dedicado."""
    dizer("[HF SYNC] A baixar checkpoints do HuggingFace...")
    garantir_diretorios()
    baixados = 0
    ausentes = 0
    falhados = 0

    # Primeiro tenta baixar manifesto
    manifest_local = None
    try:
        manifest_local = hf_hub_download(
            repo_id=HF_CHECKPOINT_REPO,
            filename="checkpoints/manifest.json",
            repo_type="dataset",
            token=HF_TOKEN,
            force_download=True
        )
        shutil.copy(manifest_local, MANIFEST_FILE)
        dizer("[HF SYNC] Manifesto baixado")
    except Exception as e:
        if "404" in str(e) or "EntryNotFound" in str(e):
            dizer("[HF SYNC] Sem manifesto na HF (normal na 1a execucao)")
        else:
            dizer(f"[HF SYNC] Falha manifesto: {str(e)[:100]}")

    for nome, path in CHECKPOINT.paths.items():
        try:
            local = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO,
                filename=f"checkpoints/{nome}.json",
                repo_type="dataset",
                token=HF_TOKEN,
                force_download=True
            )
            garantir_diretorio_para(path)
            shutil.copy(local, path)
            baixados += 1
            try:
                tam = os.path.getsize(path)
            except Exception:
                tam = -1
            dizer(f"  [HF SYNC] Checkpoint baixado: {nome}.json ({tam:,} bytes)")
        except Exception as e:
            texto = str(e)
            if "404" in texto or "EntryNotFound" in texto or "does not exist" in texto.lower():
                ausentes += 1
            else:
                falhados += 1
                dizer(f"  [HF SYNC] FALHA ao baixar {nome}.json: {texto[:140]}")

    map_files = {
        "user_unknowns_acumulado.csv": UNKNOWN_FILE_LOCAL,
        "aceites_finais.csv": SAIDA_ACEITES,
        "rejeicoes_finais.csv": SAIDA_REJEICAOES
    }
    for fname, dest in map_files.items():
        try:
            local = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO,
                filename=f"results/{fname}",
                repo_type="dataset",
                token=HF_TOKEN,
                force_download=True
            )
            garantir_diretorio_para(dest)
            shutil.copy(local, dest)
            baixados += 1
            dizer(f"  [HF SYNC] Resultado baixado: {fname} -> {dest}")
        except Exception as e:
            texto = str(e)
            if "404" in texto or "EntryNotFound" in texto:
                pass
            else:
                falhados += 1

    dizer(f"[HF SYNC] Downloads: {baixados} ok | {ausentes} ausentes | {falhados} falhados")

    # Valida integridade se manifesto existe
    if os.path.exists(MANIFEST_FILE):
        ok, msg = CHECKPOINT.validar_manifesto()
        if ok:
            dizer("[HF SYNC] ✅ Manifesto validado - integridade OK")
        else:
            dizer(f"[HF SYNC] ⚠️ Manifesto invalido: {msg}")

    return baixados

# === AUTO RESTART ===
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

# === FUNCOES DE FASE ===
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

def check_stop_file():
    if os.path.exists(STOP_FILE):
        dizer(f"[STOP FILE] Detectado {STOP_FILE}. Parando...")
        GLOBAL["emergency_stop"] = True
        return True
    return False

# === IP COLAB ===
async def obter_ip_colab():
    definir_fase("FASE 0 - IP do Colab + verificacao de blacklists")
    config = CHECKPOINT.obter_config()
    if config.get("ip_colab"):
        ip = config["ip_colab"]
        try:
            socket.inet_aton(ip)
            GLOBAL["ip_colab"] = ip
            dizer(f"  IP recuperado do checkpoint: {ip}")
            marcar_fase("FASE0_IP", True, f"IP recuperado {ip}")
            return ip
        except Exception:
            pass

    servicos = [
        "https://api.ipify.org", "https://ifconfig.me/ip",
        "https://icanhazip.com", "https://ipinfo.io/ip",
        "https://checkip.amazonaws.com",
    ]
    ips = []
    async with aiohttp.ClientSession() as session:
        for url in servicos:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        ip = (await r.text()).strip()
                        socket.inet_aton(ip)
                        ips.append(ip)
            except Exception:
                pass

    if not ips:
        marcar_fase("FASE0_IP", False, "nenhum servico respondeu")
        raise RuntimeError("Nao foi possivel obter IP do Colab")

    ip = max(set(ips), key=ips.count)
    GLOBAL["ip_colab"] = ip
    marcar_fase("FASE0_IP", True, f"IP {ip}")
    return ip

async def verificar_blacklists(ip):
    dizer(f"A verificar blacklists do IP {ip}")
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
        dizer(f"AVISO: IP Colab listado em: {', '.join(listado)}")
    else:
        dizer(f"IP Colab limpo em {len(limpo)} listas")
    return listado

# === DESEC TOKENS ===
DESEC_TOKENS_VALIDOS = []
DOM_TOKEN = {}
DOM_MIN_TTL = {}
DOM_TOKEN_LOCK = threading.RLock()
DOM_ULTIMA_ESCRITA = {}
DOM_ESCRITA_LOCK = threading.Lock()

async def testar_tokens_desec():
    """Valida tokens e constroi mapa dominio->token+minimum_ttl."""
    definir_fase("FASE 1 - Validacao dos tokens deSEC + mapa de dominios")

    async def _testar_um(tok):
        if GLOBAL.get("emergency_stop"):
            return tok, None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://desec.io/api/v1/domains/",
                    headers={"Authorization": f"Token {tok}"},
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    await asyncio.sleep(0.4)
                    if r.status == 200:
                        dados = await r.json()
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
        except Exception as e:
            debug_log(f"token test {tok[:6]}...: {str(e)[:120]}")
        return tok, None

    validos = []
    mapa_feito = 0
    try:
        tasks = [_testar_um(tok) for tok in DESEC_TOKENS_ALL]
        results = await asyncio.gather(*tasks)
        for tok, doms in results:
            if doms is None:
                continue
            validos.append(tok)
            with DOM_TOKEN_LOCK:
                for nome_dom, min_ttl in doms:
                    DOM_TOKEN[nome_dom] = tok
                    DOM_MIN_TTL[nome_dom] = min_ttl
                    mapa_feito += 1
    except Exception as e:
        debug_log(f"testar_tokens_desec: {str(e)}")

    DESEC_TOKENS_VALIDOS.clear()
    DESEC_TOKENS_VALIDOS.extend(validos)
    dizer(f"Tokens validos: {len(validos)}/{len(DESEC_TOKENS_ALL)} | dominios mapeados: {mapa_feito}")

    if mapa_feito > 0:
        amostra = sorted(DOM_TOKEN.keys())[:5]
        dizer(f"  Amostra de dominios: {', '.join(amostra)}" + (" ..." if mapa_feito > 5 else ""))

    if not validos:
        marcar_fase("FASE1_TOKENS", False, "nenhum token valido")
        raise RuntimeError("Nenhum token deSEC valido")

    marcar_fase("FASE1_TOKENS", True, f"{len(validos)} tokens, {mapa_feito} dominios")
    return validos

# === DESCARGA HF COM RETRY ===
async def download_hf_com_retry(repo_id, filename, repo_type, max_retries=4):
    ultima = None
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            raise RuntimeError("emergency_stop")
        try:
            with GLOBAL_HF_SEMAPHORE:
                return hf_hub_download(
                    repo_id=repo_id,
                    filename=filename,
                    repo_type=repo_type,
                    token=HF_TOKEN,
                    etag_timeout=60,
                )
        except Exception as e:
            ultima = e
            texto = str(e).lower()
            espera = 30 if ("429" in texto or "rate" in texto or "too many" in texto) else (2 ** tentativa)
            debug_log(f"hf_retry {tentativa + 1} {filename}: {str(e)[:180]}")
            await asyncio.sleep(espera)
    raise ultima if ultima else RuntimeError("download_hf_com_retry falhou")

# === CARREGAR CONTAS INTELIGENTE ===
async def carregar_contas_inteligente():
    """Carrega contas e seleciona 501 do dominio principal."""
    definir_fase("FASE 2A - Contas DKIM + descoberta dominio principal")

    if fase_ja_completa("FASE2A_CONTAS"):
        dizer("  Fase 2A ja concluida - a recarregar")
        dominio_salvo = CHECKPOINT.obter_dominio_principal()
        if dominio_salvo:
            GLOBAL["dominio_principal"] = dominio_salvo

    p = await download_hf_com_retry(HF_LOGS_REPO, "campaign_data/phase1_accounts_keys.csv", "dataset")
    with open(p, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    todas_contas = []
    for r in rows:
        priv = (r.get("dkim_private_key_pem") or "").strip()
        if not priv.startswith("-----BEGIN "):
            continue
        todas_contas.append({
            "identity_id": (r.get("identity_id") or "").strip(),
            "sender_email": (r.get("sender_email") or "").strip(),
            "domain": (r.get("domain") or "").strip(),
            "base": (r.get("base") or "").strip(),
            "selector": (r.get("dkim_selector") or "default").strip(),
            "private_key_pem": priv,
            "public_key_txt": (r.get("dkim_public_txt") or "").strip(),
        })

    if not todas_contas:
        marcar_fase("FASE2A_CONTAS", False, "0 contas com chave DKIM")
        raise RuntimeError("Nenhuma conta valida carregada")

    # Agrupa por dominio
    por_dominio = defaultdict(list)
    for conta in todas_contas:
        por_dominio[conta["domain"]].append(conta)

    # Identifica dominio com >=501 contas
    dominio_principal = None
    for domain, contas in por_dominio.items():
        if len(contas) >= 501:
            dominio_principal = domain
            break

    # Se nenhum tem 501+, usa o maior
    if not dominio_principal:
        dominio_principal = max(por_dominio.keys(), key=lambda d: len(por_dominio[d]))
        dizer(f"AVISO: Nenhum dominio com 501+ contas. Usando {dominio_principal} com {len(por_dominio[dominio_principal])} contas")
    else:
        dizer(f"✅ Dominio principal encontrado: {dominio_principal} com {len(por_dominio[dominio_principal])} contas")

    # Seleciona 501 contas (ou todas se < 501)
    contas_selecionadas = por_dominio[dominio_principal][:501]
    GLOBAL["contas"] = contas_selecionadas
    GLOBAL["dominio_principal"] = dominio_principal

    CHECKPOINT.salvar_contas(len(contas_selecionadas), dominio_principal)

    dizer(f"  {len(contas_selecionadas)} contas ativas do dominio {dominio_principal}")
    dizer(f"  Total de dominios no CSV: {len(por_dominio)}")
    for d, cs in sorted(por_dominio.items(), key=lambda x: -len(x[1]))[:5]:
        dizer(f"    - {d}: {len(cs)} contas")

    marcar_fase("FASE2A_CONTAS", True, f"{len(contas_selecionadas)} contas @ {dominio_principal}")
    return contas_selecionadas

async def carregar_template():
    p = await download_hf_com_retry(
        HF_LOGS_REPO,
        f"campaign_data/templates/{CAMPAIGN['template']}",
        "dataset"
    )
    with open(p, "r", encoding="utf-8") as f:
        html = f.read()
    dizer(f"Template carregado: {CAMPAIGN['template']} ({len(html)} bytes)")
    return html

# === MAIL FROM VARIATIONS ===
async def carregar_mail_from_variations():
    dizer("A carregar variacoes de Mail From...")
    variations = []
    candidatos = [
        os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL),
        MAIL_FROM_FILE_LOCAL,
        f"/content/{MAIL_FROM_FILE_LOCAL}",
        f"/content/data/{MAIL_FROM_FILE_LOCAL}",
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
            dizer(f"  Carregadas {len(variations)} variacoes de {ficheiro}")
        except Exception as e:
            dizer(f"  AVISO: Erro ao ler {ficheiro}: {str(e)[:120]}")
    else:
        try:
            local_mf = await download_hf_com_retry(
                HF_LOGS_REPO,
                f"campaign_data/templates/{MAIL_FROM_FILE_LOCAL}",
                "dataset"
            )
            garantir_diretorio_para(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
            shutil.copy(local_mf, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
            with open(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL), "r", encoding="utf-8", errors="ignore") as f:
                for linha in f:
                    linha = linha.strip()
                    if linha and not linha.startswith("#"):
                        variations.append(linha)
            dizer(f"  Carregadas {len(variations)} variacoes do HuggingFace")
        except Exception as e2:
            dizer(f"  AVISO: Falha no download HF: {str(e2)[:120]}")

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
    idx_var = idx_conta % len(variations)
    return variations[idx_var]

# === PROVEDORES E MX ===
async def listar_ficheiros_repo():
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
            debug_log(f"list_repo_files tentativa {tentativa + 1}: {str(e)[:200]}")
            await asyncio.sleep(10 * (tentativa + 1))
    raise ultima if ultima else RuntimeError("list_repo_files falhou")

async def descobrir_provedores_reais():
    definir_fase("FASE 2B - Descoberta de provedores + resolucao de MX")
    todos = await listar_ficheiros_repo()
    provs = set()
    for f in todos:
        if f.startswith("Trader_Emails/") and f.endswith(".parquet"):
            parts = f.split("/")
            if len(parts) >= 2 and parts[1]:
                provs.add(parts[1])
    GLOBAL["prov_total"] = len(provs)
    dizer(f"Provedores descobertos: {len(provs)}")
    if not provs:
        marcar_fase("FASE2B_MX", False, "0 provedores no repo")
        raise RuntimeError(f"Nenhum provedor encontrado em {HF_TRADER_REPO}")
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

async def resolver_mxs_paralelo(provedores):
    dizer(f"A resolver MX de {len(provedores)} provedores...")
    out = {}
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(WORKERS_MX_ASYNC, len(provedores) or 1, 300)) as ex:
        tasks = [loop.run_in_executor(ex, resolver_mx_um, prov) for prov in provedores]
        results = await asyncio.gather(*tasks)
        for prov, mxs in results:
            if mxs:
                out[prov] = mxs
                with GLOBAL["stats_lock"]:
                    GLOBAL["mx_found"] += len(mxs)
    dizer(f"Provedores com MX valido: {len(out)} | MXs totais: {GLOBAL['mx_found']}")
    marcar_fase("FASE2B_MX", len(out) > 0, f"{len(out)}/{len(provedores)} com MX")
    return out

# === DOWNLOAD PARQUETS ===
async def _heartbeat_snapshot(evento_parar):
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
                dizer(f"  ... snapshot em curso: {n} parquets | livre {espaco_disco_livre_gb():.1f}GB")
                ultimo = n
        except Exception:
            pass
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break

async def baixar_snapshot_repo():
    padroes = [
        "Trader_Emails/", "Trader_Emails/*/", "Trader_Emails/*/*/",
        "Trader_Emails/*.parquet", "Trader_Emails/**/*.parquet",
    ]
    os.makedirs(DIR_REPO, exist_ok=True)
    evento = asyncio.Event()
    hb_task = asyncio.create_task(_heartbeat_snapshot(evento))
    resultado = None
    ultima = None
    try:
        for tentativa in range(3):
            if GLOBAL.get("emergency_stop"):
                break
            try:
                dizer(f"  snapshot_download tentativa {tentativa + 1}/3 ...")
                inicio = time.time()
                local = snapshot_download(
                    repo_id=HF_TRADER_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN,
                    allow_patterns=padroes,
                    local_dir=DIR_REPO,
                    max_workers=max(8, min(WORKERS_DOWNLOAD_ASYNC, 64)),
                    etag_timeout=60,
                )
                decorrido = time.time() - inicio
                contar = 0
                for r, _d, fs in os.walk(local or DIR_REPO):
                    for f in fs:
                        if f.endswith(".parquet"):
                            contar += 1
                dizer(f"  Snapshot OK em {decorrido:.0f}s -> {local} ({contar} parquets)")
                resultado = local or DIR_REPO
                GLOBAL["snapshot_ok"] = True
                break
            except Exception as e:
                ultima = e
                dizer(f"  snapshot_download falhou: {str(e)[:200]}")
                debug_log(f"snapshot tentativa {tentativa + 1}:\n{traceback.format_exc()}")
                await asyncio.sleep(20 * (tentativa + 1))
    except Exception as e:
        debug_log(f"baixar_snapshot_repo outer: {str(e)}")
    finally:
        evento.set()
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass

    if resultado is None and ultima is not None:
        dizer("  AVISO: snapshot impossivel - recurso a downloads individuais")
    return resultado

def processar_provedor_emails_sync(args):
    """Processa parquets de um provedor - versao sync para ThreadPool."""
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
                        origem = hf_hub_download(
                            repo_id=HF_TRADER_REPO, filename=rel,
                            repo_type="dataset", token=HF_TOKEN, etag_timeout=60
                        )
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
                    debug_log(f"parquet {rel} ({prov}): {str(e)[:200]}")
                finally:
                    pf = None
                    if origem and veio_do_snapshot:
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
        debug_log(f"processar {prov}: {str(e)[:200]}")
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

async def baixar_todos_emails_paralelo(provedores):
    definir_fase("FASE 2C - Download HuggingFace (snapshot) + extraccao de emails")
    livres = espaco_disco_livre_gb()
    dizer(f"  Espaco livre em /content: {livres:.1f} GB")

    ja_baixados = CHECKPOINT.obter_provedores_baixados()
    if ja_baixados:
        dizer(f"  Provedores ja baixados no checkpoint: {len(ja_baixados)}")

    todos = await listar_ficheiros_repo()
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
    dizer(f"  Parquets identificados: {GLOBAL['pq_total']} em {len(args)} provedores")

    # Recupera emails de provedores ja baixados
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
        dizer(f"  Emails recuperados do checkpoint: {emails_recuperados:,}")

    raiz_local = await baixar_snapshot_repo()

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

    dizer(f"  A extrair emails para {DIR_EMAILS} ...")
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(WORKERS_DOWNLOAD_ASYNC, len(itens_args) or 1, 200)) as ex:
        tasks = [loop.run_in_executor(ex, processar_provedor_emails_sync, a) for a in itens_args]
        await asyncio.gather(*tasks)

    dizer(f"  Emails extraidos: {GLOBAL['emails_read']:,}")
    dizer(f"  Parquets processados: {GLOBAL['pq_downloaded']}/{GLOBAL['pq_total']} | falhas: {GLOBAL['pq_falhados']}")

    completos = (GLOBAL['pq_downloaded'] + GLOBAL['pq_falhados']) >= GLOBAL['pq_total']
    marcar_fase("FASE2C_DOWNLOAD", completos and GLOBAL['pq_downloaded'] > 0,
                f"{GLOBAL['pq_downloaded']}/{GLOBAL['pq_total']} parquets")

# === USER UNKNOWN ===
async def baixar_unknowns_acumulado():
    definir_fase("FASE 2D - Blacklist persistente de USER_UNKNOWN")
    dizer("A carregar blacklist de USER_UNKNOWN...")
    baixou = False
    try:
        local = await download_hf_com_retry(
            HF_CHECKPOINT_REPO,
            "results/user_unknowns_acumulado.csv",
            "dataset"
        )
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
            dizer(f"  AVISO: blacklist local ilegivel ({str(e)[:80]})")
            unknowns = set()

    # Unir CSV + checkpoint + estado atual
    try:
        cp_unk = CHECKPOINT.carregar_unknowns()
    except Exception:
        cp_unk = set()
    atual = GLOBAL.get("user_unknowns") or set()
    GLOBAL["user_unknowns"] = unknowns | cp_unk | atual

    dizer(f"  USER_UNKNOWNs activos: {len(GLOBAL['user_unknowns']):,} "
          f"(CSV: {len(unknowns):,}, checkpoint: {len(cp_unk):,}) "
          f"{'[descarregado]' if baixou else '[ficheiro novo]'}")
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
        debug_log(f"salvar_unknown: {str(e)}")

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
            debug_log(f"registrar_aceite ficheiro: {str(e)}")
        return True
    except Exception as e:
        debug_log(f"registrar_aceite: {str(e)}")
        return False

def registrar_rejeicao(email, provedor, smtp_code, smtp_message):
    try:
        with REJEICOES_FILE_LOCK:
            file_exists = os.path.exists(SAIDA_REJEICAOES)
            with open(SAIDA_REJEICAOES, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["email", "provedor", "smtp_code", "smtp_message", "timestamp"])
                writer.writerow([email.lower(), provedor, smtp_code, str(smtp_message)[:200], datetime.now().isoformat()])
    except Exception as e:
        debug_log(f"registrar_rejeicao: {str(e)}")

# === PROXIES ===
REGEX_PROXY = re.compile(
    r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})["\'\s)\]][:|\t,;/]["\'\s(\[](\d{2,5})'
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
    if "socks5" in baixo: return "socks5"
    if "socks4" in baixo: return "socks4"
    if "socks" in baixo: return "socks5"
    if "http" in baixo: return "http"
    return "socks5"

def descobrir_ficheiros_proxy():
    candidatos = []
    dirs_vistos = set()
    bloqueados = {
        "emails_db", "logs", "audit", ".git", ".cache", ".ipynb_checkpoints",
        "campaign_v19_0", "campaign_v21_0", "sample_data", "trainer_repo", "__pycache__",
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

async def carregar_urls_proxies():
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
        dizer(f"  Ficheiros de proxies encontrados: {len(ficheiros)}")
        for fp in ficheiros[:25]:
            try:
                tam = os.path.getsize(fp)
            except Exception:
                tam = 0
            dizer(f"     - {fp} ({tam:,} bytes)")

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
            debug_log(f"ler {fp}: {str(e)}")

    dizer(f"  Linhas lidas: {linhas_lidas:,}")
    dizer(f"  URLs para scraping: {len(urls)}")
    dizer(f"  IPs diretos no ficheiro: {len(diretos)}")

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
        dizer(f"  IPs diretos adicionados: {adicionados}")

    return urls, diretos

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

async def scrape_url_async(session, url, proto):
    if GLOBAL.get("emergency_stop"):
        return "", proto, url
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
            },
            allow_redirects=True,
        ) as r:
            texto = await r.text()
            if r.status == 200 or len(texto) > 60:
                with GLOBAL["stats_lock"]:
                    GLOBAL["urls_scraped"] += 1
                return texto, proto, url
            with GLOBAL["stats_lock"]:
                GLOBAL["urls_failed"] += 1
            return "", proto, url
    except Exception:
        with GLOBAL["stats_lock"]:
            GLOBAL["urls_failed"] += 1
        return "", proto, url

async def carregar_e_scrapear_proxies_async(motivo="inicial"):
    dizer(f"FASE 2.5 - Scraping de proxies ({motivo})")
    urls, diretos = await carregar_urls_proxies()
    with GLOBAL["stats_lock"]:
        GLOBAL["urls_total"] = len(urls)
        GLOBAL["urls_scraped"] = 0
        GLOBAL["urls_failed"] = 0

    if not urls:
        dizer("  Sem URLs para scraping - a usar apenas IPs diretos")
        with GLOBAL["raw_lock"]:
            GLOBAL["proxies_extracted"] = len(GLOBAL["raw_proxies"])
        return GLOBAL["raw_proxies"]

    dizer(f"  A descarregar {len(urls)} listas em paralelo asyncio...")
    pares = [(u, inferir_protocolo_url(u)) for u in urls]

    connector = aiohttp.TCPConnector(limit=min(WORKERS_VALIDACAO_ASYNC, 500), limit_per_host=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(WORKERS_VALIDACAO_ASYNC)
        async def scrape_one(u, proto):
            async with sem:
                return await scrape_url_async(session, u, proto)

        tasks = [scrape_one(u, p) for u, p in pares]
        novos = 0
        inicio = time.time()

        for coro in asyncio.as_completed(tasks):
            if GLOBAL.get("emergency_stop"):
                break
            texto, proto, url = await coro
            if not texto:
                continue
            extraidos = extrair_proxies_do_texto(texto, proto)
            if not extraidos:
                continue
            with GLOBAL["raw_lock"]:
                vistos = GLOBAL["ips_vistos"]
                lista = GLOBAL["raw_proxies"]
                for item, p in extraidos:
                    if item in vistos:
                        continue
                    vistos.add(item)
                    lista.append((item, p))
                    novos += 1

        with GLOBAL["raw_lock"]:
            total_raw = len(GLOBAL["raw_proxies"])
        with GLOBAL["stats_lock"]:
            GLOBAL["proxies_extracted"] = total_raw

    dizer(f"  Scraping concluido em {time.time() - inicio:.0f}s")
    dizer(f"  Listas OK: {GLOBAL['urls_scraped']}/{GLOBAL['urls_total']} | falharam: {GLOBAL['urls_failed']}")
    dizer(f"  Novos proxies desta ronda: {novos:,}")
    dizer(f"  Candidatos acumulados: {total_raw:,}")
    return GLOBAL["raw_proxies"]

# === VALIDACAO DE PROXIES (ASYNCIO) ===
async def _obter_exit_ip_async(proxy_ip, proxy_port, proto):
    """Obtem exit IP de um proxy de forma assincrona."""
    servicos = [
        ("api.ipify.org", 80, b"GET /?format=json HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n", "ip"),
        ("ifconfig.me", 80, b"GET /ip HTTP/1.0\r\nHost: ifconfig.me\r\n\r\n", None),
        ("icanhazip.com", 80, b"GET / HTTP/1.0\r\nHost: icanhazip.com\r\n\r\n", None),
    ]
    loop = asyncio.get_event_loop()

    for host, port, request, json_key in servicos:
        try:
            def _connect():
                if proto in ('socks5', 'socks4'):
                    s = socks.socksocket()
                    if proto == 'socks5':
                        s.set_proxy(socks.SOCKS5, proxy_ip, proxy_port)
                    else:
                        s.set_proxy(socks.SOCKS4, proxy_ip, proxy_port)
                    s.settimeout(5)
                    s.connect((host, port))
                else:
                    s = socket.socket()
                    s.settimeout(5)
                    s.connect((proxy_ip, proxy_port))
                    s.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}\r\n\r\n".encode())
                    r = s.recv(4096)
                    if b"200" not in r:
                        s.close()
                        return None
                s.sendall(request)
                data = s.recv(8192)
                try:
                    s.close()
                except Exception:
                    pass
                return data

            data = await loop.run_in_executor(None, _connect)
            if not data:
                continue
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
            continue
    return proxy_ip

async def validar_proxy_rapido_async(item, proto_sugerido):
    """Valida um proxy de forma assincrona."""
    ip, port = item
    proto = proto_sugerido if proto_sugerido else 'socks5'
    MX_TESTE = "mxs.mail.ru"
    PORT25_TIMEOUT = 5

    if proto == 'socks5':
        ordem = ['socks5', 'socks4', 'http']
    elif proto == 'socks4':
        ordem = ['socks4', 'socks5', 'http']
    else:
        ordem = ['http', 'socks5', 'socks4']

    proto_funcional = None
    loop = asyncio.get_event_loop()

    for p in ordem:
        if GLOBAL.get("emergency_stop"):
            return None

        def _test_connect():
            try:
                if p in ('socks5', 'socks4'):
                    s = socks.socksocket()
                    if p == 'socks5':
                        s.set_proxy(socks.SOCKS5, ip, port)
                    else:
                        s.set_proxy(socks.SOCKS4, ip, port)
                    s.settimeout(PORT25_TIMEOUT)
                    s.connect((MX_TESTE, 25))
                else:
                    s = socket.socket()
                    s.settimeout(PORT25_TIMEOUT)
                    s.connect((ip, port))
                    s.sendall(f"CONNECT {MX_TESTE}:25 HTTP/1.1\r\nHost: {MX_TESTE}\r\n\r\n".encode())
                    r = s.recv(4096)
                    if b"200" not in r:
                        s.close()
                        return None
                banner = s.recv(4096).decode(errors="ignore").strip()
                try:
                    s.close()
                except Exception:
                    pass
                return banner
            except Exception:
                return None

        try:
            banner = await asyncio.wait_for(
                loop.run_in_executor(None, _test_connect),
                timeout=PORT25_TIMEOUT + 2
            )
            if banner and banner.startswith("220"):
                proto_funcional = p
                break
        except asyncio.TimeoutError:
            continue
        except Exception:
            continue

    if proto_funcional is None:
        with GLOBAL["stats_lock"]:
            GLOBAL["proxies_mortos_count"] += 1
        return None

    # Obtem exit IP
    exit_ip = await _obter_exit_ip_async(ip, port, proto_funcional)
    if not ip_publico_valido(exit_ip):
        exit_ip = ip

    info = {
        "ip": ip,
        "port": port,
        "proto": proto_funcional,
        "exit_ip": exit_ip,
        "added_at": time.time(),
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

async def validar_proxies_ate_target_async(target_count):
    """Valida proxies em paralelo ate atingir o target."""
    start_idx = GLOBAL["scraping_checkpoint"]
    with GLOBAL["raw_lock"]:
        raw = list(GLOBAL["raw_proxies"])
    total = len(raw)

    if total == 0:
        dizer("  AVISO: fila de validacao vazia")
        return 0

    if start_idx >= total:
        dizer(f"  Todos os {total:,} candidatos ja foram testados")
        return 0

    validos_iniciais = len(GLOBAL["pool"])
    if validos_iniciais >= target_count:
        dizer(f"  Pool ja tem {validos_iniciais} >= alvo {target_count}")
        return 0

    dizer(f"  A validar: {total - start_idx:,} candidatos a partir do indice {start_idx} "
          f"(pool {validos_iniciais}/{target_count})")

    sem = asyncio.Semaphore(WORKERS_VALIDACAO_ASYNC)
    validos_novos = 0
    next_i = start_idx

    async def validar_one(item, proto):
        async with sem:
            return await validar_proxy_rapido_async(item, proto)

    while next_i < total and not GLOBAL.get("emergency_stop"):
        # Processa em batches de 500
        batch_end = min(next_i + 500, total)
        batch = raw[next_i:batch_end]
        tasks = [validar_one(item, proto) for item, proto in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict) and result:
                validos_novos += 1

        next_i = batch_end
        GLOBAL["scraping_checkpoint"] = next_i

        dizer(f"  Progresso: {next_i}/{total} testados | {validos_novos} novos | pool {len(GLOBAL['pool'])}/{target_count}")

        if validos_iniciais + validos_novos >= target_count:
            break

    com_pool = len(GLOBAL["pool"])
    CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
    dizer(f"  Validacao terminada: +{validos_novos} | pool agora {com_pool}/{target_count}")
    if com_pool > 0:
        GLOBAL["spf_sujo"] = True
    return validos_novos

async def revalidar_pool_recuperado():
    """Revalida pool recuperado do checkpoint."""
    with GLOBAL["pool_lock"]:
        pool = list(GLOBAL["pool"])
    if not pool:
        return 0

    dizer(f"  Revalidando {len(pool)} proxies recuperados...")
    sem = asyncio.Semaphore(64)
    MX_TESTE = "mxs.mail.ru"
    PORT25_TIMEOUT = 5
    loop = asyncio.get_event_loop()

    async def _testar(p):
        async with sem:
            def _connect():
                try:
                    proto = p.get("proto", "socks5")
                    if proto in ('socks5', 'socks4'):
                        s = socks.socksocket()
                        if proto == 'socks5':
                            s.set_proxy(socks.SOCKS5, p["ip"], p["port"])
                        else:
                            s.set_proxy(socks.SOCKS4, p["ip"], p["port"])
                        s.settimeout(PORT25_TIMEOUT)
                        s.connect((MX_TESTE, 25))
                    else:
                        s = socket.socket()
                        s.settimeout(PORT25_TIMEOUT)
                        s.connect((p["ip"], p["port"]))
                        s.sendall(f"CONNECT {MX_TESTE}:25 HTTP/1.1\r\nHost: {MX_TESTE}\r\n\r\n".encode())
                        r = s.recv(4096)
                        if b"200" not in r:
                            s.close()
                            return False
                    banner = s.recv(4096).decode(errors="ignore").strip()
                    try:
                        s.close()
                    except Exception:
                        pass
                    return banner.startswith("220")
                except Exception:
                    return False
            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, _connect),
                    timeout=PORT25_TIMEOUT + 2
                )
            except asyncio.TimeoutError:
                return False

    tasks = [_testar(p) for p in pool]
    resultados = await asyncio.gather(*tasks, return_exceptions=True)

    vivos = []
    for p, vivo in zip(pool, resultados):
        if vivo is True:
            vivos.append(p)

    mortos = len(pool) - len(vivos)
    with GLOBAL["pool_lock"]:
        GLOBAL["pool"] = vivos
    if mortos > 0:
        GLOBAL["spf_sujo"] = True
        with GLOBAL["stats_lock"]:
            GLOBAL["proxies_mortos_count"] += mortos

    dizer(f"  Pool recuperado: {len(vivos)} vivos, {mortos} mortos removidos")
    CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
    return len(vivos)

# === DESEC DNS ===
def token_para_dominio(domain):
    with DOM_TOKEN_LOCK:
        if domain in DOM_TOKEN:
            return DOM_TOKEN[domain]
    for tok in DESEC_TOKENS_VALIDOS:
        if GLOBAL.get("emergency_stop"):
            return None
        try:
            r = requests.get(
                f"https://desec.io/api/v1/domains/{domain}/",
                headers={"Authorization": f"Token {tok}"},
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

async def desec_get_com_retry_async(url, tok, domain, max_retries=4):
    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            return None, "STOP"
        agora = time.time()
        espera_min = DESEC_MIN_DELAY - (agora - GLOBAL.get("desec_last_request", 0))
        if espera_min > 0:
            await asyncio.sleep(espera_min)
        inicio_req = time.time()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers={"Authorization": f"Token {tok}"},
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as r:
                    duracao_ms = (time.time() - inicio_req) * 1000
                    GLOBAL["desec_last_request"] = time.time()
                    body = await r.text()
                    if r.status == 200:
                        log_desec_request(domain=domain, url=url, method="GET",
                                          status_code=200, response_body=f"({len(body)} bytes)",
                                          headers={}, duration_ms=duracao_ms, attempt=tentativa + 1)
                        return r, None, body
                    if r.status == 429:
                        try:
                            wait = int(r.headers.get("Retry-After", 30))
                        except Exception:
                            wait = 30
                        wait = min(wait, DESEC_MAX_WAIT_429)
                        dizer(f"  Rate limit deSEC em GET {domain}. A esperar {wait}s...")
                        await asyncio.sleep(wait + 1)
                        continue
                    if r.status in (500, 502, 503, 504):
                        await asyncio.sleep(5 * (tentativa + 1))
                        continue
                    log_desec_request(domain=domain, url=url, method="GET",
                                      status_code=r.status, response_body=body,
                                      headers=dict(r.headers), duration_ms=duracao_ms,
                                      attempt=tentativa + 1)
                    return None, f"{r.status}: {body[:150]}", None
        except Exception as e:
            debug_log(f"desec_get {url} tentativa {tentativa + 1}: {str(e)[:140]}")
            await asyncio.sleep(3 * (tentativa + 1))
    return None, "MAX_RETRIES", None

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

async def obter_rrsets_atuais_async(domain, tok):
    atuais = {}
    url = f"https://desec.io/api/v1/domains/{domain}/rrsets/?cursor="
    paginas = 0
    while url and paginas < 60:
        if GLOBAL.get("emergency_stop"):
            return None
        r, erro, body = await desec_get_com_retry_async(url, tok, domain)
        if r is None:
            debug_log(f"obter_rrsets_atuais {domain}: {erro}")
            return None
        try:
            dados = json.loads(body) if body else []
        except Exception as e:
            debug_log(f"obter_rrsets_atuais json {domain}: {str(e)}")
            return None
        if not isinstance(dados, list):
            return None
        for rs in dados:
            if isinstance(rs, dict):
                chave = (str(rs.get("subname", "")), str(rs.get("type", "")))
                atuais[chave] = rs
        paginas += 1
        url = _parse_next_link(dict(r.headers)) if r else None
    return atuais

def _normalizar_record(tipo, s):
    try:
        s = str(s).strip()
        if tipo == "TXT":
            s = re.sub(r'"\s*"', '', s)
            s = s.strip().strip('"')
            s = s.replace("  ", " ").replace("\t", " ")
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
        if atual and (tamanho + tam > DESEC_CHUNK_BYTES or len(atual) >= DESEC_MAX_RRSETS_PER_REQ):
            chunks.append(atual)
            atual = []
            tamanho = 2
        atual.append(rs)
        tamanho += tam
    if atual:
        chunks.append(atual)
    return chunks

async def desec_put_com_retry_async(url, headers, payload, domain, max_retries=5):
    payload_preview = None
    try:
        if isinstance(payload, list) and len(payload) > 0:
            payload_preview = json.dumps(payload[0], indent=2)[:2000]
            payload_preview = f"(bulk com {len(payload)} rrsets; amostra) " + payload_preview
    except Exception as e:
        payload_preview = f"erro: {str(e)}"

    for tentativa in range(max_retries):
        if GLOBAL.get("emergency_stop"):
            return False, "STOP"
        inicio_req = time.time()
        agora = time.time()
        tempo_desde_ultima = agora - GLOBAL.get("desec_last_request", 0)
        if tempo_desde_ultima < DESEC_MIN_DELAY:
            await asyncio.sleep(DESEC_MIN_DELAY - tempo_desde_ultima)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=180)
                ) as r:
                    duracao_ms = (time.time() - inicio_req) * 1000
                    GLOBAL["desec_last_request"] = time.time()
                    body = await r.text()
                    log_desec_request(
                        domain=domain, url=url, method="PUT",
                        status_code=r.status, response_body=body,
                        headers=dict(r.headers), duration_ms=duracao_ms,
                        attempt=tentativa + 1, payload_preview=payload_preview
                    )
                    if r.status in (200, 201, 204):
                        return True, r.status
                    if r.status == 429:
                        try:
                            wait_hdr = int(r.headers.get("Retry-After", DESEC_BACKOFF_BASE))
                        except Exception:
                            wait_hdr = DESEC_BACKOFF_BASE
                        if wait_hdr > DESEC_MAX_WAIT_429:
                            dizer(f"  Rate limit LONGO em {domain} (Retry-After={wait_hdr}s > {DESEC_MAX_WAIT_429}s). A saltar.")
                            return False, "RATE_LIMIT_LONGO"
                        wait_calc = int(DESEC_BACKOFF_BASE * (DESEC_BACKOFF_MULTIPLIER ** tentativa))
                        wait = min(max(wait_hdr, wait_calc), DESEC_MAX_WAIT_429)
                        dizer(f"  Rate limit deSEC em {domain} (tent {tentativa + 1}/{max_retries}). Espera {wait}s...")
                        await asyncio.sleep(wait + 1)
                        continue
                    if r.status in (500, 502, 503, 504):
                        wait = 5 * (tentativa + 1)
                        await asyncio.sleep(wait)
                        continue
                    if r.status == 400:
                        dizer(f"  Erro 400 deSEC em {domain}: {body[:250]}")
                        return False, f"400: {body[:250]}"
                    if r.status == 404:
                        return False, f"404: {body[:150]}"
                    return False, f"{r.status}: {body[:150]}"
        except Exception as e:
            duracao_ms = (time.time() - inicio_req) * 1000
            log_desec_request(
                domain=domain, url=url, method="PUT",
                status_code=-1, response_body=str(e),
                headers={}, duration_ms=duracao_ms,
                attempt=tentativa + 1, payload_preview=payload_preview
            )
            if tentativa < max_retries - 1:
                await asyncio.sleep(3 * (tentativa + 1))
                continue
            return False, "MAX_RETRIES"
    return False, "MAX_RETRIES"

async def escrever_chunk_com_isolamento(url, headers, chunk, domain, profundidade=0):
    if not chunk:
        return 0, []
    if domain in GLOBAL.get("dominios_rate_limited", []):
        falhados = [(rs.get("subname"), rs.get("type"), "DOMAIN_RATE_LIMITED") for rs in chunk]
        return 0, falhados

    # Throttle sync (dentro de asyncio usa sleep)
    desec_throttle_dominio(domain)

    ok, status = await desec_put_com_retry_async(url, headers, chunk, domain, max_retries=4)
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
        ok1, falhas1 = await escrever_chunk_com_isolamento(url, headers, chunk[:meio], domain, profundidade + 1)
        ok2, falhas2 = await escrever_chunk_com_isolamento(url, headers, chunk[meio:], domain, profundidade + 1)
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
    """Obtem IPs para SPF: proxies do pool + Colab (se ainda nao foi adicionado)."""
    ips = set()
    # Colab sempre incluido (flag controla se ja foi escrito)
    if GLOBAL.get("ip_colab"):
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
            (base, "MX", [f"10 {base}.{domain}."]),
        ]
        for subname, tipo, records in candidatos:
            chave = (subname, tipo)
            if chave in vistos:
                continue
            vistos.add(chave)
            rrsets.append({
                "subname": subname,
                "type": tipo,
                "ttl": int(ttl),
                "records": records,
            })
    return rrsets

async def atualizar_dominio_completo_async(domain, contas_dom, spf_record):
    tok = token_para_dominio(domain)
    if not tok:
        dizer(f"    AVISO: Token nao encontrado para {domain}")
        return domain, 0, 0, len(contas_dom), "SEM_TOKEN"

    min_ttl = DOM_MIN_TTL.get(domain, DESEC_TTL)
    ttl_eff = max(DESEC_TTL, min_ttl)
    desejados = construir_rrsets_completos(contas_dom, spf_record, ttl=ttl_eff)
    total_desejados = len(desejados)
    bases = len(contas_dom)

    if total_desejados == 0:
        return domain, 0, 0, bases, "SEM_RRSETS"

    atuais = await obter_rrsets_atuais_async(domain, tok)
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
        dizer(f"    {domain}: {confirmados}/{total_desejados} rrsets ja corretos - 0 writes")
        return domain, 0, confirmados, bases, "SEM_MUDANCAS"

    chunks = dividir_rrsets_por_tamanho(para_escrever)
    dizer(f"    {domain}: {confirmados} ok, {len(para_escrever)} rrsets a escrever em {len(chunks)} chunk(s)")

    url = f"https://desec.io/api/v1/domains/{domain}/rrsets/"
    headers = {"Authorization": f"Token {tok}", "Content-Type": "application/json"}
    escritos_total = 0
    falhas = []

    for i, chunk in enumerate(chunks, 1):
        if GLOBAL.get("emergency_stop"):
            break
        ok_n, falhas_chunk = await escrever_chunk_com_isolamento(url, headers, chunk, domain)
        escritos_total += ok_n
        falhas.extend(falhas_chunk)
        if ok_n > 0:
            dizer(f"    Chunk {i}/{len(chunks)}: {ok_n} rrsets OK")
        if falhas_chunk and ok_n == 0:
            dizer(f"    Chunk {i}/{len(chunks)}: FALHOU ({len(falhas_chunk)} rrsets)")
        if domain in GLOBAL.get("dominios_rate_limited", []):
            break

    confirmados_final = confirmados + escritos_total
    if escritos_total > 0 and not falhas:
        status = "OK"
    elif confirmados_final > 0:
        status = "PARCIAL"
    else:
        status = "FALHOU"
    return domain, escritos_total, confirmados_final, bases, status

async def fase3_dns_completo_async(contas):
    definir_fase("FASE 3 - DNS bulk por dominio com diff (SPF+DKIM+DMARC+A+MX)")
    ips_spf = obter_ips_para_spf()
    spf_record = montar_spf_record(ips_spf)
    dizer(f"  SPF com {len(ips_spf)} IPs (proxies + Colab)")

    por_dom = defaultdict(list)
    for c in contas:
        por_dom[c["domain"]].append(c)

    total_bases = sum(len(v) for v in por_dom.values())
    GLOBAL["dns_total"] = total_bases
    with GLOBAL["stats_lock"]:
        GLOBAL["dominios_rate_limited"] = []

    dizer(f"  Dominios: {len(por_dom)} | bases: {total_bases}")
    dizer(f"  Throttling: global {DESEC_MIN_DELAY}s | por dominio {DESEC_PER_DOMAIN_WRITE_INTERVAL}s")

    ok_bases = 0
    escritos_total = 0
    falhados = 0
    sem_mudancas = 0

    dominios_lista = sorted(por_dom.keys())
    for idx, domain in enumerate(dominios_lista, 1):
        if GLOBAL.get("emergency_stop"):
            break
        contas_dom = por_dom[domain]
        dizer(f"  [{idx}/{len(dominios_lista)}] {domain} ({len(contas_dom)} bases)...")
        try:
            dom, escritos, confirmados, bases, status = await atualizar_dominio_completo_async(
                domain, contas_dom, spf_record)
        except Exception as e:
            debug_log(f"atualizar_dominio_completo {domain}: {traceback.format_exc()}")
            falhados += 1
            continue

        escritos_total += escritos
        with GLOBAL["stats_lock"]:
            GLOBAL["dns_escritos"] = GLOBAL.get("dns_escritos", 0) + escritos

        if status in ("SEM_MUDANCAS", "OK", "PARCIAL"):
            ok_bases += confirmados // 6
        if status == "SEM_MUDANCAS":
            sem_mudancas += 1
        dizer(f"    {status} ({confirmados} confirmados, {escritos} escritos)")
        if status not in ("SEM_MUDANCAS", "OK", "PARCIAL"):
            falhados += 1

    GLOBAL["dns_updated"] = ok_bases
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    GLOBAL["spf_record_escrito"] = spf_record
    GLOBAL["spf_ips_escritos"] = list(ips_spf)
    # Marca que Colab ja foi atualizado no SPF
    GLOBAL["colab_spf_atualizado"] = True

    dizer(f"  DNS: {ok_bases}/{total_bases} bases | {escritos_total} escritos | "
          f"{sem_mudancas} sem mudancas | {falhados} falhados")
    marcar_fase("FASE3_DNS", ok_bases > 0, f"{ok_bases}/{total_bases} bases, {falhados} falhados")
    return ok_bases > 0

async def refresh_spf_bulk_async(motivo=""):
    """Refresh SPF - so escreve se mudou. Colab so atualizado uma vez."""
    ips_spf = obter_ips_para_spf()
    if not ips_spf:
        return
    spf_record = montar_spf_record(ips_spf)

    if spf_record == GLOBAL.get("spf_record_escrito"):
        GLOBAL["spf_sujo"] = False
        GLOBAL["ultimo_refresh_spf"] = time.time()
        dizer(f"  Refresh SPF ({motivo}): inalterado - 0 writes")
        return

    dizer(f"  Refresh SPF ({motivo}) com {len(ips_spf)} IPs")
    por_dom = defaultdict(list)
    for c in GLOBAL["contas"]:
        por_dom[c["domain"]].append(c)

    escritos_total = 0
    doms_ok = 0

    for domain in sorted(por_dom.keys()):
        if GLOBAL.get("emergency_stop"):
            break
        if domain in GLOBAL.get("dominios_rate_limited", []):
            continue
        tok = token_para_dominio(domain)
        if not tok:
            continue
        min_ttl = DOM_MIN_TTL.get(domain, DESEC_TTL)
        ttl_eff = max(DESEC_TTL, min_ttl)
        rrsets = []
        vistos = set()
        for c in por_dom[domain]:
            base = c["base"]
            if (base, "TXT") in vistos:
                continue
            vistos.add((base, "TXT"))
            rrsets.append({
                "subname": base,
                "type": "TXT",
                "ttl": int(ttl_eff),
                "records": [spf_record],
            })
        chunks = dividir_rrsets_por_tamanho(rrsets)
        url = f"https://desec.io/api/v1/domains/{domain}/rrsets/"
        headers = {"Authorization": f"Token {tok}", "Content-Type": "application/json"}
        dom_ok = True
        for chunk in chunks:
            ok_n, falhas = await escrever_chunk_com_isolamento(url, headers, chunk, domain)
            escritos_total += ok_n
            if falhas:
                dom_ok = False
        if dom_ok:
            doms_ok += 1

    GLOBAL["spf_record_escrito"] = spf_record
    GLOBAL["spf_ips_escritos"] = list(ips_spf)
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    dizer(f"  SPF actualizado: {escritos_total} rrsets em {doms_ok}/{len(por_dom)} dominios")

async def fase4_esperar_propagacao():
    definir_fase(f"FASE 4 - Propagacao DNS ({DNS_PROPAGACAO_SEGUNDOS}s)")
    if fase_ja_completa("FASE4_PROPAGACAO"):
        dizer("  Fase 4 ja concluida - a saltar")
        return
    restante = DNS_PROPAGACAO_SEGUNDOS
    while restante > 0:
        if GLOBAL.get("emergency_stop"):
            break
        passo = min(10, restante)
        await asyncio.sleep(passo)
        restante -= passo
        if restante % 60 == 0 and restante > 0:
            dizer(f"  ... faltam {restante}s")
    dizer("  Propagacao concluida")

async def verificar_propagacao_amostra(contas, ip_colab):
    dizer("  Verificacao de propagacao (amostra de 5)")
    ips_spf = obter_ips_para_spf()
    if not contas:
        marcar_fase("FASE4_PROPAGACAO", True, "sem contas")
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
                txt = b" ".join(rdata.strings).decode(errors="ignore")
                if "v=spf1" in txt:
                    for ip in ips_spf[:8]:
                        if ("ip4:" + str(ip)) in txt:
                            ok += 1
                            break
                    break
        except Exception:
            pass
    dizer(f"  Propagacao confirmada: {ok}/{len(amostra)}")
    marcar_fase("FASE4_PROPAGACAO", True, f"{ok}/{len(amostra)} propagados")
    return ok >= max(1, len(amostra) // 2)

# === MUTACAO ABRACADABRA ===
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
        rid = f"<!-- m-{random.randint(100000, 999999)}-{int(time.time() * 1000) % 100000} -->"
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
        debug_log(f"mutar_html_abracadabra: {str(e)}")
        return html_base

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
    msg['From'] = f"{display_name} <{conta['sender_email']}>"
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
        smtp_log(f"DKIM falhou em {conta.get('identity_id')}: {str(e)}")
    return b

# === ORCAMENTO IP E RCPT DINAMICO ===
class OrcamentoIP:
    """Gere orcamento de emails por IP x provedor."""
    def __init__(self):
        self.lock = threading.Lock()
        self.data = CHECKPOINT.obter_orcamento()  # {ip: {provedor: {enviados, ultimo_ts}}}

    def pode_enviar(self, ip, provedor):
        """Verifica se IP pode enviar para provedor (orcamento + intervalo)."""
        with self.lock:
            ip_data = self.data.get(ip, {})
            prov_data = ip_data.get(provedor, {"enviados": 0, "ultimo_ts": 0})
            if prov_data.get("enviados", 0) >= MAX_EMAILS_IP_PROVEDOR:
                return False, "ORCAMENTO_ESGOTADO"
            agora = time.time()
            if agora - prov_data.get("ultimo_ts", 0) < MIN_INTERVALO_IP_PROVEDOR:
                return False, "INTERVALO"
            return True, "OK"

    def registar_envio(self, ip, provedor, n_emails=1):
        """Regista envio e atualiza contador."""
        with self.lock:
            if ip not in self.data:
                self.data[ip] = {}
            if provedor not in self.data[ip]:
                self.data[ip][provedor] = {"enviados": 0, "ultimo_ts": 0}
            self.data[ip][provedor]["enviados"] += n_emails
            self.data[ip][provedor]["ultimo_ts"] = time.time()

    def esta_esgotado(self, ip, provedor):
        with self.lock:
            ip_data = self.data.get(ip, {})
            prov_data = ip_data.get(provedor, {})
            return prov_data.get("enviados", 0) >= MAX_EMAILS_IP_PROVEDOR

    def reset_orcamento(self, ip):
        """Reset de orcamento para todos os provedores de um IP."""
        with self.lock:
            if ip in self.data:
                for prov in self.data[ip]:
                    self.data[ip][prov]["enviados"] = 0

    def salvar(self):
        with self.lock:
            CHECKPOINT.salvar_orcamento(self.data)

ORCAMENTO = OrcamentoIP()

class RCPTDinamico:
    """RCPT aprendido por provedor."""
    def __init__(self):
        self.lock = threading.Lock()
        self.data = CHECKPOINT.obter_rcpt_aprendido()
        self.sucessos_consecutivos = {}

    def obter(self, provedor):
        with self.lock:
            if provedor in self.data:
                return self.data[provedor]
            return RCPT_LIMITS.get(provedor, RCPT_DEFAULT)

    def registar_sucesso(self, provedor):
        with self.lock:
            rcpt_atual = self.data.get(provedor, RCPT_LIMITS.get(provedor, RCPT_DEFAULT))
            rcpt_inicial = RCPT_LIMITS.get(provedor, RCPT_DEFAULT)
            self.sucessos_consecutivos[provedor] = self.sucessos_consecutivos.get(provedor, 0) + 1
            if self.sucessos_consecutivos[provedor] >= 3:
                novo = min(rcpt_atual + 5, rcpt_inicial * 2)
                if novo != rcpt_atual:
                    self.data[provedor] = novo
                    self.sucessos_consecutivos[provedor] = 0

    def registar_erro(self, provedor, tipo="too_many"):
        with self.lock:
            rcpt_atual = self.data.get(provedor, RCPT_LIMITS.get(provedor, RCPT_DEFAULT))
            if tipo == "too_many":
                novo = max(rcpt_atual // 2, 1)
            else:
                novo = max(rcpt_atual // 2, 1)
            self.data[provedor] = novo
            self.sucessos_consecutivos[provedor] = 0

    def salvar(self):
        with self.lock:
            CHECKPOINT.salvar_rcpt_aprendido(self.data)

RCPT_CTRL = RCPTDinamico()

# === FASE 6: ENVIO MASSIVO ASYNCIO ===
class RetryQueueAsync:
    def __init__(self):
        self.items = deque()
        self.tentativas = {}
        self.lock = threading.Lock()

    def adicionar(self, email, provedor):
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

RETRY_QUEUE = RetryQueueAsync()

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
            return "REPUTACAO", "Problema de reputacao"
        return "PERMANENTE", "Rejeicao permanente"
    return "DESCONHECIDA", f"Codigo {code}"

async def enviar_email_smtp_async(worker_id, proxy_info, conta, email, mx_host, html_base):
    """Envia um email via SMTP usando aiosmtplib."""
    prov = email.split("@")[-1] if "@" in email else ""
    timeout = SMTP_TIMEOUT_ASIATICO if prov in PROVEDORES_ASIATICOS else SMTP_TIMEOUT
    proxy_key = f"{proxy_info['ip']}:{proxy_info['port']}" if proxy_info else "colab"

    try:
        # Conecta ao MX via proxy ou direto
        if proxy_info:
            # Usa proxy SOCKS
            loop = asyncio.get_event_loop()

            def _connect_proxy():
                proto = proxy_info.get("proto", "socks5")
                if proto in ('socks5', 'socks4'):
                    s = socks.socksocket()
                    if proto == 'socks5':
                        s.set_proxy(socks.SOCKS5, proxy_info["ip"], proxy_info["port"])
                    else:
                        s.set_proxy(socks.SOCKS4, proxy_info["ip"], proxy_info["port"])
                    s.settimeout(timeout)
                    s.connect((mx_host, 25))
                else:
                    s = socket.socket()
                    s.settimeout(timeout)
                    s.connect((proxy_info["ip"], proxy_info["port"]))
                    s.sendall(f"CONNECT {mx_host}:25 HTTP/1.1\r\nHost: {mx_host}\r\n\r\n".encode())
                    r = s.recv(4096)
                    if b"200" not in r:
                        s.close()
                        raise ConnectionError("CONNECT fail")
                return s

            sock = await asyncio.wait_for(
                loop.run_in_executor(None, _connect_proxy),
                timeout=timeout + 3
            )
            banner = sock.recv(4096).decode(errors="ignore").strip()
            if not banner.startswith("220"):
                sock.close()
                return False, "BANNER_INVALIDO"
        else:
            # Conexao direta (Colab)
            sock = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: socket.create_connection((mx_host, 25), timeout=timeout)
                ),
                timeout=timeout + 3
            )
            banner = sock.recv(4096).decode(errors="ignore").strip()
            if not banner.startswith("220"):
                sock.close()
                return False, "BANNER_INVALIDO"

        with GLOBAL["stats_lock"]:
            GLOBAL["smtp_conexoes"] += 1

        # Handshake SMTP simples (sem TLS para performance)
        sock.sendall(f"EHLO {conta['identity_id']}\r\n".encode())
        ehlo_resp = sock.recv(8192).decode(errors="ignore")

        # MAIL FROM
        sock.sendall(f"MAIL FROM: <{conta['sender_email']}>\r\n".encode())
        mr = sock.recv(4096).decode(errors="ignore")
        if not mr.startswith("250"):
            sock.close()
            return False, "MAIL_FROM_REJEITADO"

        # RCPT TO (apenas 1 destinatario por conexao para simplicidade)
        rcpt_atual = RCPT_CTRL.obter(prov)
        sock.sendall(f"RCPT TO: <{email}>\r\n".encode())
        rr = sock.recv(4096).decode(errors="ignore")

        if rr.startswith("250") or rr.startswith("251"):
            # Sucesso RCPT
            RCPT_CTRL.registar_sucesso(prov)
        elif "452" in rr or "552" in rr:
            RCPT_CTRL.registar_erro(prov, "too_many")
            sock.sendall(b"RSET\r\n")
            sock.close()
            return False, "RCPT_TOO_MANY"
        elif rr.startswith("4"):
            sock.sendall(b"RSET\r\n")
            sock.close()
            return False, "RCPT_TEMP"
        else:
            sock.sendall(b"RSET\r\n")
            sock.close()
            return False, "RCPT_REJEITADO"

        # DATA
        sock.sendall(b"DATA\r\n")
        dr = sock.recv(4096).decode(errors="ignore")
        if not dr.startswith("354"):
            sock.sendall(b"RSET\r\n")
            sock.close()
            return False, "DATA_REJEITADO"

        # Payload
        payload = aplicar_mutacao_abracadabra(html_base, conta, [email])
        if not payload.endswith(b"\r\n"):
            payload += b"\r\n"
        payload = payload.replace(b"\r\n.", b"\r\n..")
        sock.sendall(payload)
        sock.sendall(b"\r\n.\r\n")

        fr = sock.recv(4096).decode(errors="ignore")
        try:
            sock.sendall(b"QUIT\r\n")
            sock.close()
        except Exception:
            pass

        if fr.startswith("250"):
            return True, "OK"
        else:
            return False, f"DATA_FINAL_{fr[:3]}"

    except asyncio.TimeoutError:
        return False, "TIMEOUT"
    except Exception as e:
        return False, f"ERRO_{str(e)[:50]}"

async def worker_envio_async(worker_id, proxy_info, conta, email_queue, html_base, mxs_por_provedor):
    """Worker asyncio que processa emails da queue global."""
    enviados_total = 0
    timeouts_consecutivos = 0
    erros_421 = 0
    proxy_key = f"{proxy_info['ip']}:{proxy_info['port']}" if proxy_info else "colab"
    via_proxy = proxy_info is not None

    dizer(f"  [Worker #{worker_id:03d}] iniciado | proxy={proxy_key} | conta={conta['identity_id']}")

    while not GLOBAL.get("emergency_stop"):
        # Verifica se atingiu orcamento
        if enviados_total >= MAX_EMAILS_IP_PROVEDOR * 10:  # Limite global por worker
            dizer(f"  [Worker #{worker_id:03d}] orcamento global esgotado ({enviados_total} emails) - hibernar")
            break

        # Verifica 421 consecutivos
        if erros_421 >= 5:
            dizer(f"  [Worker #{worker_id:03d}] 5x 421 - hibernar 60s")
            await asyncio.sleep(60)
            erros_421 = 0

        # Verifica timeouts consecutivos
        if timeouts_consecutivos >= WORKER_TIMEOUTS_CONSECUTIVOS_MAX:
            dizer(f"  [Worker #{worker_id:03d}] {timeouts_consecutivos} timeouts consecutivos - hibernar 60s")
            await asyncio.sleep(60)
            timeouts_consecutivos = 0

        # Pega email da queue
        try:
            email = await asyncio.wait_for(email_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue

        if not email or "@" not in email:
            email_queue.task_done()
            continue

        # Verifica USER_UNKNOWN
        with GLOBAL["user_unknowns_lock"]:
            if email.lower() in GLOBAL["user_unknowns"]:
                email_queue.task_done()
                continue

        prov = email.split("@")[-1].lower()

        # Verifica orcamento IP x provedor
        pode, razao = ORCAMENTO.pode_enviar(proxy_key, prov)
        if not pode:
            # Devolve email a queue e espera
            email_queue.put_nowait(email)
            email_queue.task_done()
            await asyncio.sleep(1)
            continue

        # Obtem MX do provedor
        mx_list = mxs_por_provedor.get(prov, [])
        if not mx_list:
            email_queue.task_done()
            continue

        # Tenta cada MX
        sucesso = False
        for mx_host in mx_list:
            if GLOBAL.get("emergency_stop"):
                break

            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_tentativas"] += 1

            inicio = time.time()
            ok, motivo = await enviar_email_smtp_async(
                worker_id, proxy_info, conta, email, mx_host, html_base
            )
            response_time = time.time() - inicio

            if ok:
                # Sucesso
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_aceites"] += 1
                    if via_proxy:
                        GLOBAL["smtp_via_proxy"] += 1
                    else:
                        GLOBAL["smtp_via_colab"] += 1
                enviados_total += 1
                timeouts_consecutivos = 0
                erros_421 = 0
                ORCAMENTO.registar_envio(proxy_key, prov)
                registrar_aceite(email, prov, conta["identity_id"], mx_host)
                RETRY_QUEUE.limpar(email, prov)
                sucesso = True
                break
            else:
                # Falha
                if "TIMEOUT" in motivo:
                    timeouts_consecutivos += 1
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_erros"] += 1
                elif "421" in motivo or "TOO_MANY" in motivo:
                    erros_421 += 1
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_4xx"] += 1
                    RCPT_CTRL.registar_erro(prov, "too_many")
                    RETRY_QUEUE.adicionar(email, prov)
                elif "RCPT_TEMP" in motivo or "TEMP" in motivo:
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_4xx"] += 1
                    RETRY_QUEUE.adicionar(email, prov)
                elif "USER_UNKNOWN" in motivo:
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_user_unknown"] += 1
                    salvar_unknown(email, prov)
                else:
                    with GLOBAL["stats_lock"]:
                        if "REJEITADO" in motivo:
                            GLOBAL["smtp_5xx"] += 1
                            GLOBAL["smtp_rejeitados"] += 1
                        else:
                            GLOBAL["smtp_erros"] += 1
                    registrar_rejeicao(email, prov, 0, motivo)

                # Se proxy morreu, sai do loop de MX
                if "ERRO" in motivo and "Connection" in motivo:
                    break

        email_queue.task_done()

        # Salva checkpoint periodicamente
        if enviados_total % CHECKPOINT_SAVE_INTERVAL == 0 and enviados_total > 0:
            ORCAMENTO.salvar()
            RCPT_CTRL.salvar()

    # Fim do worker
    ORCAMENTO.salvar()
    RCPT_CTRL.salvar()
    dizer(f"  [Worker #{worker_id:03d}] terminado | {enviados_total} emails enviados")

async def fase6_envio_massivo_async(mxs, contas, html_base):
    """Fase 6: Envio massivo com 501 workers asyncio."""
    definir_fase("FASE 6 - Envio massivo ASYNCIO (501 workers)")

    faltam = fases_faltantes_para_envio()
    if faltam:
        dizer(f"  FASE 6 BLOQUEADA - fases faltantes: {', '.join(faltam)}")
        marcar_fase("FASE6_ENVIO", False, f"bloqueada por: {', '.join(faltam)}")
        return

    if not contas:
        dizer("  FASE 6 BLOQUEADA - sem contas")
        marcar_fase("FASE6_ENVIO", False, "sem contas")
        return

    # Cria queue global de emails
    email_queue = asyncio.Queue(maxsize=100000)

    # Carrega emails de todos os provedores para a queue
    dizer("  A carregar emails para a queue global...")
    emails_carregados = 0
    for prov in mxs.keys():
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            continue
        cp_info = CHECKPOINT.obter_provedor_index(prov)
        if cp_info and cp_info.get("concluido", False):
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                offset = 0
                if cp_info:
                    offset = cp_info.get("offset", 0)
                for i, linha in enumerate(f):
                    if i < offset:
                        continue
                    email = linha.strip()
                    if email and "@" in email:
                        await email_queue.put(email)
                        emails_carregados += 1
        except Exception as e:
            debug_log(f"carregar emails {prov}: {str(e)}")

    dizer(f"  {emails_carregados:,} emails carregados na queue")

    if emails_carregados == 0:
        marcar_fase("FASE6_ENVIO", False, "0 emails pendentes")
        return

    # Adiciona retries prontos
    retries = RETRY_QUEUE.obter_prontos(max_items=10000)
    for email, prov, attempts in retries:
        await email_queue.put(email)

    # Determina numero de workers (min: proxies_vivos + 1 Colab, max: 501)
    with GLOBAL["pool_lock"]:
        n_proxies = len(GLOBAL["pool"])
    n_workers = min(n_proxies + 1, len(contas), WORKERS_ENVIO_ASYNC)

    if n_workers == 0:
        dizer("  AVISO: sem proxies nem Colab - nao ha workers")
        marcar_fase("FASE6_ENVIO", False, "sem workers")
        return

    dizer(f"  A criar {n_workers} workers asyncio ({n_proxies} proxies + 1 Colab)")
    dizer(f"  Contas disponiveis: {len(contas)}")

    # Atribui proxies e contas aos workers
    workers = []
    with GLOBAL["pool_lock"]:
        pool = list(GLOBAL["pool"])

    for i in range(n_workers):
        if i < len(pool):
            proxy_info = pool[i]
        else:
            proxy_info = None  # Worker Colab
        conta = contas[i % len(contas)]
        workers.append((i + 1, proxy_info, conta))

    # Inicia workers
    tasks = []
    for worker_id, proxy_info, conta in workers:
        task = asyncio.create_task(
            worker_envio_async(worker_id, proxy_info, conta, email_queue, html_base, mxs)
        )
        tasks.append(task)

    GLOBAL["n_workers_ativos"] = len(tasks)
    marcar_fase("FASE6_ENVIO_INICIO", True, f"{len(tasks)} workers ativos")

    # Monitor de progresso
    async def monitor_progresso():
        while not GLOBAL.get("emergency_stop") and not email_queue.empty():
            await asyncio.sleep(30)
            qsize = email_queue.qsize()
            with GLOBAL["stats_lock"]:
                aceitos = GLOBAL["smtp_aceites"]
                tentativas = GLOBAL["smtp_tentativas"]
            taxa = aceitos / max(1, tentativas) * 100
            dizer(f"  [PROGRESSO] Queue: {qsize:,} | Aceites: {aceitos:,} | Tentativas: {tentativas:,} | Taxa: {taxa:.1f}%")

    monitor_task = asyncio.create_task(monitor_progresso())

    # Aguarda workers
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        debug_log(f"fase6 gather: {str(e)}")

    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    # Salva estado final
    ORCAMENTO.salvar()
    RCPT_CTRL.salvar()

    with GLOBAL["stats_lock"]:
        aceitos = GLOBAL["smtp_aceites"]
        tentativas = GLOBAL["smtp_tentativas"]

    marcar_fase("FASE6_ENVIO", True, f"{aceitos} aceites / {tentativas} tentativas")
    dizer(f"  FASE 6 concluida: {aceitos:,} aceites / {tentativas:,} tentativas")

# === DASHBOARD 3D HTML ===
def gerar_dashboard_3d_html(rt):
    """Gera dashboard HTML profissional com dados reais."""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    campaign_id = GLOBAL.get("campaign_id", "N/A")
    exec_id = GLOBAL.get("exec_id", "N/A")
    campaign_started = GLOBAL.get("campaign_started_at", "N/A")
    run_started = GLOBAL.get("run_started_at", 0)
    if run_started:
        try:
            run_started_str = datetime.fromtimestamp(run_started).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            run_started_str = "N/A"
    else:
        run_started_str = "N/A"

    elapsed = time.time() - GLOBAL["start_time"]
    h, m, s = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)

    # Estatisticas
    aceitos = rt.get("aceites", 0)
    tentativas = rt.get("tentados", 0)
    rejeitados = rt.get("rejeitados", 0)
    erros = rt.get("erros", 0)
    taxa = aceitos / max(1, tentativas) * 100

    # Checkpoint info
    config = CHECKPOINT.obter_config()
    total_runs = config.get("total_runs", 0)
    status = config.get("status", "UNKNOWN")

    # Proxies
    pool_size = len(GLOBAL.get("pool", []))
    mortos = GLOBAL.get("proxies_mortos_count", 0)
    banidos = GLOBAL.get("proxies_banidos", 0)

    # DNS
    dns_ok = GLOBAL.get("dns_updated", 0)
    dns_total = GLOBAL.get("dns_total", 0)

    # Historico de runs
    run_history = config.get("run_history", [])

    # Por provedor
    por_provedor = rt.get("por_provedor", {})

    html = f"""<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VERISCOPE V21.0.0 Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: 'Segoe UI', Tahoma, sans-serif;
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    color: #fff;
    min-height: 100vh;
    padding: 20px;
}}
.container {{ max-width: 1400px; margin: 0 auto; }}
h1 {{
    text-align: center;
    font-size: 2.5em;
    margin-bottom: 10px;
    background: linear-gradient(45deg, #00d4ff, #7b2ff7);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-shadow: 0 0 30px rgba(0, 212, 255, 0.3);
}}
.subtitle {{
    text-align: center;
    color: #888;
    margin-bottom: 30px;
}}
.grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
    margin-bottom: 30px;
}}
.card {{
    background: rgba(255, 255, 255, 0.05);
    border-radius: 15px;
    padding: 20px;
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.1);
    transition: transform 0.3s, box-shadow 0.3s;
}}
.card:hover {{
    transform: translateY(-5px);
    box-shadow: 0 10px 40px rgba(0, 212, 255, 0.2);
}}
.card-title {{
    font-size: 0.9em;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 10px;
}}
.card-value {{
    font-size: 2.5em;
    font-weight: bold;
    margin-bottom: 5px;
}}
.card-subtitle {{
    font-size: 0.85em;
    color: #aaa;
}}
.ok {{ color: #00ff88; }}
.warn {{ color: #ffaa00; }}
.error {{ color: #ff4466; }}
.info {{ color: #00d4ff; }}

/* Cubo 3D */
.cube-container {{
    perspective: 1000px;
    width: 200px;
    height: 200px;
    margin: 40px auto;
}}
.cube {{
    width: 100%;
    height: 100%;
    position: relative;
    transform-style: preserve-3d;
    animation: rotate 20s infinite linear;
}}
@keyframes rotate {{
    from {{ transform: rotateX(0) rotateY(0); }}
    to {{ transform: rotateX(360deg) rotateY(360deg); }}
}}
.face {{
    position: absolute;
    width: 200px;
    height: 200px;
    background: rgba(0, 212, 255, 0.15);
    border: 2px solid rgba(0, 212, 255, 0.5);
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    font-size: 14px;
    backdrop-filter: blur(5px);
}}
.face .label {{ font-size: 11px; color: #888; margin-bottom: 5px; }}
.face .value {{ font-size: 24px; font-weight: bold; color: #00d4ff; }}
.face.front  {{ transform: translateZ(100px); }}
.face.back   {{ transform: rotateY(180deg) translateZ(100px); }}
.face.right  {{ transform: rotateY(90deg) translateZ(100px); }}
.face.left   {{ transform: rotateY(-90deg) translateZ(100px); }}
.face.top    {{ transform: rotateX(90deg) translateZ(100px); }}
.face.bottom {{ transform: rotateX(-90deg) translateZ(100px); }}

/* Tabela */
table {{
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
    background: rgba(255, 255, 255, 0.03);
    border-radius: 10px;
    overflow: hidden;
}}
th, td {{
    padding: 12px 15px;
    text-align: left;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}}
th {{
    background: rgba(0, 212, 255, 0.2);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.85em;
    letter-spacing: 1px;
}}
tr:hover {{ background: rgba(255, 255, 255, 0.05); }}

.section-title {{
    font-size: 1.5em;
    margin: 30px 0 15px;
    color: #00d4ff;
    border-bottom: 2px solid rgba(0, 212, 255, 0.3);
    padding-bottom: 10px;
}}

.footer {{
    text-align: center;
    color: #666;
    margin-top: 40px;
    font-size: 0.85em;
}}

/* Progress bar */
.progress {{
    background: rgba(255, 255, 255, 0.1);
    border-radius: 10px;
    height: 8px;
    overflow: hidden;
    margin-top: 10px;
}}
.progress-bar {{
    height: 100%;
    background: linear-gradient(90deg, #00d4ff, #7b2ff7);
    transition: width 0.3s;
}}
</style>
</head>
<body>
<div class="container">
    <h1>🚀 VERISCOPE V21.0.0</h1>
    <div class="subtitle">Dashboard Profissional | Gerado em {agora}</div>

    <!-- Cubo 3D -->
    <div class="cube-container">
        <div class="cube">
            <div class="face front">
                <div class="label">ACEITES</div>
                <div class="value">{aceitos:,}</div>
            </div>
            <div class="face back">
                <div class="label">TENTATIVAS</div>
                <div class="value">{tentativas:,}</div>
            </div>
            <div class="face right">
                <div class="label">TAXA</div>
                <div class="value">{taxa:.1f}%</div>
            </div>
            <div class="face left">
                <div class="label">PROXIES</div>
                <div class="value">{pool_size}</div>
            </div>
            <div class="face top">
                <div class="label">STATUS</div>
                <div class="value">{status}</div>
            </div>
            <div class="face bottom">
                <div class="label">RUNS</div>
                <div class="value">{total_runs}</div>
            </div>
        </div>
    </div>

    <!-- Cards principais -->
    <div class="grid">
        <div class="card">
            <div class="card-title">🎯 Taxa de Sucesso</div>
            <div class="card-value {'ok' if taxa > 10 else 'warn' if taxa > 5 else 'error'}">{taxa:.2f}%</div>
            <div class="card-subtitle">{aceitos:,} de {tentativas:,} tentativas</div>
            <div class="progress"><div class="progress-bar" style="width: {min(taxa, 100):.1f}%"></div></div>
        </div>
        <div class="card">
            <div class="card-title">⏱️ Tempo de Execucao</div>
            <div class="card-value info">{h:02d}:{m:02d}:{s:02d}</div>
            <div class="card-subtitle">Limite: 05:40:00</div>
        </div>
        <div class="card">
            <div class="card-title">🔌 Pool de Proxies</div>
            <div class="card-value {'ok' if pool_size >= 100 else 'warn' if pool_size >= 10 else 'error'}">{pool_size}</div>
            <div class="card-subtitle">Mortos: {mortos:,} | Banidos: {banidos:,}</div>
        </div>
        <div class="card">
            <div class="card-title">🔐 DNS Configurado</div>
            <div class="card-value ok">{dns_ok:,}/{dns_total:,}</div>
            <div class="card-subtitle">Bases confirmadas</div>
        </div>
        <div class="card">
            <div class="card-title">📧 Rejeitados</div>
            <div class="card-value error">{rejeitados:,}</div>
            <div class="card-subtitle">5xx permanentes</div>
        </div>
        <div class="card">
            <div class="card-title">⚠️ Erros</div>
            <div class="card-value warn">{erros:,}</div>
            <div class="card-subtitle">Conexoes falhadas</div>
        </div>
    </div>

    <!-- Informacao da campanha -->
    <h2 class="section-title">📋 Informacao da Campanha</h2>
    <div class="grid">
        <div class="card">
            <div class="card-title">Campaign ID</div>
            <div class="card-value" style="font-size: 1.2em;">{campaign_id}</div>
        </div>
        <div class="card">
            <div class="card-title">Execution ID</div>
            <div class="card-value" style="font-size: 1.2em;">{exec_id}</div>
        </div>
        <div class="card">
            <div class="card-title">Inicio Campanha</div>
            <div class="card-value" style="font-size: 1.2em;">{campaign_started}</div>
        </div>
        <div class="card">
            <div class="card-title">Inicio Run Atual</div>
            <div class="card-value" style="font-size: 1.2em;">{run_started_str}</div>
        </div>
        <div class="card">
            <div class="card-title">Total de Runs</div>
            <div class="card-value info">{total_runs}</div>
        </div>
        <div class="card">
            <div class="card-title">Status</div>
            <div class="card-value {'ok' if status == 'COMPLETED' else 'info'}">{status}</div>
        </div>
    </div>

    <!-- Historico de runs -->
    <h2 class="section-title">📜 Historico de Execucoes</h2>
    <table>
        <tr>
            <th>Run #</th>
            <th>Exec ID</th>
            <th>Inicio</th>
            <th>Fim</th>
            <th>Status</th>
            <th>Motivo</th>
        </tr>"""

    for i, run in enumerate(run_history[-10:], 1):  # Ultimas 10 runs
        html += f"""
        <tr>
            <td>{i}</td>
            <td>{run.get('exec_id', 'N/A')}</td>
            <td>{run.get('run_started_at', 'N/A')}</td>
            <td>{run.get('run_ended_at', 'N/A')}</td>
            <td class="{'ok' if run.get('status') == 'COMPLETED' else 'warn'}">{run.get('status', 'N/A')}</td>
            <td>{run.get('motivo', 'N/A')}</td>
        </tr>"""

    html += """
    </table>

    <!-- Por provedor -->
    <h2 class="section-title">📊 Resultados por Provedor (Top 20)</h2>
    <table>
        <tr>
            <th>Provedor</th>
            <th>Aceites</th>
            <th>Tentados</th>
            <th>Rejeitados</th>
            <th>Erros</th>
            <th>Taxa</th>
        </tr>"""

    sorted_provs = sorted(por_provedor.items(), key=lambda x: x[1].get("aceites", 0), reverse=True)[:20]
    for prov, r in sorted_provs:
        taxa_prov = r.get("aceites", 0) / max(1, r.get("tentados", 0)) * 100
        html += f"""
        <tr>
            <td>{prov}</td>
            <td class="ok">{r.get('aceites', 0):,}</td>
            <td>{r.get('tentados', 0):,}</td>
            <td class="error">{r.get('rejeitados', 0):,}</td>
            <td class="warn">{r.get('erros', 0):,}</td>
            <td class="{'ok' if taxa_prov > 10 else 'warn' if taxa_prov > 5 else 'error'}">{taxa_prov:.1f}%</td>
        </tr>"""

    html += f"""
    </table>

    <!-- Footer -->
    <div class="footer">
        <p>VERISCOPE V21.0.0 | Arquitectura Asyncio | 501 Workers</p>
        <p>Checkpoints: {CP_DIR} | Repositorio: {HF_CHECKPOINT_REPO}</p>
        <p>Ultima atualizacao: {agora}</p>
    </div>
</div>
</body>
</html>"""

    return html

# === MONITOR DE PROXIES ===
async def monitor_proxies_continuo_async():
    """Monitor continuo de proxies com revalidacao."""
    dizer("[MONITOR] Thread de monitoramento de proxies iniciada")

    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        if not GLOBAL.get("scraping_inicial_feito"):
            await asyncio.sleep(10)
            continue

        await asyncio.sleep(60)

        with GLOBAL["pool_lock"]:
            pool_atual = len(GLOBAL["pool"])

        # Refresh SPF se necessario
        decorrido_refresh = time.time() - GLOBAL.get("ultimo_refresh_spf", 0)
        if (GLOBAL.get("spf_sujo") and
            decorrido_refresh >= SPF_REFRESH_INTERVALO_MINIMO and
            fase_ja_completa("FASE3_DNS")):
            try:
                await refresh_spf_bulk_async("agendado")
            except Exception as e:
                debug_log(f"refresh SPF agendado: {str(e)}")

        # Refill se pool baixo
        if pool_atual < MIN_PROXIES_REFILL:
            dizer(f"[MONITOR] Pool baixo ({pool_atual}/{TARGET_VALID_PROXIES}) - refill")
            try:
                # Re-scrape se necessario
                with GLOBAL["raw_lock"]:
                    n_raw = len(GLOBAL["raw_proxies"])
                if GLOBAL["scraping_checkpoint"] >= n_raw:
                    await carregar_e_scrapear_proxies_async(motivo="refill-monitor")
                await validar_proxies_ate_target_async(TARGET_VALID_PROXIES)
            except Exception as e:
                debug_log(f"refill monitor: {str(e)}")

# === CHECKPOINTS ===
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
        debug_log(f"salvar_checkpoint_smtp: {str(e)}")

async def periodic_checkpoint_thread_async():
    """Checkpoint periodico LOCAL apenas (sem upload HF)."""
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        await asyncio.sleep(CHECKPOINT_TIME_INTERVAL)
        if GLOBAL.get("emergency_stop"):
            break
        dizer("[CHECKPOINT 1h] A salvar estado local...")
        try:
            salvar_checkpoint_smtp()
            CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status=GLOBAL.get("status", "RUNNING"))
            CHECKPOINT.salvar_unknowns(GLOBAL.get("user_unknowns", set()))
            CHECKPOINT.salvar_aceites(GLOBAL.get("aceites_emails", set()))
            ORCAMENTO.salvar()
            RCPT_CTRL.salvar()
            # Gera manifesto
            ok_count, manifest = CHECKPOINT.salvar_tudo_com_manifesto()
            GLOBAL["ultimo_checkpoint_tempo"] = time.time()
            dizer(f"[CHECKPOINT 1h] Estado salvo localmente ({ok_count} ficheiros)")
        except Exception as e:
            debug_log(f"Erro checkpoint 1h: {str(e)}")

async def time_limit_watcher_async():
    """Watchdog para limite de 5h40m. Usa run_started_at (nao campaign_started_at)."""
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        # Calcula tempo desde o INICIO DA RUN ATUAL (nao da campanha)
        run_started = GLOBAL.get("run_started_at", GLOBAL["start_time"])
        elapsed = time.time() - run_started
        if elapsed >= MAX_EXECUTION_SECONDS:
            dizer("[TIME LIMIT] 5h40m atingidos. Iniciando shutdown inteligente...")
            GLOBAL["emergency_stop"] = True
            GLOBAL["status"] = "TIME_LIMIT_SHUTDOWN"
            await asyncio.sleep(5)
            try:
                salvar_checkpoint_smtp()
                CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
                CHECKPOINT.registar_fim_run(motivo="TIME_LIMIT", status_final="TIME_LIMIT_SHUTDOWN")
                CHECKPOINT.salvar_unknowns(GLOBAL["user_unknowns"])
                CHECKPOINT.salvar_aceites(GLOBAL["aceites_emails"])
                ORCAMENTO.salvar()
                RCPT_CTRL.salvar()
                # Upload HF apenas no shutdown
                upload_all_checkpoints_to_hf()
            except Exception as e:
                debug_log(f"Erro no shutdown: {str(e)}")
            if GLOBAL.get("status") != "COMPLETED":
                trigger_next_run()
            dizer("[TIME LIMIT] Shutdown completo.")
            os._exit(0)
        await asyncio.sleep(30)

# === FUNCOES FINAIS ===
async def upload_unknowns_hf():
    if not os.path.exists(UNKNOWN_FILE_LOCAL):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        criar_repo_hf_se_nao_existe(HF_CHECKPOINT_REPO, "dataset")
        api.upload_file(
            path_or_fileobj=UNKNOWN_FILE_LOCAL,
            path_in_repo="results/user_unknowns_acumulado.csv",
            repo_id=HF_CHECKPOINT_REPO,
            repo_type="dataset",
            token=HF_TOKEN
        )
        dizer(f"  USER_UNKNOWNs enviados ({len(GLOBAL['user_unknowns']):,})")
    except Exception as e:
        dizer(f"  AVISO: upload blacklist falhou ({str(e)[:100]})")

async def gerar_ficheiros_finais():
    definir_fase("GERACAO DE FICHEIROS FINAIS")
    dizer("A gerar ficheiro de aceites 250 OK...")
    try:
        aceites_set = set()
        with GLOBAL["aceites_lock"]:
            aceites_set = set(GLOBAL.get("aceites_emails", set()))
        if os.path.exists(SAIDA_ACEITES) and os.path.getsize(SAIDA_ACEITES) > 60:
            dizer(f"  Ficheiro {SAIDA_ACEITES} ja existe com {len(aceites_set):,} aceites")
        else:
            with open(SAIDA_ACEITES, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                ts = datetime.now().isoformat()
                for email in aceites_set:
                    writer.writerow([email, "", "", "", ts])
            dizer(f"  Ficheiro {SAIDA_ACEITES} criado com {len(aceites_set):,} aceites")
    except Exception as e:
        dizer(f"  ERRO ao gerar ficheiro de aceites: {str(e)}")

    dizer("A gerar ficheiro final de user_unknowns...")
    try:
        if os.path.exists(UNKNOWN_FILE_LOCAL):
            shutil.copy(UNKNOWN_FILE_LOCAL, SAIDA_UNKNOWN)
            with GLOBAL["user_unknowns_lock"]:
                n_unk = len(GLOBAL["user_unknowns"])
            dizer(f"  Ficheiro {SAIDA_UNKNOWN} criado com {n_unk:,} user_unknowns")
    except Exception as e:
        dizer(f"  ERRO ao copiar user_unknowns: {str(e)}")

    dizer("A enviar ficheiros para HuggingFace...")
    try:
        api = HfApi(token=HF_TOKEN)
        criar_repo_hf_se_nao_existe(HF_CHECKPOINT_REPO, "dataset")
        ts_upload = datetime.now().strftime('%Y%m%d_%H%M%S')
        for fpath, prefix in [(SAIDA_ACEITES, "aceites"), (SAIDA_UNKNOWN, "unknowns"), (SAIDA_REJEICAOES, "rejeicoes")]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(
                        path_or_fileobj=fpath,
                        path_in_repo=f"results/{prefix}_{ts_upload}.csv",
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    dizer(f"  {prefix} enviado para HuggingFace")
                except Exception as e:
                    dizer(f"  ERRO upload {prefix}: {str(e)[:120]}")
    except Exception as e:
        dizer(f"  ERRO geral no upload: {str(e)}")

async def enviar_email_unico(dest, assunto, html, conta):
    """Envia email unico para relatorio final."""
    try:
        dominio = dest.split("@")[-1]
        try:
            answers = dns.resolver.resolve(dominio, 'MX')
            mxs_sorted = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
            mx_list = [mm[1] for mm in mxs_sorted]
        except Exception:
            return False

        if not mx_list:
            return False

        # Tenta com proxies primeiro
        for mx_host in mx_list[:3]:
            # Tenta com cada proxy
            with GLOBAL["pool_lock"]:
                pool = list(GLOBAL["pool"])
            for proxy_info in pool[:5]:
                ok, motivo = await enviar_email_smtp_async(999, proxy_info, conta, dest, mx_host, html)
                if ok:
                    return True

            # Fallback: Colab direto
            ok, motivo = await enviar_email_smtp_async(999, None, conta, dest, mx_host, html)
            if ok:
                return True

        return False
    except Exception as e:
        debug_log(f"enviar_email_unico: {traceback.format_exc()}")
        return False

async def salvar_logs_hf():
    if GLOBAL.get("logs_uploaded"):
        return
    try:
        api = HfApi(token=HF_TOKEN)
        criar_repo_hf_se_nao_existe(HF_CHECKPOINT_REPO, "dataset")
        enviados = 0
        for fname, fpath in [("log", LOG_FILE), ("smtp", SMTP_LOG), ("debug", DEBUG_LOG), ("desec", DESEC_LOG)]:
            if not fpath or not os.path.exists(fpath):
                continue
            try:
                api.upload_file(
                    path_or_fileobj=fpath,
                    path_in_repo=f"logs/{fname}_{GLOBAL['exec_id']}.txt",
                    repo_id=HF_CHECKPOINT_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                enviados += 1
            except Exception as e:
                debug_log(f"upload log {fname}: {str(e)[:160]}")
        dizer(f"  Logs enviados para HuggingFace: {enviados}/4")
        GLOBAL["logs_uploaded"] = True
    except Exception as e:
        dizer(f"  AVISO: upload de logs falhou ({str(e)[:100]})")

async def baixar_ficheiros_campanha():
    dizer("A baixar ficheiros de campanha do HuggingFace...")
    try:
        local_proxies = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/proxies.txt",
            repo_type="dataset",
            token=HF_TOKEN,
            force_download=True
        )
        os.makedirs(DIR_PROXIES, exist_ok=True)
        shutil.copy(local_proxies, os.path.join(DIR_PROXIES, "proxies.txt"))
        dizer(f"  proxies.txt descarregado para {DIR_PROXIES}")
    except Exception:
        pass
    try:
        local_mailfrom = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename=f"campaign_data/templates/{MAIL_FROM_FILE_LOCAL}",
            repo_type="dataset",
            token=HF_TOKEN,
            force_download=True
        )
        garantir_diretorio_para(os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
        shutil.copy(local_mailfrom, os.path.join(BASE_LOCAL, MAIL_FROM_FILE_LOCAL))
        dizer(f"  mail_from descarregado")
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
    if not preservar_saida:
        for f in [SAIDA_ACEITES, SAIDA_UNKNOWN, SAIDA_REJEICAOES]:
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass

# === DASHBOARD LIVE ===
async def render_dashboard_live():
    """Renderiza dashboard no console a cada 10s."""
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        try:
            elapsed = time.time() - GLOBAL["start_time"]
            h, m, s = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)

            with GLOBAL["stats_lock"]:
                aceitos = GLOBAL["smtp_aceites"]
                tentativas = GLOBAL["smtp_tentativas"]
                rejeitados = GLOBAL["smtp_rejeitados"]
                erros = GLOBAL["smtp_erros"]
                user_unk = GLOBAL["smtp_user_unknown"]
                via_proxy = GLOBAL["smtp_via_proxy"]
                via_colab = GLOBAL["smtp_via_colab"]
                smtp_4xx = GLOBAL.get("smtp_4xx", 0)
                smtp_5xx = GLOBAL.get("smtp_5xx", 0)
                smtp_retries = GLOBAL.get("smtp_retries", 0)
                conexoes = GLOBAL["smtp_conexoes"]

            pool_size = len(GLOBAL.get("pool", []))
            rate = aceitos / elapsed if elapsed > 0 else 0
            taxa = aceitos / max(1, tentativas) * 100
            disco = espaco_disco_livre_gb()

            print(CLEAR_SCREEN, end="", flush=True)
            print("╔" + "═" * 78 + "╗", flush=True)
            print(f"║ 🚀 VERISCOPE V21.0.0 | ASYNCIO 501 WORKERS | {GLOBAL.get('exec_id', '')[:20]:20s} ║", flush=True)
            print(f"║ ⏱️ {h:02d}:{m:02d}:{s:02d} | 💾 {disco:.0f}GB | 📊 Status: {GLOBAL.get('status', 'RUNNING'):30s} ║", flush=True)
            print("╠" + "═" * 78 + "╣", flush=True)
            print(f"║ 🌐 IP: {GLOBAL.get('ip_colab', '---'):15s} | 📧 Contas: {len(GLOBAL.get('contas', [])):4d} | 📦 PQ: {GLOBAL.get('pq_downloaded', 0)}/{GLOBAL.get('pq_total', 0)} ║", flush=True)
            print(f"║ 🏢 Provs: {GLOBAL.get('prov_processed', 0)}/{GLOBAL.get('prov_total', 0):3d} | 🌐 MXs: {GLOBAL.get('mx_found', 0):4d} | 🔐 DNS: {GLOBAL.get('dns_updated', 0)}/{GLOBAL.get('dns_total', 0)} ║", flush=True)
            print("╠" + "═" * 78 + "╣", flush=True)
            print(f"║ 🔌 Proxies: {pool_size:3d}/{TARGET_VALID_PROXIES:3d} | 💀 Mortos: {GLOBAL.get('proxies_mortos_count', 0):,6d} | 🚫 Ban: {GLOBAL.get('proxies_banidos', 0):5d} ║", flush=True)
            print(f"║ 📤 Tentativas: {tentativas:8,d} | ⚡ {rate:6.1f}/s | 🔌 Con: {conexoes:8,d} | 🏆 Concl: {len(GLOBAL.get('provedores_concluidos_set', set()))} ║", flush=True)
            print(f"║ SMTP: ✅{aceitos:7,d} ❌{rejeitados:7,d} ⚠️{erros:7,d} 👻{user_unk:7,d} | 4xx:{smtp_4xx:6,d} 5xx:{smtp_5xx:6,d} ║", flush=True)
            print(f"║ 📡 Via Proxy: {via_proxy:7,d} | 🖥️ Via Colab: {via_colab:7,d} | 💡 Taxa: {taxa:6.2f}% | Retry:{smtp_retries:5,d} ║", flush=True)
            print("╠" + "═" * 78 + "╣", flush=True)
            print(f"║ 🛑 touch /tmp/veriscope_stop para parar | 📋 Dashboard: {DIR_AUDIT}/dashboard.html ║", flush=True)
            print("╠" + "═" * 78 + "╣", flush=True)

            logs = list(GLOBAL["log_buffer"])[-6:]
            for log in logs:
                l = log[:76] if len(log) <= 76 else log[:74] + ".."
                print(f"║  {l:76s} ║", flush=True)
            for _ in range(6 - len(logs)):
                print(f"║ {'':76s} ║", flush=True)

            print("╚" + "═" * 78 + "╝", flush=True)

        except Exception:
            pass
        await asyncio.sleep(10)

# === MAIN ===
async def main():
    global LOG_FILE, SMTP_LOG, DEBUG_LOG

    diagnostico_ambiente_hf()
    execution_id = reset_global_state()

    garantir_diretorios()
    n_baixados = download_all_checkpoints_from_hf()
    CHECKPOINT._carregar_tudo()

    config = CHECKPOINT.obter_config()

    # Verifica se campanha ja foi completada
    if config.get("status") == "COMPLETED":
        print("=" * 70, flush=True)
        print("[FIM] A campanha ja foi totalmente concluida em execucao anterior.", flush=True)
        print("[FIM] Status: COMPLETED. A sair sem enviar nada.", flush=True)
        print("=" * 70, flush=True)
        sys.exit(0)

    tem_checkpoint = CHECKPOINT.existe_checkpoint()

    if tem_checkpoint:
        print("", flush=True)
        print("=" * 70, flush=True)
        print("[RECUPERACAO] Checkpoint detectado - a retomar execucao anterior", flush=True)
        print("=" * 70, flush=True)
        GLOBAL["modo_recuperacao"] = True
        GLOBAL["checkpoint_loaded"] = True
        GLOBAL["status"] = "RUNNING"

        # IMPORTANTE: Recupera campaign_id e campaign_started_at (persistentes)
        if config.get("campaign_id"):
            GLOBAL["campaign_id"] = config["campaign_id"]
        if config.get("campaign_started_at"):
            GLOBAL["campaign_started_at"] = config["campaign_started_at"]

        # GERA NOVO exec_id e run_started_at para esta run
        GLOBAL["exec_id"] = execution_id
        GLOBAL["run_started_at"] = time.time()  # Nova run = novo relogio
        GLOBAL["start_time"] = time.time()

        if config.get("ip_colab"):
            GLOBAL["ip_colab"] = config["ip_colab"]

        # Recupera estatisticas SMTP acumuladas
        stats_antigos = CHECKPOINT.obter_smtp_stats()
        for k, v in stats_antigos.items():
            if k in GLOBAL:
                GLOBAL[k] = v
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]

        # Recupera pool
        pool_antigo, scraping_cp, raw_count = CHECKPOINT.carregar_pool()
        GLOBAL["pool"] = pool_antigo
        GLOBAL["scraping_checkpoint"] = scraping_cp

        # Recupera por_provedor
        GLOBAL["por_provedor"] = CHECKPOINT.obter_por_provedor()
        por_prov_cp = CHECKPOINT.obter_por_provedor()
        for prov_nome, prov_info in por_prov_cp.items():
            if isinstance(prov_info, dict) and prov_info.get("concluido", False):
                GLOBAL["provedores_concluidos_set"].add(prov_nome)
        GLOBAL["provs_concluidos"] = len(GLOBAL["provedores_concluidos_set"])

        # Recupera fases
        with GLOBAL["stats_lock"]:
            for chave, info in CHECKPOINT.states["fases"].items():
                GLOBAL["fases"][chave] = info

        # Recupera unknowns e aceites
        try:
            unknowns_cp = CHECKPOINT.carregar_unknowns()
            if unknowns_cp:
                GLOBAL["user_unknowns"] = set(GLOBAL.get("user_unknowns", set())) | unknowns_cp
                dizer(f"[RECUPERACAO] User unknowns: {len(unknowns_cp):,}")
            aceites_cp = CHECKPOINT.carregar_aceites()
            if aceites_cp:
                GLOBAL["aceites_emails"] = set(GLOBAL.get("aceites_emails", set())) | aceites_cp
                dizer(f"[RECUPERACAO] Aceites: {len(aceites_cp):,}")
        except Exception as e:
            debug_log(f"recuperar unknowns/aceites: {str(e)}")

        # Recupera dominio principal
        dominio_cp = CHECKPOINT.obter_dominio_principal()
        if dominio_cp:
            GLOBAL["dominio_principal"] = dominio_cp

        # Recupera orcamento e RCPT
        GLOBAL["orcamento_ip"] = CHECKPOINT.obter_orcamento()
        GLOBAL["rcpt_aprendido"] = CHECKPOINT.obter_rcpt_aprendido()

        # Atualiza config com novo exec_id e run_started_at
        CHECKPOINT.salvar_config(execution_id, GLOBAL.get("ip_colab"), status="RUNNING",
                                  campaign_id=GLOBAL.get("campaign_id"))

        limpar_logs_antigos(preservar_checkpoints=True, preservar_saida=True)
    else:
        dizer(f"[CHECKPOINT] Nenhum checkpoint (baixados da HF: {n_baixados}) - execucao NOVA")
        # Nova campanha = novo campaign_id
        GLOBAL["campaign_id"] = f"camp_{int(time.time())}"
        GLOBAL["campaign_started_at"] = datetime.now().isoformat()
        limpar_logs_antigos(preservar_checkpoints=False, preservar_saida=False)

    # Configura logs
    LOG_FILE = os.path.join(LOG_DIR, f"log_{execution_id}.txt")
    SMTP_LOG = os.path.join(LOG_DIR, f"smtp_{execution_id}.txt")
    DEBUG_LOG = os.path.join(LOG_DIR, f"debug_{execution_id}.txt")
    try:
        open(LOG_FILE, "w", encoding="utf-8").close()
        open(SMTP_LOG, "w", encoding="utf-8").close()
        open(DEBUG_LOG, "w", encoding="utf-8").close()
        open(DESEC_LOG, "w", encoding="utf-8").close()
    except Exception:
        pass

    GLOBAL["user_unknowns_file"] = UNKNOWN_FILE_LOCAL

    # Inicia tasks de background
    dashboard_task = asyncio.create_task(render_dashboard_live())
    checkpoint_task = asyncio.create_task(periodic_checkpoint_thread_async())
    timelimit_task = asyncio.create_task(time_limit_watcher_async())

    await asyncio.sleep(1)

    dizer(f"VERISCOPE V21.0.0 ASYNCIO [{execution_id}]")
    dizer(f"Campaign ID: {GLOBAL.get('campaign_id')}")
    dizer(f"Modo: {'RECUPERACAO' if tem_checkpoint else 'NOVO'}")
    dizer(f"Workers: {WORKERS_ENVIO_ASYNC} (500 proxies + 1 Colab)")
    dizer(f"Download: {WORKERS_DOWNLOAD_ASYNC} | Validacao: {WORKERS_VALIDACAO_ASYNC}")

    if tem_checkpoint:
        dizer(f"  Aceites recuperados: {GLOBAL['smtp_aceites']:,}")
        dizer(f"  Tentativas recuperadas: {GLOBAL['smtp_tentativas']:,}")
        dizer(f"  Proxies no pool: {len(GLOBAL['pool'])}")
        dizer(f"  User unknowns: {len(GLOBAL['user_unknowns']):,}")

    await baixar_ficheiros_campanha()

    contas = []
    html_base = ""
    mxs = {}

    try:
        # FASE 0: IP Colab
        ip_colab = await obter_ip_colab()
        CHECKPOINT.salvar_config(execution_id, ip_colab, status="RUNNING",
                                  campaign_id=GLOBAL.get("campaign_id"))
        GLOBAL["status"] = "RUNNING"
        await verificar_blacklists(ip_colab)

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 1: Tokens deSEC
        await testar_tokens_desec()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 2A: Contas inteligentes (501 do dominio principal)
        contas = await carregar_contas_inteligente()
        html_base = await carregar_template()
        await carregar_mail_from_variations()

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 2B: MXs
        provedores = await descobrir_provedores_reais()
        mxs = await resolver_mxs_paralelo(provedores)

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 2C: Download parquets
        await baixar_todos_emails_paralelo(sorted(mxs.keys()))

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 2D: Blacklist USER_UNKNOWN
        definir_fase("FASE 2D - Blacklist persistente de USER_UNKNOWN")
        await baixar_unknowns_acumulado()

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 2.5: Proxies
        definir_fase("FASE 2.5 - Proxies: scraping + validacao massiva")
        pool_ok = False
        if len(GLOBAL["pool"]) >= MIN_PROXIES_REFILL:
            dizer(f"  Pool recuperado: {len(GLOBAL['pool'])} proxies")
            vivos = await revalidar_pool_recuperado()
            if vivos >= MIN_PROXIES_REFILL:
                pool_ok = True
                marcar_fase("FASE25_PROXIES", True, f"pool revalidado {vivos}")
            else:
                dizer(f"  Pool morreu ({vivos} vivos) - scraping novo")

        if not pool_ok:
            try:
                await carregar_e_scrapear_proxies_async(motivo="inicial")
            except Exception as e:
                dizer(f"  AVISO: scraping falhou ({str(e)[:140]})")
            try:
                await validar_proxies_ate_target_async(TARGET_VALID_PROXIES)
            except Exception as e:
                dizer(f"  AVISO: validacao falhou ({str(e)[:140]})")

        with GLOBAL["pool_lock"]:
            n_pool = len(GLOBAL["pool"])
        with GLOBAL["raw_lock"]:
            n_raw = len(GLOBAL["raw_proxies"])

        proxy_ok = (n_pool > 0) or (n_raw > 0)
        marcar_fase("FASE25_PROXIES", proxy_ok,
                    f"pool={n_pool} raw={n_raw:,}")

        if n_pool == 0:
            dizer("  AVISO: pool vazio. Sem proxies nao ha envio.")

        GLOBAL["scraping_inicial_feito"] = True
        GLOBAL["ultimo_rescrape"] = time.time()

        # Monitor de proxies
        monitor_task = asyncio.create_task(monitor_proxies_continuo_async())

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 3: DNS bulk
        await fase3_dns_completo_async(contas)

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 4: Propagacao
        await fase4_esperar_propagacao()
        await verificar_propagacao_amostra(contas, ip_colab)

        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()

        # FASE 6: Envio massivo asyncio
        await fase6_envio_massivo_async(mxs, contas, html_base)

    except KeyboardInterrupt:
        GLOBAL["emergency_stop"] = True
        dizer("Interrompido pelo utilizador")
    except Exception as e:
        dizer(f"ERRO GLOBAL: {str(e)}")
        debug_log(f"main:\n{traceback.format_exc()}")
        traceback.print_exc()
    finally:
        GLOBAL["monitor_running"] = False

        # Cancela tasks de background
        for task in [dashboard_task, checkpoint_task, timelimit_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        try:
            dizer("[CHECKPOINT] Salvamento final...")
            salvar_checkpoint_smtp()
            CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))

            final_status = GLOBAL.get("status", "RUNNING")
            if final_status not in ("COMPLETED", "TIME_LIMIT_SHUTDOWN"):
                final_status = "RUNNING"

            # Regista fim da run
            motivo = "NORMAL" if final_status == "COMPLETED" else "INTERRUPTED"
            CHECKPOINT.registar_fim_run(motivo=motivo, status_final=final_status)

            CHECKPOINT.salvar_unknowns(GLOBAL["user_unknowns"])
            CHECKPOINT.salvar_aceites(GLOBAL["aceites_emails"])
            ORCAMENTO.salvar()
            RCPT_CTRL.salvar()

            # Gera manifesto e salva tudo
            ok_count, manifest = CHECKPOINT.salvar_tudo_com_manifesto()
            dizer(f"[CHECKPOINT] {ok_count} ficheiros salvos com manifesto")

            # Upload HF apenas no SHUTDOWN
            upload_all_checkpoints_to_hf()
            dizer("[CHECKPOINT] Estado salvo e enviado para HF")

        except Exception as e:
            dizer(f"ERRO ao salvar checkpoint final: {str(e)}")
            debug_log(f"checkpoint final: {traceback.format_exc()}")

        try:
            await gerar_ficheiros_finais()
        except Exception as e:
            dizer(f"ERRO ao gerar ficheiros finais: {str(e)}")

        # Compila resultados
        rt = {
            "aceites": 0, "tentados": 0, "rejeitados": 0, "erros": 0,
            "via_proxy": 0, "via_colab": 0,
            "por_provedor": GLOBAL["por_provedor"]
        }
        for r in GLOBAL["por_provedor"].values():
            for k in ("aceites", "tentados", "rejeitados", "erros", "via_proxy", "via_colab"):
                if k in r:
                    rt[k] += r[k]

        # Usa contadores globais se por_provedor vazio
        if rt["aceites"] == 0:
            rt["aceites"] = GLOBAL.get("smtp_aceites", 0)
            rt["tentados"] = GLOBAL.get("smtp_tentativas", 0)
            rt["rejeitados"] = GLOBAL.get("smtp_rejeitados", 0)
            rt["erros"] = GLOBAL.get("smtp_erros", 0)
            rt["via_proxy"] = GLOBAL.get("smtp_via_proxy", 0)
            rt["via_colab"] = GLOBAL.get("smtp_via_colab", 0)

        if GLOBAL.get("emergency_stop"):
            dizer("Paragem de emergencia - estado guardado")
        else:
            dizer(f"FASE 7 - Relatorio final para {FINAL_DESTINATARIO}")
            cf = random.choice(contas) if contas else None
            if cf and html_base:
                ok1 = await enviar_email_unico(FINAL_DESTINATARIO, CAMPAIGN["subject"], html_base, cf)
                dizer(f"  Email da campanha: {'OK' if ok1 else 'FALHOU'}")
                await asyncio.sleep(5)

        # Gera dashboard HTML
        try:
            dashboard_html = gerar_dashboard_3d_html(rt)
            dashboard_path = os.path.join(DIR_AUDIT, "dashboard.html")
            with open(dashboard_path, "w", encoding="utf-8") as f:
                f.write(dashboard_html)
            dizer(f"  Dashboard salvo: {dashboard_path}")

            # Upload dashboard para HF
            try:
                api = HfApi(token=HF_TOKEN)
                criar_repo_hf_se_nao_existe(HF_CHECKPOINT_REPO, "dataset")
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                api.upload_file(
                    path_or_fileobj=dashboard_path,
                    path_in_repo=f"reports/dashboard_v21_{ts}.html",
                    repo_id=HF_CHECKPOINT_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                dizer("  Dashboard enviado para HuggingFace")
            except Exception as e:
                dizer(f"  AVISO: upload dashboard falhou ({str(e)[:110]})")
        except Exception as e:
            dizer(f"  ERRO ao gerar dashboard: {str(e)}")

        # Upload unknowns e logs
        try:
            await upload_unknowns_hf()
        except Exception:
            pass
        try:
            await salvar_logs_hf()
        except Exception:
            pass

        # Resumo final
        taxa = rt['aceites'] / max(1, rt['tentados']) * 100
        print("", flush=True)
        print("=" * 70, flush=True)
        print(f"EXECUCAO TERMINADA - {GLOBAL.get('exec_id')}", flush=True)
        print(f"  Campaign ID     : {GLOBAL.get('campaign_id')}", flush=True)
        print(f"  Aceites (250 OK): {rt['aceites']:,}", flush=True)
        print(f"  Tentativas      : {rt['tentados']:,}", flush=True)
        print(f"  Taxa            : {taxa:.2f}%", flush=True)
        print(f"  Via proxy       : {rt['via_proxy']:,}", flush=True)
        print(f"  Via Colab       : {rt['via_colab']:,}", flush=True)
        print(f"  Pool final      : {len(GLOBAL['pool'])}/{TARGET_VALID_PROXIES}", flush=True)
        print(f"  Banidos         : {GLOBAL['proxies_banidos']}", flush=True)
        print(f"  USER_UNKNOWN    : {len(GLOBAL['user_unknowns']):,}", flush=True)
        print(f"  Parquets        : {GLOBAL['pq_downloaded']}/{GLOBAL['pq_total']}", flush=True)
        print(f"  DNS confirmado  : {GLOBAL.get('dns_updated', 0)}/{GLOBAL.get('dns_total', 0)}", flush=True)
        print(f"  DESec Requests  : {GLOBAL.get('desec_requests', 0)} | OK: {GLOBAL.get('desec_success', 0)}", flush=True)
        print(f"  Dashboard       : {DIR_AUDIT}/dashboard.html", flush=True)
        print(f"  MODO RECUPERACAO: {'SIM' if GLOBAL.get('modo_recuperacao') else 'NAO'}", flush=True)
        print(f"  STATUS FINAL    : {GLOBAL.get('status', 'UNKNOWN')}", flush=True)
        print("=" * 70, flush=True)

# Entry point
if __name__ == "__main__":
    asyncio.run(main())
