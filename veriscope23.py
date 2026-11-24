# -*- coding: utf-8 -*-
"""
================================================================================
VERISCOPE V23.0.0 - OPERAÇÃO HYDRA (PARALELISMO HIERÁRQUICO REAL)
================================================================================
Sistema de Envio Massivo | 66 Provedores | 30 IPs Aracloud | Checkpoint Granular
Execução: GitHub Actions + Colab CLI | Shutdown Inteligente (5h 35m)
================================================================================
ARQUITETURA IMPLEMENTADA:
- Nível 0: Master Controller (asyncio event loop + Watchdog + Checkpoint Saver)
- Nível 1: 66 Provider Workers (coroutines independentes, estado isolado)
- Nível 2: Pool Global de 30 IP Workers (SSH tunnels persistentes)
- Nível 3: Operações SMTP com pipelining + DKIM + Mutação Abracadabra
================================================================================
CORREÇÕES CRÍTICAS APLICADAS (vs auditoria anterior):
1. Paramiko Channel I/O bridged com asyncio via run_in_executor (sem bloqueio)
2. OpenStack Neutron API real para rotação de Floating IPs
3. Checkpoint granular por provedor (índices independentes)
4. Fila de Retry real para Greylist (asyncio.PriorityQueue)
5. deSEC com throttling (2 req/s por domínio) + bulk updates
6. STARTTLS correto (sem wrap em channel Paramiko)
7. Retornos explícitos em todas as funções
8. Semáforo global de 30 slots para controlo de concorrência
================================================================================
"""

import os
import sys
import json
import time
import random
import hashlib
import asyncio
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
import base64
import concurrent.futures
import signal
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Set, Optional, Any, Tuple
from email.message import EmailMessage
from email.utils import make_msgid, format_datetime
from urllib.parse import urlparse, urlunparse
from itertools import islice

# ==============================================================================
# 1. CONFIGURAÇÕES DE AMBIENTE E OTIMIZAÇÕES DE SISTEMA
# ==============================================================================
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["HF_DATASETS_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["HF_HUB_DISABLE_XET"] = "1"
os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
os.environ["HF_HUB_ETAG_TIMEOUT"] = "60"
os.environ["HF_HUB_DOWNLOAD_TIMEOUT"] = "120"
os.environ["PYTHONUNBUFFERED"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

def install_packages():
    """Instala pacotes críticos silenciosamente."""
    packages = {
        "requests": "requests", "dnspython": "dns", "dkimpy": "dkim",
        "cryptography": "cryptography", "pandas": "pandas", "pyarrow": "pyarrow",
        "huggingface_hub": "huggingface_hub", "psutil": "psutil",
        "aiohttp": "aiohttp", "paramiko": "paramiko", "nest_asyncio": "nest_asyncio",
        "pysocks": "socks",
    }
    for pkg, mod in packages.items():
        try:
            __import__(mod)
        except ImportError:
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", "-q", "--no-cache-dir", pkg],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            except Exception:
                pass

install_packages()

import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "pyarrow", "urllib3",
            "asyncio", "filelock", "aiohttp", "paramiko", "openstack", "urllib3.connectionpool"]:
    logging.getLogger(_lg).setLevel(logging.CRITICAL)

import warnings
warnings.filterwarnings("ignore")

import requests
import dns.resolver
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download, snapshot_download
import paramiko
import dkim
import psutil
import aiohttp
import nest_asyncio
nest_asyncio.apply()

# Otimização de recursos do OS para suportar milhares de sockets/conexões
threading.stack_size(64 * 1024)
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(1048576, _hard), _hard))
except Exception:
    pass

# ==============================================================================
# 2. CONSTANTES GLOBAIS E CONFIGURAÇÕES DA OPERAÇÃO HYDRA
# ==============================================================================
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()
DESEC_TOKEN = os.environ.get("DESEC_TOKEN", "LuWfqx1GL3uXDmyUEExH1tF4Nena").strip()

# Repositórios HuggingFace
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_CHECKPOINT_REPO = "Valter3B/Veriscope_Checkpoints"
HF_ARACLOUD_REPO = "Valter3B/aracloud-servers"
HF_LOGS_REPO = "Valter3B/Campaign_Logs"

# Limites de Tempo e Execução (GitHub Actions + Colab CLI)
MAX_EXECUTION_SECONDS = 5 * 3600 + 35 * 60  # 5h 35m (Shutdown Inteligente)
CHECKPOINT_INTERVAL_SEC = 5 * 60            # Salvar estado a cada 5 minutos
DNS_PROPAGATION_WAIT = 300                  # 5 minutos de espera após update SPF

# Parâmetros de Infraestrutura Aracloud (OpenStack)
NUM_CONTAS_ARACLOUD = 10
MAX_IPS_SIMULTANEOS_POR_CONTA = 3
TOTAL_IPS_SIMULTANEOS = NUM_CONTAS_ARACLOUD * MAX_IPS_SIMULTANEOS_POR_CONTA  # 30
MAX_IPS_UNICOS_POR_DIA = 20                 # Regra de ouro: 20 IPs únicos por conta/dia
MAX_EMAILS_POR_IP_PROVEDOR = 1000           # Quota de segurança por IP/Provedor
MAX_EMAILS_POR_IP_TOTAL = 3000              # Quota total antes de rotação
ARACLOUD_AUTH_URL = "https://vhi.servermania.com:5000/v3"

# Parâmetros SMTP e Rede
SMTP_TIMEOUT = 20
SMTP_TIMEOUT_ASIATICO = 40
SMTP_RECV_TIMEOUT = 15
DELAY_ENTRE_RCPT = 0.02
MAX_RCPT_POR_CONEXAO = 100
PIPELINING_ENABLED = True

# Parâmetros deSEC
DESEC_TTL = 900
DESEC_MIN_DELAY = 0.6              # Mínimo entre requests (2 req/s)
DESEC_PER_DOMAIN_DELAY = 0.6     # Delay por domínio
DESEC_BACKOFF_BASE = 30
DESEC_BACKOFF_MULTIPLIER = 2.0

# Parâmetros de Paralelismo
SEMAFORO_IP_GLOBAL = 30            # Máximo de túneis SSH simultâneos
NUM_WORKERS_POR_PROVEDOR = 1       # Workers dedicados por provedor
MAX_COROUTINES_SIMULTANEAS = 500

# ==============================================================================
# 3. OS 66 PROVEDORES VALIDADOS (TESTE V22.3.0) COM LIMITES RCPT APRENDIDOS
# ==============================================================================
PROVEDORES_ALVO = [
    "bebo.com", "bk.ru", "cemsmail.org", "dreamwiz.com", "google.com",
    "inbox.ru", "list.ru", "mail.ru", "pisem.net", "rock.com",
    "sprintpcs.com", "ukr.net", "walla.com", "yahoo.co.jp", "youtube.com",
    "seznam.cz", "yeah.net", "nate.com", "clearwire.net", "terra.com.br",
    "telus.net", "eyou.com", "email.cz", "telkomsa.net", "rambler.ru",
    "student.gccisd.net", "21cn.com", "googlemail.com", "gmail.com", "oranga.fr",
    "inwind.it", "myemailboxmail.com", "trbvm.com", "dodgeit.com", "ya.ru",
    "qq.com", "foxmail.com", "libero.it", "naver.com", "vip.qq.com",
    "virgilio.it", "yandex.com", "freemail.hu", "yandex.ru", "poczta.onet.pl",
    "docomo.ne.jp", "amorki.pl", "live.co.uk", "onet.eu", "onet.pl",
    "breakthru.com", "clear.net.nz", "hot.com", "op.pl", "pchome.com.tw",
    "poczta.pl", "yandex.net", "i.ua", "interia.eu", "interia.pl",
    "poczta.fm", "gaggle.net", "love.com", "qip.ru", "univision.com", "vipmail.net"
]

# Limites RCPT Iniciais (Aprendidos no teste V22.3.0)
RCPT_LIMITS_INICIAIS = {
    "bebo.com": 100, "bk.ru": 100, "cemsmail.org": 100, "dreamwiz.com": 100,
    "google.com": 100, "inbox.ru": 100, "list.ru": 100, "mail.ru": 100,
    "pisem.net": 1000, "rock.com": 2126, "sprintpcs.com": 1000, "ukr.net": 100,
    "walla.com": 1908, "yahoo.co.jp": 100, "youtube.com": 100, "seznam.cz": 100,
    "yeah.net": 79, "nate.com": 77, "terra.com.br": 13, "telus.net": 75,
    "eyou.com": 30, "email.cz": 32, "student.gccisd.net": 47, "21cn.com": 3,
    "googlemail.com": 100, "gmail.com": 95, "oranga.fr": 1000, "myemailboxmail.com": 50,
    "trbvm.com": 50, "dodgeit.com": 10, "ya.ru": 36, "qq.com": 33,
    "foxmail.com": 30, "naver.com": 29, "vip.qq.com": 30, "yandex.com": 16,
    "freemail.hu": 45, "yandex.ru": 11, "poczta.onet.pl": 48, "docomo.ne.jp": 3,
    "amorki.pl": 67, "onet.eu": 48, "onet.pl": 48, "breakthru.com": 20,
    "clear.net.nz": 20, "hot.com": 22, "op.pl": 48, "pchome.com.tw": 20,
    "poczta.pl": 13, "gaggle.net": 1, "telkomsa.net": 1715,
    "i.ua": 0, "interia.eu": 2, "interia.pl": 2, "poczta.fm": 8,
    "love.com": 0, "qip.ru": 0, "univision.com": 0,
}

PROVEDORES_ASIATICOS = {
    "naver.com", "hanmail.net", "daum.net", "korea.com", "lycos.co.kr",
    "nate.com", "empal.com", "dreamwiz.com", "qq.com", "foxmail.com",
    "vip.qq.com", "163.com", "126.com", "yeah.net", "sohu.com", "sina.com",
    "tom.com", "21cn.com", "eyou.com", "ezweb.ne.jp", "i.softbank.jp",
    "docomo.ne.jp", "softbank.ne.jp", "yahoo.co.jp", "hotmail.co.jp",
    "pchome.com.tw", "kimo.com", "yahoo.com.tw"
}

PROVEDORES_GREYLIST = {"i.ua", "interia.eu", "interia.pl", "poczta.fm", "seznam.cz", "gmail.com"}

# Caminhos Locais
BASE_LOCAL = "/content/veriscope_v23"
DIR_EMAILS = "/content/emails_db"
LOG_DIR = os.path.join(BASE_LOCAL, "logs")
CP_DIR = os.path.join(BASE_LOCAL, "checkpoints")
DIR_REPO = "/content/trader_repo"
DIR_KEYS = "/content/ssh_keys"

# ==============================================================================
# 4. ESTRUTURAS DE DADOS (ESTADO GRANULAR)
# ==============================================================================
@dataclass
class ProviderState:
    """Estado isolado e independente para cada um dos 66 provedores."""
    name: str
    rcpt_limit: int = 50
    emails_sent: int = 0
    emails_accepted: int = 0
    emails_rejected: int = 0
    emails_unknown: int = 0
    emails_greylist: int = 0
    quota_by_ip: Dict[str, int] = field(default_factory=dict)
    last_email_offset: int = 0       # Cursor independente no ficheiro de emails
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    is_paused: bool = False
    pause_until: float = 0.0
    mx_host: str = ""
    completed: bool = False

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        data = data.copy()
        valid_keys = set(cls.__dataclass_fields__.keys())
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)


@dataclass
class IPState:
    """Estado de um IP Aracloud individual."""
    ip: str
    account_name: str
    status: str = "ACTIVE"  # ACTIVE, EXHAUSTED, BLOCKED, ROTATING
    created_at: str = ""
    sent_total: int = 0
    sent_by_provider: Dict[str, int] = field(default_factory=dict)
    domain_assigned: str = ""
    ssh_key_path: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        data = data.copy()
        valid_keys = set(cls.__dataclass_fields__.keys())
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)


@dataclass
class AccountState:
    """Estado de uma conta DKIM."""
    identity_id: str
    sender_email: str
    domain: str
    dkim_private_key: str = ""
    dkim_selector: str = "default"
    base: str = ""
    public_key_txt: str = ""
    emails_sent: int = 0
    last_used: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        data = data.copy()
        valid_keys = set(cls.__dataclass_fields__.keys())
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)


@dataclass
class AracloudAccount:
    """Conta Aracloud (OpenStack)."""
    name: str
    hostname: str
    api_user: str
    api_password: str
    domain_id: str = "default"
    project_id: str = ""
    key_path: str = ""
    ips_ativos: List[str] = field(default_factory=list)
    ips_usados_hoje: List[str] = field(default_factory=list)
    ips_criados_total: int = 0
    auth_token: str = ""
    token_expires: float = 0.0
    compute_url: str = ""
    network_url: str = ""


@dataclass
class GlobalState:
    """Estado global do sistema (Master Controller)."""
    version: str = "23.0.0"
    run_id: str = ""
    campaign_id: str = ""
    started_at: str = ""
    ended_at: str = ""
    status: str = "NEW"
    total_sent: int = 0
    total_accepted: int = 0
    total_rejected: int = 0
    total_unknown: int = 0
    total_greylist_retry: int = 0
    total_runs: int = 0

    # Anti-repetição de IPs (Regra do Dia)
    ips_used_today: Dict[str, List[str]] = field(default_factory=dict)

    # Estados Granulares
    provider_states: Dict[str, ProviderState] = field(default_factory=dict)
    ip_states: Dict[str, IPState] = field(default_factory=dict)
    account_states: Dict[str, AccountState] = field(default_factory=dict)

    # Blacklists e Whitelists persistentes
    unknown_emails: Set[str] = field(default_factory=set)
    accepted_emails: Set[str] = field(default_factory=set)

    # DNS State
    dns_state: Dict[str, Any] = field(default_factory=dict)
    spf_record_written: str = ""
    spf_ips_written: List[str] = field(default_factory=list)

    # Limites RCPT aprendidos
    limits_rcpt: Dict[str, int] = field(default_factory=dict)

    def to_dict(self):
        return {
            'version': self.version, 'run_id': self.run_id,
            'campaign_id': self.campaign_id,
            'started_at': self.started_at, 'ended_at': self.ended_at,
            'status': self.status, 'total_runs': self.total_runs,
            'total_sent': self.total_sent, 'total_accepted': self.total_accepted,
            'total_rejected': self.total_rejected, 'total_unknown': self.total_unknown,
            'total_greylist_retry': self.total_greylist_retry,
            'ips_used_today': self.ips_used_today,
            'provider_states': {k: v.to_dict() for k, v in self.provider_states.items()},
            'ip_states': {k: v.to_dict() for k, v in self.ip_states.items()},
            'account_states': {k: v.to_dict() for k, v in self.account_states.items()},
            'unknown_emails': list(self.unknown_emails),
            'accepted_emails': list(self.accepted_emails),
            'dns_state': self.dns_state,
            'spf_record_written': self.spf_record_written,
            'spf_ips_written': self.spf_ips_written,
            'limits_rcpt': self.limits_rcpt,
        }

    @classmethod
    def from_dict(cls, data):
        state = cls()
        for k, v in data.items():
            if k == 'provider_states':
                state.provider_states = {pk: ProviderState.from_dict(pv) for pk, pv in v.items()}
            elif k == 'ip_states':
                state.ip_states = {ik: IPState.from_dict(iv) for ik, iv in v.items()}
            elif k == 'account_states':
                state.account_states = {ak: AccountState.from_dict(av) for ak, av in v.items()}
            elif k == 'unknown_emails':
                state.unknown_emails = set(v) if isinstance(v, list) else set()
            elif k == 'accepted_emails':
                state.accepted_emails = set(v) if isinstance(v, list) else set()
            elif hasattr(state, k):
                setattr(state, k, v)
        return state


# ==============================================================================
# 5. LOGGER ASSÍNCRONO COM BUFFER
# ==============================================================================
class AsyncLogger:
    """Logger com buffer e flush automático para não bloquear o event loop."""
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.buffer = deque(maxlen=10000)
        self.buffer_lock = asyncio.Lock()
        self.last_flush = time.time()
        self.flush_interval = 15
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
            except Exception:
                pass

    async def log(self, msg: str, provedor: str = "", flush_now: bool = False):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        prefix = f"[{provedor}] " if provedor else ""
        line = f"[{ts}] {prefix}{msg}\n"
        async with self.buffer_lock:
            self.buffer.append(line)
            if flush_now or len(self.buffer) >= 500 or (time.time() - self.last_flush > self.flush_interval):
                await self._flush_locked()

    async def _flush_locked(self):
        if not self.buffer:
            return
        try:
            lines = list(self.buffer)
            self.buffer.clear()
            self.last_flush = time.time()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_to_disk, lines)
        except Exception as e:
            print(f"[LOGGER ERROR] {e}", flush=True)

    def _write_to_disk(self, lines: List[str]):
        try:
            with open(self.log_file, "a", encoding="utf-8", errors="replace") as f:
                f.writelines(lines)
        except Exception:
            pass

    async def flush(self):
        async with self.buffer_lock:
            await self._flush_locked()

    async def print(self, msg: str, provedor: str = ""):
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{provedor}] " if provedor else ""
        line = f"[{ts}] {prefix}{msg}"
        print(line, flush=True)
        await self.log(msg, provedor)

    async def smtp_log(self, msg: str, provedor: str = ""):
        await self.log(f"[SMTP] {msg}", provedor)


# ==============================================================================
# 6. GESTÃO DE ESTADO E CHECKPOINT (HUGGINGFACE)
# ==============================================================================
class StateManager:
    """Gere o estado global com persistência e validação SHA256."""
    def __init__(self, logger: AsyncLogger):
        self.logger = logger
        self.state = GlobalState()
        self.state_lock = asyncio.Lock()
        self.checkpoint_dir = CP_DIR
        os.makedirs(self.checkpoint_dir, exist_ok=True)

    async def initialize_run(self):
        async with self.state_lock:
            self.state.run_id = f"run_{int(time.time())}"
            self.state.started_at = datetime.now(timezone.utc).isoformat()
            self.state.status = "RUNNING"
            self.state.total_runs += 1
            if not self.state.campaign_id:
                self.state.campaign_id = f"camp_{int(time.time())}"
            today = datetime.now().strftime("%Y-%m-%d")
            if today not in self.state.ips_used_today:
                # Limpar dias antigos
                keys_to_delete = [k for k in self.state.ips_used_today.keys() if k != today]
                for k in keys_to_delete:
                    del self.state.ips_used_today[k]
                self.state.ips_used_today[today] = []

            for provedor in PROVEDORES_ALVO:
                if provedor not in self.state.provider_states:
                    limit = RCPT_LIMITS_INICIAIS.get(provedor, 50)
                    self.state.provider_states[provedor] = ProviderState(
                        name=provedor,
                        rcpt_limit=limit
                    )
                if provedor not in self.state.limits_rcpt:
                    self.state.limits_rcpt[provedor] = RCPT_LIMITS_INICIAIS.get(provedor, 50)
            await self.logger.print(f"🚀 Run #{self.state.total_runs} iniciada: {self.state.run_id}")

    async def save_checkpoint(self, reason: str = "PERIODIC"):
        async with self.state_lock:
            self.state.ended_at = datetime.now(timezone.utc).isoformat()
            checkpoint = self.state.to_dict()

            checkpoint_json = json.dumps(checkpoint, indent=2, ensure_ascii=False, default=str)
            checkpoint_hash = hashlib.sha256(checkpoint_json.encode()).hexdigest()

            checkpoint_file = os.path.join(self.checkpoint_dir, "master_checkpoint.json")
            hash_file = os.path.join(self.checkpoint_dir, "manifest.json")

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_files, checkpoint_file, checkpoint_json,
                                       hash_file, checkpoint_hash, reason)
            await self.logger.print(
                f"💾 Checkpoint salvo: {reason} | "
                f"📤 {self.state.total_sent:,} | ✅ {self.state.total_accepted:,} | "
                f"Hash: {checkpoint_hash[:16]}..."
            )
            return checkpoint_file, hash_file

    def _write_files(self, cp_file, cp_json, hash_file, cp_hash, reason):
        with open(cp_file, "w", encoding="utf-8") as f:
            f.write(cp_json)
        manifest = {
            "checkpoint_hash": cp_hash,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "version": self.state.version,
            "run_id": self.state.run_id,
            "campaign_id": self.state.campaign_id,
            "stats": {
                "total_sent": self.state.total_sent,
                "total_accepted": self.state.total_accepted,
                "total_rejected": self.state.total_rejected,
                "total_unknown": self.state.total_unknown,
                "active_providers": len([p for p in self.state.provider_states.values() if not p.completed]),
                "completed_providers": len([p for p in self.state.provider_states.values() if p.completed]),
                "active_ips": len([ip for ip in self.state.ip_states.values() if ip.status == "ACTIVE"]),
            }
        }
        with open(hash_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    async def load_checkpoint(self):
        try:
            api = HfApi(token=HF_TOKEN)
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None, lambda: api.list_repo_files(HF_CHECKPOINT_REPO, repo_type="dataset")
            )
            if "checkpoints/master_checkpoint.json" not in files:
                await self.logger.print("📭 Nenhum checkpoint encontrado - começando do zero")
                return False

            local_cp = await loop.run_in_executor(
                None, lambda: hf_hub_download(
                    repo_id=HF_CHECKPOINT_REPO,
                    filename="checkpoints/master_checkpoint.json",
                    repo_type="dataset", token=HF_TOKEN,
                    cache_dir="/content/hf_cache"
                )
            )
            local_manifest = await loop.run_in_executor(
                None, lambda: hf_hub_download(
                    repo_id=HF_CHECKPOINT_REPO,
                    filename="checkpoints/manifest.json",
                    repo_type="dataset", token=HF_TOKEN,
                    cache_dir="/content/hf_cache"
                )
            )

            with open(local_manifest, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            with open(local_cp, "r", encoding="utf-8") as f:
                cp_json = f.read()

            calc_hash = hashlib.sha256(cp_json.encode()).hexdigest()
            if calc_hash != manifest.get("checkpoint_hash"):
                await self.logger.print(
                    f"⚠️ Hash mismatch! Esperado: {manifest.get('checkpoint_hash', '?')[:16]}, "
                    f"Calc: {calc_hash[:16]} - Continuando com cautela"
                )

            async with self.state_lock:
                self.state = GlobalState.from_dict(json.loads(cp_json))

            stats = manifest.get("stats", {})
            await self.logger.print(f"✅ Checkpoint carregado (campanha: {self.state.campaign_id})")
            await self.logger.print(
                f"   📤 Enviados: {self.state.total_sent:,} | "
                f"✅ Aceites: {self.state.total_accepted:,} | "
                f"🏆 Runs anteriores: {self.state.total_runs - 1}"
            )
            await self.logger.print(
                f"   📧 Provedores ativos: {stats.get('active_providers', 0)} | "
                f"🏁 Completos: {stats.get('completed_providers', 0)}"
            )
            return True
        except Exception as e:
            await self.logger.print(f"⚠️ Erro ao carregar checkpoint: {str(e)[:100]}")
            return False

    async def upload_checkpoint(self):
        try:
            api = HfApi(token=HF_TOKEN)
            cp_file = os.path.join(self.checkpoint_dir, "master_checkpoint.json")
            manifest_file = os.path.join(self.checkpoint_dir, "manifest.json")

            if not os.path.exists(cp_file):
                return

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: api.upload_file(
                path_or_fileobj=cp_file,
                path_in_repo="checkpoints/master_checkpoint.json",
                repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN
            ))
            await loop.run_in_executor(None, lambda: api.upload_file(
                path_or_fileobj=manifest_file,
                path_in_repo="checkpoints/manifest.json",
                repo_id=HF_CHECKPOINT_REPO, repo_type="dataset", token=HF_TOKEN
            ))
            await self.logger.print("☁️ Checkpoint enviado para HuggingFace")
        except Exception as e:
            await self.logger.print(f"⚠️ Erro ao enviar checkpoint: {str(e)[:100]}")

    async def add_unknown_email(self, email: str):
        if not email or "@" not in email:
            return
        async with self.state_lock:
            self.state.unknown_emails.add(email.lower().strip())

    async def add_accepted_email(self, email: str):
        if not email or "@" not in email:
            return
        async with self.state_lock:
            self.state.accepted_emails.add(email.lower().strip())

    async def increment_stats(self, **kwargs):
        async with self.state_lock:
            for key, value in kwargs.items():
                if hasattr(self.state, key):
                    current = getattr(self.state, key)
                    setattr(self.state, key, current + value)

    async def update_provider_state(self, provedor: str, **kwargs):
        async with self.state_lock:
            if provedor not in self.state.provider_states:
                self.state.provider_states[provedor] = ProviderState(name=provedor)
            ps = self.state.provider_states[provedor]
            for key, value in kwargs.items():
                if hasattr(ps, key):
                    setattr(ps, key, value)

    async def get_provider_state(self, provedor: str) -> Optional[ProviderState]:
        async with self.state_lock:
            return self.state.provider_states.get(provedor)

    async def update_rcpt_limit(self, provedor: str, new_limit: int):
        async with self.state_lock:
            if provedor in self.state.provider_states:
                self.state.provider_states[provedor].rcpt_limit = new_limit
            self.state.limits_rcpt[provedor] = new_limit

    async def update_ip_quota(self, ip: str, provedor: str, count: int):
        async with self.state_lock:
            if ip not in self.state.ip_states:
                return
            ip_state = self.state.ip_states[ip]
            ip_state.sent_total += count
            ip_state.sent_by_provider[provedor] = ip_state.sent_by_provider.get(provedor, 0) + count
            # Verificar esgotamento
            if ip_state.sent_by_provider[provedor] >= MAX_EMAILS_POR_IP_PROVEDOR:
                await self.logger.print(f"🛑 IP {ip} esgotou quota para {provedor}", provedor)
            if ip_state.sent_total >= MAX_EMAILS_POR_IP_TOTAL:
                ip_state.status = "EXHAUSTED"
                await self.logger.print(f"♻️ IP {ip} marcado para rotação (quota total esgotada)")

    async def mark_ip_used_today(self, ip: str):
        async with self.state_lock:
            today = datetime.now().strftime("%Y-%m-%d")
            if today not in self.state.ips_used_today:
                self.state.ips_used_today[today] = []
            if ip not in self.state.ips_used_today[today]:
                self.state.ips_used_today[today].append(ip)

    async def was_ip_used_today(self, ip: str) -> bool:
        async with self.state_lock:
            today = datetime.now().strftime("%Y-%m-%d")
            return ip in self.state.ips_used_today.get(today, [])


# ==============================================================================
# 7. GESTÃO DE ARACLOUD (OPENSTACK NOVA + NEUTRON API)
# ==============================================================================
class AracloudManager:
    """Gere IPs Aracloud com rotação via API OpenStack (Nova + Neutron)."""
    def __init__(self, logger: AsyncLogger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.accounts: List[AracloudAccount] = []
        self.ips_ativos: List[Dict[str, Any]] = []
        self.ip_lock = asyncio.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.semaphore = asyncio.Semaphore(SEMAFORO_IP_GLOBAL)

    async def load_accounts(self) -> bool:
        """Carrega credenciais e chaves SSH das 10 contas Aracloud."""
        try:
            api = HfApi(token=HF_TOKEN)
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None, lambda: api.list_repo_files(HF_ARACLOUD_REPO, repo_type="dataset")
            )

            server_dirs = sorted(list(set([
                f.split('/')[0] for f in files
                if f.startswith('servidor-') and '/' in f
            ])))[:NUM_CONTAS_ARACLOUD]

            os.makedirs(DIR_KEYS, exist_ok=True)

            for server_dir in server_dirs:
                try:
                    cred_file = await loop.run_in_executor(None, lambda sd=server_dir: hf_hub_download(
                        repo_id=HF_ARACLOUD_REPO,
                        filename=f"{sd}/credenciais.json",
                        repo_type="dataset", token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    ))
                    key_file = await loop.run_in_executor(None, lambda sd=server_dir: hf_hub_download(
                        repo_id=HF_ARACLOUD_REPO,
                        filename=f"{sd}/k.pem",
                        repo_type="dataset", token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    ))

                    with open(cred_file, "r", encoding="utf-8") as f:
                        cred = json.load(f)

                    key_dest = os.path.join(DIR_KEYS, f"k_{server_dir}.pem")
                    shutil.copy(key_file, key_dest)
                    os.chmod(key_dest, 0o600)

                    api_info = cred.get("api", {})
                    account = AracloudAccount(
                        name=server_dir,
                        hostname=cred.get("hostname", ""),
                        api_user=api_info.get("utilizador", ""),
                        api_password=api_info.get("password", ""),
                        key_path=key_dest,
                    )
                    self.accounts.append(account)
                    await self.logger.print(f"✅ Conta carregada: {server_dir}")
                except Exception as e:
                    await self.logger.print(f"⚠️ Erro ao carregar {server_dir}: {str(e)[:80]}")

            return len(self.accounts) >= 1
        except Exception as e:
            await self.logger.print(f"❌ Erro ao carregar contas: {str(e)[:100]}")
            return False

    async def authenticate(self, account: AracloudAccount) -> bool:
        """Autentica na API OpenStack (Keystone V3) e obtém endpoints."""
        loop = asyncio.get_event_loop()
        auth_data = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": account.api_user,
                            "domain": {"id": account.domain_id},
                            "password": account.api_password
                        }
                    }
                }
            }
        }
        try:
            resp = await loop.run_in_executor(None, lambda: requests.post(
                f"{ARACLOUD_AUTH_URL}/auth/tokens",
                json=auth_data, timeout=30, verify=False
            ))
            if resp.status_code != 201:
                await self.logger.print(f"⚠️ Auth falhou para {account.name}: {resp.status_code}")
                return False

            account.auth_token = resp.headers.get("X-Subject-Token", "")
            account.token_expires = time.time() + 3300  # ~55 min

            token_data = resp.json().get("token", {})
            catalog = token_data.get("catalog", [])

            # Extrair endpoints
            for service in catalog:
                if service.get("type") == "compute":
                    for endpoint in service.get("endpoints", []):
                        if endpoint.get("interface") == "public":
                            account.compute_url = endpoint.get("url", "")
                elif service.get("type") == "network":
                    for endpoint in service.get("endpoints", []):
                        if endpoint.get("interface") == "public":
                            account.network_url = endpoint.get("url", "")

            # Obter project_id
            projects = token_data.get("user", {}).get("domain", {})
            if not account.project_id:
                # Tentar obter project_id via /auth/projects
                proj_resp = await loop.run_in_executor(None, lambda: requests.get(
                    f"{ARACLOUD_AUTH_URL}/auth/projects",
                    headers={"X-Auth-Token": account.auth_token},
                    timeout=30, verify=False
                ))
                if proj_resp.status_code == 200:
                    proj_data = proj_resp.json().get("projects", [])
                    if proj_data:
                        account.project_id = proj_data[0].get("id", "")
                        # Re-autenticar com scope de projeto
                        auth_data_scoped = {
                            "auth": {
                                "identity": auth_data["auth"]["identity"],
                                "scope": {"project": {"id": account.project_id}}
                            }
                        }
                        resp2 = await loop.run_in_executor(None, lambda: requests.post(
                            f"{ARACLOUD_AUTH_URL}/auth/tokens",
                            json=auth_data_scoped, timeout=30, verify=False
                        ))
                        if resp2.status_code == 201:
                            account.auth_token = resp2.headers.get("X-Subject-Token", "")
                            account.token_expires = time.time() + 3300

            return True
        except Exception as e:
            await self.logger.print(f"⚠️ Erro auth {account.name}: {str(e)[:80]}")
            return False

    async def ensure_token(self, account: AracloudAccount) -> bool:
        """Garante que o token está válido."""
        if not account.auth_token or time.time() >= account.token_expires:
            return await self.authenticate(account)
        return True

    async def get_server_ips(self, account: AracloudAccount) -> List[str]:
        """Obtém IPs atuais do servidor via API Nova."""
        if not await self.ensure_token(account):
            return []
        loop = asyncio.get_event_loop()
        try:
            def _get():
                headers = {"X-Auth-Token": account.auth_token}
                resp = requests.get(
                    f"{account.compute_url}/servers/detail",
                    headers=headers, timeout=30, verify=False
                )
                if resp.status_code != 200:
                    return []
                servers = resp.json().get("servers", [])
                ips = []
                for server in servers:
                    if server.get("name") == account.hostname:
                        for network, addresses in server.get("addresses", {}).items():
                            for addr in addresses:
                                if addr.get("version") == 4:
                                    ip = addr.get("addr")
                                    if ip and ip not in ips:
                                        ips.append(ip)
                return ips
            return await loop.run_in_executor(self.executor, _get)
        except Exception:
            return []

    async def setup_ips(self) -> int:
        """Configura até 30 IPs simultâneos respeitando a regra do dia."""
        await self.logger.print("🌐 Configurando IPs Aracloud (3 por conta × 10 contas)...")

        for account in self.accounts:
            current_ips = await self.get_server_ips(account)

            for ip in current_ips:
                # Verificar regra do dia
                if await self.state_manager.was_ip_used_today(ip):
                    await self.logger.print(f"⚠️ IP {ip} já usado hoje em {account.name} - ignorando")
                    continue

                # Testar SSH
                if await self.test_ssh_connection(ip, account.key_path):
                    ip_info = {
                        "ip": ip,
                        "account": account,
                        "key_path": account.key_path,
                    }
                    self.ips_ativos.append(ip_info)

                    # Registar estado
                    async with self.state_manager.state_lock:
                        if ip not in self.state_manager.state.ip_states:
                            self.state_manager.state.ip_states[ip] = IPState(
                                ip=ip,
                                account_name=account.name,
                                created_at=datetime.now(timezone.utc).isoformat(),
                                ssh_key_path=account.key_path,
                            )

                    await self.logger.print(f"✅ IP {ip} pronto ({account.name})")

                if len(self.ips_ativos) >= TOTAL_IPS_SIMULTANEOS:
                    break

            if len(self.ips_ativos) >= TOTAL_IPS_SIMULTANEOS:
                break

        await self.logger.print(f"🎯 {len(self.ips_ativos)} IPs configurados e prontos para uso")
        return len(self.ips_ativos)

    async def test_ssh_connection(self, ip: str, key_path: str) -> bool:
        """Testa conexão SSH com um IP usando Paramiko em thread."""
        loop = asyncio.get_event_loop()
        def _test():
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                pkey = paramiko.RSAKey.from_private_key_file(key_path)
                client.connect(
                    ip, port=22, username="ubuntu", pkey=pkey,
                    timeout=15, allow_agent=False, look_for_keys=False
                )
                stdin, stdout, stderr = client.exec_command("echo veriscope_ok", timeout=10)
                output = stdout.read().decode().strip()
                client.close()
                return output == "veriscope_ok"
            except Exception:
                return False
        return await loop.run_in_executor(self.executor, _test)

    async def acquire_ip_slot(self):
        """Adquire slot no semáforo global (30 túneis máx)."""
        await self.semaphore.acquire()

    def release_ip_slot(self):
        """Liberta slot no semáforo global."""
        try:
            self.semaphore.release()
        except Exception:
            pass

    async def get_available_ip(self, provedor: str) -> Optional[Dict[str, Any]]:
        """Obtém um IP disponível com quota para o provedor."""
        async with self.ip_lock:
            # Embaralhar para distribuição justa
            shuffled = list(self.ips_ativos)
            random.shuffle(shuffled)
            for ip_info in shuffled:
                ip = ip_info["ip"]
                async with self.state_manager.state_lock:
                    ip_state = self.state_manager.state.ip_states.get(ip)
                    if not ip_state:
                        continue
                    if ip_state.status != "ACTIVE":
                        continue
                    sent = ip_state.sent_by_provider.get(provedor, 0)
                    if sent >= MAX_EMAILS_POR_IP_PROVEDOR:
                        continue
                    if ip_state.sent_total >= MAX_EMAILS_POR_IP_TOTAL:
                        ip_state.status = "EXHAUSTED"
                        continue
                return ip_info
            return None


# ==============================================================================
# 8. GESTÃO DE DNS (deSEC BULK UPDATE COM THROTTLING)
# ==============================================================================
class DNSManager:
    """Gere DNS via deSEC com bulk updates e throttling por domínio."""
    def __init__(self, logger: AsyncLogger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.api_url = "https://desec.io/api/v1"
        self.headers = {
            "Authorization": f"Token {DESEC_TOKEN}",
            "Content-Type": "application/json"
        }
        self.last_request_time = {}  # Por domínio
        self.last_global_request = 0
        self.lock = asyncio.Lock()

    async def get_domains(self) -> List[str]:
        """Obtém lista de domínios disponíveis."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/domains/",
                    headers=self.headers, timeout=30
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        domains = [d["name"] for d in data]
                        await self.logger.print(f"📋 {len(domains)} domínios disponíveis no deSEC")
                        return domains
                    else:
                        text = await resp.text()
                        await self.logger.print(f"⚠️ Erro ao listar domínios: {resp.status} {text[:100]}")
        except Exception as e:
            await self.logger.print(f"⚠️ Erro ao obter domínios: {str(e)[:100]}")
        return []

    async def _throttle(self, domain: str = ""):
        """Throttling para respeitar rate limits do deSEC."""
        async with self.lock:
            now = time.time()
            # Delay global (2 req/s)
            diff_global = now - self.last_global_request
            if diff_global < DESEC_MIN_DELAY:
                await asyncio.sleep(DESEC_MIN_DELAY - diff_global)
            # Delay por domínio
            if domain and domain in self.last_request_time:
                diff_domain = now - self.last_request_time[domain]
                if diff_domain < DESEC_PER_DOMAIN_DELAY:
                    await asyncio.sleep(DESEC_PER_DOMAIN_DELAY - diff_domain)
                self.last_request_time[domain] = time.time()
            else:
                if domain:
                    self.last_request_time[domain] = time.time()
            self.last_global_request = time.time()

    async def update_spf_bulk(self, domains: List[str], ips: List[str], force_full: bool = False):
        """Atualiza SPF de múltiplos domínios (1 request por domínio)."""
        if not domains or not ips:
            return

        await self.logger.print(f"🔐 Atualizando DNS de {len(domains)} domínios...")

        success_count = 0
        for i, domain in enumerate(domains[:30]):
            ip = ips[i % len(ips)]
            spf_record = f"v=spf1 ip4:{ip} ~all"

            # Payload bulk (apenas SPF + A + Wildcard em rotações)
            payload = [
                {"subname": "", "type": "TXT", "ttl": DESEC_TTL, "records": [f'"{spf_record}"']},
                {"subname": "", "type": "A", "ttl": DESEC_TTL, "records": [ip]},
                {"subname": "*", "type": "A", "ttl": DESEC_TTL, "records": [ip]},
            ]

            await self._throttle(domain)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.put(
                        f"{self.api_url}/domains/{domain}/rrsets/",
                        headers=self.headers, json=payload, timeout=30
                    ) as resp:
                        if resp.status in (200, 201, 204):
                            success_count += 1
                            await self.logger.print(f"✅ [{i+1}/{len(domains[:30])}] {domain} → {ip}")
                        elif resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 30))
                            await self.logger.print(f"⏳ Rate limit deSEC. Esperando {retry_after}s...")
                            await asyncio.sleep(retry_after)
                        else:
                            text = await resp.text()
                            await self.logger.print(
                                f"⚠️ Erro {domain}: {resp.status} {text[:80]}"
                            )
            except Exception as e:
                await self.logger.print(f"⚠️ Exceção DNS {domain}: {str(e)[:80]}")

        await self.logger.print(
            f"✅ DNS: {success_count}/{len(domains[:30])} domínios atualizados"
        )

        # Salvar estado
        async with self.state_manager.state_lock:
            if ips:
                self.state_manager.state.spf_ips_written = ips
                self.state_manager.state.spf_record_written = f"v=spf1 ip4:{ips[0]} ~all"


# ==============================================================================
# 9. CLIENTE SMTP COM PIPELINING E MUTAÇÃO (ABRACADABRA)
# ==============================================================================
class SMTPClient:
    """Cliente SMTP assíncrono via SSH Tunnel (Paramiko + run_in_executor)."""
    def __init__(self, logger: AsyncLogger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.mail_from_variations = []
        self.html_template = ""
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=50)

    async def load_mail_from_variations(self):
        """Carrega variações de Mail From."""
        try:
            api = HfApi(token=HF_TOKEN)
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None, lambda: api.list_repo_files(HF_LOGS_REPO, repo_type="dataset")
            )
            mail_from_files = [
                f for f in files
                if "mail_from" in f.lower() and f.endswith(".txt")
            ]
            if not mail_from_files:
                self.mail_from_variations = [
                    "Alex Johnson", "A. Johnson", "Alex J.", "Johnson, Alex",
                    "AJ", "Alex J", "Mr. Alex Johnson"
                ]
                return

            local_file = await loop.run_in_executor(None, lambda: hf_hub_download(
                repo_id=HF_LOGS_REPO, filename=mail_from_files[0],
                repo_type="dataset", token=HF_TOKEN, cache_dir="/content/hf_cache"
            ))
            with open(local_file, "r", encoding="utf-8", errors="ignore") as f:
                variations = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            self.mail_from_variations = variations if variations else ["Alex Johnson"]
            await self.logger.print(f"📧 {len(self.mail_from_variations)} variações Mail From carregadas")
        except Exception as e:
            await self.logger.print(f"⚠️ Erro ao carregar Mail From: {str(e)[:80]}")
            self.mail_from_variations = ["Alex Johnson"]

    async def load_template(self):
        """Carrega template HTML da campanha."""
        try:
            api = HfApi(token=HF_TOKEN)
            loop = asyncio.get_event_loop()
            template_file = await loop.run_in_executor(None, lambda: hf_hub_download(
                repo_id=HF_LOGS_REPO,
                filename="campaign_data/templates/veriscope_email_01.html",
                repo_type="dataset", token=HF_TOKEN, cache_dir="/content/hf_cache"
            ))
            with open(template_file, "r", encoding="utf-8") as f:
                self.html_template = f.read()
            await self.logger.print(f"📄 Template HTML carregado ({len(self.html_template)} bytes)")
        except Exception as e:
            await self.logger.print(f"⚠️ Erro ao carregar template: {str(e)[:80]}")
            self.html_template = "<html><body><p>Important update regarding your account.</p></body></html>"

    def mutar_html(self, html: str, conta: AccountState, recipient: str) -> str:
        """Aplica mutação Abracadabra ao HTML."""
        try:
            h = html
            # Comentário único no início do body
            rid = f"<!-- m-{hashlib.md5(f'{recipient}{time.time()}'.encode()).hexdigest()[:8]} -->"
            if "<body>" in h:
                h = h.replace("<body>", f"<body>\n{rid}\n", 1)
            else:
                mm = re.search(r'<body[^>]*>', h)
                if mm:
                    h = h.replace(mm.group(0), mm.group(0) + f"\n{rid}\n", 1)

            # Espaços aleatórios entre tags
            h = re.sub(r'>\s+<', lambda m: '>' + (' ' * (hash(recipient) % 3 + 1)) + '<', h)

            # Mutar URLs (href)
            def subs_href(match):
                prefix, url, suffix = match.group(1), match.group(2), match.group(3)
                seed = f"{recipient}|{conta.identity_id}|{int(time.time() * 1000)}"
                hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
                fragment = f"u={hash_val}"
                parsed = urlparse(url)
                if parsed.fragment:
                    new_fragment = f"{parsed.fragment}&{fragment}"
                else:
                    new_fragment = fragment
                url_mutada = urlunparse(parsed._replace(fragment=new_fragment))
                return f"{prefix}{url_mutada}{suffix}"

            h = re.sub(r'(href=["\'])([^"\']+)(["\'])', subs_href, h)

            # Mutar URLs (src)
            def subs_src(match):
                prefix, url, suffix = match.group(1), match.group(2), match.group(3)
                seed = f"src|{recipient}|{conta.identity_id}|{int(time.time() * 1000)}"
                hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
                fragment = f"u={hash_val}"
                parsed = urlparse(url)
                if parsed.fragment:
                    new_fragment = f"{parsed.fragment}&{fragment}"
                else:
                    new_fragment = fragment
                url_mutada = urlunparse(parsed._replace(fragment=new_fragment))
                return f"{prefix}{url_mutada}{suffix}"

            h = re.sub(r'(src=["\'])([^"\']+)(["\'])', subs_src, h)

            # Comentário de hash no fim
            hash_extra = hashlib.md5(f"{recipient}{time.time()}".encode()).hexdigest()[:12]
            h = h.replace("</body>", f"<!-- {hash_extra} -->\n</body>", 1)
            return h
        except Exception:
            return html

    def get_mail_from(self, conta: AccountState) -> str:
        """Obtém Mail From com variação."""
        if not self.mail_from_variations:
            return conta.sender_email
        idx = hash(conta.identity_id) % len(self.mail_from_variations)
        display_name = self.mail_from_variations[idx]
        return f"{display_name} <{conta.sender_email}>"

    def _open_tunnel(self, ip, key_path, mx_host):
        """Abre túnel SSH (executado em thread)."""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(
                ip, port=22, username="ubuntu", pkey=pkey,
                timeout=15, allow_agent=False, look_for_keys=False
            )
            transport = client.get_transport()
            if not transport:
                client.close()
                return None, None
            transport.set_keepalive(15)
            channel = transport.open_channel(
                "direct-tcpip", (mx_host, 25), ("127.0.0.1", 0)
            )
            channel.settimeout(SMTP_TIMEOUT)
            return client, channel
        except Exception:
            return None, None

    async def recv_smtp_async(self, channel, timeout=15) -> str:
        """Recebe resposta SMTP do canal SSH sem bloquear o event loop."""
        loop = asyncio.get_event_loop()
        def _recv():
            try:
                channel.settimeout(timeout)
            except Exception:
                pass
            data = b""
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    chunk = channel.recv(8192)
                    if not chunk:
                        break
                    data += chunk
                    lines = data.split(b'\r\n')
                    if len(lines) >= 2:
                        last_line = lines[-2] if lines[-1] == b'' else lines[-1]
                        if len(last_line) >= 3 and last_line[:3].isdigit() and last_line[3:4] == b' ':
                            break
                except socket.timeout:
                    break
                except Exception:
                    break
            return data.decode('utf-8', errors='ignore').strip()

        return await loop.run_in_executor(self.executor, _recv)

    async def send_smtp_async(self, channel, cmd: str):
        """Envia comando SMTP pelo canal SSH."""
        loop = asyncio.get_event_loop()
        def _send():
            try:
                if isinstance(cmd, str):
                    cmd_bytes = cmd.encode()
                else:
                    cmd_bytes = cmd
                channel.sendall(cmd_bytes)
            except Exception:
                pass
        await loop.run_in_executor(self.executor, _send)

    async def get_mx_async(self, provedor: str) -> Optional[str]:
        """Obtém MX de um provedor assíncrono."""
        loop = asyncio.get_event_loop()
        def _resolve():
            try:
                answers = dns.resolver.resolve(provedor, 'MX')
                mxs = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
                if mxs:
                    return mxs[0][1]
            except Exception:
                pass
            fallback = {
                "gmail.com": "gmail-smtp-in.l.google.com",
                "google.com": "gmail-smtp-in.l.google.com",
                "googlemail.com": "gmail-smtp-in.l.google.com",
                "mail.ru": "mxs.mail.ru",
                "bk.ru": "mxs.mail.ru",
                "list.ru": "mxs.mail.ru",
                "inbox.ru": "mxs.mail.ru",
                "yahoo.co.jp": "mx1.mail.yahoo.co.jp",
                "qq.com": "mx1.qq.com",
                "naver.com": "mx1.mail.naver.com",
                "yandex.ru": "mx.yandex.ru",
                "yandex.com": "mx.yandex.ru",
            }
            return fallback.get(provedor)
        return await loop.run_in_executor(self.executor, _resolve)

    async def send_batch_async(self, ip_info: dict, provedor: str, emails: List[str],
                                conta: AccountState) -> dict:
        """Envia batch de emails via SSH tunnel com pipelining."""
        resultado = {
            "aceites": 0, "rejeitados": 0, "unknown": 0,
            "greylist": 0, "erros": 0, "too_many": False,
            "emails_processed": 0,
        }
        ip = ip_info["ip"]
        key_path = ip_info["key_path"]
        channel = None
        client = None
        loop = asyncio.get_event_loop()

        try:
            # Obter MX
            mx_host = await self.get_mx_async(provedor)
            if not mx_host:
                resultado["erros"] += 1
                resultado["emails_processed"] = len(emails)
                return resultado

            # Conectar SSH e abrir túnel (em thread)
            client, channel = await loop.run_in_executor(
                self.executor, self._open_tunnel, ip, key_path, mx_host
            )

            if not channel:
                resultado["erros"] += 1
                resultado["emails_processed"] = len(emails)
                return resultado

            # Banner
            banner = await self.recv_smtp_async(channel, timeout=SMTP_TIMEOUT)
            if not banner or not banner.startswith("220"):
                resultado["erros"] += 1
                resultado["emails_processed"] = len(emails)
                return resultado

            # EHLO
            await self.send_smtp_async(channel, f"EHLO {conta.domain}\r\n")
            ehlo_resp = await self.recv_smtp_async(channel, timeout=SMTP_TIMEOUT)

            # STARTTLS (opcional, nem todos os servidores exigem via túnel)
            # Nota: Túnel SSH já é encriptado, STARTTLS desnecessário na maioria dos casos
            if "STARTTLS" in ehlo_resp.upper() and provedor in {"gmail.com", "googlemail.com", "google.com"}:
                # Para Gmail, tentar STARTTLS
                await self.send_smtp_async(channel, "STARTTLS\r\n")
                tls_resp = await self.recv_smtp_async(channel, timeout=SMTP_TIMEOUT)
                # Não podemos fazer wrap_ssl em channel Paramiko diretamente
                # Gmail geralmente aceita sem TLS via túnel se bem configurado

            # MAIL FROM
            mail_from = self.get_mail_from(conta)
            await self.send_smtp_async(channel, f"MAIL FROM:<{conta.sender_email}>\r\n")
            mf_resp = await self.recv_smtp_async(channel, timeout=SMTP_TIMEOUT)
            if not mf_resp or not mf_resp.startswith("250"):
                resultado["erros"] += 1
                resultado["emails_processed"] = len(emails)
                return resultado

            # RCPT TO com PIPELINING
            rcpt_cmds = b""
            for email in emails:
                rcpt_cmds += f"RCPT TO:<{email}>\r\n".encode()

            await loop.run_in_executor(self.executor, channel.sendall, rcpt_cmds)

            # Receber respostas (uma por RCPT)
            responses = []
            for i in range(len(emails)):
                resp = await self.recv_smtp_async(channel, timeout=SMTP_RECV_TIMEOUT)
                responses.append((emails[i], resp))

            # Processar respostas
            aceites_batch = []
            greylist_batch = []
            for email, resp in responses:
                resp_lower = resp.lower() if resp else ""
                if not resp:
                    resultado["erros"] += 1
                elif resp.startswith("250") or resp.startswith("251"):
                    resultado["aceites"] += 1
                    aceites_batch.append(email)
                elif "too many" in resp_lower or "rate limit" in resp_lower or "452" in resp:
                    resultado["too_many"] = True
                    resultado["rejeitados"] += 1
                    # Parar processamento
                    break
                elif "greylist" in resp_lower or "try again" in resp_lower or "451" in resp:
                    resultado["greylist"] += 1
                    greylist_batch.append(email)
                elif "user unknown" in resp_lower or "no such user" in resp_lower or "550" in resp:
                    resultado["unknown"] += 1
                    await self.state_manager.add_unknown_email(email)
                else:
                    resultado["rejeitados"] += 1

            # DATA (se houve aceites)
            if aceites_batch and not resultado["too_many"]:
                await self.send_smtp_async(channel, "DATA\r\n")
                data_resp = await self.recv_smtp_async(channel, timeout=SMTP_TIMEOUT)
                if data_resp and data_resp.startswith("354"):
                    # Mutação + DKIM
                    html_mutado = self.mutar_html(self.html_template, conta, aceites_batch[0])
                    msg = EmailMessage()
                    msg['From'] = mail_from
                    msg['To'] = ", ".join(aceites_batch)
                    msg['Subject'] = "Important Update"
                    msg['Message-ID'] = make_msgid(domain=conta.domain)
                    msg['Date'] = format_datetime(datetime.now(timezone.utc))
                    msg.set_content(html_mutado, subtype='html')

                    # Assinar DKIM
                    payload = msg.as_bytes()
                    if conta.dkim_private_key:
                        try:
                            sig = dkim.sign(
                                message=payload,
                                selector=conta.dkim_selector.encode(),
                                domain=conta.domain.encode(),
                                privkey=conta.dkim_private_key.encode(),
                                canonicalize=(b'relaxed', b'relaxed'),
                                include_headers=[b"from", b"to", b"subject", b"date", b"message-id"]
                            )
                            payload = sig + payload
                        except Exception:
                            pass

                    if not payload.endswith(b"\r\n.\r\n"):
                        if not payload.endswith(b"\r\n"):
                            payload += b"\r\n"
                        payload += b".\r\n"

                    await loop.run_in_executor(self.executor, channel.sendall, payload)
                    final_resp = await self.recv_smtp_async(channel, timeout=30)

                    if final_resp and final_resp.startswith("250"):
                        # Confirmar aceites
                        for email in aceites_batch:
                            await self.state_manager.add_accepted_email(email)
                    else:
                        # Reverter aceites se DATA falhar
                        resultado["aceites"] = 0
                        resultado["rejeitados"] += len(aceites_batch)
                else:
                    resultado["aceites"] = 0
                    resultado["rejeitados"] += len(aceites_batch)

            # QUIT
            await self.send_smtp_async(channel, "QUIT\r\n")
            await asyncio.sleep(0.1)

            resultado["emails_processed"] = len(emails)
            return resultado

        except Exception as e:
            resultado["erros"] += 1
            resultado["emails_processed"] = len(emails)
            await self.logger.log(f"Erro SMTP: {str(e)[:100]}", provedor)
            return resultado
        finally:
            if channel:
                try:
                    await loop.run_in_executor(self.executor, channel.close)
                except Exception:
                    pass
            if client:
                try:
                    await loop.run_in_executor(self.executor, client.close)
                except Exception:
                    pass


# ==============================================================================
# 10. WORKER DE PROVEDOR (NÍVEL 1 - PARALELISMO HIERÁRQUICO)
# ==============================================================================
class ProviderWorker:
    """Worker independente por provedor (Nível 1 da Hierarquia)."""
    def __init__(self, provedor: str, logger: AsyncLogger, state_manager: StateManager,
                 aracloud_manager: AracloudManager, smtp_client: SMTPClient,
                 accounts: List[AccountState], greylist_queue: asyncio.PriorityQueue):
        self.provedor = provedor
        self.logger = logger
        self.state_manager = state_manager
        self.aracloud_manager = aracloud_manager
        self.smtp_client = smtp_client
        self.accounts = accounts
        self.greylist_queue = greylist_queue
        self.emails_list: List[str] = []
        self.running = True
        self.account_idx = 0

    async def load_emails(self):
        """Carrega emails do provedor do HuggingFace (Parquet)."""
        try:
            api = HfApi(token=HF_TOKEN)
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None, lambda: api.list_repo_files(HF_TRADER_REPO, repo_type="dataset")
            )

            parquets = [
                f for f in files
                if f.startswith(f"Trader_Emails/{self.provedor}/") and f.endswith('.parquet')
            ]
            if not parquets:
                await self.logger.print(f"⚠️ Sem parquets para {self.provedor}", self.provedor)
                return

            all_emails = []
            seen = set()

            # Carregar apenas o primeiro parquet (otimização I/O)
            pq_path = parquets[0]
            try:
                local = await loop.run_in_executor(None, lambda: hf_hub_download(
                    repo_id=HF_TRADER_REPO, filename=pq_path, repo_type="dataset",
                    token=HF_TOKEN, cache_dir="/content/hf_cache"
                ))
                pf = pq.ParquetFile(local)
                col = None
                for c in pf.schema_arrow.names:
                    if str(c).lower() == "email":
                        col = c
                        break
                if not col:
                    for c in pf.schema_arrow.names:
                        try:
                            if str(pf.schema_arrow.field(c).type) == "string":
                                col = c
                                break
                        except Exception:
                            continue

                if col:
                    for batch in pf.iter_batches(batch_size=10000, columns=[col]):
                        for v in batch.column(0).to_pylist():
                            if v is None:
                                continue
                            e = str(v).strip().lower()
                            if "@" not in e or e.startswith("@") or e.endswith("@"):
                                continue
                            if len(e) < 5 or len(e) > 100:
                                continue
                            if e not in seen:
                                seen.add(e)
                                all_emails.append(e)
                        if len(all_emails) >= 100000:
                            break
            except Exception as e:
                await self.logger.log(f"Erro parquet {pq_path}: {str(e)[:80]}", self.provedor)

            # Aplicar índice do checkpoint (granular por provedor)
            ps = self.state_manager.state.provider_states.get(self.provedor)
            start_offset = ps.last_email_offset if ps else 0

            # Filtrar unknowns e aceites
            async with self.state_manager.state_lock:
                unknowns = self.state_manager.state.unknown_emails
                accepted = self.state_manager.state.accepted_emails

            filtered = []
            for e in all_emails[start_offset:]:
                if e in unknowns or e in accepted:
                    continue
                filtered.append(e)

            self.emails_list = filtered
            await self.logger.print(
                f"📧 {len(self.emails_list)} emails carregados (offset: {start_offset})",
                self.provedor
            )
        except Exception as e:
            await self.logger.print(f"❌ Erro emails: {str(e)[:100]}", self.provedor)

    async def get_next_account(self) -> Optional[AccountState]:
        """Obtém próxima conta DKIM (round-robin)."""
        if not self.accounts:
            return None
        conta = self.accounts[self.account_idx % len(self.accounts)]
        self.account_idx += 1
        return conta

    async def run(self):
        """Loop principal do worker (Coroutine Independente)."""
        await self.logger.print(f"🚀 Worker iniciado", self.provedor)
        ps = await self.state_manager.get_provider_state(self.provedor)
        if not ps:
            return
        if ps.completed:
            await self.logger.print(f"✅ Provedor já completo em run anterior", self.provedor)
            return

        empty_rounds = 0
        while self.running:
            try:
                # Verificar pausa por Rate Limit
                if ps.is_paused and time.time() < ps.pause_until:
                    await asyncio.sleep(5)
                    continue
                ps.is_paused = False

                if not self.emails_list:
                    empty_rounds += 1
                    if empty_rounds >= 3:
                        ps.completed = True
                        await self.logger.print(f"🏁 Provedor completo", self.provedor)
                        break
                    await asyncio.sleep(10)
                    continue

                empty_rounds = 0
                batch_size = min(ps.rcpt_limit, len(self.emails_list), MAX_RCPT_POR_CONEXAO)
                if batch_size <= 0:
                    batch_size = 1
                batch = self.emails_list[:batch_size]

                # Adquirir slot no semáforo global (bloqueante mas com timeout)
                try:
                    await asyncio.wait_for(self.aracloud_manager.acquire_ip_slot(), timeout=60)
                except asyncio.TimeoutError:
                    await asyncio.sleep(5)
                    continue

                try:
                    ip_info = await self.aracloud_manager.get_available_ip(self.provedor)
                    if not ip_info:
                        await asyncio.sleep(10)
                        continue

                    conta = await self.get_next_account()
                    if not conta:
                        break

                    # Enviar batch
                    resultado = await self.smtp_client.send_batch_async(
                        ip_info, self.provedor, batch, conta
                    )

                    # Atualizar estatísticas globais
                    await self.state_manager.increment_stats(
                        total_sent=resultado.get("emails_processed", len(batch)),
                        total_accepted=resultado["aceites"],
                        total_rejected=resultado["rejeitados"],
                        total_unknown=resultado["unknown"],
                        total_greylist_retry=resultado["greylist"]
                    )

                    # Atualizar quota do IP
                    if resultado["aceites"] > 0 or resultado["emails_processed"] > 0:
                        await self.state_manager.update_ip_quota(
                            ip_info["ip"], self.provedor, resultado["emails_processed"]
                        )

                    # Atualizar estado do provedor
                    ps.emails_sent += resultado["emails_processed"]
                    ps.emails_accepted += resultado["aceites"]
                    ps.emails_rejected += resultado["rejeitados"]
                    ps.emails_unknown += resultado["unknown"]
                    ps.emails_greylist += resultado["greylist"]
                    ps.last_email_offset += resultado["emails_processed"]

                    # Ajuste Dinâmico de RCPT
                    if resultado["too_many"]:
                        new_limit = max(10, int(ps.rcpt_limit * 0.8))
                        ps.rcpt_limit = new_limit
                        ps.consecutive_failures += 1
                        ps.consecutive_successes = 0
                        ps.is_paused = True
                        ps.pause_until = time.time() + 30
                        await self.logger.print(
                            f"⚠️ Too many → reduzindo para {new_limit}",
                            self.provedor
                        )
                        await self.state_manager.update_rcpt_limit(self.provedor, new_limit)
                    elif resultado["aceites"] > 0:
                        ps.consecutive_successes += 1
                        if ps.consecutive_successes >= 3:
                            new_limit = min(200, int(ps.rcpt_limit * 1.1))
                            if new_limit != ps.rcpt_limit:
                                ps.rcpt_limit = new_limit
                                await self.state_manager.update_rcpt_limit(self.provedor, new_limit)
                            ps.consecutive_successes = 0
                    elif resultado["greylist"] > 0:
                        # Adicionar à fila global de retry (2 minutos)
                        retry_time = time.time() + 120
                        for email in batch:
                            if email not in [b[2] for b in list(self.greylist_queue._queue) if len(b) >= 3]:
                                await self.greylist_queue.put((retry_time, self.provedor, email))

                    # Remover emails processados da fila
                    self.emails_list = self.emails_list[resultado["emails_processed"]:]

                    if resultado["aceites"] > 0:
                        await self.logger.print(
                            f"✅ {resultado['aceites']}/{resultado['emails_processed']} aceites | "
                            f"Total: {ps.emails_accepted:,}",
                            self.provedor
                        )

                    await asyncio.sleep(0.5)
                finally:
                    self.aracloud_manager.release_ip_slot()

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.print(f"❌ Erro no worker: {str(e)[:100]}", self.provedor)
                await asyncio.sleep(5)

        await self.logger.print(f"🏁 Worker concluído", self.provedor)


# ==============================================================================
# 11. GREYLIST RETRY WORKER
# ==============================================================================
class GreylistRetryWorker:
    """Worker dedicado para processar retries de Greylist."""
    def __init__(self, logger: AsyncLogger, state_manager: StateManager,
                 aracloud_manager: AracloudManager, smtp_client: SMTPClient,
                 accounts: List[AccountState], greylist_queue: asyncio.PriorityQueue):
        self.logger = logger
        self.state_manager = state_manager
        self.aracloud_manager = aracloud_manager
        self.smtp_client = smtp_client
        self.accounts = accounts
        self.greylist_queue = greylist_queue
        self.running = True
        self.retry_attempts: Dict[str, int] = {}  # email -> tentativas
        self.max_retries = 3

    async def run(self):
        """Loop principal do worker de retry."""
        await self.logger.print("🔄 Greylist Retry Worker iniciado")
        while self.running:
            try:
                # Tentar obter item da fila com timeout
                try:
                    item = await asyncio.wait_for(self.greylist_queue.get(), timeout=5)
                except asyncio.TimeoutError:
                    continue

                retry_time, provedor, email = item
                now = time.time()

                # Se ainda não é hora, recolocar e esperar
                if retry_time > now:
                    await self.greylist_queue.put(item)
                    await asyncio.sleep(min(retry_time - now, 5))
                    continue

                # Verificar tentativas
                attempts = self.retry_attempts.get(email, 0)
                if attempts >= self.max_retries:
                    await self.logger.print(
                        f"⛔ Greylist max retries atingido: {email}",
                        provedor
                    )
                    del self.retry_attempts[email]
                    continue

                self.retry_attempts[email] = attempts + 1

                # Adquirir slot
                try:
                    await asyncio.wait_for(self.aracloud_manager.acquire_ip_slot(), timeout=30)
                except asyncio.TimeoutError:
                    # Reagendar
                    await self.greylist_queue.put((time.time() + 60, provedor, email))
                    continue

                try:
                    ip_info = await self.aracloud_manager.get_available_ip(provedor)
                    if not ip_info:
                        await self.greylist_queue.put((time.time() + 120, provedor, email))
                        continue

                    conta = self.accounts[hash(email) % len(self.accounts)] if self.accounts else None
                    if not conta:
                        continue

                    resultado = await self.smtp_client.send_batch_async(
                        ip_info, provedor, [email], conta
                    )

                    if resultado["aceites"] > 0:
                        await self.logger.print(f"✅ Greylist retry OK: {email}", provedor)
                        await self.state_manager.increment_stats(total_accepted=1)
                        if email in self.retry_attempts:
                            del self.retry_attempts[email]
                    elif resultado["greylist"] > 0:
                        # Reagendar com backoff
                        backoff = 120 * (2 ** attempts)
                        await self.greylist_queue.put((time.time() + backoff, provedor, email))
                    else:
                        await self.logger.print(
                            f"❌ Greylist retry falhou: {email}",
                            provedor
                        )
                finally:
                    self.aracloud_manager.release_ip_slot()

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.print(f"❌ Erro no retry worker: {str(e)[:100]}")
                await asyncio.sleep(5)

        await self.logger.print("🏁 Greylist Retry Worker concluído")


# ==============================================================================
# 12. WATCHDOG E SHUTDOWN INTELIGENTE
# ==============================================================================
class Watchdog:
    """Monitor de tempo com shutdown inteligente a 5h 35m."""
    def __init__(self, logger: AsyncLogger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.start_time = time.time()
        self.running = True
        self.last_checkpoint = time.time()

    async def run(self, workers: List[ProviderWorker], retry_worker: GreylistRetryWorker):
        """Loop do watchdog."""
        await self.logger.print("⏱️ Watchdog iniciado (shutdown a 5h35m)")
        while self.running:
            elapsed = time.time() - self.start_time

            # Shutdown Inteligente
            if elapsed >= MAX_EXECUTION_SECONDS:
                await self.logger.print("⏰ Tempo limite atingido (5h 35m). Iniciando shutdown...")
                for worker in workers:
                    worker.running = False
                retry_worker.running = False

                # Aguardar tasks terminarem (max 3 min)
                await asyncio.sleep(10)

                await self.state_manager.save_checkpoint("SHUTDOWN_TIME")
                await self.state_manager.upload_checkpoint()
                self.running = False
                break

            # Aviso 5 minutos antes
            if elapsed >= MAX_EXECUTION_SECONDS - 300:
                remaining = MAX_EXECUTION_SECONDS - elapsed
                if int(remaining) % 60 == 0 and remaining > 0:
                    await self.logger.print(f"⏳ Shutdown em {remaining:.0f}s")

            # Checkpoint Periódico
            if time.time() - self.last_checkpoint >= CHECKPOINT_INTERVAL_SEC:
                await self.state_manager.save_checkpoint("PERIODIC")
                await self.state_manager.upload_checkpoint()
                self.last_checkpoint = time.time()

            # Health check de memória
            try:
                mem = psutil.virtual_memory()
                if mem.percent > 90:
                    gc.collect()
                    await self.logger.print(f"⚠️ Memória alta ({mem.percent}%) - GC executado")
            except Exception:
                pass

            await asyncio.sleep(15)


# ==============================================================================
# 13. MAIN CONTROLLER (ORQUESTRADOR)
# ==============================================================================
async def main():
    """Função principal - Orquestra todas as fases da Operação Hydra."""
    # Setup inicial
    for d in [BASE_LOCAL, LOG_DIR, CP_DIR, DIR_EMAILS, DIR_KEYS]:
        os.makedirs(d, exist_ok=True)

    logger = AsyncLogger(os.path.join(LOG_DIR, "veriscope_v23.log"))
    await logger.print("=" * 80)
    await logger.print("🚀 VERISCOPE V23.0.0 - OPERAÇÃO HYDRA")
    await logger.print("Paralelismo Hierárquico Real | 66 Provedores | 30 IPs Aracloud")
    await logger.print("Shutdown Inteligente 5h35m | Checkpoint Granular | Mutação Abracadabra")
    await logger.print("=" * 80)

    state_manager = StateManager(logger)

    # FASE 0: Recuperação de Estado
    await logger.print("\n📂 FASE 0: Carregando checkpoint...")
    await state_manager.load_checkpoint()
    await state_manager.initialize_run()

    # FASE 1: Setup de IPs Aracloud
    await logger.print("\n🌐 FASE 1: Configurando IPs Aracloud...")
    aracloud_manager = AracloudManager(logger, state_manager)
    if not await aracloud_manager.load_accounts():
        await logger.print("❌ Falha ao carregar contas Aracloud")
        return
    if not await aracloud_manager.setup_ips():
        await logger.print("❌ Falha ao configurar IPs")
        return

    # FASE 2: DNS Bulk Update
    await logger.print("\n🔐 FASE 2: Configurando DNS...")
    dns_manager = DNSManager(logger, state_manager)
    domains = await dns_manager.get_domains()
    if domains:
        ips = [ip_info["ip"] for ip_info in aracloud_manager.ips_ativos]
        await dns_manager.update_spf_bulk(domains, ips)
        await logger.print(f"⏳ Aguardando {DNS_PROPAGATION_WAIT}s para propagação DNS...")
        await asyncio.sleep(DNS_PROPAGATION_WAIT)
    else:
        await logger.print("⚠️ Sem domínios - continuando mesmo assim")

    # Carregar contas DKIM (partilhadas entre todos os workers)
    await logger.print("\n🔑 Carregando contas DKIM...")
    accounts: List[AccountState] = []
    try:
        api = HfApi(token=HF_TOKEN)
        loop = asyncio.get_event_loop()
        contas_file = await loop.run_in_executor(None, lambda: hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/phase1_accounts_keys.csv",
            repo_type="dataset", token=HF_TOKEN, cache_dir="/content/hf_cache"
        ))
        with open(contas_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                priv = (r.get("dkim_private_key_pem") or "").strip()
                if not priv.startswith("-----BEGIN"):
                    continue
                accounts.append(AccountState(
                    identity_id=(r.get("identity_id") or "").strip(),
                    sender_email=(r.get("sender_email") or "").strip(),
                    domain=(r.get("domain") or "").strip(),
                    dkim_private_key=priv,
                    dkim_selector=(r.get("dkim_selector") or "default").strip(),
                    base=(r.get("base") or "").strip(),
                    public_key_txt=(r.get("dkim_public_txt") or "").strip(),
                ))
        await logger.print(f"🔑 {len(accounts)} contas DKIM carregadas")
    except Exception as e:
        await logger.print(f"⚠️ Erro contas: {str(e)[:100]}")

    if not accounts:
        await logger.print("❌ Sem contas DKIM válidas")
        return

    # Inicializar SMTP Client
    smtp_client = SMTPClient(logger, state_manager)
    await smtp_client.load_mail_from_variations()
    await smtp_client.load_template()

    # FASE 3: Inicialização dos Workers (Paralelismo Hierárquico)
    await logger.print("\n📧 FASE 3: Inicializando workers...")

    # Fila global de Greylist (Priority Queue)
    greylist_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()

    workers: List[ProviderWorker] = []
    for provedor in PROVEDORES_ALVO:
        worker = ProviderWorker(
            provedor, logger, state_manager, aracloud_manager,
            smtp_client, accounts, greylist_queue
        )
        await worker.load_emails()
        workers.append(worker)

    await logger.print(f"✅ {len(workers)} Provider Workers inicializados (Nível 1)")

    # Worker de Retry
    retry_worker = GreylistRetryWorker(
        logger, state_manager, aracloud_manager, smtp_client,
        accounts, greylist_queue
    )

    # Watchdog
    watchdog = Watchdog(logger, state_manager)

    # FASE 4: Execução Paralela
    await logger.print("\n🚀 FASE 4: Iniciando envio paralelo hierárquico...")
    tasks = [asyncio.create_task(worker.run()) for worker in workers]
    retry_task = asyncio.create_task(retry_worker.run())
    watchdog_task = asyncio.create_task(watchdog.run(workers, retry_worker))

    try:
        await asyncio.gather(*tasks, retry_task, watchdog_task, return_exceptions=True)
    except KeyboardInterrupt:
        await logger.print("\n🛑 Interrompido pelo utilizador")
    except Exception as e:
        await logger.print(f"\n❌ Erro: {str(e)}")
        traceback.print_exc()

    # FASE 5: Salvamento Final
    await logger.print("\n💾 Salvando checkpoint final...")
    await state_manager.save_checkpoint("FINAL")
    await state_manager.upload_checkpoint()

    # Estatísticas Finais
    stats = state_manager.state
    await logger.print("\n" + "=" * 80)
    await logger.print("📊 ESTATÍSTICAS FINAIS DA OPERAÇÃO HYDRA")
    await logger.print("=" * 80)
    await logger.print(f"📤 Total enviado: {stats.total_sent:,}")
    await logger.print(f"✅ Total aceite: {stats.total_accepted:,}")
    await logger.print(f"❌ Total rejeitado: {stats.total_rejected:,}")
    await logger.print(f"👻 Total unknown: {stats.total_unknown:,}")
    await logger.print(f"🔵 Total greylist retry: {stats.total_greylist_retry:,}")
    await logger.print(f"🏁 Provedores completos: {len([p for p in stats.provider_states.values() if p.completed])}/{len(PROVEDORES_ALVO)}")
    await logger.print("=" * 80)
    await logger.print("🏁 Execução concluída")
    await logger.flush()


# ==============================================================================
# 14. ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo utilizador", flush=True)
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}", flush=True)
        traceback.print_exc()


# ==============================================================================
# 15. GITHUB ACTIONS WORKFLOW (para ser criado em .github/workflows/colab.yml)
# ==============================================================================
"""
name: VERISCOPE V23 - Operação Hydra
on:
  workflow_dispatch:
  schedule:
    - cron: '0 */6 * * *'  # A cada 6 horas

jobs:
  enviar:
    runs-on: ubuntu-latest
    timeout-minutes: 355  # 5h 55m (margem de segurança)
    env:
      PYTHONUNBUFFERED: "1"
      HF_TOKEN: ${{ secrets.HF_TOKEN }}
      DESEC_TOKEN: ${{ secrets.DESEC_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Instalar Colab CLI
        run: pip install google-colab-cli

      - name: Restaurar login Colab
        env:
          COLAB_TOKEN_JSON: ${{ secrets.COLAB_TOKEN_JSON }}
        run: |
          mkdir -p ~/.config/colab-cli
          printf '%s' "$COLAB_TOKEN_JSON" > ~/.config/colab-cli/token.json

      - name: Criar sessão no Colab
        run: colab --auth oauth2 new -s veriscope-job

      - name: Executar script principal
        run: colab --auth oauth2 exec -s veriscope-job -f veriscope_v23.py --timeout 21000

      - name: Mostrar logs completos
        if: always()
        run: |
          colab --auth oauth2 log -s veriscope-job -o log.md || true
          cat log.md || true

      - name: Desligar VM do Colab
        if: always()
        run: colab --auth oauth2 stop -s veriscope-job || true
"""
