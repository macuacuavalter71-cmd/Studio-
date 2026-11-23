# -*- coding: utf-8 -*-
"""
VERISCOPE V23.0.0 - SISTEMA DE ENVIO MASSIVO COMPLETO
Paralelismo Hierárquico | 30 IPs Aracloud | 407 Provedores | 2116 Contas
Checkpoint Inteligente | Mutação Abracadabra | DNS Bulk Update

Execução: GitHub Actions + Colab CLI
Tempo por run: 5h 40m (shutdown inteligente)
"""

import os, sys, json, time, threading, traceback, socket, re, csv, shutil, subprocess, resource, asyncio, ssl
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Set, Optional, Any, Tuple
import hashlib
import base64
from email.message import EmailMessage
from email.utils import make_msgid, format_datetime
from urllib.parse import urlparse, urlunparse

# ============================================================
# CONFIGURAÇÕES GLOBAIS
# ============================================================
ARACLOUD_IP = "209.127.114.175"
ARACLOUD_SSH_HOST = "209.127.114.175"
SSH_USER = "ubuntu"
SSH_PKEY_PATH = "/content/k.pem"

# Tokens (HF_TOKEN vem das secrets do GitHub Actions)
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()
DESEC_TOKEN = os.environ.get("DESEC_TOKEN", "LuWfqx1GL3uXDmyUEExH1tF4Nena").strip()

# Repositórios HuggingFace
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_CHECKPOINT_REPO = "Valter3B/Veriscope_Checkpoints"
HF_ARACLOUD_REPO = "Valter3B/aracloud-servers"
HF_LOGS_REPO = "Valter3B/Campaign_Logs"

# Parâmetros de execução
MAX_EXECUTION_SECONDS = 5 * 3600 + 35 * 60  # 5h 35m
CHECKPOINT_INTERVAL = 5 * 60  # 5 minutos
SMTP_TIMEOUT = 20
MAX_IPS_SIMULTANEOS = 30
MAX_EMAILS_POR_IP_PROVEDOR = 1000
MAX_IPS_UNICOS_POR_CONTA = 20
NUM_CONTAS_ARACLOUD = 10

# Paralelismo
NUM_WORKERS_POR_PROVEDOR = 5
MAX_COROUTINES_SIMULTANEAS = 500

# Provedores confirmados (66 total do teste V22.3.0)
PROVEDORES_ALVO = [
    "bebo.com", "bk.ru", "cemsmail.org", "dreamwiz.com", "google.com",
    "inbox.ru", "list.ru", "mail.ru", "pisem.net", "rock.com",
    "sprintpcs.com", "ukr.net", "walla.com", "yahoo.co.jp", "youtube.com",
    "seznam.cz", "yeah.net", "nate.com", "clearwire.net", "terra.com.br",
    "telus.net", "eyou.com", "email.cz", "telkomsa.net", "rambler.ru",
    "student.gccisd.net", "21cn.com",
    "googlemail.com", "gmail.com", "oranga.fr", "inwind.it",
    "myemailboxmail.com", "trbvm.com", "dodgeit.com", "ya.ru",
    "qq.com", "foxmail.com", "libero.it", "naver.com", "vip.qq.com",
    "virgilio.it", "yandex.com", "freemail.hu", "yandex.ru",
    "poczta.onet.pl", "docomo.ne.jp", "amorki.pl", "live.co.uk",
    "onet.eu", "onet.pl", "breakthru.com", "clear.net.nz",
    "hot.com", "op.pl", "pchome.com.tw", "poczta.pl", "yandex.net",
    "i.ua", "interia.eu", "interia.pl", "poczta.fm",
    "gaggle.net", "love.com", "qip.ru", "univision.com", "vipmail.net"
]

# Provedores com greylist
PROVEDORES_GREYLIST = {"i.ua", "interia.eu", "interia.pl", "poczta.fm", "seznam.cz", "gmail.com"}

# ============================================================
# ESTRUTURAS DE DADOS
# ============================================================
@dataclass
class ProviderState:
    """Estado isolado por provedor"""
    name: str
    rcpt_limit: int = 50
    emails_sent: int = 0
    emails_accepted: int = 0
    emails_rejected: int = 0
    emails_unknown: int = 0
    greylist_pending: List[str] = field(default_factory=list)
    quota_by_ip: Dict[str, int] = field(default_factory=dict)
    last_activity: float = 0.0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

@dataclass
class IPState:
    """Estado de um IP Aracloud"""
    ip: str
    account_name: str
    status: str = "ACTIVE"  # ACTIVE, EXHAUSTED, BLOCKED, ROTATING
    created_at: str = ""
    sent_total: int = 0
    sent_by_provider: Dict[str, int] = field(default_factory=dict)
    ssh_client: Any = None
    
    def to_dict(self):
        d = asdict(self)
        d.pop('ssh_client', None)
        return d
    
    @classmethod
    def from_dict(cls, data):
        data = data.copy()
        data.pop('ssh_client', None)
        return cls(**data)

@dataclass
class AccountState:
    """Estado de uma conta DKIM"""
    identity_id: str
    sender_email: str
    domain: str
    dkim_private_key: str = ""
    dkim_selector: str = "default"
    emails_sent: int = 0
    last_used: str = ""
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

@dataclass
class GlobalState:
    """Estado global do sistema"""
    version: str = "23.0.0"
    run_id: str = ""
    started_at: str = ""
    ended_at: str = ""
    
    # Estado global
    last_email_index: int = 0
    total_sent: int = 0
    total_accepted: int = 0
    total_rejected: int = 0
    total_unknown: int = 0
    total_greylist_retry: int = 0
    
    # IPs usados hoje (anti-repetição)
    ips_used_today: Dict[str, List[str]] = field(default_factory=dict)
    
    # Estado por provedor
    provider_states: Dict[str, ProviderState] = field(default_factory=dict)
    
    # Estado por IP
    ip_states: Dict[str, IPState] = field(default_factory=dict)
    
    # Estado por conta
    account_states: Dict[str, AccountState] = field(default_factory=dict)
    
    # Limites RCPT aprendidos
    limits_rcpt: Dict[str, int] = field(default_factory=dict)
    
    # Blacklist persistente
    unknown_emails: Set[str] = field(default_factory=set)
    
    # Lista de aceites
    accepted_emails: Set[str] = field(default_factory=set)
    
    # DNS state
    dns_state: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self):
        d = {
            'version': self.version,
            'run_id': self.run_id,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'last_email_index': self.last_email_index,
            'total_sent': self.total_sent,
            'total_accepted': self.total_accepted,
            'total_rejected': self.total_rejected,
            'total_unknown': self.total_unknown,
            'total_greylist_retry': self.total_greylist_retry,
            'ips_used_today': self.ips_used_today,
            'provider_states': {k: v.to_dict() for k, v in self.provider_states.items()},
            'ip_states': {k: v.to_dict() for k, v in self.ip_states.items()},
            'account_states': {k: v.to_dict() for k, v in self.account_states.items()},
            'limits_rcpt': self.limits_rcpt,
            'unknown_emails': list(self.unknown_emails),
            'accepted_emails': list(self.accepted_emails),
            'dns_state': self.dns_state,
        }
        return d
    
    @classmethod
    def from_dict(cls, data):
        state = cls()
        state.version = data.get('version', '23.0.0')
        state.run_id = data.get('run_id', '')
        state.started_at = data.get('started_at', '')
        state.ended_at = data.get('ended_at', '')
        state.last_email_index = data.get('last_email_index', 0)
        state.total_sent = data.get('total_sent', 0)
        state.total_accepted = data.get('total_accepted', 0)
        state.total_rejected = data.get('total_rejected', 0)
        state.total_unknown = data.get('total_unknown', 0)
        state.total_greylist_retry = data.get('total_greylist_retry', 0)
        state.ips_used_today = data.get('ips_used_today', {})
        
        for k, v in data.get('provider_states', {}).items():
            state.provider_states[k] = ProviderState.from_dict(v)
        
        for k, v in data.get('ip_states', {}).items():
            state.ip_states[k] = IPState.from_dict(v)
        
        for k, v in data.get('account_states', {}).items():
            state.account_states[k] = AccountState.from_dict(v)
        
        state.limits_rcpt = data.get('limits_rcpt', {})
        state.unknown_emails = set(data.get('unknown_emails', []))
        state.accepted_emails = set(data.get('accepted_emails', []))
        state.dns_state = data.get('dns_state', {})
        
        return state

# ============================================================
# INSTALAÇÃO DE PACOTES
# ============================================================
def install_packages():
    """Instala pacotes necessários"""
    packages = {
        "requests": "requests",
        "dnspython": "dns",
        "cryptography": "cryptography",
        "pandas": "pandas",
        "pyarrow": "pyarrow",
        "huggingface_hub": "huggingface_hub",
        "psutil": "psutil",
        "paramiko": "paramiko",
        "dkimpy": "dkim",
        "aiohttp": "aiohttp",
    }
    for pkg, mod in packages.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

install_packages()

# Imports após instalação
import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "urllib3", "paramiko", "asyncio"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)
import warnings
warnings.filterwarnings("ignore")

import requests
import dns.resolver
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
import paramiko
import dkim
import psutil
import aiohttp

# Otimizações de sistema
threading.stack_size(64 * 1024)
try:
    resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
except Exception:
    pass

# ============================================================
# LOGGING
# ============================================================
class Logger:
    """Sistema de logging com buffer e flush automático"""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush = time.time()
        self.buffer_size = 1000
        self.flush_interval = 30
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Limpar ficheiro antigo
        if os.path.exists(log_file):
            os.remove(log_file)
    
    def log(self, msg: str, provedor: str = ""):
        """Adiciona mensagem ao log"""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        prefix = f"[{provedor}] " if provedor else ""
        line = f"[{ts}] {prefix}{msg}\n"
        
        with self.buffer_lock:
            self.buffer.append(line)
            
            # Flush automático
            if len(self.buffer) >= self.buffer_size or \
               time.time() - self.last_flush > self.flush_interval:
                self._flush_locked()
    
    def _flush_locked(self):
        """Flush do buffer para ficheiro (deve ser chamado com lock)"""
        if not self.buffer:
            return
        
        try:
            with open(self.log_file, "a", encoding="utf-8", errors="replace") as f:
                f.writelines(self.buffer)
            self.buffer = []
            self.last_flush = time.time()
        except Exception as e:
            print(f"[LOGGER ERROR] {e}", flush=True)
    
    def flush(self):
        """Flush manual do buffer"""
        with self.buffer_lock:
            self._flush_locked()
    
    def print(self, msg: str, provedor: str = ""):
        """Imprime no stdout e adiciona ao log"""
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{provedor}] " if provedor else ""
        line = f"[{ts}] {prefix}{msg}"
        print(line, flush=True)
        self.log(msg, provedor)

# ============================================================
# GESTÃO DE ESTADO GLOBAL
# ============================================================
class StateManager:
    """Gere o estado global com persistência"""
    
    def __init__(self, logger: Logger):
        self.logger = logger
        self.state = GlobalState()
        self.state_lock = threading.Lock()
        self.checkpoint_dir = "/content/checkpoints"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
    
    def initialize_run(self):
        """Inicializa uma nova run"""
        with self.state_lock:
            self.state.run_id = f"run_{int(time.time())}"
            self.state.started_at = datetime.now(timezone.utc).isoformat()
            
            # Inicializar estados de provedores
            for provedor in PROVEDORES_ALVO:
                if provedor not in self.state.provider_states:
                    self.state.provider_states[provedor] = ProviderState(
                        name=provedor,
                        rcpt_limit=self.state.limits_rcpt.get(provedor, 50)
                    )
            
            self.logger.print(f"🚀 Run {self.state.run_id} iniciada")
    
    def save_checkpoint(self, reason: str = "PERIODIC"):
        """Salva checkpoint completo"""
        with self.state_lock:
            self.state.ended_at = datetime.now(timezone.utc).isoformat()
            
            checkpoint = self.state.to_dict()
            
            # Calcular hashes para integridade
            checkpoint_json = json.dumps(checkpoint, indent=2, ensure_ascii=False, default=str)
            checkpoint_hash = hashlib.sha256(checkpoint_json.encode()).hexdigest()
            
            # Salvar localmente
            checkpoint_file = os.path.join(self.checkpoint_dir, "master_checkpoint.json")
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                f.write(checkpoint_json)
            
            # Salvar hash
            hash_file = os.path.join(self.checkpoint_dir, "manifest.json")
            manifest = {
                "checkpoint_hash": checkpoint_hash,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
                "stats": {
                    "total_sent": self.state.total_sent,
                    "total_accepted": self.state.total_accepted,
                    "total_rejected": self.state.total_rejected,
                    "total_unknown": self.state.total_unknown,
                    "last_email_index": self.state.last_email_index,
                    "active_ips": len([ip for ip in self.state.ip_states.values() if ip.status == "ACTIVE"]),
                }
            }
            with open(hash_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            
            self.logger.print(f"💾 Checkpoint salvo: {reason} | Hash: {checkpoint_hash[:16]}...")
            
            return checkpoint_file, hash_file
    
    def load_checkpoint(self):
        """Carrega checkpoint do HuggingFace ou local"""
        try:
            # Tentar baixar do HuggingFace
            api = HfApi(token=HF_TOKEN)
            
            # Verificar se existe checkpoint
            files = api.list_repo_files(HF_CHECKPOINT_REPO, repo_type="dataset")
            if "checkpoints/master_checkpoint.json" not in files:
                self.logger.print("📭 Nenhum checkpoint encontrado - começando do zero")
                return False
            
            # Baixar checkpoint
            local_checkpoint = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO,
                filename="checkpoints/master_checkpoint.json",
                repo_type="dataset",
                token=HF_TOKEN,
                cache_dir="/content/hf_cache"
            )
            
            # Baixar manifest
            local_manifest = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO,
                filename="checkpoints/manifest.json",
                repo_type="dataset",
                token=HF_TOKEN,
                cache_dir="/content/hf_cache"
            )
            
            # Validar integridade
            with open(local_manifest, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            
            with open(local_checkpoint, "r", encoding="utf-8") as f:
                checkpoint_json = f.read()
            
            calculated_hash = hashlib.sha256(checkpoint_json.encode()).hexdigest()
            
            if calculated_hash != manifest["checkpoint_hash"]:
                self.logger.print(f"⚠️ Hash mismatch! Esperado: {manifest['checkpoint_hash'][:16]}, Calculado: {calculated_hash[:16]}")
                self.logger.print("🔄 Continuando mesmo assim...")
            
            # Carregar estado
            checkpoint = json.loads(checkpoint_json)
            self.state = GlobalState.from_dict(checkpoint)
            
            stats = manifest.get("stats", {})
            self.logger.print(f"✅ Checkpoint carregado:")
            self.logger.print(f"   📤 Enviados: {stats.get('total_sent', 0):,}")
            self.logger.print(f"   ✅ Aceites: {stats.get('total_accepted', 0):,}")
            self.logger.print(f"   📍 Último email: #{stats.get('last_email_index', 0):,}")
            self.logger.print(f"   🌐 IPs ativos: {stats.get('active_ips', 0)}")
            
            return True
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao carregar checkpoint: {str(e)[:100]}")
            return False
    
    def upload_checkpoint(self):
        """Upload do checkpoint para HuggingFace"""
        try:
            api = HfApi(token=HF_TOKEN)
            
            checkpoint_file = os.path.join(self.checkpoint_dir, "master_checkpoint.json")
            manifest_file = os.path.join(self.checkpoint_dir, "manifest.json")
            
            if not os.path.exists(checkpoint_file):
                self.logger.print("⚠️ Checkpoint não existe localmente")
                return
            
            # Upload checkpoint
            api.upload_file(
                path_or_fileobj=checkpoint_file,
                path_in_repo="checkpoints/master_checkpoint.json",
                repo_id=HF_CHECKPOINT_REPO,
                repo_type="dataset",
                token=HF_TOKEN
            )
            
            # Upload manifest
            api.upload_file(
                path_or_fileobj=manifest_file,
                path_in_repo="checkpoints/manifest.json",
                repo_id=HF_CHECKPOINT_REPO,
                repo_type="dataset",
                token=HF_TOKEN
            )
            
            self.logger.print("☁️ Checkpoint enviado para HuggingFace")
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao enviar checkpoint: {str(e)[:100]}")
    
    def update_provider_state(self, provedor: str, **kwargs):
        """Atualiza estado de um provedor"""
        with self.state_lock:
            if provedor not in self.state.provider_states:
                self.state.provider_states[provedor] = ProviderState(name=provedor)
            
            ps = self.state.provider_states[provedor]
            for key, value in kwargs.items():
                if hasattr(ps, key):
                    setattr(ps, key, value)
            ps.last_activity = time.time()
    
    def increment_stats(self, **kwargs):
        """Incrementa estatísticas globais"""
        with self.state_lock:
            for key, value in kwargs.items():
                if hasattr(self.state, key):
                    current = getattr(self.state, key)
                    setattr(self.state, key, current + value)
    
    def add_unknown_email(self, email: str):
        """Adiciona email à blacklist"""
        with self.state_lock:
            self.state.unknown_emails.add(email.lower().strip())
    
    def add_accepted_email(self, email: str):
        """Adiciona email à lista de aceites"""
        with self.state_lock:
            self.state.accepted_emails.add(email.lower().strip())
    
    def is_email_unknown(self, email: str) -> bool:
        """Verifica se email está na blacklist"""
        return email.lower().strip() in self.state.unknown_emails
    
    def is_email_accepted(self, email: str) -> bool:
        """Verifica se email já foi aceite"""
        return email.lower().strip() in self.state.accepted_emails

# ============================================================
# GESTÃO DE ARACLOUD IPs
# ============================================================
class AracloudManager:
    """Gere IPs Aracloud com rotação inteligente"""
    
    def __init__(self, logger: Logger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.accounts = []
        self.ips_ativos = []
        self.ip_lock = threading.Lock()
        
    def load_accounts(self):
        """Carrega credenciais das 10 contas Aracloud do HuggingFace"""
        try:
            api = HfApi(token=HF_TOKEN)
            files = api.list_repo_files(HF_ARACLOUD_REPO, repo_type="dataset")
            
            # Encontrar pastas de servidores
            server_dirs = set()
            for f in files:
                parts = f.split('/')
                if len(parts) >= 2 and parts[0].startswith('servidor-'):
                    server_dirs.add(parts[0])
            
            if len(server_dirs) < 10:
                self.logger.print(f"⚠️ Apenas {len(server_dirs)} servidores encontrados (esperado 10)")
            
            # Carregar credenciais de cada servidor
            for server_dir in sorted(server_dirs)[:10]:
                try:
                    # Baixar credenciais
                    cred_file = hf_hub_download(
                        repo_id=HF_ARACLOUD_REPO,
                        filename=f"{server_dir}/credenciais.json",
                        repo_type="dataset",
                        token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    )
                    
                    # Baixar chave SSH
                    key_file = hf_hub_download(
                        repo_id=HF_ARACLOUD_REPO,
                        filename=f"{server_dir}/k.pem",
                        repo_type="dataset",
                        token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    )
                    
                    with open(cred_file, "r", encoding="utf-8") as f:
                        cred = json.load(f)
                    
                    # Copiar chave para localização padrão
                    key_dest = f"/content/k_{server_dir}.pem"
                    shutil.copy(key_file, key_dest)
                    os.chmod(key_dest, 0o600)
                    
                    self.accounts.append({
                        "name": server_dir,
                        "hostname": cred.get("hostname", ""),
                        "api_user": cred["api"]["utilizador"],
                        "api_password": cred["api"]["password"],
                        "key_path": key_dest,
                        "current_ips": [],
                    })
                    
                    self.logger.print(f"✅ Conta carregada: {server_dir}")
                    
                except Exception as e:
                    self.logger.print(f"⚠️ Erro ao carregar {server_dir}: {str(e)[:80]}")
            
            self.logger.print(f"📋 {len(self.accounts)} contas Aracloud carregadas")
            return len(self.accounts) >= 10
            
        except Exception as e:
            self.logger.print(f"❌ Erro ao carregar contas: {str(e)[:100]}")
            return False
    
    def get_ip_from_account(self, account: dict) -> Optional[str]:
        """Obtém IP de uma conta via API Aracloud"""
        try:
            # Autenticar
            auth_url = "https://vhi.servermania.com:5000/v3/auth/tokens"
            auth_data = {
                "auth": {
                    "identity": {
                        "methods": ["password"],
                        "password": {
                            "user": {
                                "name": account["api_user"],
                                "domain": {"id": "default"},
                                "password": account["api_password"]
                            }
                        }
                    }
                }
            }
            
            response = requests.post(auth_url, json=auth_data, timeout=30, verify=False)
            if response.status_code != 201:
                self.logger.print(f"⚠️ Auth falhou para {account['name']}: {response.status_code}")
                return None
            
            token = response.headers.get("X-Subject-Token")
            headers = {"X-Auth-Token": token}
            
            # Listar servidores
            servers_url = f"{response.json()['token']['catalog'][0]['endpoints'][0]['url']}/servers/detail"
            servers_response = requests.get(servers_url, headers=headers, timeout=30, verify=False)
            
            if servers_response.status_code != 200:
                return None
            
            servers = servers_response.json().get("servers", [])
            
            # Encontrar servidor pelo hostname
            target_server = None
            for server in servers:
                if server.get("name") == account["hostname"]:
                    target_server = server
                    break
            
            if not target_server:
                self.logger.print(f"⚠️ Servidor {account['hostname']} não encontrado")
                return None
            
            # Extrair IP
            for network, addresses in target_server.get("addresses", {}).items():
                for addr in addresses:
                    if addr.get("version") == 4:
                        return addr.get("addr")
            
            return None
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao obter IP de {account['name']}: {str(e)[:80]}")
            return None
    
    def setup_ips(self):
        """Configura 30 IPs simultâneos (3 por conta)"""
        self.logger.print("🌐 Configurando 30 IPs Aracloud...")
        
        today = datetime.now().strftime("%Y-%m-%d")
        ips_usados_hoje = self.state_manager.state.ips_used_today.get(today, [])
        
        ips_configurados = []
        
        for account in self.accounts[:10]:
            self.logger.print(f"📡 Processando conta: {account['name']}")
            
            # Obter IP atual
            ip = self.get_ip_from_account(account)
            
            if not ip:
                self.logger.print(f"⚠️ Não foi possível obter IP de {account['name']}")
                continue
            
            # Verificar se IP já foi usado hoje
            if ip in ips_usados_hoje:
                self.logger.print(f"⚠️ IP {ip} já foi usado hoje - marcando para rotação")
                # TODO: Implementar rotação de IP aqui
            
            # Testar conectividade SSH
            if self.test_ssh_connection(ip, account["key_path"]):
                ips_configurados.append({
                    "ip": ip,
                    "account": account,
                    "key_path": account["key_path"],
                })
                self.logger.print(f"✅ IP {ip} pronto ({account['name']})")
            else:
                self.logger.print(f"❌ SSH falhou para {ip}")
        
        self.ips_ativos = ips_configurados
        self.logger.print(f"🎯 {len(self.ips_ativos)} IPs configurados e prontos")
        
        return len(self.ips_ativos) >= 1
    
    def test_ssh_connection(self, ip: str, key_path: str) -> bool:
        """Testa conexão SSH com um IP"""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(
                ip,
                port=22,
                username=SSH_USER,
                pkey=pkey,
                timeout=15,
                allow_agent=False,
                look_for_keys=False
            )
            
            # Testar comando simples
            stdin, stdout, stderr = client.exec_command("echo test", timeout=10)
            output = stdout.read().decode().strip()
            
            client.close()
            
            return output == "test"
            
        except Exception as e:
            return False
    
    def get_available_ip(self, provedor: str) -> Optional[dict]:
        """Obtém um IP disponível com quota para o provedor"""
        with self.ip_lock:
            for ip_info in self.ips_ativos:
                ip = ip_info["ip"]
                
                # Verificar quota
                ip_state = self.state_manager.state.ip_states.get(ip)
                if ip_state:
                    sent = ip_state.sent_by_provider.get(provedor, 0)
                    if sent >= MAX_EMAILS_POR_IP_PROVEDOR:
                        continue  # IP esgotado para este provedor
                
                return ip_info
            
            return None
    
    def update_ip_quota(self, ip: str, provedor: str, count: int):
        """Atualiza quota de um IP"""
        with self.ip_lock:
            if ip not in self.state_manager.state.ip_states:
                self.state_manager.state.ip_states[ip] = IPState(
                    ip=ip,
                    account_name="",
                    created_at=datetime.now(timezone.utc).isoformat()
                )
            
            ip_state = self.state_manager.state.ip_states[ip]
            ip_state.sent_total += count
            ip_state.sent_by_provider[provedor] = ip_state.sent_by_provider.get(provedor, 0) + count

# ============================================================
# GESTÃO DE DNS (deSEC)
# ============================================================
class DNSManager:
    """Gere DNS via deSEC com bulk updates"""
    
    def __init__(self, logger: Logger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.api_url = "https://desec.io/api/v1"
        self.headers = {
            "Authorization": f"Token {DESEC_TOKEN}",
            "Content-Type": "application/json"
        }
    
    def get_domains(self) -> List[str]:
        """Obtém lista de domínios disponíveis"""
        try:
            response = requests.get(
                f"{self.api_url}/domains/",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                domains = [d["name"] for d in response.json()]
                self.logger.print(f"📋 {len(domains)} domínios disponíveis no deSEC")
                return domains
            else:
                self.logger.print(f"⚠️ Erro ao listar domínios: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao obter domínios: {str(e)[:100]}")
            return []
    
    def update_spf_bulk(self, domains: List[str], ip: str):
        """Atualiza SPF de múltiplos domínios em bulk"""
        if not domains:
            return
        
        self.logger.print(f"🔐 Atualizando SPF de {len(domains)} domínios para IP {ip}")
        
        spf_record = f"v=spf1 ip4:{ip} ~all"
        
        # Preparar payload bulk
        bulk_payload = []
        for domain in domains:
            bulk_payload.append({
                "subname": "",
                "type": "TXT",
                "ttl": 3600,
                "records": [f"\"{spf_record}\""]
            })
        
        try:
            # Fazer request bulk para cada domínio
            for domain in domains[:30]:  # Limitar a 30 domínios
                response = requests.put(
                    f"{self.api_url}/domains/{domain}/rrsets/",
                    headers=self.headers,
                    json=bulk_payload,
                    timeout=30
                )
                
                if response.status_code not in [200, 201, 202]:
                    self.logger.print(f"⚠️ Erro ao atualizar {domain}: {response.status_code}")
                
                time.sleep(0.1)  # Rate limiting
            
            self.logger.print(f"✅ SPF atualizado para {len(domains[:30])} domínios")
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao atualizar SPF: {str(e)[:100]}")
    
    def setup_dns_for_ips(self, ips: List[dict]):
        """Configura DNS para todos os IPs ativos"""
        domains = self.get_domains()
        if not domains:
            self.logger.print("⚠️ Sem domínios disponíveis")
            return
        
        # Distribuir domínios entre IPs
        for i, ip_info in enumerate(ips[:30]):
            ip = ip_info["ip"]
            domain = domains[i % len(domains)]
            
            self.logger.print(f"🔗 Associando {domain} → {ip}")
            self.update_spf_bulk([domain], ip)
            time.sleep(0.5)

# ============================================================
# CLIENTE SMTP COM PIPELINING
# ============================================================
class SMTPClient:
    """Cliente SMTP com suporte a pipelining e mutação"""
    
    def __init__(self, logger: Logger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.mail_from_variations = self.load_mail_from_variations()
    
    def load_mail_from_variations(self) -> List[str]:
        """Carrega variações de Mail From"""
        try:
            api = HfApi(token=HF_TOKEN)
            files = api.list_repo_files(HF_LOGS_REPO, repo_type="dataset")
            
            # Encontrar ficheiro de variações
            for f in files:
                if "mail_from" in f.lower() and f.endswith(".txt"):
                    local_file = hf_hub_download(
                        repo_id=HF_LOGS_REPO,
                        filename=f,
                        repo_type="dataset",
                        token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    )
                    
                    with open(local_file, "r", encoding="utf-8") as file:
                        variations = [line.strip() for line in file if line.strip()]
                    
                    self.logger.print(f"📧 {len(variations)} variações de Mail From carregadas")
                    return variations
            
            self.logger.print("⚠️ Ficheiro de variações não encontrado")
            return ["Alex Johnson", "A. Johnson", "Alex J."]
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao carregar variações: {str(e)[:80]}")
            return ["Alex Johnson"]
    
    def mutar_html(self, html: str, conta: AccountState, recipient: str) -> str:
        """Aplica mutação Abracadabra ao HTML"""
        h = html
        
        # Comentário único no início do body
        rid = f"<!-- m-{hashlib.md5(f'{recipient}{time.time()}'.encode()).hexdigest()[:8]} -->"
        h = h.replace("<body>", f"<body>\n{rid}\n", 1)
        
        # Espaços aleatórios entre tags
        h = re.sub(r'>\s+<', lambda m: '>' + (' ' * (hash(recipient) % 3 + 1)) + '<', h)
        
        # Mutar URLs
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
        
        # Comentário de hash no fim
        hash_extra = hashlib.md5(f"{recipient}{time.time()}".encode()).hexdigest()[:12]
        h = h.replace("</body>", f"<!-- {hash_extra} -->\n</body>", 1)
        
        return h
    
    def get_mail_from(self, conta: AccountState) -> str:
        """Obtém Mail From com variação"""
        if not self.mail_from_variations:
            return conta.sender_email
        
        idx = hash(conta.identity_id) % len(self.mail_from_variations)
        display_name = self.mail_from_variations[idx]
        return f"{display_name} <{conta.sender_email}>"
    
    async def send_batch_async(self, ip_info: dict, provedor: str, emails: List[str], 
                               conta: AccountState, html_template: str) -> dict:
        """Envia batch de emails via SSH tunnel com pipelining"""
        resultado = {
            "aceites": 0,
            "rejeitados": 0,
            "unknown": 0,
            "greylist": 0,
            "erros": 0,
            "too_many": False,
        }
        
        ip = ip_info["ip"]
        key_path = ip_info["key_path"]
        
        sock = None
        try:
            # Conectar via SSH tunnel
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(
                ip,
                port=22,
                username=SSH_USER,
                pkey=pkey,
                timeout=15,
                allow_agent=False,
                look_for_keys=False
            )
            
            # Obter MX do provedor
            mx_host = await self.get_mx_async(provedor)
            if not mx_host:
                resultado["erros"] += 1
                client.close()
                return resultado
            
            # Abrir túnel SSH para MX
            transport = client.get_transport()
            channel = transport.open_channel(
                "direct-tcpip",
                (mx_host, 25),
                ("127.0.0.1", 0)
            )
            channel.settimeout(SMTP_TIMEOUT)
            
            sock = channel.makefile('rwb')
            
            # Banner
            banner = await self.recv_smtp_async(sock)
            if not banner.startswith("220"):
                resultado["erros"] += 1
                client.close()
                return resultado
            
            # EHLO
            await self.send_smtp_async(sock, f"EHLO {conta.domain}\r\n")
            ehlo_resp = await self.recv_smtp_async(sock)
            
            # STARTTLS se suportado
            if "STARTTLS" in ehlo_resp.upper():
                await self.send_smtp_async(sock, "STARTTLS\r\n")
                tls_resp = await self.recv_smtp_async(sock)
                if tls_resp.startswith("220"):
                    context = ssl.create_default_context()
                    sock._sock = context.wrap_socket(sock._sock, server_hostname=mx_host)
                    
                    # EHLO novamente após TLS
                    await self.send_smtp_async(sock, f"EHLO {conta.domain}\r\n")
                    await self.recv_smtp_async(sock)
            
            # MAIL FROM
            mail_from = self.get_mail_from(conta)
            await self.send_smtp_async(sock, f"MAIL FROM:<{conta.sender_email}>\r\n")
            mf_resp = await self.recv_smtp_async(sock)
            
            if not mf_resp.startswith("250"):
                resultado["erros"] += 1
                client.close()
                return resultado
            
            # RCPT TO com pipelining
            rcpt_cmds = b""
            for email in emails:
                rcpt_cmds += f"RCPT TO:<{email}>\r\n".encode()
            
            sock.sendall(rcpt_cmds)
            
            # Receber todas as respostas
            responses = []
            for i in range(len(emails)):
                resp = await self.recv_smtp_async(sock, timeout=10)
                responses.append((emails[i], resp))
            
            # Processar respostas
            for email, resp in responses:
                resp_lower = resp.lower()
                
                if resp.startswith("250") or resp.startswith("251"):
                    resultado["aceites"] += 1
                    self.state_manager.add_accepted_email(email)
                elif "too many" in resp_lower or "rate limit" in resp_lower:
                    resultado["too_many"] = True
                    resultado["rejeitados"] += 1
                    break
                elif "greylist" in resp_lower or "try again" in resp_lower:
                    resultado["greylist"] += 1
                elif "user unknown" in resp_lower or "no such user" in resp_lower:
                    resultado["unknown"] += 1
                    self.state_manager.add_unknown_email(email)
                else:
                    resultado["rejeitados"] += 1
            
            # DATA (se houve aceites)
            if resultado["aceites"] > 0 and not resultado["too_many"]:
                await self.send_smtp_async(sock, "DATA\r\n")
                data_resp = await self.recv_smtp_async(sock)
                
                if data_resp.startswith("354"):
                    # Preparar payload
                    html_mutado = self.mutar_html(html_template, conta, emails[0])
                    
                    msg = EmailMessage()
                    msg['From'] = mail_from
                    msg['To'] = emails[0]
                    msg['Subject'] = "Important Update"
                    msg.set_content(html_mutado, subtype='html')
                    
                    # Assinar DKIM
                    if conta.dkim_private_key:
                        try:
                            sig = dkim.sign(
                                msg.as_bytes(),
                                conta.dkim_selector.encode(),
                                conta.domain.encode(),
                                conta.dkim_private_key.encode(),
                                canonicalize=(b'relaxed', b'relaxed')
                            )
                            payload = sig + msg.as_bytes()
                        except:
                            payload = msg.as_bytes()
                    else:
                        payload = msg.as_bytes()
                    
                    if not payload.endswith(b"\r\n.\r\n"):
                        if not payload.endswith(b"\r\n"):
                            payload += b"\r\n"
                        payload += b".\r\n"
                    
                    sock.sendall(payload)
                    await self.recv_smtp_async(sock, timeout=30)
            
            # QUIT
            await self.send_smtp_async(sock, "QUIT\r\n")
            
            client.close()
            
        except Exception as e:
            resultado["erros"] += 1
            self.logger.log(f"Erro SMTP: {str(e)[:100]}", provedor)
        
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    async def recv_smtp_async(self, sock, timeout=15) -> str:
        """Recebe resposta SMTP assíncrona"""
        try:
            sock.settimeout(timeout)
        except:
            pass
        
        data = b""
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            try:
                chunk = sock.recv(8192)
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
    
    async def send_smtp_async(self, sock, cmd: str):
        """Envia comando SMTP assíncrono"""
        if isinstance(cmd, str):
            cmd = cmd.encode()
        sock.sendall(cmd)
    
    async def get_mx_async(self, provedor: str) -> Optional[str]:
        """Obtém MX de um provedor assíncrono"""
        try:
            answers = dns.resolver.resolve(provedor, 'MX')
            mxs = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
            if mxs:
                return mxs[0][1]
            return None
        except Exception:
            # Fallback MX
            fallback = {
                "gmail.com": "gmail-smtp-in.l.google.com",
                "google.com": "gmail-smtp-in.l.google.com",
                "yahoo.com": "mta5.am0.yahoodns.net",
                "outlook.com": "outlook-com.olc.protection.outlook.com",
            }
            return fallback.get(provedor)

# ============================================================
# WORKER DE PROVEDOR (PARALELISMO HIERÁRQUICO)
# ============================================================
class ProviderWorker:
    """Worker independente por provedor"""
    
    def __init__(self, provedor: str, logger: Logger, state_manager: StateManager,
                 aracloud_manager: AracloudManager, smtp_client: SMTPClient):
        self.provedor = provedor
        self.logger = logger
        self.state_manager = state_manager
        self.aracloud_manager = aracloud_manager
        self.smtp_client = smtp_client
        
        self.emails_queue = []
        self.accounts = []
        self.html_template = ""
        self.running = True
        
    async def initialize(self):
        """Inicializa o worker"""
        # Carregar emails do provedor
        await self.load_emails()
        
        # Carregar contas DKIM
        await self.load_accounts()
        
        # Carregar template HTML
        await self.load_template()
    
    async def load_emails(self):
        """Carrega emails do provedor do HuggingFace"""
        try:
            api = HfApi(token=HF_TOKEN)
            files = api.list_repo_files(HF_TRADER_REPO, repo_type="dataset")
            
            # Encontrar parquets do provedor
            parquets = [f for f in files 
                       if f.startswith(f"Trader_Emails/{self.provedor}/") 
                       and f.endswith('.parquet')]
            
            if not parquets:
                self.logger.print(f"⚠️ Nenhum parquet encontrado para {self.provedor}", self.provedor)
                return
            
            # Carregar emails
            all_emails = []
            seen = set()
            
            for pq_path in parquets[:5]:  # Limitar a 5 parquets por provedor
                try:
                    local = hf_hub_download(
                        repo_id=HF_TRADER_REPO,
                        filename=pq_path,
                        repo_type="dataset",
                        token=HF_TOKEN,
                        cache_dir="/content/hf_cache"
                    )
                    
                    pf = pq.ParquetFile(local)
                    
                    # Encontrar coluna de email
                    col = None
                    for c in pf.schema_arrow.names:
                        if str(c).lower() == "email":
                            col = c
                            break
                    
                    if not col:
                        continue
                    
                    # Extrair emails
                    for batch in pf.iter_batches(batch_size=10000, columns=[col]):
                        for v in batch.column(0).to_pylist():
                            if v is None:
                                continue
                            e = str(v).strip().lower()
                            if "@" not in e or e.startswith("@") or e.endswith("@"):
                                continue
                            if len(e) < 5 or len(e) > 100:
                                continue
                            if e not in seen and not self.state_manager.is_email_unknown(e):
                                seen.add(e)
                                all_emails.append(e)
                            
                            if len(all_emails) >= 10000:
                                break
                        
                        if len(all_emails) >= 10000:
                            break
                    
                    if len(all_emails) >= 10000:
                        break
                        
                except Exception as e:
                    self.logger.log(f"Erro ao carregar {pq_path}: {str(e)[:80]}", self.provedor)
            
            # Filtrar emails já aceites
            self.emails_queue = [e for e in all_emails if not self.state_manager.is_email_accepted(e)]
            
            # Aplicar índice global
            start_idx = self.state_manager.state.last_email_index % max(1, len(self.emails_queue))
            self.emails_queue = self.emails_queue[start_idx:]
            
            self.logger.print(f"📧 {len(self.emails_queue)} emails carregados", self.provedor)
            
        except Exception as e:
            self.logger.print(f"❌ Erro ao carregar emails: {str(e)[:100]}", self.provedor)
    
    async def load_accounts(self):
        """Carrega contas DKIM"""
        try:
            api = HfApi(token=HF_TOKEN)
            
            # Baixar ficheiro de contas
            contas_file = hf_hub_download(
                repo_id=HF_LOGS_REPO,
                filename="campaign_data/contas.json",
                repo_type="dataset",
                token=HF_TOKEN,
                cache_dir="/content/hf_cache"
            )
            
            with open(contas_file, "r", encoding="utf-8") as f:
                contas_data = json.load(f)
            
            self.accounts = [AccountState.from_dict(c) for c in contas_data[:100]]  # Limitar a 100 contas
            
            self.logger.print(f"🔑 {len(self.accounts)} contas DKIM carregadas", self.provedor)
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao carregar contas: {str(e)[:100]}", self.provedor)
            # Criar conta dummy
            self.accounts = [AccountState(
                identity_id="dummy",
                sender_email="test@example.com",
                domain="example.com"
            )]
    
    async def load_template(self):
        """Carrega template HTML"""
        try:
            api = HfApi(token=HF_TOKEN)
            
            # Baixar template
            template_file = hf_hub_download(
                repo_id=HF_LOGS_REPO,
                filename="campaign_data/templates/veriscope_email_01.html",
                repo_type="dataset",
                token=HF_TOKEN,
                cache_dir="/content/hf_cache"
            )
            
            with open(template_file, "r", encoding="utf-8") as f:
                self.html_template = f.read()
            
            self.logger.print(f"📄 Template HTML carregado", self.provedor)
            
        except Exception as e:
            self.logger.print(f"⚠️ Erro ao carregar template: {str(e)[:100]}", self.provedor)
            self.html_template = "<html><body>Test</body></html>"
    
    async def run(self):
        """Loop principal do worker"""
        self.logger.print(f"🚀 Worker iniciado", self.provedor)
        
        # Obter estado do provedor
        ps = self.state_manager.state.provider_states.get(self.provedor)
        if not ps:
            ps = ProviderState(name=self.provedor)
            self.state_manager.state.provider_states[self.provedor] = ps
        
        batch_size = ps.rcpt_limit
        
        while self.running and self.emails_queue:
            try:
                # Obter batch de emails
                batch = self.emails_queue[:batch_size]
                if not batch:
                    break
                
                # Obter IP disponível
                ip_info = self.aracloud_manager.get_available_ip(self.provedor)
                if not ip_info:
                    self.logger.print(f"⏸️ Sem IPs disponíveis - aguardando...", self.provedor)
                    await asyncio.sleep(5)
                    continue
                
                # Obter conta
                if not self.accounts:
                    self.logger.print(f"❌ Sem contas disponíveis", self.provedor)
                    break
                
                conta = self.accounts[hash(self.provedor + str(time.time())) % len(self.accounts)]
                
                # Enviar batch
                resultado = await self.smtp_client.send_batch_async(
                    ip_info, self.provedor, batch, conta, self.html_template
                )
                
                # Processar resultado
                self.state_manager.increment_stats(
                    total_sent=len(batch),
                    total_accepted=resultado["aceites"],
                    total_rejected=resultado["rejeitados"],
                    total_unknown=resultado["unknown"],
                    total_greylist_retry=resultado["greylist"]
                )
                
                self.state_manager.update_provider_state(
                    self.provedor,
                    emails_sent=ps.emails_sent + len(batch),
                    emails_accepted=ps.emails_accepted + resultado["aceites"],
                    emails_rejected=ps.emails_rejected + resultado["rejeitados"],
                    emails_unknown=ps.emails_unknown + resultado["unknown"]
                )
                
                # Atualizar quota do IP
                self.aracloud_manager.update_ip_quota(
                    ip_info["ip"],
                    self.provedor,
                    len(batch)
                )
                
                # Atualizar índice global
                self.state_manager.state.last_email_index += len(batch)
                
                # Ajustar limite RCPT baseado no resultado
                if resultado["too_many"]:
                    # Reduzir limite
                    ps.rcpt_limit = max(10, int(ps.rcpt_limit * 0.8))
                    ps.consecutive_failures += 1
                    ps.consecutive_successes = 0
                    self.logger.print(f"⚠️ Too many - reduzindo para {ps.rcpt_limit}", self.provedor)
                elif resultado["aceites"] > 0:
                    # Aumentar limite gradualmente
                    ps.consecutive_successes += 1
                    if ps.consecutive_successes >= 3:
                        ps.rcpt_limit = min(200, int(ps.rcpt_limit * 1.1))
                        ps.consecutive_successes = 0
                        self.logger.print(f"📈 Aumentando para {ps.rcpt_limit}", self.provedor)
                
                # Remover emails processados da fila
                self.emails_queue = self.emails_queue[batch_size:]
                
                # Log de progresso
                if resultado["aceites"] > 0:
                    self.logger.print(
                        f"✅ {resultado['aceites']}/{len(batch)} aceites | "
                        f"Total: {ps.emails_accepted:,}",
                        self.provedor
                    )
                
                # Pausa entre batches
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.print(f"❌ Erro no worker: {str(e)[:100]}", self.provedor)
                await asyncio.sleep(5)
        
        self.logger.print(f"🏁 Worker concluído", self.provedor)

# ============================================================
# WATCHDOG (MONITOR DE TEMPO)
# ============================================================
class Watchdog:
    """Monitor de tempo com shutdown inteligente"""
    
    def __init__(self, logger: Logger, state_manager: StateManager):
        self.logger = logger
        self.state_manager = state_manager
        self.start_time = time.time()
        self.running = True
    
    def check_time(self) -> bool:
        """Verifica se deve iniciar shutdown"""
        elapsed = time.time() - self.start_time
        
        if elapsed >= MAX_EXECUTION_SECONDS:
            self.logger.print(f"⏰ Tempo limite atingido ({elapsed/3600:.2f}h)")
            return True
        
        # Aviso 5 minutos antes
        if elapsed >= MAX_EXECUTION_SECONDS - 300:
            remaining = MAX_EXECUTION_SECONDS - elapsed
            self.logger.print(f"⏳ Shutdown em {remaining:.0f}s")
        
        return False
    
    async def run(self, workers: List[ProviderWorker]):
        """Loop do watchdog"""
        while self.running:
            if self.check_time():
                self.logger.print("🛑 Iniciando shutdown inteligente...")
                
                # Parar todos os workers
                for worker in workers:
                    worker.running = False
                
                # Aguardar workers terminarem (max 3 min)
                self.logger.print("⏳ Aguardando workers terminarem...")
                await asyncio.sleep(180)
                
                # Salvar checkpoint final
                self.state_manager.save_checkpoint("SHUTDOWN")
                self.state_manager.upload_checkpoint()
                
                self.running = False
                break
            
            # Checkpoint periódico
            elapsed = time.time() - self.start_time
            if elapsed % CHECKPOINT_INTERVAL < 5:
                self.state_manager.save_checkpoint("PERIODIC")
                self.state_manager.upload_checkpoint()
            
            await asyncio.sleep(10)

# ============================================================
# MAIN CONTROLLER
# ============================================================
async def main():
    """Função principal"""
    
    # Inicializar logger
    logger = Logger("/content/logs/veriscope.log")
    logger.print("=" * 80)
    logger.print("🚀 VERISCOPE V23.0.0 - SISTEMA DE ENVIO MASSIVO")
    logger.print("=" * 80)
    
    # Inicializar state manager
    state_manager = StateManager(logger)
    
    # Carregar checkpoint
    logger.print("\n📂 FASE 0: Carregando checkpoint...")
    state_manager.load_checkpoint()
    
    # Inicializar run
    state_manager.initialize_run()
    
    # Inicializar Aracloud manager
    logger.print("\n🌐 FASE 1: Configurando IPs Aracloud...")
    aracloud_manager = AracloudManager(logger, state_manager)
    
    if not aracloud_manager.load_accounts():
        logger.print("❌ Falha ao carregar contas Aracloud")
        return
    
    if aracloud_manager.setup_ips() < 1:
        logger.print("❌ Falha ao configurar IPs")
        return
    
    # Inicializar DNS manager
    logger.print("\n🔐 FASE 2: Configurando DNS...")
    dns_manager = DNSManager(logger, state_manager)
    dns_manager.setup_dns_for_ips(aracloud_manager.ips_ativos)
    
    # Aguardar propagação DNS
    logger.print("⏳ Aguardando 300s para propagação DNS...")
    await asyncio.sleep(300)
    
    # Inicializar SMTP client
    logger.print("\n📧 FASE 3: Inicializando workers...")
    smtp_client = SMTPClient(logger, state_manager)
    
    # Criar workers para cada provedor
    workers = []
    for provedor in PROVEDORES_ALVO:
        worker = ProviderWorker(provedor, logger, state_manager, aracloud_manager, smtp_client)
        await worker.initialize()
        workers.append(worker)
    
    logger.print(f"✅ {len(workers)} workers inicializados")
    
    # Inicializar watchdog
    watchdog = Watchdog(logger, state_manager)
    
    # Iniciar todos os workers em paralelo
    logger.print("\n🚀 FASE 4: Iniciando envio paralelo...")
    
    tasks = [asyncio.create_task(worker.run()) for worker in workers]
    watchdog_task = asyncio.create_task(watchdog.run(workers))
    
    # Aguardar conclusão
    try:
        await asyncio.gather(*tasks, watchdog_task, return_exceptions=True)
    except KeyboardInterrupt:
        logger.print("\n🛑 Interrompido pelo utilizador")
    except Exception as e:
        logger.print(f"\n❌ Erro: {str(e)}")
        logger.print(traceback.format_exc())
    
    # Salvar checkpoint final
    logger.print("\n💾 Salvando checkpoint final...")
    state_manager.save_checkpoint("FINAL")
    state_manager.upload_checkpoint()
    
    # Estatísticas finais
    stats = state_manager.state
    logger.print("\n" + "=" * 80)
    logger.print("📊 ESTATÍSTICAS FINAIS")
    logger.print("=" * 80)
    logger.print(f"📤 Total enviado: {stats.total_sent:,}")
    logger.print(f"✅ Total aceite: {stats.total_accepted:,}")
    logger.print(f"❌ Total rejeitado: {stats.total_rejected:,}")
    logger.print(f"👻 Total unknown: {stats.total_unknown:,}")
    logger.print(f"🔵 Total greylist retry: {stats.total_greylist_retry:,}")
    logger.print(f"📍 Último email: #{stats.last_email_index:,}")
    logger.print("=" * 80)
    
    logger.print("\n🏁 Execução concluída")
    logger.flush()

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo utilizador")
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        traceback.print_exc()
