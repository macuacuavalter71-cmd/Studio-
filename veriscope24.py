# -*- coding: utf-8 -*-
"""
VERISCOPE V23.0.0 - ENGINE COMPLETO DE ENVIO
============================================
Arquitetura:
- 10 contas Aracloud × 3 IPs = 30 IPs simultâneos
- 30 contas deSEC com bulk update
- Paralelismo hierárquico: 51 provedores independentes
- Checkpoint inteligente (shutdown 5h40m)
- Mutação Abracadabra + variações Mail From
- Greylist retries + Rate limit adaptativo

Para usar:
  colab --auth oauth2 exec -s job -f veriscope_v23.py --timeout 21000
"""
import os, sys, json, time, random, hashlib, threading, traceback, socket
import re, csv, gc, shutil, subprocess, resource, ssl
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from email.utils import make_msgid, format_datetime
from itertools import islice
from urllib.parse import urlparse, urlunparse

# ============================================================
# CONFIGURAÇÕES GLOBAIS
# ============================================================
VERSION = "23.0.0"

# HuggingFace
HF_TOKEN = os.environ.get("HF_TOKEN", "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").strip()
HF_TRADER_REPO = "Valter3B/Trader_Emails"
HF_CHECKPOINT_REPO = "Valter3B/Veriscope_Checkpoints"
HF_LOGS_REPO = "Valter3B/Campaign_Logs"
HF_ARACLOUD_REPO = "Valter3B/aracloud-servers"

# deSEC
DESEC_TOKEN = os.environ.get("DESEC_TOKEN", "LuWfqx1GL3uXDmyUEExH1tF4Nena").strip()
DESEC_TTL = 300
DESEC_API = "https://desec.io/api/v1"

# Aracloud
SSH_USER = "ubuntu"
SSH_PORT = 22
SSH_KEY_DIR = "/content/ssh_keys"

# SMTP
SMTP_TIMEOUT = 20
DELAY_ENTRE_RCPT = 0.05
DELAY_ENTRE_EMAILS = 0.5
MAX_EMAILS_IP_PROVEDOR = 1000
MAX_IPS_POR_CONTA = 3
MAX_IPS_UNICOS_POR_CONTA = 20

# Timing
MAX_EXECUTION_SECONDS = 5 * 3600 + 40 * 60  # 5h40m
CHECKPOINT_INTERVAL = 300  # 5 min
DNS_PROPAGATION = 60  # segundos

# Workers
NUM_WORKERS = 30  # 1 worker por IP
NUM_PROVEDORES_PARALELOS = 51

# Ficheiros
BASE_LOCAL = "/content/veriscope_v23"
DIR_EMAILS = os.path.join(BASE_LOCAL, "emails_db")
DIR_SSH_KEYS = "/content/ssh_keys"
LOG_DIR = os.path.join(BASE_LOCAL, "logs")
CP_DIR = os.path.join(BASE_LOCAL, "checkpoints")
UNKNOWN_FILE = os.path.join(BASE_LOCAL, "user_unknowns_acumulado.csv")
ACEITES_FILE = os.path.join(BASE_LOCAL, "aceites_finais.csv")
REJEICOES_FILE = os.path.join(BASE_LOCAL, "rejeicoes_finais.csv")

# Ambiente
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TQDM_DISABLE"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

def install_packages():
    packages = {
        "requests": "requests", "dnspython": "dns", "dkimpy": "dkim",
        "cryptography": "cryptography", "pandas": "pandas", "pyarrow": "pyarrow",
        "huggingface_hub": "huggingface_hub", "psutil": "psutil",
        "paramiko": "paramiko", "aiohttp": "aiohttp"
    }
    for pkg, mod in packages.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

install_packages()

import logging
for _lg in ["huggingface_hub", "datasets", "fsspec", "httpx", "urllib3", "paramiko", "asyncio"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)
import warnings
warnings.filterwarnings("ignore")

import requests
import dns.resolver
import dkim
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
import paramiko

threading.stack_size(64 * 1024)
try:
    resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
except Exception:
    pass

# ============================================================
# RCPT LIMITS DESCOBERTOS (do teste V22.3.0)
# ============================================================
RCPT_LIMITS = {
    # Limite alto (1000+)
    "pisem.net": 950, "sprintpcs.com": 950, "oranga.fr": 950,
    "rock.com": 950, "walla.com": 950, "telkomsa.net": 950,
    # Limite padrão Google/Yahoo (100)
    "bebo.com": 95, "bk.ru": 95, "cemsmail.org": 95,
    "dreamwiz.com": 95, "google.com": 95, "inbox.ru": 95,
    "list.ru": 95, "mail.ru": 95, "ukr.net": 95,
    "yahoo.co.jp": 95, "youtube.com": 95, "googlemail.com": 95,
    # Limite médio (50-80)
    "nate.com": 70, "yeah.net": 70, "telus.net": 70,
    "seznam.cz": 70, "amorki.pl": 60,
    "myemailboxmail.com": 45, "trbvm.com": 45,
    "poczta.onet.pl": 45, "onet.eu": 45, "onet.pl": 45,
    "student.gccisd.net": 40, "freemail.hu": 40,
    # Limite baixo (10-40)
    "ya.ru": 30, "qq.com": 30, "foxmail.com": 25,
    "vip.qq.com": 25, "naver.com": 25, "eyou.com": 25,
    "hot.com": 20, "breakthru.com": 18, "clear.net.nz": 18,
    "pchome.com.tw": 18, "yandex.com": 14,
    "poczta.pl": 10, "yandex.ru": 10, "dodgeit.com": 8,
    # Muito baixo
    "21cn.com": 2, "docomo.ne.jp": 2, "gaggle.net": 1,
}
RCPT_DEFAULT = 50

# ============================================================
# PROVEDORES
# ============================================================
PROVEDORES_ATIVOS = [
    "bebo.com", "bk.ru", "cemsmail.org", "dreamwiz.com", "google.com",
    "inbox.ru", "list.ru", "mail.ru", "pisem.net", "rock.com",
    "sprintpcs.com", "ukr.net", "walla.com", "yahoo.co.jp", "youtube.com",
    "seznam.cz", "yeah.net", "nate.com", "telus.net",
    "googlemail.com", "oranga.fr", "myemailboxmail.com", "trbvm.com",
    "dodgeit.com", "ya.ru", "qq.com", "foxmail.com", "naver.com",
    "vip.qq.com", "yandex.com", "freemail.hu", "yandex.ru",
    "poczta.onet.pl", "docomo.ne.jp", "amorki.pl",
    "onet.eu", "onet.pl", "breakthru.com", "clear.net.nz",
    "hot.com", "op.pl", "pchome.com.tw", "poczta.pl", "yandex.net",
    "telkomsa.net", "eyou.com", "email.cz", "student.gccisd.net",
    "21cn.com", "terra.com.br", "gaggle.net",
]

PROVEDORES_GREYLIST = ["gmail.com", "seznam.cz", "i.ua", "interia.eu", "interia.pl", "poczta.fm"]

# Greylist retry schedule (segundos)
GREYLIST_SCHEDULE = [120, 300, 900]

# ============================================================
# ESTADO GLOBAL
# ============================================================
GLOBAL = {
    "start_time": time.time(),
    "run_started_at": time.time(),
    "emergency_stop": False,
    "smtp_tentativas": 0,
    "smtp_aceites": 0,
    "smtp_rejeitados": 0,
    "smtp_user_unknown": 0,
    "smtp_erros": 0,
    "smtp_greylist": 0,
    "smtp_blocked": 0,
    "stats_lock": threading.Lock(),
    "user_unknowns": set(),
    "user_unknowns_lock": threading.Lock(),
    "aceites_emails": set(),
    "aceites_lock": threading.Lock(),
    "por_provedor": defaultdict(lambda: {
        "offset": 0, "total_enviados": 0, "aceites": 0,
        "rejeitados": 0, "user_unknown": 0, "greylist": 0,
        "concluido": False
    }),
    "provedores_lock": threading.Lock(),
    "orcamento_ip": {},
    "orcamento_lock": threading.Lock(),
    "rcpt_aprendido": dict(RCPT_LIMITS),
    "rcpt_lock": threading.Lock(),
    "contas_dkim": [],
    "dominio_principal": None,
    "html_base": "",
    "mail_from_variations": [],
    "dns_autenticacao": {},  # Salvo entre runs
}

# ============================================================
# LOGGING
# ============================================================
def garantir_diretorios():
    for d in [BASE_LOCAL, DIR_EMAILS, DIR_SSH_KEYS, LOG_DIR, CP_DIR]:
        os.makedirs(d, exist_ok=True)

garantir_diretorios()

LOG_FILE = os.path.join(LOG_DIR, f"log_{int(time.time())}.txt")
SMTP_LOG = os.path.join(LOG_DIR, f"smtp_{int(time.time())}.txt")
RAW_LOG = os.path.join(LOG_DIR, f"raw_{int(time.time())}.txt")

LOG_LOCK = threading.Lock()
RAW_LOCK = threading.Lock()
RAW_BUFFER = []
RAW_LAST_FLUSH = time.time()

def dizer(msg, save=True):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if save:
        try:
            with LOG_LOCK:
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except:
            pass

def raw_log(msg):
    global RAW_BUFFER, RAW_LAST_FLUSH
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}\n"
    with RAW_LOCK:
        RAW_BUFFER.append(line)
        if len(RAW_BUFFER) >= 500 or (time.time() - RAW_LAST_FLUSH) > 10:
            _flush_raw()

def _flush_raw():
    global RAW_BUFFER, RAW_LAST_FLUSH
    if not RAW_BUFFER:
        return
    try:
        with open(RAW_LOG, "a", encoding="utf-8") as f:
            f.writelines(RAW_BUFFER)
        RAW_BUFFER = []
        RAW_LAST_FLUSH = time.time()
    except:
        pass

def smtp_log(msg):
    try:
        with LOG_LOCK:
            with open(SMTP_LOG, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
    except:
        pass

def flush_all():
    _flush_raw()

# ============================================================
# API ARACLOUD
# ============================================================
class AracloudAPI:
    """Interface com a API OpenStack do Aracloud"""
    AUTH_URL = "https://vhi.servermania.com:5000/v3"
    
    def __init__(self, username, password, domain_id, project_id):
        self.username = username
        self.password = password
        self.domain_id = domain_id
        self.project_id = project_id
        self.token = None
        self.token_expiry = 0
        self.compute_url = None
    
    def authenticate(self):
        if self.token and time.time() < self.token_expiry - 60:
            return True
        try:
            # Step 1: Unscoped token
            identity = {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": self.username,
                        "domain": {"id": self.domain_id},
                        "password": self.password
                    }
                }
            }
            r = requests.post(
                f"{self.AUTH_URL}/auth/tokens",
                json={"auth": {"identity": identity}},
                timeout=20
            )
            if r.status_code != 201:
                dizer(f"❌ Aracloud auth failed: {r.status_code}")
                return False
            
            # Step 2: Scoped token (project)
            scope = {"project": {"id": self.project_id}}
            r = requests.post(
                f"{self.AUTH_URL}/auth/tokens",
                json={"auth": {"identity": identity, "scope": scope}},
                timeout=20
            )
            if r.status_code != 201:
                dizer(f"❌ Aracloud project auth failed: {r.status_code}")
                return False
            
            self.token = r.headers["X-Subject-Token"]
            self.token_expiry = time.time() + 3500
            
            # Get compute URL from catalog
            catalog = r.json()["token"]["catalog"]
            for service in catalog:
                if service["type"] == "compute":
                    for endpoint in service["endpoints"]:
                        if endpoint["interface"] == "public":
                            self.compute_url = endpoint["url"]
                            break
            
            return True
        except Exception as e:
            dizer(f"❌ Aracloud auth error: {str(e)[:100]}")
            return False
    
    def _headers(self):
        return {"X-Auth-Token": self.token}
    
    def list_servers(self):
        if not self.authenticate():
            return []
        try:
            r = requests.get(
                f"{self.compute_url}/servers/detail",
                headers=self._headers(),
                timeout=20
            )
            if r.status_code == 200:
                return r.json().get("servers", [])
            return []
        except:
            return []
    
    def list_floating_ips(self):
        if not self.authenticate():
            return []
        try:
            r = requests.get(
                f"{self.compute_url}/os-floating-ips",
                headers=self._headers(),
                timeout=20
            )
            if r.status_code == 200:
                return r.json().get("floating_ips", [])
            return []
        except:
            return []
    
    def create_floating_ip(self, pool="public"):
        if not self.authenticate():
            return None
        try:
            r = requests.post(
                f"{self.compute_url}/os-floating-ips",
                headers=self._headers(),
                json={"pool": pool},
                timeout=30
            )
            if r.status_code in (200, 201):
                return r.json().get("floating_ip", {})
            return None
        except:
            return None
    
    def delete_floating_ip(self, ip_id):
        if not self.authenticate():
            return False
        try:
            r = requests.delete(
                f"{self.compute_url}/os-floating-ips/{ip_id}",
                headers=self._headers(),
                timeout=20
            )
            return r.status_code in (200, 202, 204)
        except:
            return False

# ============================================================
# SSH TUNNEL
# ============================================================
class SSHTunnel:
    def __init__(self, ip, port, username, pkey_path):
        self.ip = ip
        self.port = port
        self.username = username
        self.pkey_path = pkey_path
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connected = False
        self.lock = threading.Lock()
    
    def connect(self):
        with self.lock:
            if self.connected and self.client.get_transport() and self.client.get_transport().is_active():
                return True
            try:
                if not os.path.exists(self.pkey_path):
                    return False
                pkey = paramiko.RSAKey.from_private_key_file(self.pkey_path)
                self.client.connect(
                    self.ip, port=self.port, username=self.username,
                    pkey=pkey, timeout=15, allow_agent=False, look_for_keys=False
                )
                transport = self.client.get_transport()
                if transport:
                    transport.set_keepalive(15)
                self.connected = True
                return True
            except Exception as e:
                self.connected = False
                return False
    
    def get_socket(self, dest_host, dest_port):
        if not self.connected:
            if not self.connect():
                return None
        try:
            transport = self.client.get_transport()
            if not transport or not transport.is_active():
                self.connected = False
                if not self.connect():
                    return None
                transport = self.client.get_transport()
            channel = transport.open_channel(
                "direct-tcpip", (dest_host, dest_port), ("127.0.0.1", 0)
            )
            channel.settimeout(SMTP_TIMEOUT)
            return channel
        except:
            self.connected = False
            if self.connect():
                try:
                    transport = self.client.get_transport()
                    channel = transport.open_channel(
                        "direct-tcpip", (dest_host, dest_port), ("127.0.0.1", 0)
                    )
                    channel.settimeout(SMTP_TIMEOUT)
                    return channel
                except:
                    pass
            return None
    
    def close(self):
        try:
            self.client.close()
        except:
            pass
        self.connected = False

# ============================================================
# API deSEC (DNS)
# ============================================================
class DesecAPI:
    def __init__(self, token):
        self.token = token
        self.base_url = DESEC_API
        self.headers = {
            "Authorization": f"Token {token}",
            "Content-Type": "application/json"
        }
    
    def list_domains(self):
        try:
            r = requests.get(
                f"{self.base_url}/domains/",
                headers=self.headers,
                timeout=20
            )
            if r.status_code == 200:
                return r.json()
            return []
        except:
            return []
    
    def get_rrsets(self, domain):
        try:
            r = requests.get(
                f"{self.base_url}/domains/{domain}/rrsets/",
                headers=self.headers,
                timeout=20
            )
            if r.status_code == 200:
                return r.json()
            return []
        except:
            return []
    
    def bulk_update_rrsets(self, domain, rrsets):
        """Atualiza múltiplos rrsets de uma vez"""
        try:
            r = requests.put(
                f"{self.base_url}/domains/{domain}/rrsets/",
                headers=self.headers,
                json=rrsets,
                timeout=60
            )
            if r.status_code in (200, 201, 204):
                return True
            dizer(f"⚠️ deSEC bulk update {domain}: {r.status_code} {r.text[:100]}")
            return False
        except Exception as e:
            dizer(f"❌ deSEC error {domain}: {str(e)[:100]}")
            return False
    
    def update_spf_only(self, domain, new_ip):
        """Atualiza apenas SPF + A records (quando IP muda)"""
        spf_record = f'"v=spf1 ip4:{new_ip} ~all"'
        rrsets = [
            {"subname": "", "type": "A", "records": [new_ip], "ttl": DESEC_TTL},
            {"subname": "*", "type": "A", "records": [new_ip], "ttl": DESEC_TTL},
            {"subname": "", "type": "TXT", "records": [spf_record], "ttl": DESEC_TTL},
        ]
        return self.bulk_update_rrsets(domain, rrsets)
    
    def setup_full_auth(self, domain, base, selector, dkim_pub, new_ip):
        """Configuração completa (primeira vez)"""
        spf_record = f'"v=spf1 ip4:{new_ip} ~all"'
        dmarc_record = '"v=DMARC1; p=none; sp=none;"'
        dkim_wrapped = f'"{dkim_pub}"' if len(dkim_pub) <= 255 else self._chunk_txt(dkim_pub)
        mx_record = f"10 {base}.{domain}."
        
        rrsets = [
            {"subname": "", "type": "A", "records": [new_ip], "ttl": DESEC_TTL},
            {"subname": "*", "type": "A", "records": [new_ip], "ttl": DESEC_TTL},
            {"subname": "", "type": "TXT", "records": [spf_record], "ttl": DESEC_TTL},
            {"subname": "_dmarc", "type": "TXT", "records": [dmarc_record], "ttl": DESEC_TTL},
            {"subname": f"{selector}._domainkey", "type": "TXT", "records": [dkim_wrapped] if isinstance(dkim_wrapped, str) else dkim_wrapped, "ttl": DESEC_TTL},
            {"subname": "", "type": "MX", "records": [mx_record], "ttl": DESEC_TTL},
        ]
        return self.bulk_update_rrsets(domain, rrsets)
    
    def _chunk_txt(self, text, max_len=255):
        if len(text) <= max_len:
            return [f'"{text}"']
        chunks = []
        while text:
            if len(text) <= max_len:
                chunks.append(f'"{text}"')
                break
            split = text.rfind(' ', 0, max_len)
            if split == -1:
                split = max_len
            chunks.append(f'"{text[:split]}"')
            text = text[split:].lstrip()
        return chunks

# ============================================================
# CHECKPOINT MANAGER
# ============================================================
class CheckpointManager:
    def __init__(self):
        self.cp_dir = CP_DIR
        self.files = {
            "config": os.path.join(self.cp_dir, "config.json"),
            "smtp": os.path.join(self.cp_dir, "smtp.json"),
            "provedores": os.path.join(self.cp_dir, "provedores.json"),
            "unknowns": os.path.join(self.cp_dir, "unknowns.json"),
            "aceites": os.path.join(self.cp_dir, "aceites.json"),
            "orcamento": os.path.join(self.cp_dir, "orcamento.json"),
            "rcpt": os.path.join(self.cp_dir, "rcpt.json"),
            "ips": os.path.join(self.cp_dir, "ips.json"),
            "dns": os.path.join(self.cp_dir, "dns_auth.json"),
        }
        self.lock = threading.Lock()
    
    def save_all(self):
        with self.lock:
            try:
                # Config
                config = {
                    "exec_id": GLOBAL.get("exec_id"),
                    "campaign_id": GLOBAL.get("campaign_id"),
                    "status": "RUNNING",
                    "last_save": datetime.now().isoformat(),
                }
                self._save("config", config)
                
                # SMTP stats
                smtp = {
                    "smtp_tentativas": GLOBAL["smtp_tentativas"],
                    "smtp_aceites": GLOBAL["smtp_aceites"],
                    "smtp_rejeitados": GLOBAL["smtp_rejeitados"],
                    "smtp_user_unknown": GLOBAL["smtp_user_unknown"],
                    "smtp_erros": GLOBAL["smtp_erros"],
                    "smtp_greylist": GLOBAL["smtp_greylist"],
                    "smtp_blocked": GLOBAL["smtp_blocked"],
                }
                self._save("smtp", smtp)
                
                # Provedores (offsets e progresso)
                with GLOBAL["provedores_lock"]:
                    prov_data = dict(GLOBAL["por_provedor"])
                self._save("provedores", prov_data)
                
                # Unknowns
                with GLOBAL["user_unknowns_lock"]:
                    unk_list = list(GLOBAL["user_unknowns"])
                self._save("unknowns", {"emails": unk_list})
                
                # Aceites
                with GLOBAL["aceites_lock"]:
                    ace_list = list(GLOBAL["aceites_emails"])
                self._save("aceites", {"emails": ace_list})
                
                # Orçamento
                with GLOBAL["orcamento_lock"]:
                    orc = dict(GLOBAL["orcamento_ip"])
                self._save("orcamento", orc)
                
                # RCPT aprendido
                with GLOBAL["rcpt_lock"]:
                    rcpt = dict(GLOBAL["rcpt_aprendido"])
                self._save("rcpt", rcpt)
                
                # DNS auth
                self._save("dns", GLOBAL.get("dns_autenticacao", {}))
                
                dizer(f"💾 Checkpoint salvo ({len(prov_data)} provedores)")
                return True
            except Exception as e:
                dizer(f"❌ Erro ao salvar checkpoint: {str(e)[:100]}")
                return False
    
    def _save(self, name, data):
        path = self.files[name]
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            os.replace(tmp, path)
        except Exception as e:
            dizer(f"⚠️ Erro ao salvar {name}: {str(e)[:80]}")
    
    def load_all(self):
        loaded = 0
        for name, path in self.files.items():
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if name == "smtp":
                        for k, v in data.items():
                            if k in GLOBAL:
                                GLOBAL[k] = v
                    elif name == "provedores":
                        with GLOBAL["provedores_lock"]:
                            for p, info in data.items():
                                GLOBAL["por_provedor"][p] = info
                    elif name == "unknowns":
                        with GLOBAL["user_unknowns_lock"]:
                            GLOBAL["user_unknowns"] = set(data.get("emails", []))
                    elif name == "aceites":
                        with GLOBAL["aceites_lock"]:
                            GLOBAL["aceites_emails"] = set(data.get("emails", []))
                    elif name == "orcamento":
                        with GLOBAL["orcamento_lock"]:
                            GLOBAL["orcamento_ip"] = data
                    elif name == "rcpt":
                        with GLOBAL["rcpt_lock"]:
                            GLOBAL["rcpt_aprendido"].update(data)
                    elif name == "dns":
                        GLOBAL["dns_autenticacao"] = data
                    elif name == "config":
                        GLOBAL["exec_id"] = data.get("exec_id")
                        GLOBAL["campaign_id"] = data.get("campaign_id")
                    loaded += 1
                except Exception as e:
                    dizer(f"⚠️ Erro ao carregar {name}: {str(e)[:80]}")
        return loaded
    
    def exists(self):
        return os.path.exists(self.files["config"])

CHECKPOINT = CheckpointManager()

# ============================================================
# HUGGINGFACE SYNC
# ============================================================
def download_from_hf():
    """Baixa checkpoints e dados do HuggingFace"""
    dizer("📥 A baixar dados do HuggingFace...")
    baixados = 0
    
    # Checkpoints locais
    for name, local_path in CHECKPOINT.files.items():
        try:
            hf_path = f"checkpoints/{name}.json"
            local = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO, filename=hf_path,
                repo_type="dataset", token=HF_TOKEN, force_download=True
            )
            shutil.copy(local, local_path)
            baixados += 1
        except Exception as e:
            pass  # Ficheiro não existe ainda
    
    # CSVs de resultados
    csv_map = {
        "results/user_unknowns_acumulado.csv": UNKNOWN_FILE,
        "results/aceites_finais.csv": ACEITES_FILE,
    }
    for hf_path, local_path in csv_map.items():
        try:
            local = hf_hub_download(
                repo_id=HF_CHECKPOINT_REPO, filename=hf_path,
                repo_type="dataset", token=HF_TOKEN, force_download=True
            )
            shutil.copy(local, local_path)
            baixados += 1
        except:
            pass
    
    dizer(f"📥 {baixados} ficheiros baixados do HuggingFace")
    return baixados

def upload_to_hf():
    """Envia resultados para o HuggingFace"""
    dizer("☁️ A enviar para HuggingFace...")
    try:
        api = HfApi(token=HF_TOKEN)
        enviados = 0
        
        # Checkpoints
        for name, local_path in CHECKPOINT.files.items():
            if os.path.exists(local_path):
                try:
                    api.upload_file(
                        path_or_fileobj=local_path,
                        path_in_repo=f"checkpoints/{name}.json",
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    enviados += 1
                except:
                    pass
        
        # CSVs finais
        for local_path, remote_name in [
            (UNKNOWN_FILE, "results/user_unknowns_acumulado.csv"),
            (ACEITES_FILE, "results/aceites_finais.csv"),
            (REJEICOES_FILE, "results/rejeicoes_finais.csv"),
        ]:
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                try:
                    api.upload_file(
                        path_or_fileobj=local_path,
                        path_in_repo=remote_name,
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    enviados += 1
                except:
                    pass
        
        # Logs
        for log_file in [LOG_FILE, SMTP_LOG, RAW_LOG]:
            if os.path.exists(log_file):
                try:
                    exec_id = GLOBAL.get("exec_id", "unknown")
                    api.upload_file(
                        path_or_fileobj=log_file,
                        path_in_repo=f"logs/{os.path.basename(log_file)}_{exec_id}",
                        repo_id=HF_CHECKPOINT_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    enviados += 1
                except:
                    pass
        
        dizer(f"☁️ {enviados} ficheiros enviados")
    except Exception as e:
        dizer(f"❌ Erro no upload: {str(e)[:100]}")

# ============================================================
# DOWNLOAD DE CREDENCIAIS ARACLOUD
# ============================================================
def baixar_credenciais_aracloud():
    """Baixa credenciais e chaves SSH das 10 contas Aracloud"""
    dizer("🔑 A baixar credenciais Aracloud...")
    try:
        api = HfApi(token=HF_TOKEN)
        ficheiros = api.list_repo_files(HF_ARACLOUD_REPO, repo_type="dataset")
    except Exception as e:
        dizer(f"❌ Erro ao listar repo Aracloud: {str(e)[:100]}")
        return []
    
    contas = []
    pastas = set()
    for f in ficheiros:
        if f.startswith("servidor-") and "/" in f:
            pastas.add(f.split("/")[0])
    
    for pasta in sorted(pastas):
        try:
            # Baixar k.pem
            local_pem = hf_hub_download(
                repo_id=HF_ARACLOUD_REPO,
                filename=f"{pasta}/k.pem",
                repo_type="dataset",
                token=HF_TOKEN
            )
            # Baixar credenciais.json
            local_cred = hf_hub_download(
                repo_id=HF_ARACLOUD_REPO,
                filename=f"{pasta}/credenciais.json",
                repo_type="dataset",
                token=HF_TOKEN
            )
            
            with open(local_cred, "r") as f:
                cred = json.load(f)
            
            # Copiar chave SSH
            pem_dest = os.path.join(DIR_SSH_KEYS, f"{pasta}.pem")
            shutil.copy(local_pem, pem_dest)
            os.chmod(pem_dest, 0o600)
            
            contas.append({
                "name": cred.get("name", pasta),
                "hostname": cred.get("hostname", ""),
                "api_user": cred["api"]["utilizador"],
                "api_pass": cred["api"]["password"],
                "pasta": pasta,
                "pem_path": pem_dest,
            })
        except Exception as e:
            dizer(f"⚠️ Erro ao baixar {pasta}: {str(e)[:80]}")
    
    dizer(f"🔑 {len(contas)} contas Aracloud carregadas")
    return contas

# ============================================================
# GESTÃO DE IPs ARACLOUD
# ============================================================
class IPManager:
    def __init__(self, contas_aracloud):
        self.contas = contas_aracloud
        self.ip_state = {}  # ip -> {conta, tunnel, usado_hoje, ...}
        self.ips_hoje = set()  # IPs usados hoje
        self.lock = threading.Lock()
        self.hoje = datetime.now().strftime("%Y-%m-%d")
    
    def inicializar_ips(self):
        """Cria 3 IPs por conta (30 no total)"""
        dizer("🌐 A inicializar IPs Aracloud...")
        
        for conta in self.contas:
            api = AracloudAPI(
                conta["api_user"],
                conta["api_pass"],
                domain_id="default",  # Ajustar conforme necessário
                project_id=""  # Será obtido via API
            )
            
            # Listar IPs existentes
            ips_existentes = api.list_floating_ips()
            
            # Criar IPs até ter 3
            ips_ativos = [ip for ip in ips_existentes if ip.get("ip")]
            while len(ips_ativos) < MAX_IPS_POR_CONTA:
                new_ip = api.create_floating_ip()
                if new_ip and new_ip.get("ip"):
                    ips_ativos.append(new_ip)
                    dizer(f"  ✅ Novo IP criado: {new_ip['ip']} ({conta['name']})")
                    time.sleep(1)
                else:
                    break
            
            # Configurar tunnel para cada IP
            for ip_info in ips_ativos[:MAX_IPS_POR_CONTA]:
                ip = ip_info.get("ip")
                if not ip:
                    continue
                
                # Verificar se IP já foi usado hoje
                if ip in self.ips_hoje:
                    dizer(f"  ⚠️ IP {ip} já usado hoje, a saltar")
                    continue
                
                tunnel = SSHTunnel(ip, SSH_PORT, SSH_USER, conta["pem_path"])
                if tunnel.connect():
                    with self.lock:
                        self.ip_state[ip] = {
                            "conta": conta,
                            "api": api,
                            "ip_info": ip_info,
                            "tunnel": tunnel,
                            "emails_enviados": {},  # provedor -> count
                            "bloqueado": False,
                            "usado_desde": time.time(),
                        }
                    dizer(f"  🔌 Tunnel estabelecido: {ip}")
                else:
                    dizer(f"  ❌ Falha ao conectar: {ip}")
        
        with self.lock:
            num_ips = len(self.ip_state)
        dizer(f"🌐 {num_ips} IPs Aracloud prontos")
        return num_ips > 0
    
    def get_ip_disponivel(self, provedor):
        """Retorna um IP disponível para envio ao provedor"""
        with self.lock:
            for ip, state in self.ip_state.items():
                if state["bloqueado"]:
                    continue
                enviados = state["emails_enviados"].get(provedor, 0)
                if enviados < MAX_EMAILS_IP_PROVEDOR:
                    return ip, state["tunnel"]
        return None, None
    
    def registar_envio(self, ip, provedor):
        with self.lock:
            if ip in self.ip_state:
                state = self.ip_state[ip]
                state["emails_enviados"][provedor] = state["emails_enviados"].get(provedor, 0) + 1
    
    def esgotou_ip(self, ip):
        """Verifica se IP atingiu limite em todos os provedores"""
        with self.lock:
            if ip not in self.ip_state:
                return True
            state = self.ip_state[ip]
            total = sum(state["emails_enviados"].values())
            # Considera esgotado se atingiu limite em >= 10 provedores
            esgotados = sum(1 for c in state["emails_enviados"].values() if c >= MAX_EMAILS_IP_PROVEDOR)
            return esgotados >= 10
    
    def trocar_ip(self, ip_antigo):
        """Apaga IP antigo e cria novo"""
        with self.lock:
            if ip_antigo not in self.ip_state:
                return None
            state = self.ip_state[ip_antigo]
            api = state["api"]
            ip_info = state["ip_info"]
            conta = state["conta"]
            
            # Marcar como usado hoje
            self.ips_hoje.add(ip_antigo)
            
            # Fechar tunnel
            state["tunnel"].close()
            
            # Apagar IP
            ip_id = ip_info.get("id")
            if ip_id:
                api.delete_floating_ip(ip_id)
                dizer(f"🗑️ IP apagado: {ip_antigo}")
            
            # Remover do estado
            del self.ip_state[ip_antigo]
            
            # Criar novo IP
            time.sleep(2)
            new_ip_info = api.create_floating_ip()
            if not new_ip_info or not new_ip_info.get("ip"):
                dizer(f"❌ Falha ao criar novo IP para {conta['name']}")
                return None
            
            new_ip = new_ip_info["ip"]
            
            # Verificar se novo IP já foi usado hoje
            if new_ip in self.ips_hoje:
                dizer(f"⚠️ Novo IP {new_ip} já usado hoje!")
                # Tentar criar outro
                api.delete_floating_ip(new_ip_info.get("id"))
                time.sleep(1)
                new_ip_info = api.create_floating_ip()
                if not new_ip_info:
                    return None
                new_ip = new_ip_info["ip"]
            
            # Configurar novo tunnel
            tunnel = SSHTunnel(new_ip, SSH_PORT, SSH_USER, conta["pem_path"])
            if tunnel.connect():
                self.ip_state[new_ip] = {
                    "conta": conta,
                    "api": api,
                    "ip_info": new_ip_info,
                    "tunnel": tunnel,
                    "emails_enviados": {},
                    "bloqueado": False,
                    "usado_desde": time.time(),
                }
                dizer(f"✅ Novo IP criado: {new_ip}")
                return new_ip
            return None
    
    def get_todos_ips(self):
        with self.lock:
            return list(self.ip_state.keys())
    
    def shutdown_all(self):
        """Fecha todos os tunnels"""
        with self.lock:
            for ip, state in self.ip_state.items():
                try:
                    state["tunnel"].close()
                except:
                    pass

IP_MANAGER = None  # Será inicializado no main

# ============================================================
# CARREGAR CONTAS DKIM + TEMPLATE
# ============================================================
def carregar_contas_dkim():
    """Carrega contas DKIM do HuggingFace"""
    dizer("📧 A carregar contas DKIM...")
    try:
        local = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/phase1_accounts_keys.csv",
            repo_type="dataset",
            token=HF_TOKEN
        )
    except Exception as e:
        dizer(f"❌ Erro ao baixar contas: {str(e)[:100]}")
        return []
    
    contas = []
    with open(local, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            priv = (row.get("dkim_private_key_pem") or "").strip()
            if not priv.startswith("-----BEGIN "):
                continue
            contas.append({
                "identity_id": (row.get("identity_id") or "").strip(),
                "sender_email": (row.get("sender_email") or "").strip(),
                "domain": (row.get("domain") or "").strip(),
                "base": (row.get("base") or "").strip(),
                "selector": (row.get("dkim_selector") or "default").strip(),
                "private_key_pem": priv,
                "public_key_txt": (row.get("dkim_public_txt") or "").strip(),
            })
    
    dizer(f"📧 {len(contas)} contas DKIM carregadas")
    return contas

def carregar_template():
    """Carrega template HTML"""
    try:
        local = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/veriscope_email_01.html",
            repo_type="dataset",
            token=HF_TOKEN
        )
        with open(local, "r", encoding="utf-8") as f:
            return f.read()
    except:
        return ""

def carregar_mail_from_variations():
    """Carrega variações de Mail From"""
    try:
        local = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/mail_from_alex_liquidity_alert_2115.txt",
            repo_type="dataset",
            token=HF_TOKEN
        )
        variations = []
        with open(local, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    variations.append(line)
        return variations
    except:
        return ["Alex Johnson", "Alex", "A. Johnson"]

# ============================================================
# MUTAÇÃO ABRACADABRA
# ============================================================
def mutar_html(html_base, conta, email_destinatario):
    """Aplica mutação única ao HTML para cada destinatário"""
    h = html_base
    
    # 1. Identificador único invisível
    rid = f"<!-- m-{random.randint(100000, 999999)}-{int(time.time() * 1000) % 100000} -->"
    if "<body>" in h:
        h = h.replace("<body>", f"<body>\n{rid}\n", 1)
    else:
        mm = re.search(r'<body[^>]*>', h)
        if mm:
            h = h.replace(mm.group(0), mm.group(0) + f"\n{rid}\n", 1)
    
    # 2. Espaços aleatórios entre tags
    h = re.sub(r'>\s+<', lambda m: '>' + (' ' * random.randint(1, 3)) + '<', h)
    
    # 3. URLs mutados
    def mutar_url(match):
        prefix, url_orig, suffix = match.group(1), match.group(2), match.group(3)
        seed = f"{email_destinatario}|{conta['identity_id']}|{int(time.time() * 1000) % 999999}"
        hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
        if "#" in url_orig:
            url_mutada = url_orig + f"&u={hash_val}"
        else:
            url_mutada = url_orig + f"#u={hash_val}"
        return prefix + url_mutada + suffix
    
    h = re.sub(r'(href=["\'])([^"\']+)(["\'])', mutar_url, h)
    h = re.sub(r'(src=["\'])([^"\']+)(["\'])', mutar_url, h)
    
    # 4. Comentário hash no final
    comentario = f"<!-- {hashlib.md5((email_destinatario + str(time.time())).encode()).hexdigest()[:12]} -->"
    h = h.replace("</body>", f"{comentario}\n</body>", 1)
    
    return h

def obter_display_name(conta, idx):
    """Retorna variação de display name"""
    variations = GLOBAL.get("mail_from_variations", ["Alex"])
    return variations[idx % len(variations)]

def construir_email(html_mutado, conta, email_dest, subject, idx_conta):
    """Constrói email completo com DKIM"""
    msg = EmailMessage()
    msg['Subject'] = subject
    display_name = obter_display_name(conta, idx_conta)
    msg['From'] = f"{display_name} <{conta['sender_email']}>"
    msg['To'] = email_dest
    msg['Message-ID'] = make_msgid(domain=conta['domain'])
    msg['Date'] = format_datetime(datetime.now(timezone.utc))
    msg.set_content(html_mutado, subtype='html')
    
    b = msg.as_bytes()
    
    # Assinar DKIM
    try:
        sig = dkim.sign(
            message=b,
            selector=conta['selector'].encode(),
            domain=conta['domain'].encode(),
            privkey=conta['private_key_pem'].encode(),
            include_headers=[b"from", b"to", b"subject", b"date", b"message-id"]
        )
        return sig + b
    except Exception as e:
        smtp_log(f"DKIM falhou: {str(e)[:80]}")
        return b

# ============================================================
# DOWNLOAD DE EMAILS DOS PROVEDORES
# ============================================================
_REPO_FILES_CACHE = None

def listar_parquets():
    global _REPO_FILES_CACHE
    if _REPO_FILES_CACHE:
        return _REPO_FILES_CACHE
    try:
        api = HfApi(token=HF_TOKEN)
        _REPO_FILES_CACHE = api.list_repo_files(HF_TRADER_REPO, repo_type="dataset")
        return _REPO_FILES_CACHE
    except:
        return []

def baixar_emails_provedor(provedor, max_emails=10000):
    """Baixa emails de um provedor do HuggingFace"""
    path_local = os.path.join(DIR_EMAILS, f"{provedor}.txt")
    
    # Cache local
    if os.path.exists(path_local):
        try:
            with open(path_local, "r", encoding="utf-8", errors="ignore") as f:
                emails = [l.strip() for l in f if l.strip() and "@" in l]
            if len(emails) >= 100:
                return emails[:max_emails]
        except:
            pass
    
    # Encontrar parquets
    todos = listar_parquets()
    prov_lower = provedor.lower()
    parquets = [f for f in todos if f.endswith('.parquet') and f.startswith('Trader_Emails/')
                and len(f.split('/')) >= 2 and f.split('/')[1].lower() == prov_lower]
    
    if not parquets:
        return []
    
    all_emails = []
    seen = set()
    
    for pq_path in sorted(parquets):
        if len(all_emails) >= max_emails:
            break
        try:
            local = hf_hub_download(
                repo_id=HF_TRADER_REPO, filename=pq_path,
                repo_type="dataset", token=HF_TOKEN, etag_timeout=60
            )
            pf = pq.ParquetFile(local)
            col = None
            for c in pf.schema_arrow.names:
                if str(c).lower() == "email":
                    col = c
                    break
            if col is None:
                for c in pf.schema_arrow.names:
                    try:
                        if str(pf.schema_arrow.field(c).type) in ("string", "large_string"):
                            col = c
                            break
                    except:
                        continue
            if col is None:
                continue
            
            for batch in pf.iter_batches(batch_size=10000, columns=[col]):
                try:
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
                        if len(all_emails) >= max_emails:
                            break
                except:
                    continue
                if len(all_emails) >= max_emails:
                    break
        except:
            pass
    
    # Salvar cache
    if all_emails:
        try:
            with open(path_local, "w", encoding="utf-8") as f:
                f.write("\n".join(all_emails) + "\n")
        except:
            pass
    
    return all_emails[:max_emails]

# ============================================================
# MX RESOLUTION
# ============================================================
MX_FALLBACK = {
    "gmail.com": "gmail-smtp-in.l.google.com",
    "googlemail.com": "gmail-smtp-in.l.google.com",
    "google.com": "gmail-smtp-in.l.google.com",
    "youtube.com": "gmail-smtp-in.l.google.com",
    "bebo.com": "aspmx.l.google.com",
    "mail.ru": "mxs.mail.ru", "bk.ru": "mxs.mail.ru",
    "list.ru": "mxs.mail.ru", "inbox.ru": "mxs.mail.ru",
    "qq.com": "mx3.qq.com", "vip.qq.com": "mx3.qq.com",
    "foxmail.com": "mx3.qq.com",
    "naver.com": "mx1.mail.naver.com",
}

def resolver_mx(provedor):
    try:
        ans = dns.resolver.resolve(provedor, 'MX')
        mxs = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in ans])
        if mxs:
            return [m[1] for m in mxs]
        fb = MX_FALLBACK.get(provedor)
        return [fb] if fb else []
    except:
        fb = MX_FALLBACK.get(provedor)
        return [fb] if fb else []

# ============================================================
# CLASSIFICAÇÃO DE RESPOSTAS SMTP
# ============================================================
TOO_MANY_KW = ['too many recipients', 'too many connections', 'rate limit',
               'ratelimit', '452 4.5.3', '552 too many', 'recipient limit',
               'connection limit', 'throttl']
GREYLIST_KW = ['greylist', 'graylist', 'try again later', 'temporarily deferred',
               'temporarily unavailable', 'please try again']
UNKNOWN_KW = ['user unknown', 'no such user', 'mailbox not found', 'no such mailbox',
              'invalid recipient', 'recipient rejected', 'does not exist',
              'user not found', 'mailbox unavailable', 'recipient unknown']
BLOCK_KW = ['spamhaus', 'mimecast', 'barracuda', 'spamcop', 'dnsbl', 'rbl',
            'cannot find your hostname', 'reverse dns', 'client host rejected',
            'blocked', 'blacklist', 'poor reputation']

def classificar(resp):
    r = resp.lower()
    for kw in TOO_MANY_KW:
        if kw in r: return 'TOO_MANY'
    if resp.startswith("451") or resp.startswith("452"):
        for kw in GREYLIST_KW:
            if kw in r: return 'GREYLIST'
    for kw in UNKNOWN_KW:
        if kw in r: return 'USER_UNKNOWN'
    for kw in BLOCK_KW:
        if kw in r: return 'BLOCKED'
    if resp.startswith("250") or resp.startswith("251"): return 'ACEITE'
    if resp.startswith("451") or resp.startswith("452"): return 'GREYLIST'
    return 'OUTRO'

# ============================================================
# REGISTAR RESULTADOS
# ============================================================
UNKNOWN_LOCK = threading.Lock()
ACEITES_LOCK = threading.Lock()

def salvar_unknown(email, provedor):
    try:
        email = email.lower().strip()
        if not email or "@" not in email:
            return
        with GLOBAL["user_unknowns_lock"]:
            if email in GLOBAL["user_unknowns"]:
                return
            GLOBAL["user_unknowns"].add(email)
        with UNKNOWN_LOCK:
            exists = os.path.exists(UNKNOWN_FILE)
            with open(UNKNOWN_FILE, "a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if not exists:
                    w.writerow(["email", "provedor", "primeiro_visto"])
                w.writerow([email, provedor, datetime.now().isoformat()])
    except:
        pass

def salvar_aceite(email, provedor, conta_id, mx_host):
    try:
        email = email.lower().strip()
        with GLOBAL["aceites_lock"]:
            if email in GLOBAL["aceites_emails"]:
                return False
            GLOBAL["aceites_emails"].add(email)
        with ACEITES_LOCK:
            exists = os.path.exists(ACEITES_FILE)
            with open(ACEITES_FILE, "a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if not exists:
                    w.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                w.writerow([email, provedor, conta_id, mx_host, datetime.now().isoformat()])
        return True
    except:
        return False

def salvar_rejeicao(email, provedor, code, msg):
    try:
        exists = os.path.exists(REJEICOES_FILE)
        with open(REJEICOES_FILE, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["email", "provedor", "smtp_code", "smtp_message", "timestamp"])
            w.writerow([email.lower(), provedor, code, str(msg)[:200], datetime.now().isoformat()])
    except:
        pass

# cole aqui embaixo o seguinte código
# ============================================================
# SMTP - RECEBER/ENVIAR
# ============================================================
def recv_smtp(sock, timeout=15):
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
                last = lines[-2] if lines[-1] == b'' else lines[-1]
                if len(last) >= 3 and last[:3].isdigit() and last[3:4] == b' ':
                    break
        except socket.timeout:
            break
        except:
            break
    return data.decode('utf-8', errors='ignore').strip()

def send_smtp(sock, cmd):
    if isinstance(cmd, str):
        cmd = cmd.encode()
    sock.sendall(cmd)

# ============================================================
# ENVIO DE EMAIL ÚNICO
# ============================================================
def enviar_email(tunnel, mx_host, conta, email_dest, html_base, idx_conta, provedor):
    """Envia um email e retorna o tipo de resposta"""
    sock = None
    try:
        sock = tunnel.get_socket(mx_host, 25)
        if not sock:
            return 'ERRO_CONEXAO', ''
        
        banner = recv_smtp(sock)
        if not banner.startswith("220"):
            return 'ERRO_CONEXAO', banner
        
        send_smtp(sock, f"EHLO {conta['domain']}\r\n")
        recv_smtp(sock)
        
        send_smtp(sock, f"MAIL FROM:<{conta['sender_email']}>\r\n")
        mf_resp = recv_smtp(sock)
        if not mf_resp.startswith("250"):
            return 'ERRO_CONEXAO', mf_resp
        
        send_smtp(sock, f"RCPT TO:<{email_dest}>\r\n")
        rcpt_resp = recv_smtp(sock, timeout=10)
        tipo = classificar(rcpt_resp)
        
        if tipo == 'ACEITE':
            # Enviar DATA
            send_smtp(sock, b"DATA\r\n")
            data_resp = recv_smtp(sock)
            if data_resp.startswith("354"):
                html_mutado = mutar_html(html_base, conta, email_dest)
                payload = construir_email(html_mutado, conta, email_dest, 
                                          "Can I show you something tomorrow?", idx_conta)
                if not payload.endswith(b"\r\n.\r\n"):
                    if not payload.endswith(b"\r\n"):
                        payload += b"\r\n"
                    payload += b".\r\n"
                sock.sendall(payload)
                final_resp = recv_smtp(sock, timeout=30)
                if final_resp.startswith("250"):
                    send_smtp(sock, b"QUIT\r\n")
                    return 'ACEITE', final_resp
                else:
                    send_smtp(sock, b"QUIT\r\n")
                    return classificar(final_resp), final_resp
            else:
                send_smtp(sock, b"QUIT\r\n")
                return classificar(data_resp), data_resp
        else:
            send_smtp(sock, b"QUIT\r\n")
            return tipo, rcpt_resp
    
    except Exception as e:
        return 'ERRO_CONEXAO', str(e)[:100]
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass

# ============================================================
# RCPT DINÂMICO
# ============================================================
def obter_rcpt_limit(provedor):
    with GLOBAL["rcpt_lock"]:
        return GLOBAL["rcpt_aprendido"].get(provedor, RCPT_DEFAULT)

def registar_erro_rcpt(provedor):
    with GLOBAL["rcpt_lock"]:
        atual = GLOBAL["rcpt_aprendido"].get(provedor, RCPT_DEFAULT)
        novo = max(atual // 2, 5)
        GLOBAL["rcpt_aprendido"][provedor] = novo

def registar_sucesso_rcpt(provedor):
    with GLOBAL["rcpt_lock"]:
        atual = GLOBAL["rcpt_aprendido"].get(provedor, RCPT_DEFAULT)
        novo = min(atual + 5, RCPT_LIMITS.get(provedor, RCPT_DEFAULT) * 2)
        GLOBAL["rcpt_aprendido"][provedor] = novo

# ============================================================
# WORKER DE PROVEDOR (PARALELISMO HIERÁRQUICO)
# ============================================================
def worker_provedor(provedor, worker_id):
    """Worker independente para cada provedor"""
    smtp_log(f"\n{'='*60}")
    smtp_log(f"[W{worker_id:03d}] INICIO: {provedor} via ARAcloud")
    smtp_log(f"{'='*60}")
    
    # Resolver MX
    mx_hosts = resolver_mx(provedor)
    if not mx_hosts:
        dizer(f"[{provedor}] ❌ MX não resolvido")
        return
    
    mx_host = mx_hosts[0]
    
    # Baixar emails
    emails = baixar_emails_provedor(provedor)
    if not emails:
        dizer(f"[{provedor}] ⚠️ Sem emails")
        return
    
    # Filtrar unknowns
    with GLOBAL["user_unknowns_lock"]:
        emails_validos = [e for e in emails if e.lower() not in GLOBAL["user_unknowns"]]
    
    # Obter offset do checkpoint
    with GLOBAL["provedores_lock"]:
        offset = GLOBAL["por_provedor"][provedor].get("offset", 0)
    
    emails_restantes = emails_validos[offset:]
    if not emails_restantes:
        dizer(f"[{provedor}] ✅ Já concluído (offset={offset})")
        with GLOBAL["provedores_lock"]:
            GLOBAL["por_provedor"][provedor]["concluido"] = True
        return
    
    dizer(f"[{provedor}] 📧 {len(emails_restantes)} emails a enviar (offset={offset})")
    
    # Limite RCPT para este provedor
    rcpt_limit = obter_rcpt_limit(provedor)
    consecutivos_erro = 0
    consecutivos_sucesso = 0
    
    # Processar em batches
    batch_size = min(rcpt_limit, 50)  # Máximo 50 por batch
    greylist_queue = []  # Emails com greylist para retry
    
    for i in range(0, len(emails_restantes), batch_size):
        if GLOBAL["emergency_stop"]:
            break
        
        batch = emails_restantes[i:i+batch_size]
        batch_aceites = 0
        batch_rejeitados = 0
        batch_unknowns = 0
        batch_greylist = 0
        
        for email in batch:
            if GLOBAL["emergency_stop"]:
                break
            
            # Obter IP disponível
            ip, tunnel = IP_MANAGER.get_ip_disponivel(provedor)
            if not tunnel:
                dizer(f"[{provedor}] ⚠️ Sem IPs disponíveis")
                time.sleep(10)
                continue
            
            # Selecionar conta DKIM (round-robin)
            contas = GLOBAL["contas_dkim"]
            if not contas:
                break
            idx_conta = (offset + i) % len(contas)
            conta = contas[idx_conta]
            
            # Enviar email
            tipo, resp = enviar_email(
                tunnel, mx_host, conta, email,
                GLOBAL["html_base"], idx_conta, provedor
            )
            
            # Processar resposta
            with GLOBAL["stats_lock"]:
                GLOBAL["smtp_tentativas"] += 1
            
            if tipo == 'ACEITE':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_aceites"] += 1
                salvar_aceite(email, provedor, conta['identity_id'], mx_host)
                batch_aceites += 1
                consecutivos_sucesso += 1
                consecutivos_erro = 0
                IP_MANAGER.registar_envio(ip, provedor)
                
                # Verificar se IP esgotou
                if IP_MANAGER.esgotou_ip(ip):
                    dizer(f"[{provedor}] 🔄 IP {ip} esgotado, a trocar...")
                    new_ip = IP_MANAGER.trocar_ip(ip)
                    if new_ip:
                        # Atualizar DNS para novo IP
                        atualizar_dns_novo_ip(new_ip)
                
                smtp_log(f"[W{worker_id:03d}] [{provedor}] ✅ ACEITE: {email}")
            
            elif tipo == 'TOO_MANY':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_rejeitados"] += 1
                batch_rejeitados += 1
                consecutivos_erro += 1
                consecutivos_sucesso = 0
                registar_erro_rcpt(provedor)
                salvar_rejeicao(email, provedor, "452", resp)
                smtp_log(f"[W{worker_id:03d}] [{provedor}] 🛑 TOO_MANY: {email}")
                
                # Reduzir batch size
                batch_size = max(batch_size // 2, 5)
                
                # Se muitos erros seguidos, trocar IP
                if consecutivos_erro >= 3:
                    dizer(f"[{provedor}] ⚠️ {consecutivos_erro} TOO_MANY seguidos")
                    time.sleep(5)
            
            elif tipo == 'USER_UNKNOWN':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_user_unknown"] += 1
                batch_unknowns += 1
                salvar_unknown(email, provedor)
                IP_MANAGER.registar_envio(ip, provedor)
                smtp_log(f"[W{worker_id:03d}] [{provedor}] 👻 UNKNOWN: {email}")
            
            elif tipo == 'GREYLIST':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_greylist"] += 1
                batch_greylist += 1
                greylist_queue.append((email, mx_host, conta, idx_conta))
                smtp_log(f"[W{worker_id:03d}] [{provedor}] 🔵 GREYLIST: {email}")
            
            elif tipo == 'BLOCKED':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_blocked"] += 1
                batch_rejeitados += 1
                consecutivos_erro += 1
                salvar_rejeicao(email, provedor, "550", resp)
                smtp_log(f"[W{worker_id:03d}] [{provedor}] 🚫 BLOCKED: {email}")
                
                # Bloquear IP para este provedor
                with GLOBAL["orcamento_lock"]:
                    if ip not in GLOBAL["orcamento_ip"]:
                        GLOBAL["orcamento_ip"][ip] = {}
                    GLOBAL["orcamento_ip"][ip][provedor] = "BLOCKED"
            
            else:
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_erros"] += 1
                batch_rejeitados += 1
                smtp_log(f"[W{worker_id:03d}] [{provedor}] ❌ ERRO: {email} - {resp[:80]}")
            
            # Atualizar offset
            with GLOBAL["provedores_lock"]:
                GLOBAL["por_provedor"][provedor]["offset"] += 1
                GLOBAL["por_provedor"][provedor]["total_enviados"] += 1
                GLOBAL["por_provedor"][provedor]["aceites"] += (1 if tipo == 'ACEITE' else 0)
                GLOBAL["por_provedor"][provedor]["rejeitados"] += (1 if tipo in ('TOO_MANY', 'BLOCKED') else 0)
                GLOBAL["por_provedor"][provedor]["user_unknown"] += (1 if tipo == 'USER_UNKNOWN' else 0)
                GLOBAL["por_provedor"][provedor]["greylist"] += (1 if tipo == 'GREYLIST' else 0)
            
            # Delay entre emails
            time.sleep(DELAY_ENTRE_EMAILS)
        
        # Ajustar RCPT baseado no sucesso do batch
        if batch_aceites > batch_rejeitados:
            consecutivos_sucesso += 1
            if consecutivos_sucesso >= 3:
                registar_sucesso_rcpt(provedor)
                batch_size = min(batch_size + 5, obter_rcpt_limit(provedor))
        else:
            consecutivos_sucesso = 0
        
        dizer(f"[{provedor}] 📊 Batch: {batch_aceites}✅ {batch_rejeitados}❌ {batch_unknowns}👻 {batch_greylist}🔵")
    
    # Processar greylist retries
    if greylist_queue and provedor in PROVEDORES_GREYLIST:
        processar_greylist_retries(provedor, greylist_queue, worker_id)
    
    # Marcar como concluído
    with GLOBAL["provedores_lock"]:
        GLOBAL["por_provedor"][provedor]["concluido"] = True
    
    smtp_log(f"[W{worker_id:03d}] FIM: {provedor}")
    dizer(f"[{provedor}] 🏁 Concluído")

# ============================================================
# GREYLIST RETRIES
# ============================================================
def processar_greylist_retries(provedor, queue, worker_id):
    """Processa fila de greylist com retries"""
    if not queue:
        return
    
    dizer(f"[{provedor}] 🔵 Processando {len(queue)} greylist retries...")
    
    for tentativa, espera in enumerate(GREYLIST_SCHEDULE):
        if not queue or GLOBAL["emergency_stop"]:
            break
        
        dizer(f"[{provedor}] ⏳ Tentativa {tentativa+1}/{len(GREYLIST_SCHEDULE)}, esperando {espera}s...")
        time.sleep(espera)
        
        ainda_greylist = []
        for email, mx_host, conta, idx_conta in queue:
            if GLOBAL["emergency_stop"]:
                break
            
            ip, tunnel = IP_MANAGER.get_ip_disponivel(provedor)
            if not tunnel:
                ainda_greylist.append((email, mx_host, conta, idx_conta))
                continue
            
            tipo, resp = enviar_email(
                tunnel, mx_host, conta, email,
                GLOBAL["html_base"], idx_conta, provedor
            )
            
            if tipo == 'ACEITE':
                with GLOBAL["stats_lock"]:
                    GLOBAL["smtp_aceites"] += 1
                salvar_aceite(email, provedor, conta['identity_id'], mx_host)
                IP_MANAGER.registar_envio(ip, provedor)
                smtp_log(f"[W{worker_id:03d}] [{provedor}] ✅ GREYLIST OK (tent {tentativa+1}): {email}")
            elif tipo == 'GREYLIST':
                ainda_greylist.append((email, mx_host, conta, idx_conta))
            else:
                smtp_log(f"[W{worker_id:03d}] [{provedor}] ❌ GREYLIST fail: {email} -> {tipo}")
            
            time.sleep(DELAY_ENTRE_EMAILS)
        
        queue = ainda_greylist
        dizer(f"[{provedor}] 🔵 Restam {len(queue)} em greylist")

# ============================================================
# DNS UPDATE QUANDO IP MUDA
# ============================================================
def atualizar_dns_novo_ip(new_ip):
    """Atualiza SPF + A records para o novo IP em todos os domínios"""
    dizer(f"🔄 Atualizando DNS para IP {new_ip}...")
    
    desec = DesecAPI(DESEC_TOKEN)
    dominios = GLOBAL.get("dns_autenticacao", {})
    
    for domain, info in dominios.items():
        try:
            ok = desec.update_spf_only(domain, new_ip)
            if ok:
                dizer(f"  ✅ DNS atualizado: {domain}")
            else:
                dizer(f"  ❌ Falha DNS: {domain}")
        except Exception as e:
            dizer(f"  ⚠️ Erro DNS {domain}: {str(e)[:60]}")
    
    # Esperar propagação
    dizer(f"⏳ Esperando propagação DNS ({DNS_PROPAGATION}s)...")
    time.sleep(DNS_PROPAGATION)

def configurar_dns_inicial(ips_ativos):
    """Configura DNS completo na primeira vez"""
    dizer("🧬 Configurando DNS inicial...")
    
    desec = DesecAPI(DESEC_TOKEN)
    dominios = desec.list_domains()
    
    if not dominios:
        dizer("❌ Nenhum domínio encontrado no deSEC")
        return False
    
    contas = GLOBAL["contas_dkim"]
    if not contas:
        dizer("❌ Sem contas DKIM")
        return False
    
    # Usar primeiro IP ativo
    ip_ativo = ips_ativos[0] if ips_ativos else "209.127.114.175"
    
    for conta in contas[:30]:  # Até 30 contas
        domain = conta["domain"]
        base = conta["base"]
        selector = conta["selector"]
        dkim_pub = conta.get("public_key_txt", "")
        
        # Verificar se já está configurado
        if domain in GLOBAL.get("dns_autenticacao", {}):
            # Apenas atualizar SPF
            desec.update_spf_only(domain, ip_ativo)
        else:
            # Configuração completa
            ok = desec.setup_full_auth(domain, base, selector, dkim_pub, ip_ativo)
            if ok:
                GLOBAL["dns_autenticacao"][domain] = {
                    "base": base,
                    "selector": selector,
                    "dkim_pub": dkim_pub,
                    "configurado_em": datetime.now().isoformat(),
                }
                dizer(f"  ✅ DNS configurado: {domain}")
            else:
                dizer(f"  ❌ Falha DNS: {domain}")
    
    # Guardar autenticação
    CHECKPOINT.save_all()
    
    dizer(f"⏳ Esperando propagação DNS ({DNS_PROPAGATION}s)...")
    time.sleep(DNS_PROPAGATION)
    return True

# ============================================================
# DASHBOARD
# ============================================================
def painel():
    el = time.time() - GLOBAL["start_time"]
    h, m, s = int(el // 3600), int((el % 3600) // 60), int(el % 60)
    taxa = GLOBAL["smtp_aceites"] / max(1, GLOBAL["smtp_tentativas"]) * 100
    
    # Provedores concluídos
    with GLOBAL["provedores_lock"]:
        concluidos = sum(1 for p, info in GLOBAL["por_provedor"].items() if info.get("concluido"))
        total_provs = len(PROVEDORES_ATIVOS)
    
    return f"""
╔{'═'*78}╗
║ 🚀 VERISCOPE V{VERSION} | 🌩️ ARAcloud | ⏱️ {h:02d}:{m:02d}:{s:02d}                     ║
╠{'═'*78}╣
║ 📤 Tentativas: {GLOBAL['smtp_tentativas']:<10,} ✅ Aceites: {GLOBAL['smtp_aceites']:<10,} 💡 Taxa: {taxa:.1f}%    ║
║ ❌ Rejeitados: {GLOBAL['smtp_rejeitados']:<10,} 👻 Unknown: {GLOBAL['smtp_user_unknown']:<10,} 🔵 Grey: {GLOBAL['smtp_greylist']:<5,} ║
║ 🚫 Blocked: {GLOBAL['smtp_blocked']:<10,} ⚠️ Erros: {GLOBAL['smtp_erros']:<10,}                             ║
╠{'═'*78}╣
║ 🏆 Provedores: {concluidos}/{total_provs} concluídos                                      ║
║ 🌐 IPs ativos: {len(IP_MANAGER.get_todos_ips()) if IP_MANAGER else 0:<5}                                                     ║
╚{'═'*78}╝"""

# ============================================================
# TIME LIMIT WATCHER
# ============================================================
def time_limit_watcher():
    """Monitoriza tempo e faz shutdown inteligente"""
    while not GLOBAL["emergency_stop"]:
        elapsed = time.time() - GLOBAL["run_started_at"]
        
        if elapsed >= MAX_EXECUTION_SECONDS:
            dizer("⏰ 5h40m atingidos! Shutdown inteligente...")
            GLOBAL["emergency_stop"] = True
            
            # Esperar workers terminarem
            time.sleep(10)
            
            # Salvar checkpoint final
            flush_all()
            CHECKPOINT.save_all()
            upload_to_hf()
            
            dizer("🏁 Shutdown completo. Estado guardado.")
            return
        
        # Dashboard a cada 60s
        if int(elapsed) % 60 == 0:
            print(painel(), flush=True)
        
        time.sleep(30)

# ============================================================
# CHECKPOINT PERIÓDICO
# ============================================================
def checkpoint_periodico():
    while not GLOBAL["emergency_stop"]:
        time.sleep(CHECKPOINT_INTERVAL)
        if GLOBAL["emergency_stop"]:
            break
        flush_all()
        CHECKPOINT.save_all()
        upload_to_hf()

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 80)
    print(f"🚀 VERISCOPE V{VERSION} - ENGINE COMPLETO")
    print("=" * 80)
    
    GLOBAL["exec_id"] = f"exec_{int(time.time())}_{random.randint(1000, 9999)}"
    GLOBAL["campaign_id"] = f"camp_{int(time.time())}"
    
    # 1. Baixar checkpoints do HuggingFace
    download_from_hf()
    
    # 2. Carregar checkpoints locais
    loaded = CHECKPOINT.load_all()
    if loaded > 0 and CHECKPOINT.exists():
        dizer(f"♻️ Recuperação: {loaded} ficheiros carregados")
        dizer(f"   Aceites anteriores: {GLOBAL['smtp_aceites']:,}")
        dizer(f"   Unknowns: {len(GLOBAL['user_unknowns']):,}")
    else:
        dizer("🆕 Execução nova")
    
    # 3. Carregar contas DKIM
    GLOBAL["contas_dkim"] = carregar_contas_dkim()
    if not GLOBAL["contas_dkim"]:
        dizer("❌ Sem contas DKIM. A sair.")
        return
    GLOBAL["dominio_principal"] = GLOBAL["contas_dkim"][0]["domain"]
    
    # 4. Carregar template + variações
    GLOBAL["html_base"] = carregar_template()
    if not GLOBAL["html_base"]:
        dizer("❌ Sem template HTML. A sair.")
        return
    GLOBAL["mail_from_variations"] = carregar_mail_from_variations()
    
    # 5. Carregar credenciais Aracloud
    contas_aracloud = baixar_credenciais_aracloud()
    if not contas_aracloud:
        dizer("❌ Sem contas Aracloud. A sair.")
        return
    
    # 6. Inicializar IPs
    global IP_MANAGER
    IP_MANAGER = IPManager(contas_aracloud)
    if not IP_MANAGER.inicializar_ips():
        dizer("❌ Falha ao inicializar IPs. A sair.")
        return
    
    ips_ativos = IP_MANAGER.get_todos_ips()
    dizer(f"🌐 {len(ips_ativos)} IPs Aracloud ativos")
    
    # 7. Configurar DNS
    configurar_dns_inicial(ips_ativos)
    
    # 8. Carregar unknowns do CSV
    if os.path.exists(UNKNOWN_FILE):
        try:
            with open(UNKNOWN_FILE, "r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and "email" in row and row["email"]:
                        with GLOBAL["user_unknowns_lock"]:
                            GLOBAL["user_unknowns"].add(row["email"].lower().strip())
            dizer(f"📋 {len(GLOBAL['user_unknowns']):,} user_unknowns carregados")
        except:
            pass
    
    # 9. Iniciar watchers
    time_thread = threading.Thread(target=time_limit_watcher, daemon=True)
    time_thread.start()
    
    cp_thread = threading.Thread(target=checkpoint_periodico, daemon=True)
    cp_thread.start()
    
    # 10. Iniciar workers de provedores em paralelo
    dizer(f"🚀 Iniciando {NUM_PROVEDORES_PARALELOS} workers paralelos...")
    print(painel(), flush=True)
    
    # Usar ThreadPoolExecutor para paralelismo
    with ThreadPoolExecutor(max_workers=NUM_PROVEDORES_PARALELOS) as executor:
        futures = {}
        for idx, provedor in enumerate(PROVEDORES_ATIVOS):
            future = executor.submit(worker_provedor, provedor, idx + 1)
            futures[future] = provedor
        
        # Aguardar conclusão
        for future in as_completed(futures):
            provedor = futures[future]
            try:
                future.result()
            except Exception as e:
                dizer(f"❌ Erro no worker {provedor}: {str(e)[:80]}")
    
    # 11. Shutdown final
    dizer("🏁 Todos os workers concluídos")
    flush_all()
    CHECKPOINT.save_all()
    upload_to_hf()
    
    # Fechar tunnels
    if IP_MANAGER:
        IP_MANAGER.shutdown_all()
    
    # Relatório final
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"  🏆 Aceites:      {GLOBAL['smtp_aceites']:,}")
    print(f"  📤 Tentativas:   {GLOBAL['smtp_tentativas']:,}")
    print(f"  ❌ Rejeitados:   {GLOBAL['smtp_rejeitados']:,}")
    print(f"  👻 Unknowns:     {GLOBAL['smtp_user_unknown']:,}")
    print(f"  🔵 Greylist:     {GLOBAL['smtp_greylist']:,}")
    print(f"  🚫 Blocked:      {GLOBAL['smtp_blocked']:,}")
    print(f"  ⚠️ Erros:        {GLOBAL['smtp_erros']:,}")
    
    taxa = GLOBAL['smtp_aceites'] / max(1, GLOBAL['smtp_tentativas']) * 100
    print(f"  💡 Taxa:         {taxa:.2f}%")
    
    # Provedores concluídos
    with GLOBAL["provedores_lock"]:
        concluidos = [p for p, info in GLOBAL["por_provedor"].items() if info.get("concluido")]
    print(f"  🏁 Concluídos:   {len(concluidos)}/{len(PROVEDORES_ATIVOS)}")
    
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        dizer("🛑 Interrompido pelo utilizador")
        GLOBAL["emergency_stop"] = True
        flush_all()
        CHECKPOINT.save_all()
        upload_to_hf()
        if IP_MANAGER:
            IP_MANAGER.shutdown_all()
    except Exception as e:
        dizer(f"❌ ERRO FATAL: {e}")
        traceback.print_exc()
        flush_all()
        CHECKPOINT.save_all()
        upload_to_hf()
        if IP_MANAGER:
            IP_MANAGER.shutdown_all()
