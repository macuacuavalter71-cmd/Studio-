# -- coding: utf-8 --
"""
VERISCOPE V19.5.0 - ARQUITETURA HEADLESS (COLAB CLI + GITHUB ACTIONS)
SMART SHUTDOWN (5h40m) + AUTO-RESTART + CHECKPOINT HF + RASTREIO EXATO
"""
import os
import sys
HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()
if not HF_TOKEN:
    HF_TOKEN = os.environ.get("HF_TOKEN ", "").strip()
if not HF_TOKEN:
    HF_TOKEN = os.environ.get("HUGGING_FACE_HUB_TOKEN", "").strip()
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
        "pyarrow", "huggingface_hub", "psutil", "pysocks"
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
GLOBAL_DESEC_SEMAPHORE = threading.Semaphore(4)
GLOBAL_FILEWRITE_SEMAPHORE = threading.Semaphore(32)
UNKNOWN_FILE_LOCK = threading.Lock()
ACEITES_FILE_LOCK = threading.Lock()
HF_LOGS_REPO = "Valter3B/Campaign_Logs"
HF_TRADER_REPO = "Valter3B/Trader_Emails"
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
DESEC_CHUNK = 400
EXIT_IP_TIMEOUT = 8
PORT25_TIMEOUT = 5
TARGET_VALID_PROXIES = 100
MIN_PROXIES_REFILL = 10
PROXY_COOLDOWN_SEGUNDOS = 120
MAX_TIMEOUTS_BAN = 3
RESCRAPE_INTERVALO_MINIMO = 300
JANELA_VALIDACAO_MAX = 400
MAX_SPF_IPS = 250
DELAY_ENTRE_RCPT = 0.3
MAX_RCPT_POR_CONEXAO = 100
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
DESEC_MIN_DELAY = 0.5
DESEC_BACKOFF_BASE = 30
DESEC_BACKOFF_MULTIPLIER = 2.0
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
CP_FASES = os.path.join(BASE_LOCAL, "cp_fases.json")
CP_PROVEDORES = os.path.join(BASE_LOCAL, "cp_provedores.json")
CP_POOL = os.path.join(BASE_LOCAL, "cp_pool.json")
CP_SMTP = os.path.join(BASE_LOCAL, "cp_smtp.json")
CP_CONTAS = os.path.join(BASE_LOCAL, "cp_contas.json")
CP_CONFIG = os.path.join(BASE_LOCAL, "cp_config.json")
PROXY_SEARCH_DIRS = [
    DIR_PROXIES,
    "/content/proxies",
    "/content",
    "/content/data",
    "/content/uploads",
    "./",
]
FASES_OBRIGATORIAS_ANTES_ENVIO = [
    "FASE0_IP",
    "FASE1_TOKENS",
    "FASE2A_CONTAS",
    "FASE2B_MX",
    "FASE2C_DOWNLOAD",
    "FASE25_PROXIES",
    "FASE3_DNS",
    "FASE4_PROPAGACAO",
]
def diagnostico_ambiente_hf():
    print("\n" + "=" * 80, flush=True)
    print("🔍 DIAGNÓSTICO CRU DE AMBIENTE E AUTENTICAÇÃO HUGGING FACE", flush=True)
    print("=" * 80, flush=True)
    hf_token = os.environ.get("HF_TOKEN", "")
    print(f"[ENV] HF_TOKEN existe: {'SIM' if hf_token else 'NÃO'}", flush=True)
    print(f"[ENV] HF_TOKEN length: {len(hf_token)}", flush=True)
    print(f"[ENV] HF_TOKEN starts with 'hf_': {hf_token.startswith('hf_') if hf_token else False}", flush=True)
    print("\n[ENV] Variáveis de ambiente críticas:", flush=True)
    critical_keys = ["HF_TOKEN", "COLAB_TOKEN_JSON", "GITHUB_PAT", "GITHUB_REPOSITORY", "PYTHONUNBUFFERED"]
    for k in critical_keys:
        v = os.environ.get(k, "")
        if k == "HF_TOKEN" and v:
            print(f"  - {k}: [REDACTED - {len(v)} chars]", flush=True)
        elif k == "COLAB_TOKEN_JSON" and v:
            print(f"  - {k}: [REDACTED JSON]", flush=True)
        else:
            print(f"  - {k}: {v}", flush=True)
    print("\n[HF HUB] A testar api.whoami()...", flush=True)
    try:
        from huggingface_hub import HfApi
        import huggingface_hub
        print(f"[HF HUB] huggingface_hub version: {huggingface_hub.__version__}", flush=True)
        api = HfApi(token=hf_token if hf_token else None)
        info = api.whoami()
        print(f"[HF HUB] ✅ whoami() SUCCESS. User: {info.get('name')}", flush=True)
    except Exception as e:
        print(f"[HF HUB] ❌ whoami() FAILED.", flush=True)
        print(f"[HF HUB] Exception Type: {type(e).__name__}", flush=True)
        print(f"[HF HUB] Exception Message: {str(e)}", flush=True)
        if hasattr(e, 'response') and e.response is not None:
            print(f"[HF HUB] HTTP Status: {e.response.status_code}", flush=True)
            print(f"[HF HUB] Response Headers: {dict(e.response.headers)}", flush=True)
            print(f"[HF HUB] Response Text: {e.response.text[:500]}", flush=True)
        import traceback
        print(f"[HF HUB] Traceback cru:", flush=True)
        traceback.print_exc()
    print("=" * 80 + "\n", flush=True)
class CheckpointManager:
    def __init__(self):
        self.locks = {
            "fases": threading.Lock(),
            "provedores": threading.Lock(),
            "pool": threading.Lock(),
            "smtp": threading.Lock(),
            "contas": threading.Lock(),
            "config": threading.Lock(),
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
                "provedor_index": {},
                "por_provedor": {},
            },
            "contas": {"contas_carregadas": 0, "contas_idx": 0},
            "config": {
                "exec_id": None,
                "ip_colab": None,
                "start_time": None,
                "last_save": None,
                "status": "NEW",
            },
        }
        self.paths = {
            "fases": CP_FASES,
            "provedores": CP_PROVEDORES,
            "pool": CP_POOL,
            "smtp": CP_SMTP,
            "contas": CP_CONTAS,
            "config": CP_CONFIG,
        }
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
                        self.states[nome] = dados
                        print("[CHECKPOINT] Carregado local: " + nome, flush=True)
                except Exception as e:
                    print("[CHECKPOINT] AVISO ao carregar " + nome + ": " + str(e)[:100], flush=True)
    def carregar_tudo(self):
        self._carregar_tudo()
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
                if k != "provedor_index" and k != "por_provedor":
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
        self.states = {k: {} if k != "pool" else {"pool": [], "scraping_checkpoint": 0, "raw_count": 0}
                       for k in self.paths.keys()}
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
        "dns_total": 0, "dns_updated": 0,
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
        "logs_uploaded": False,
        "pool": [],
        "pool_lock": threading.Lock(),
        "scraping_checkpoint": 0,
        "raw_proxies": [],
        "raw_lock": threading.Lock(),
        "ips_vistos": set(),
        "proxy_state": {},
        "proxy_state_lock": threading.Lock(),
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
                    "aceites": 0,
                    "rejeitados": 0,
                    "erros_consecutivos": 0,
                    "rate_current": float(RATE_BASELINE_EMAILS_MIN),
                    "ultima_acao": time.time(),
                    "ultima_resposta_4xx": 0,
                    "ultima_resposta_5xx": 0,
                    "throttled_ate": 0,
                    "provedor_stats": {},
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
def limpar_logs_antigos(preservar_checkpoints=True):
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
    sys.excepthook(exc_type, exc_value, exc_traceback)
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
                f.write("Headers: " + str(headers) + "\n")
                if payload_preview:
                    f.write("Payload Preview: " + str(payload_preview)[:2000] + "\n")
                f.write("Response Body: " + str(response_body)[:1000] + "\n")
                f.write("=" * 80 + "\n\n")
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
                    api.upload_file(
                        path_or_fileobj=path,
                        path_in_repo="checkpoints/" + nome + ".json",
                        repo_id=HF_LOGS_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                except Exception as e:
                    debug_log("HF upload " + nome + ": " + str(e)[:100])
        for fpath in [UNKNOWN_FILE_LOCAL, SAIDA_ACEITES, SAIDA_REJEICAOES]:
            if os.path.exists(fpath):
                try:
                    api.upload_file(
                        path_or_fileobj=fpath,
                        path_in_repo="results/" + os.path.basename(fpath),
                        repo_id=HF_LOGS_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
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
                local = hf_hub_download(
                    repo_id=HF_LOGS_REPO,
                    filename="checkpoints/" + nome + ".json",
                    repo_type="dataset",
                    token=HF_TOKEN,
                    force_download=True
                )
                shutil.copy(local, path)
                dizer("  Baixado: " + nome)
            except Exception:
                pass
        map_files = {
            "user_unknowns_acumulado.csv": UNKNOWN_FILE_LOCAL,
            "aceites_finais.csv": SAIDA_ACEITES,
            "rejeicoes_finais.csv": SAIDA_REJEICAOES
        }
        for fname, dest in map_files.items():
            try:
                local = hf_hub_download(
                    repo_id=HF_LOGS_REPO,
                    filename="results/" + fname,
                    repo_type="dataset",
                    token=HF_TOKEN,
                    force_download=True
                )
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
    dizer(simbolo + " " + chave + " -> " + str(detalhe)[:150])
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
            dizer("  Carregadas " + str(len(variations)) + " variacoes de Mail From de " + ficheiro)
        except Exception as e:
            dizer("  AVISO: Erro ao ler " + ficheiro + ": " + str(e)[:120])
    else:
        dizer("  AVISO: Ficheiro de variações não encontrado. Usando apenas 'Alex'.")
    GLOBAL["mail_from_variations"] = variations
    return variations
def obter_display_name(conta, idx_conta):
    variations = GLOBAL.get("mail_from_variations", [])
    if idx_conta == 0 or not variations:
        base = conta.get("base", "")
        if base:
            nome_limpo = base.replace("-", " ").replace("_", " ").title()
            return nome_limpo
        return "Alex"
    idx_var = (idx_conta - 1) % len(variations)
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
    print("║ 🚀 VERISCOPE V19.5.0 | HEADLESS | " + modo + "   ⏱️  {:02d}:{:02d}:{:02d}   💾 {:<8}    ║".format(h, m, s, disco_str), flush=True)
    print("║ STATUS: " + status.ljust(66) + " ║", flush=True)
    print("║ FASE: " + fase_atual.ljust(66) + " ║", flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║ 🌐 IP Colab: {:<15} 📧 Contas: {:<6} 📧 Emails: {:<18,} ║".format(ip_str, len(GLOBAL['contas']), GLOBAL['emails_read']), flush=True)
    print("║ 📦 Parquets: {:<14} 🏢 Provs: {:<14} 🌐 MXs: {:<15} ║".format(pq_str, prov_str, GLOBAL['mx_found']), flush=True)
    print("║ 🔐 DNS: {:<16} 💡 Taxa: {:<7.2f}%  Snap: {:<15} ║".format(dns_str, taxa_total, snap), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║ 🔌 Proxies: {:<11} 💀 Mortos: {:<6} 🚫 Banidos: {:<15} ║".format(pool_str, GLOBAL['proxies_mortos_count'], GLOBAL['proxies_banidos']), flush=True)
    print("║ 📤 Tentativas: {:<12,} ⚡ {:.1f}/s     🔌 Con: {:<15,} ║".format(GLOBAL['smtp_tentativas'], rate, GLOBAL['smtp_conexoes']), flush=True)
    print("║ SMTP: ✅{:<8,} ❌{:<8,} ⚠️{:<8,} 👻{:<8,}              ║".format(
        GLOBAL['smtp_aceites'], GLOBAL['smtp_rejeitados'], GLOBAL['smtp_erros'], GLOBAL['smtp_user_unknown']), flush=True)
    print("║ 📡 Via Proxy: {:<8,} 🖥️  Via Colab: {:<8,} 👻 UK: {:<10} ║".format(
        GLOBAL['smtp_via_proxy'], GLOBAL['smtp_via_colab'], unk_str), flush=True)
    print("║ 🏆 Concluídos: {:<14}  4xx:{:<7,}  5xx:{:<7,}  Retry:{:<7,}  ║".format(
        prov_conc, GLOBAL.get('smtp_4xx', 0), GLOBAL.get('smtp_5xx', 0), GLOBAL.get('smtp_retries', 0)), flush=True)
    print("║ 📥 Aceites registados: {:<10,}  URLs scrapadas: {:<20} ║".format(aceites_set, scrape_str), flush=True)
    print("║ 🔒 DESec: {} req | {} OK | {} fail                               ║".format(desec_req, desec_ok, desec_fail), flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    print("║ 🛑 Para parar remotamente: touch /tmp/veriscope_stop                  ║", flush=True)
    print("╠" + "═" * 76 + "╣", flush=True)
    logs = list(GLOBAL["log_buffer"])[-4:]
    for log in logs:
        l = log[:74] if len(log) <= 74 else log[:72] + ".."
        print("║   " + l.ljust(74) + " ║", flush=True)
    for _ in range(4 - len(logs)):
        print("║   " + " " * 74 + " ║", flush=True)
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
    servicos = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://ipinfo.io/ip",
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
    CHECKPOINT.salvar_config(GLOBAL["exec_id"], ip, status="RUNNING")
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
DOM_TOKEN_LOCK = threading.Lock()
def testar_tokens_desec():
    definir_fase("FASE 1 - Validacao dos tokens deSEC")
    if fase_ja_completa("FASE1_TOKENS"):
        dizer("  Fase 1 ja concluida no checkpoint - a recarregar tokens")
        DESEC_TOKENS_VALIDOS_LOCAL = list(DESEC_TOKENS_ALL)
        dizer("   " + str(len(DESEC_TOKENS_VALIDOS_LOCAL)) + " tokens disponiveis (assumidos validos)")
        return DESEC_TOKENS_VALIDOS_LOCAL
    def _testar_um(tok):
        if GLOBAL.get("emergency_stop"):
            return None
        try:
            r = requests.get(
                "https://desec.io/api/v1/domains/",
                headers={"Authorization": "Token " + tok},
                timeout=15
            )
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
def download_hf_com_retry(repo_id, filename, repo_type, max_retries=4):
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
            debug_log("hf retry " + str(tentativa + 1) + " " + filename + " : " + str(e)[:180])
            time.sleep(espera)
    raise ultima if ultima else RuntimeError("download_hf_com_retry falhou")
def carregar_contas():
    definir_fase("FASE 2A - Contas DKIM + template da campanha")
    if fase_ja_completa("FASE2A_CONTAS"):
        dizer("  Fase 2A ja concluida no checkpoint - a recarregar contas")
    p = download_hf_com_retry(HF_LOGS_REPO, "campaign_data/phase1_accounts_keys.csv", "dataset")
    if not p or not os.path.exists(p):
        marcar_fase("FASE2A_CONTAS", False, "CSV de contas nao encontrado")
        raise RuntimeError("Ficheiro phase1_accounts_keys.csv nao descarregado")
    with open(p, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    dizer("  Linhas no CSV de contas: " + str(len(rows)))
    if not rows:
        marcar_fase("FASE2A_CONTAS", False, "CSV vazio")
        raise RuntimeError("CSV de contas esta vazio")
    contas = []
    for r in rows:
        priv = (r.get("dkim_private_key_pem") or "").strip()
        priv = priv.replace("\\n", "\n").replace("\\r", "")
        if not priv.startswith("-----BEGIN"):
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
    GLOBAL["contas"] = contas
    CHECKPOINT.salvar_contas(len(contas))
    if not contas:
        marcar_fase("FASE2A_CONTAS", False, "0 contas com chave DKIM")
        raise RuntimeError("Nenhuma conta valida carregada")
    marcar_fase("FASE2A_CONTAS", True, str(len(contas)) + " contas")
    dizer("  Contas carregadas: " + str(len(contas)))
    return contas
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
    marcar_fase("FASE2B_MX", len(out) > 0,
                str(len(out)) + "/" + str(len(provedores)) + " com MX")
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
        "Trader_Emails/",
        "Trader_Emails/*/",
        "Trader_Emails/*/*/",
        "Trader_Emails/*.parquet",
        "Trader_Emails/**/*.parquet",
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
                    repo_id=HF_TRADER_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN,
                    allow_patterns=padroes,
                    local_dir=DIR_REPO,
                    max_workers=SNAPSHOT_MAX_WORKERS,
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
                debug_log("snapshot tentativa " + str(tentativa + 1) + ":\n" + traceback.format_exc())
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
    dizer("  Parquets identificados: " + str(GLOBAL['pq_total']) +
          " em " + str(len(args)) + " provedores")
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
    dizer("  Threads de extraccao: " + str(trabalhadores) + " (config=" + str(WORKERS_DOWNLOAD) + ")")
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
        local = download_hf_com_retry(
            HF_LOGS_REPO,
            "blacklists/user_unknowns_acumulado.csv",
            "dataset"
        )
        if local and os.path.exists(local):
            shutil.copy(local, UNKNOWN_FILE_LOCAL)
            baixou = True
            dizer("  Ficheiro de blacklist descarregado do HuggingFace")
    except Exception as e:
        dizer("  Ficheiro ainda nao existe no HuggingFace (normal na 1a execucao)")
        debug_log("blacklist download: " + str(e)[:200])
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
            debug_log("ler blacklist local: " + traceback.format_exc())
            unknowns = set()
    GLOBAL["user_unknowns"] = unknowns
    dizer("  USER_UNKNOWNs activos: " + "{:,}".format(len(unknowns)) +
          (" (descarregado)" if baixou else " (ficheiro novo, sera criado)"))
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
    file_exists = os.path.exists(UNKNOWN_FILE_LOCAL)
    try:
        with UNKNOWN_FILE_LOCK:
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
            repo_id=HF_LOGS_REPO,
            repo_type="dataset",
            token=HF_TOKEN
        )
        dizer("  USER_UNKNOWNs enviados para HuggingFace (" +
              "{:,}".format(len(GLOBAL['user_unknowns'])) + ")")
    except Exception as e:
        dizer("  AVISO: upload de blacklist falhou (" + str(e)[:100] + ")")
        debug_log("upload unknowns: " + traceback.format_exc())
def gerar_ficheiros_finais():
    definir_fase("GERACAO DE FICHEIROS FINAIS")
    dizer("A gerar ficheiro de aceites 250 OK...")
    try:
        aceites_set = set()
        with GLOBAL["aceites_lock"]:
            aceites_set = set(GLOBAL.get("aceites_emails", set()))
        if os.path.exists(SAIDA_ACEITES):
            dizer("  Ficheiro " + SAIDA_ACEITES + " ja existe com " +
                  "{:,}".format(len(aceites_set)) + " aceites")
        else:
            with open(SAIDA_ACEITES, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["email", "provedor", "conta_id", "mx_host", "timestamp"])
                ts = datetime.now().isoformat()
                for email in aceites_set:
                    writer.writerow([email, "", "", "", ts])
            dizer("  Ficheiro " + SAIDA_ACEITES + " criado com " +
                  "{:,}".format(len(aceites_set)) + " aceites")
    except Exception as e:
        dizer("  ERRO ao gerar ficheiro de aceites: " + str(e))
        debug_log("gerar_ficheiros_finais aceites: " + traceback.format_exc())
    dizer("A gerar ficheiro final de user_unknowns...")
    try:
        if os.path.exists(UNKNOWN_FILE_LOCAL):
            shutil.copy(UNKNOWN_FILE_LOCAL, SAIDA_UNKNOWN)
            with GLOBAL["user_unknowns_lock"]:
                n_unk = len(GLOBAL["user_unknowns"])
            dizer("  Ficheiro " + SAIDA_UNKNOWN + " criado com " +
                  "{:,}".format(n_unk) + " user_unknowns")
        else:
            dizer("  (sem ficheiro de user_unknowns para copiar)")
    except Exception as e:
        dizer("  ERRO ao copiar user_unknowns: " + str(e))
        debug_log("gerar_ficheiros_finais unknowns: " + traceback.format_exc())
    dizer("A enviar ficheiros para HuggingFace...")
    try:
        api = HfApi(token=HF_TOKEN)
        ts_upload = datetime.now().strftime('%Y%m%d_%H%M%S')
        if os.path.exists(SAIDA_ACEITES):
            try:
                api.upload_file(
                    path_or_fileobj=SAIDA_ACEITES,
                    path_in_repo="results/aceites_250ok_" + ts_upload + ".csv",
                    repo_id=HF_LOGS_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                dizer("  Ficheiro de aceites enviado para HuggingFace")
            except Exception as e:
                dizer("  ERRO upload aceites: " + str(e)[:120])
        if os.path.exists(SAIDA_UNKNOWN):
            try:
                api.upload_file(
                    path_or_fileobj=SAIDA_UNKNOWN,
                    path_in_repo="results/user_unknowns_" + ts_upload + ".csv",
                    repo_id=HF_LOGS_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                dizer("  Ficheiro de user_unknowns enviado para HuggingFace")
            except Exception as e:
                dizer("  ERRO upload unknowns: " + str(e)[:120])
        if os.path.exists(SAIDA_REJEICAOES):
            try:
                api.upload_file(
                    path_or_fileobj=SAIDA_REJEICAOES,
                    path_in_repo="results/rejeicoes_5xx_" + ts_upload + ".csv",
                    repo_id=HF_LOGS_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                dizer("  Ficheiro de rejeicoes enviado para HuggingFace")
            except Exception as e:
                dizer("  ERRO upload rejeicoes: " + str(e)[:120])
    except Exception as e:
        dizer("  ERRO geral no upload: " + str(e))
        debug_log("upload_finais: " + traceback.format_exc())
REGEX_PROXY = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})["\s)\]:|\t,;/]+["\s\[](\d{2,5})')
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
        if len(ficheiros) > 25:
            dizer("     - ... e mais " + str(len(ficheiros) - 25) + " ficheiro(s)")
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
                    u,
                    timeout=20,
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
            debug_log("scrape " + u + " -> HTTP " + str(r.status_code))
            return "", proto, u
        except Exception as e:
            ultima_exc = e
            time.sleep(1 + tentativa)
    with GLOBAL["stats_lock"]:
        GLOBAL["urls_failed"] += 1
    debug_log("scrape falhou " + u + ": " + str(ultima_exc)[:160])
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
        total = len(GLOBAL["raw_proxies"])
        dizer("  Total acumulado de candidatos: " + "{:,}".format(total))
        return GLOBAL["raw_proxies"]
    dizer("  A descarregar " + str(len(urls)) + " listas em paralelo...")
    pares = [(u, inferir_protocolo_url(u)) for u in urls]
    trabalhadores = max(8, min(WORKERS_SCRAPER, len(pares), 250))
    dizer("  Threads de scraping: " + str(trabalhadores) + " (config=" + str(WORKERS_SCRAPER) + ")")
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
        debug_log("Proxy " + str(ip) + ": " + str(port) + " com exit_ip invalido: " + str(exit_ip)[:50])
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
def validar_proxies_ate_target(target_count):
    start_idx = GLOBAL["scraping_checkpoint"]
    with GLOBAL["raw_lock"]:
        raw = list(GLOBAL["raw_proxies"])
    total = len(raw)
    if total == 0:
        dizer("  AVISO: fila de validacao vazia - nada para testar")
        return 0
    if start_idx >= total:
        dizer("  Todos os " + "{:,}".format(total) + " candidatos ja foram testados (checkpoint esgotado)")
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
          str(com_pool) + "/" + str(target_count) +
          " | mortos descartados " + str(GLOBAL["proxies_mortos_count"]))
    if com_pool > 0:
        GLOBAL["spf_sujo"] = True
    return validos_novos
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
                smtp_log("PROXY " + str(proxy_key) + " BANIDO para " + str(provedor))
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
                    with DOM_TOKEN_LOCK:
                        DOM_TOKEN[domain] = tok
                    return tok
            except Exception:
                continue
    return None
def desec_put_com_retry(url, headers, payload, domain, max_retries=5):
    payload_preview = None
    try:
        if isinstance(payload, list) and len(payload) > 0:
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
            log_desec_request(
                domain=domain,
                url=url,
                method="PUT",
                status_code=r.status_code,
                response_body=r.text,
                headers=dict(r.headers),
                duration_ms=duracao_ms,
                attempt=tentativa + 1,
                payload_preview=payload_preview
            )
            if r.status_code in (200, 201, 204):
                return True, r.status_code
            if r.status_code == 429:
                try:
                    wait = int(r.headers.get("Retry-After", DESEC_BACKOFF_BASE))
                except Exception:
                    wait = DESEC_BACKOFF_BASE
                wait = int(DESEC_BACKOFF_BASE * (DESEC_BACKOFF_MULTIPLIER ** tentativa))
                dizer("  Rate limit deSEC (tentativa " + str(tentativa + 1) + "/" + str(max_retries) +
                      "). A esperar " + str(wait) + "s...")
                debug_log("Rate limit deSEC: " + r.text[:200])
                time.sleep(wait + 1)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = 5 * (tentativa + 1)
                dizer("  Erro de servidor deSEC (" + str(r.status_code) + "). A esperar " + str(wait) + "s...")
                time.sleep(wait)
                continue
            if r.status_code == 400:
                dizer("  Erro 400 deSEC (formato invalido): " + r.text[:200])
                return False, str(r.status_code) + ": " + r.text[:200]
            dizer("  Erro deSEC: " + str(r.status_code) + " - " + r.text[:150])
            return False, str(r.status_code) + ": " + r.text[:150]
        except Exception as e:
            duracao_ms = (time.time() - inicio_req) * 1000
            log_desec_request(
                domain=domain,
                url=url,
                method="PUT",
                status_code=-1,
                response_body=str(e),
                headers={},
                duration_ms=duracao_ms,
                attempt=tentativa + 1,
                payload_preview=payload_preview
            )
            debug_log("desec_put tentativa " + str(tentativa + 1) + ": " + str(e)[:160])
            if tentativa < max_retries - 1:
                wait = 3 * (tentativa + 1)
                time.sleep(wait)
                continue
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
    full_spf = " ".join(spf_parts)
    return _dividir_em_chunks_com_aspas(full_spf, 255)
def obter_ips_para_spf():
    ips = set()
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
def construir_rrsets_completos(contas_do_dominio, spf_record):
    rrsets = []
    dmarc_record = '"v=DMARC1; p=none; sp=none;"'
    for c in contas_do_dominio:
        base = c["base"]
        domain = c["domain"]
        selector = c["selector"]
        pub = (c.get("public_key_txt") or "").replace('"', "").strip()
        if not pub:
            continue
        if not pub.startswith("v=DKIM1"):
            pub = ("v=DKIM1; k=rsa; p=" + pub) if "p=" in pub else ("v=DKIM1; k=rsa; " + pub)
        rrsets.append({
            "subname": base,
            "type": "TXT",
            "ttl": DESEC_TTL,
            "records": [spf_record]
        })
        dkim_wrapped = _dividir_em_chunks_com_aspas(pub, 255)
        rrsets.append({
            "subname": selector + "._domainkey." + base,
            "type": "TXT",
            "ttl": DESEC_TTL,
            "records": [dkim_wrapped],
        })
        rrsets.append({
            "subname": "_dmarc." + base,
            "type": "TXT",
            "ttl": DESEC_TTL,
            "records": [dmarc_record]
        })
        ip_colab = GLOBAL.get("ip_colab") or "127.0.0.1"
        rrsets.append({
            "subname": "*." + base,
            "type": "A",
            "ttl": DESEC_TTL,
            "records": [ip_colab]
        })
        rrsets.append({
            "subname": base,
            "type": "A",
            "ttl": DESEC_TTL,
            "records": [ip_colab]
        })
        rrsets.append({
            "subname": base,
            "type": "MX",
            "ttl": DESEC_TTL,
            "records": ["10 " + base + "." + domain + "."]
        })
    return rrsets
def atualizar_dominio_completo(domain, contas_dom, spf_record):
    tok = token_para_dominio(domain)
    if not tok:
        dizer("  AVISO: Token nao encontrado para " + domain)
        return domain, 0, 0
    rrsets = construir_rrsets_completos(contas_dom, spf_record)
    total_rrsets = len(rrsets)
    if total_rrsets == 0:
        dizer("  AVISO: Nenhum rrset para " + domain)
        return domain, 0, 0
    ok_count = 0
    url = "https://desec.io/api/v1/domains/" + domain + "/rrsets/"
    headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
    dizer("  Atualizando " + str(total_rrsets) + " rrsets para " + domain + "...")
    for i in range(0, total_rrsets, DESEC_CHUNK):
        if GLOBAL.get("emergency_stop"):
            break
        chunk = rrsets[i:i + DESEC_CHUNK]
        chunk_num = (i // DESEC_CHUNK) + 1
        total_chunks = (total_rrsets + DESEC_CHUNK - 1) // DESEC_CHUNK
        dizer("    Chunk " + str(chunk_num) + "/" + str(total_chunks) + " (" + str(len(chunk)) + " rrsets)...")
        try:
            for rrset in chunk:
                if rrset.get("type") == "TXT":
                    debug_log("DESec TXT Sample for " + domain + " (chunk " + str(chunk_num) + "): " + json.dumps(rrset))
                    break
        except Exception as e:
            debug_log("Erro ao logar payload: " + str(e))
        ok, status = desec_put_com_retry(url, headers, chunk, domain)
        if ok:
            ok_count += len(chunk)
            dizer("    Chunk " + str(chunk_num) + " OK (" + str(status) + ")")
        else:
            dizer("    Chunk " + str(chunk_num) + " FALHOU: " + str(status)[:100])
            debug_log("chunk " + str(chunk_num) + " de " + domain + " falhou: " + str(status))
        time.sleep(1.0)
    return domain, ok_count, total_rrsets
def fase3_dns_completo(contas):
    definir_fase("FASE 3 - DNS completo (SPF bulk + DKIM + DMARC + A + MX)")
    if fase_ja_completa("FASE3_DNS"):
        dizer("  Fase 3 ja concluida no checkpoint - a saltar")
        return True
    if contas is None:
        dizer("  ERRO CRÍTICO: contas é None na FASE 3")
        marcar_fase("FASE3_DNS", False, "contas é None")
        return False
    if not contas or len(contas) == 0:
        dizer("  ERRO CRÍTICO: contas está vazio na FASE 3")
        marcar_fase("FASE3_DNS", False, "contas vazio")
        return False
    ips_spf = obter_ips_para_spf()
    spf_record = montar_spf_record(ips_spf)
    dizer("  SPF com " + str(len(ips_spf)) + " IPs (proxies + Colab)")
    debug_log("SPF final (com aspas literais): " + spf_record[:500])
    por_dom = defaultdict(list)
    for c in contas:
        por_dom[c["domain"]].append(c)
    total_bases = sum(len(v) for v in por_dom.values())
    GLOBAL["dns_total"] = total_bases
    dizer("  Dominios: " + str(len(por_dom)) + " | bases: " + str(total_bases))
    ok_bases = 0
    falhados = 0
    dominios_lista = sorted(por_dom.keys())
    for idx, domain in enumerate(dominios_lista, 1):
        if GLOBAL.get("emergency_stop"):
            break
        contas_dom = por_dom[domain]
        dizer("  [" + str(idx) + "/" + str(len(dominios_lista)) + "] " + domain +
              " (" + str(len(contas_dom)) + " contas)...")
        dom, ok, total = atualizar_dominio_completo(domain, contas_dom, spf_record)
        bases_sucesso = (ok // 6) if ok > 0 else 0
        ok_bases += bases_sucesso
        if ok == 0:
            falhados += 1
            dizer("     FALHOU (" + str(ok) + "/" + str(total) + " rrsets)")
        else:
            dizer("     OK (" + str(ok) + "/" + str(total) + " rrsets, " + str(bases_sucesso) + " bases)")
        GLOBAL["dns_updated"] = ok_bases
        time.sleep(2.0)
    GLOBAL["dns_updated"] = ok_bases
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    dizer("  DNS: " + str(ok_bases) + "/" + str(total_bases) + " bases | dominios falhados: " + str(falhados))
    marcar_fase("FASE3_DNS", ok_bases > 0, str(ok_bases) + "/" + str(total_bases) + " bases")
    return ok_bases > 0
def refresh_spf_bulk(motivo=""):
    ips_spf = obter_ips_para_spf()
    if not ips_spf:
        return
    spf_record = montar_spf_record(ips_spf)
    dizer("  Refresh SPF (" + str(motivo) + ") com " + str(len(ips_spf)) + " IPs")
    por_dom = defaultdict(list)
    for c in GLOBAL["contas"]:
        por_dom[c["domain"]].append(c)
    ok_dom = 0
    for domain, contas_dom in por_dom.items():
        if GLOBAL.get("emergency_stop"):
            break
        tok = token_para_dominio(domain)
        if not tok:
            continue
        rrsets = []
        for c in contas_dom:
            rrsets.append({
                "subname": c["base"],
                "type": "TXT",
                "ttl": DESEC_TTL,
                "records": [spf_record]
            })
        if not rrsets:
            continue
        url = "https://desec.io/api/v1/domains/" + domain + "/rrsets/"
        headers = {"Authorization": "Token " + tok, "Content-Type": "application/json"}
        for i in range(0, len(rrsets), DESEC_CHUNK):
            if GLOBAL.get("emergency_stop"):
                break
            chunk = rrsets[i:i + DESEC_CHUNK]
            ok, status = desec_put_com_retry(url, headers, chunk, domain)
            if ok:
                ok_dom += 1
            else:
                debug_log("refresh_spf " + domain + ": " + str(status))
            time.sleep(0.5)
    GLOBAL["ultimo_refresh_spf"] = time.time()
    GLOBAL["spf_sujo"] = False
    dizer("  SPF actualizado em " + str(ok_dom) + " blocos")
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
                txt = b" ".join(rdata.strings).decode(errors="ignore")
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
        smtp_log("DKIM falhou em " + str(conta.get('identity_id')) + ": " + str(e))
        debug_log("DKIM: " + traceback.format_exc())
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
            "!aNULL:!eNULL:!MD5:!RC4:!DES:!3DES"
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
                                          "rejected for policy", "not allowed",
                                          "reputation"]):
            return "REPUTACAO", "Problema de reputacao/politica"
        if any(kw in msg_low for kw in ["invalid message", "malformed", "syntax"]):
            return "INVALIDA", "Mensagem invalida"
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
        next_try = time.time() + delay
        with self.lock:
            self.items.append((email, provedor, attempts, next_try))
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
def tentar_conexao_com_proxy(proxy_info, mx_host, timeout, provedor):
    proxy_key = str(proxy_info['ip']) + ":" + str(proxy_info['port'])
    if not proxy_pode_usar(proxy_key, provedor):
        return None, None, "banido"
    sock = None
    try:
        smtp_log("    PROXY " + proxy_key + " -> " + mx_host + ":25")
        with GLOBAL_CONNECTION_SEMAPHORE:
            sock = _socket_proxy(
                proxy_info["ip"], proxy_info["port"], proxy_info["proto"],
                mx_host, 25, timeout
            )
            banner = sock.recv(4096).decode(errors="ignore").strip()
            smtp_log("    Banner: " + banner[:90])
            if not banner.startswith("220"):
                try:
                    sock.close()
                except Exception:
                    pass
                registrar_timeout(proxy_key, provedor)
                return None, None, "banner_invalido"
            return sock, banner, "ok"
    except socket.timeout:
        smtp_log("    TIMEOUT em " + mx_host + " via " + proxy_key)
        registrar_timeout(proxy_key, provedor)
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "timeout"
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        smtp_log("    RESET/ERRO " + proxy_key + ": " + str(e)[:110])
        registrar_timeout(proxy_key, provedor)
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "reset"
    except Exception as e:
        smtp_log("    ERRO " + proxy_key + ": " + str(e)[:110])
        registrar_timeout(proxy_key, provedor)
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "erro"
def tentar_conexao_direta(mx_host, timeout, provedor):
    sock = None
    try:
        smtp_log("    IP COLAB -> " + mx_host + ":25")
        with GLOBAL_CONNECTION_SEMAPHORE:
            sock = socket.create_connection((mx_host, 25), timeout=timeout)
            banner = sock.recv(4096).decode(errors="ignore").strip()
            smtp_log("    Banner: " + banner[:90])
            if not banner.startswith("220"):
                try:
                    sock.close()
                except Exception:
                    pass
                return None, None, "banner_invalido"
            return sock, banner, "ok"
    except socket.timeout:
        smtp_log("    TIMEOUT direto em " + mx_host)
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "timeout"
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        smtp_log("    RESET direto " + mx_host + ": " + str(e)[:110])
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "reset"
    except Exception as e:
        smtp_log("    ERRO direto " + mx_host + ": " + str(e)[:110])
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return None, None, "erro"
def obter_socket_para_mx(mx_list, provedor, ip_colab):
    if not mx_list:
        return None, None, None, None
    timeout = get_timeout(provedor)
    with GLOBAL["pool_lock"]:
        proxies_disponiveis = list(GLOBAL["pool"])
        if proxies_disponiveis:
            desloc = random.randint(0, len(proxies_disponiveis) - 1)
            proxies_disponiveis = proxies_disponiveis[desloc:] + proxies_disponiveis[:desloc]
    for proxy in proxies_disponiveis:
        if GLOBAL.get("emergency_stop"):
            break
        proxy_key = str(proxy['ip']) + ":" + str(proxy['port'])
        if not proxy_pode_usar(proxy_key, provedor):
            continue
        smtp_log("  PROXY-FIRST " + proxy_key + " a percorrer " + str(len(mx_list)) + " MX de " + provedor)
        for mx_host in mx_list:
            if GLOBAL.get("emergency_stop"):
                break
            sock, banner, status = tentar_conexao_com_proxy(proxy, mx_host, timeout, provedor)
            if sock is not None:
                return sock, mx_host, True, proxy_key
    if ip_colab:
        smtp_log("  IP COLAB (fallback) a percorrer " + str(len(mx_list)) + " MX de " + provedor)
        for mx_host in mx_list:
            if GLOBAL.get("emergency_stop"):
                break
            sock, banner, status = tentar_conexao_direta(mx_host, timeout, provedor)
            if sock is not None:
                return sock, mx_host, False, "colab"
    smtp_log("  TODOS os proxies e o IP Colab falharam para " + provedor)
    return None, None, None, None
def handshake_smtp(sock, conta, mx_host):
    try:
        ehlo_cmd = "EHLO " + str(conta['identity_id']) + "\r\n"
        sock.sendall(ehlo_cmd.encode())
        smtp_log("  -> EHLO " + str(conta['identity_id']))
        ehlo_resp = recv_smtp(sock)
        ehlo_text = "\n".join(ehlo_resp) if ehlo_resp else ""
        smtp_log("    <- EHLO: " + ehlo_text[:120])
        if not ehlo_resp:
            return sock, False
        if "STARTTLS" in ehlo_text.upper():
            sock.sendall(b"STARTTLS\r\n")
            smtp_log("  -> STARTTLS")
            tls_resp = recv_smtp(sock)
            smtp_log("    <- STARTTLS: " + str(tls_resp)[:120])
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
        return sock, True
    except Exception as e:
        smtp_log("  Handshake falhou: " + str(e)[:140])
        return sock, False
def aguardar_pool_ou_seguir(timeout_seg=180):
    inicio = time.time()
    avisado = False
    while time.time() - inicio < timeout_seg:
        if GLOBAL.get("emergency_stop"):
            return False
        with GLOBAL["pool_lock"]:
            n = len(GLOBAL["pool"])
            if n > 0:
                return True
        if not avisado:
            smtp_log("  Pool vazio - a aguardar reposicao (" + str(timeout_seg) + "s max)")
            avisado = True
        time.sleep(10)
    smtp_log("  Pool continua vazio apos " + str(timeout_seg) + "s - seguir com IP Colab")
    return False
def enviar_para_provedor(task):
    prov, mx_list, contas_pool, html_base = task
    path = os.path.join(DIR_EMAILS, prov + ".txt")
    res = {
        "provedor": prov,
        "aceites": 0, "tentados": 0, "rejeitados": 0,
        "erros": 0, "user_unknown": 0,
        "via_proxy": 0, "via_colab": 0,
        "smtp_4xx": 0, "smtp_5xx": 0, "smtp_retries": 0,
    }
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        GLOBAL["por_provedor"][prov] = res
        return res
    if not contas_pool:
        GLOBAL["por_provedor"][prov] = res
        return res
    total_linhas = 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                total_linhas += 1
    except Exception as e:
        debug_log("Contar linhas " + prov + ": " + str(e))
        total_linhas = 0
    cp_info = CHECKPOINT.obter_provedor_index(prov)
    offset_inicial = 0
    aceites_iniciais = 0
    if cp_info:
        offset_inicial = cp_info.get("offset", 0)
        aceites_iniciais = cp_info.get("aceites", 0)
        res["aceites"] = aceites_iniciais
    if cp_info.get("concluido", False) or (total_linhas > 0 and offset_inicial >= total_linhas):
        dizer("   " + prov + ": Ja totalmente tentado (" + str(offset_inicial) + "/" + str(total_linhas) + "). Saltando.")
        res["concluido"] = True
        GLOBAL["por_provedor"][prov] = res
        return res
    dizer("  CHECKPOINT " + prov + ": offset=" + str(offset_inicial) + "/" + str(total_linhas) + " aceites=" + str(aceites_iniciais))
    rcpt_limit = RCPT_LIMITS.get(prov, RCPT_DEFAULT)
    conta_idx = random.randint(0, max(0, len(contas_pool) - 1))
    smtp_log("\n" + "=" * 70 + "\nINICIO ENVIO: " + prov + " | MXs: " + str(mx_list) + "\n" + "=" * 70)
    try:
        fh = open(path, "r", encoding="utf-8", errors="ignore")
    except Exception as e:
        debug_log("abrir " + path + ": " + str(e))
        GLOBAL["por_provedor"][prov] = res
        return res
    emails_saltados = 0
    try:
        while emails_saltados < offset_inicial:
            linha = fh.readline()
            if not linha:
                break
            emails_saltados += 1
        if emails_saltados > 0:
            dizer("   " + prov + ": saltados " + str(emails_saltados) + " emails do checkpoint")
    except Exception as e:
        debug_log("saltar offset " + prov + ": " + str(e))
    try:
        while res["aceites"] < META_ACEITES_POR_PROVEDOR and not GLOBAL.get("emergency_stop"):
            if check_stop_file():
                break
            janela_bruta = [l.strip() for l in islice(fh, EMAILS_JANELA_ENVIO)]
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
            retries_prontos = RETRY_QUEUE.obter_prontos(max_items=200)
            for retry_email, retry_prov, retry_attempts in retries_prontos:
                if retry_prov == prov and retry_email not in janela:
                    janela.append(retry_email)
            if not janela:
                break
            random.shuffle(janela)
            fila = deque(janela)
            while fila and res["aceites"] < META_ACEITES_POR_PROVEDOR and not GLOBAL.get("emergency_stop"):
                if check_stop_file():
                    break
                with GLOBAL["pool_lock"]:
                    pool_atual = len(GLOBAL["pool"])
                if pool_atual == 0:
                    aguardar_pool_ou_seguir(timeout_seg=min(180, PROXY_COOLDOWN_SEGUNDOS))
                sock, mx_host, via_proxy, proxy_key = obter_socket_para_mx(
                    mx_list, prov, GLOBAL.get("ip_colab")
                )
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
                pode, espera = RATE_CTRL.pode_enviar(conta["identity_id"], prov)
                if not pode and espera > 0:
                    smtp_log("  Rate Controller: a aguardar " + "{:.2f}".format(espera) + "s para " + conta["identity_id"])
                    time.sleep(min(espera, 5.0))
                sock, hs_ok = handshake_smtp(sock, conta, mx_host)
                if not hs_ok:
                    res["erros"] += 1
                    with GLOBAL["stats_lock"]:
                        GLOBAL["smtp_erros"] += 1
                    if via_proxy and proxy_key:
                        registrar_timeout(proxy_key, prov)
                    try:
                        sock.close()
                    except Exception:
                        pass
                    continue
                enviados_conta = 0
                while (enviados_conta < MAX_EMAILS_POR_CONTA and fila and
                       res["aceites"] < META_ACEITES_POR_PROVEDOR and
                       not GLOBAL.get("emergency_stop")):
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
                        sock.sendall(("MAIL FROM: <" + conta['sender_email'] + ">\r\n").encode())
                        smtp_log("  -> MAIL FROM: <" + conta['sender_email'] + ">")
                        mr = recv_smtp(sock)
                        smtp_log("   <- MAIL FROM: " + str(mr)[:140])
                        if not mr or not mr[0].startswith("250"):
                            smtp_log("  MAIL FROM rejeitado")
                            try:
                                sock.close()
                            except Exception:
                                pass
                            res["erros"] += len(batch)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_erros"] += len(batch)
                            fila.extendleft(reversed(batch))
                            if via_proxy and proxy_key:
                                registrar_timeout(proxy_key, prov)
                            break
                    except Exception as e:
                        smtp_log("  MAIL FROM erro: " + str(e)[:140])
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
                    for em in batch:
                        if GLOBAL.get("emergency_stop"):
                            break
                        inicio_req = time.time()
                        try:
                            sock.sendall(("RCPT TO: <" + em + ">\r\n").encode())
                            smtp_log("  -> RCPT TO: <" + em + ">")
                            rr = recv_smtp(sock)
                            response_time = time.time() - inicio_req
                            smtp_log("   <- RCPT: " + str(rr)[:140] + " (" + "{:.2f}".format(response_time) + "s)")
                            if rr:
                                primeira = rr[0]
                                smtp_code = 0
                                try:
                                    smtp_code = int(primeira[:3])
                                except Exception:
                                    pass
                                smtp_message = " ".join(rr)
                                registar_resposta_smtp(
                                    message_id=None,
                                    smtp_code=smtp_code,
                                    smtp_message=smtp_message,
                                    provider=prov,
                                    recipient_domain=em.split("@")[-1] if "@" in em else "",
                                    sending_account=conta.get("sender_email", ""),
                                    sending_ip=GLOBAL.get("ip_colab", ""),
                                    domain=conta.get("domain", ""),
                                    subdomain=conta.get("base", ""),
                                    attempt_number=1,
                                    response_time=response_time,
                                )
                                if rr[0].startswith("250") or rr[0].startswith("251"):
                                    aceitos.append(em)
                                    RATE_CTRL.registar_sucesso(conta["identity_id"], prov)
                                    if via_proxy and proxy_key:
                                        registrar_sucesso(proxy_key, prov)
                                else:
                                    classe, motivo = classificar_resposta_smtp(smtp_code, smtp_message, prov)
                                    if classe == "USER_UNKNOWN":
                                        res["user_unknown"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_user_unknown"] += 1
                                        salvar_unknown(em, prov)
                                    elif classe == "TEMPORARIA" or classe == "THROTTLE":
                                        res["smtp_4xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_4xx"] += 1
                                        RATE_CTRL.registar_erro_temporario(conta["identity_id"], prov, smtp_code)
                                        RETRY_QUEUE.adicionar(em, prov, attempts=1)
                                    elif classe == "PERMANENTE" or classe == "REPUTACAO":
                                        res["smtp_5xx"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_5xx"] += 1
                                        RATE_CTRL.registar_erro_permanente(conta["identity_id"], prov, smtp_code)
                                        registrar_rejeicao(em, prov, smtp_code, smtp_message)
                                        res["rejeitados"] += 1
                                        with GLOBAL["stats_lock"]:
                                            GLOBAL["smtp_rejeitados"] += 1
                                    if classe == "THROTTLE":
                                        smtp_log("  THROTTLE DETECTADO - a pausar " + prov)
                                        RATE_CTRL.registar_throttle(conta["identity_id"], prov, 60)
                                        break
                            time.sleep(DELAY_ENTRE_RCPT)
                        except Exception as e:
                            smtp_log("  RCPT erro: " + str(e)[:140])
                            res["erros"] += 1
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_erros"] += 1
                            rcpt_erro_fatal = True
                            break
                    if rcpt_erro_fatal:
                        nao_processados = [b for b in batch if b not in set(aceitos)]
                        if nao_processados:
                            fila.extendleft(reversed(nao_processados))
                        smtp_log("  A devolver " + str(len(nao_processados)) + " emails para a fila")
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
                        smtp_log("  -> DATA")
                        dr = recv_smtp(sock)
                        smtp_log("   <- DATA: " + str(dr)[:140])
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
                        smtp_log("  DATA erro: " + str(e)[:140])
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
                        smtp_log("  -> PAYLOAD (" + str(len(aceitos)) + " RCPTs, " +
                                 str(len(payload)) + " bytes)")
                        fr = recv_smtp(sock, timeout=30)
                        smtp_log("   <- FINAL: " + str(fr)[:160])
                        if fr and fr[0].startswith("250"):
                            res["aceites"] += len(aceitos)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_aceites"] += len(aceitos)
                            if via_proxy:
                                GLOBAL["smtp_via_proxy"] += len(aceitos)
                            else:
                                GLOBAL["smtp_via_colab"] += len(aceitos)
                            for em_aceite in aceitos:
                                registrar_aceite(em_aceite, prov, conta.get("identity_id", ""), mx_host)
                            if via_proxy and proxy_key:
                                registrar_sucesso(proxy_key, prov)
                            talvez_salvar_checkpoint()
                        else:
                            res["rejeitados"] += len(aceitos)
                            with GLOBAL["stats_lock"]:
                                GLOBAL["smtp_rejeitados"] += len(aceitos)
                    except Exception as e:
                        smtp_log("  PAYLOAD erro: " + str(e)[:140])
                        res["erros"] += len(aceitos)
                        with GLOBAL["stats_lock"]:
                            GLOBAL["smtp_erros"] += len(aceitos)
                        try:
                            sock.sendall(b"RSET\r\n")
                            recv_smtp(sock)
                        except Exception:
                            try:
                                sock.close()
                            except Exception:
                                pass
                        fila.extendleft(reversed(aceitos))
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
            try:
                pos_atual = fh.tell()
                concluido = (pos_atual >= os.path.getsize(path)) if os.path.exists(path) else False
                CHECKPOINT.atualizar_provedor_index(
                    prov, pos_atual, res["aceites"],
                    concluido=concluido,
                    total_linhas=total_linhas
                )
            except Exception as e:
                debug_log("atualizar_provedor_index " + prov + ": " + str(e))
            if res["aceites"] >= META_ACEITES_POR_PROVEDOR:
                with GLOBAL["stats_lock"]:
                    GLOBAL["provedores_concluidos_set"].add(prov)
                    GLOBAL["provs_concluidos"] = len(GLOBAL["provedores_concluidos_set"])
                break
    except Exception as e:
        debug_log("enviar_para_provedor " + prov + ": " + traceback.format_exc())
    finally:
        try:
            fh.close()
        except Exception:
            pass
    smtp_log("FIM " + prov + ": " + str(res['aceites']) + " aceites / " + str(res['tentados']) + " tentados")
    GLOBAL["por_provedor"][prov] = res
    CHECKPOINT.salvar_por_provedor(GLOBAL["por_provedor"])
    return res
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
        dizer("  Pool " + str(n) + "/" + str(minimo) + " - a aguardar... (" +
              "{:.0f}".format(time.time() - inicio) + "s)")
        time.sleep(20)
    with GLOBAL["pool_lock"]:
        n = len(GLOBAL["pool"])
    dizer("  AVISO: timeout a aguardar pool (tem " + str(n) + "). O envio usa IP Colab como fallback.")
    return n > 0
def fase6_envio_massivo(mxs, contas, html_base):
    definir_fase("FASE 6 - Envio massivo hibrido (PROXY-FIRST)")
    faltam = fases_faltantes_para_envio()
    if faltam:
        dizer("  FASE 6 BLOQUEADA - fases obrigatorias incompletas: " + ", ".join(faltam))
        marcar_fase("FASE6_ENVIO", False, "bloqueada por: " + ", ".join(faltam))
        return
    if not contas:
        dizer("  FASE 6 BLOQUEADA - sem contas DKIM")
        marcar_fase("FASE6_ENVIO", False, "sem contas")
        return
    tasks = []
    for prov, mx_list in mxs.items():
        if not mx_list:
            continue
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            cp_info = CHECKPOINT.obter_provedor_index(prov)
            if cp_info and cp_info.get("concluido", False):
                dizer("   " + prov + " já totalmente processado em run anterior. Saltando.")
                continue
            tasks.append((prov, mx_list, contas, html_base))
    GLOBAL["prov_total_envio"] = len(tasks)
    dizer("  Provedores prontos para envio: " + str(len(tasks)))
    if not tasks:
        marcar_fase("FASE6_ENVIO", False, "0 provedores com emails pendentes")
        return
    with GLOBAL["pool_lock"]:
        n_pool = len(GLOBAL["pool"])
    trabalhadores = max(1, min(WORKERS_ENVIO, len(tasks), 400))
    dizer("  Threads de envio: " + str(trabalhadores) + " (config=" + str(WORKERS_ENVIO) +
          ") | pool=" + str(n_pool))
    marcar_fase("FASE6_ENVIO_INICIO", True, str(len(tasks)) + " provedores")
    with concurrent.futures.ThreadPoolExecutor(max_workers=trabalhadores) as ex:
        futs = [ex.submit(enviar_para_provedor, t) for t in tasks]
        while not all(f.done() for f in futs) and not GLOBAL.get("emergency_stop"):
            time.sleep(3)
        for f in concurrent.futures.as_completed(futs):
            try:
                f.result()
            except Exception as e:
                dizer("  Task de envio com erro: " + str(e)[:140])
                debug_log("fase6 task: " + traceback.format_exc())
    salvar_checkpoint_smtp()
    all_done = True
    for prov, mx_list in mxs.items():
        path = os.path.join(DIR_EMAILS, prov + ".txt")
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            continue
        cp_info = CHECKPOINT.obter_provedor_index(prov)
        if not cp_info or not cp_info.get("concluido", False):
            all_done = False
            break
    if all_done and len(mxs) > 0:
        dizer("[STATUS] TODOS OS PROVEDORES FORAM TOTALMENTE PROCESSADOS. CAMPANHA CONCLUÍDA.")
        GLOBAL["status"] = "COMPLETED"
        CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status="COMPLETED")
        upload_all_checkpoints_to_hf()
    marcar_fase("FASE6_ENVIO", True,
                str(GLOBAL['smtp_aceites']) + " aceites / " + str(GLOBAL['smtp_tentativas']) + " tentativas")
def monitor_proxies_continuo():
    dizer("[MONITOR] Thread de monitoramento de proxies iniciada (aguarda scraping inicial)")
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        if GLOBAL.get("scraping_inicial_feito"):
            break
        time.sleep(10)
    dizer("[MONITOR] Scraping inicial confirmado - monitor activo")
    while GLOBAL.get("monitor_running") and not GLOBAL.get("emergency_stop"):
        time.sleep(60)
        with GLOBAL["pool_lock"]:
            pool_atual = len(GLOBAL["pool"])
        with GLOBAL["raw_lock"]:
            raw_total = len(GLOBAL["raw_proxies"])
            checkpoint = GLOBAL["scraping_checkpoint"]
        smtp_log("[MONITOR] pool=" + str(pool_atual) + " raw=" + str(raw_total) +
                 " checkpoint=" + str(checkpoint))
        if pool_atual >= MIN_PROXIES_REFILL:
            if GLOBAL.get("spf_sujo") or (time.time() - GLOBAL.get("ultimo_refresh_spf", 0) > 900):
                try:
                    refresh_spf_bulk("agendado")
                except Exception as e:
                    debug_log("refresh SPF agendado: " + str(e))
            continue
        dizer("[MONITOR] Pool baixo (" + str(pool_atual) + "/" + str(TARGET_VALID_PROXIES) +
              ") - cooldown " + str(PROXY_COOLDOWN_SEGUNDOS) + "s")
        for _i in range(PROXY_COOLDOWN_SEGUNDOS // 10):
            if GLOBAL.get("emergency_stop") or not GLOBAL.get("monitor_running"):
                return
            time.sleep(10)
        if GLOBAL.get("emergency_stop"):
            return
        if not GLOBAL["scrape_lock"].acquire(blocking=False):
            dizer("[MONITOR] Outro refill em curso - a saltar este ciclo")
            continue
        try:
            with GLOBAL["pool_lock"]:
                pool_atual = len(GLOBAL["pool"])
                if pool_atual >= MIN_PROXIES_REFILL:
                    continue
            checkpoint = GLOBAL["scraping_checkpoint"]
            com_raw = len(GLOBAL["raw_proxies"])
            agora = time.time()
            if checkpoint >= com_raw and (agora - GLOBAL.get("ultimo_rescrape", 0)) >= RESCRAPE_INTERVALO_MINIMO:
                dizer("[MONITOR] Candidatos esgotados - a fazer novo scraping das listas")
                GLOBAL["ultimo_rescrape"] = agora
                try:
                    carregar_e_scrapear_proxies(motivo="refill-monitor")
                except Exception as e:
                    debug_log("rescrape monitor: " + traceback.format_exc())
                    dizer("[MONITOR] Re-scrape falhou: " + str(e)[:120])
            dizer("[MONITOR] Refill (checkpoint=" + str(GLOBAL["scraping_checkpoint"]) + ")")
            try:
                novos = validar_proxies_ate_target(TARGET_VALID_PROXIES)
                if novos > 0:
                    GLOBAL["spf_sujo"] = True
                    refresh_spf_bulk("refill +" + str(novos))
            except Exception as e:
                debug_log("refill monitor: " + traceback.format_exc())
                dizer("[MONITOR] Refill falhou: " + str(e)[:120])
        except Exception as e:
            debug_log("monitor_proxies_continuo: " + str(e))
        finally:
            try:
                GLOBAL["scrape_lock"].release()
            except Exception:
                pass
def enviar_email_unico(dest, assunto, html, conta):
    sock = None
    try:
        payload = aplicar_mutacao_abracadabra(html, conta, dest, subject=assunto)
        dominio = dest.split("@")[-1]
        try:
            answers = dns.resolver.resolve(dominio, 'MX')
            mxs_sorted = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
        except Exception as e:
            smtp_log("FINAL: sem MX para " + dominio + " (" + str(e)[:100] + ")")
            return False
        for _pref, mxh in mxs_sorted:
            sock = None
            try:
                sock = socket.create_connection((mxh, 25), timeout=30)
                sock.settimeout(30)
                banner = sock.recv(4096).decode(errors="ignore").strip()
                smtp_log("FINAL: " + mxh + " -> " + banner[:60])
                if not banner.startswith("220"):
                    try:
                        sock.close()
                    except Exception:
                        pass
                    sock = None
                    continue
                sock.sendall(("EHLO " + str(conta['identity_id']) + "\r\n").encode())
                recv_smtp(sock)
                sock.sendall(("MAIL FROM: <" + conta['sender_email'] + ">\r\n").encode())
                mr = recv_smtp(sock)
                if not mr or not mr[0].startswith("250"):
                    try:
                        sock.close()
                    except Exception:
                        pass
                    sock = None
                    continue
                sock.sendall(("RCPT TO: <" + dest + ">\r\n").encode())
                rr = recv_smtp(sock)
                if not rr or not rr[0].startswith("250"):
                    try:
                        sock.close()
                    except Exception:
                        pass
                    sock = None
                    continue
                sock.sendall(b"DATA\r\n")
                dr = recv_smtp(sock)
                if not dr or not dr[0].startswith("354"):
                    try:
                        sock.close()
                    except Exception:
                        pass
                    sock = None
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
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
                sock = None
                if fr and fr[0].startswith("250"):
                    return True
            except Exception as e:
                smtp_log("FINAL erro em " + mxh + ": " + str(e)[:140])
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                sock = None
                continue
        return False
    except Exception as e:
        debug_log("enviar_email_unico: " + traceback.format_exc())
        smtp_log("enviar_email_unico fatal: " + str(e)[:140])
        return False
def gerar_relatorio(rt):
    assunto = "VERISCOPE V19.5.0 - " + datetime.now().strftime('%Y-%m-%d %H:%M')
    linhas_fases = ""
    try:
        with GLOBAL["stats_lock"]:
            fases = dict(GLOBAL.get("fases", {}))
            for chave in sorted(fases.keys()):
                info = fases[chave]
                marca = "OK" if info.get("ok") else "FALHOU"
                linhas_fases += ("<tr><td>" + str(chave) + "</td><td>" + marca + "</td><td>" +
                                 str(info.get("ts", "")) + "</td><td>" +
                                 str(info.get("detalhe", "")) + "</td></tr>")
    except Exception:
        linhas_fases = "<tr><td colspan='4'>sem dados de fases</td></tr>"
    linhas_prov = ""
    try:
        for p, r in sorted(rt['por_provedor'].items()):
            taxa = r['aceites'] / max(1, r['tentados']) * 100
            linhas_prov += ("<tr><td>" + str(p) + "</td>"
                            "<td class='ok'>" + "{:,}".format(r['aceites']) + "</td>"
                            "<td>" + "{:,}".format(r['tentados']) + "</td>"
                            "<td>" + "{:,}".format(r['rejeitados']) + "</td>"
                            "<td>" + "{:,}".format(r.get('user_unknown', 0)) + "</td>"
                            "<td>" + "{:,}".format(r['erros']) + "</td>"
                            "<td>" + "{:.1f}".format(taxa) + "%</td></tr>")
    except Exception:
        linhas_prov = "<tr><td colspan='7'>sem dados</td></tr>"
    html = """<html><head><meta charset="utf-8"><style>
body{font-family:Arial,Helvetica,sans-serif;margin:20px;color:#222}
h1{color:#2c5f2d}
h2{color:#3a7d44;margin-top:28px}
table{border-collapse:collapse;width:100%;margin-bottom:16px}
th,td{border:1px solid #ddd;padding:7px;font-size:13px}
th{background:#3a7d44;color:#fff;text-align:left}
tr:nth-child(even){background:#f6f9f6}
.ok{color:#2c5f2d;font-weight:bold}
.no{color:#c0392b;font-weight:bold}
ul{line-height:1.7}
</style></head><body>
<h1>VERISCOPE V19.5.0 - HEADLESS COM AUTO-RESTART</h1>
<h2>Resumo</h2>
<ul>
<li>IP Colab: <b>""" + str(GLOBAL.get('ip_colab')) + """</b></li>
<li>Snapshot HuggingFace: <b>""" + ("SIM" if GLOBAL.get('snapshot_ok') else "nao") + """</b></li>
<li>Parquets processados: <b>""" + str(GLOBAL.get('pq_downloaded')) + "/" + str(GLOBAL.get('pq_total')) + """</b> (falhas: """ + str(GLOBAL.get('pq_falhados')) + """)</li>
<li>Emails na base local: <b>""" + "{:,}".format(GLOBAL.get('emails_read', 0)) + """</b></li>
<li>Provedores com MX: <b>""" + str(GLOBAL.get('mx_found')) + """</b> registos MX</li>
<li>Ficheiros de proxy lidos: <b>""" + str(GLOBAL.get('ficheiros_proxy')) + """</b></li>
<li>URLs de proxy scrapadas: <b>""" + str(GLOBAL.get('urls_scraped')) + "/" + str(GLOBAL.get('urls_total')) + """</b></li>
<li>Candidatos extraidos: <b>""" + "{:,}".format(GLOBAL.get('proxies_extracted', 0)) + """</b></li>
<li>Proxies validos no pool: <b>""" + str(len(GLOBAL.get('pool', []))) + "/" + str(TARGET_VALID_PROXIES) + """</b></li>
<li>Proxies descartados (mortos): <b>""" + str(GLOBAL.get('proxies_mortos_count')) + """</b></li>
<li>Blocos proxy/provedor banidos: <b>""" + str(GLOBAL.get('proxies_banidos')) + """</b></li>
<li>Bases DNS actualizadas: <b>""" + str(GLOBAL.get('dns_updated')) + "/" + str(GLOBAL.get('dns_total')) + """</b></li>
<li>Variacoes Mail From: <b>""" + str(len(GLOBAL.get('mail_from_variations', []))) + """</b></li>
<li><b>MODO RECUPERACAO: """ + ("SIM" if GLOBAL.get("modo_recuperacao") else "NAO") + """</b></li>
<li><b>DESec Requests: """ + str(GLOBAL.get('desec_requests', 0)) + """ | OK: """ + str(GLOBAL.get('desec_success', 0)) + """ | Fail: """ + str(GLOBAL.get('desec_failures', 0)) + """</b></li>
</ul>
<h2>Envio</h2>
<ul>
<li>Aceites (250 OK): <span class="ok">""" + "{:,}".format(rt['aceites']) + """</span></li>
<li>Tentados: """ + "{:,}".format(rt['tentados']) + """</li>
<li>Rejeitados: <span class="no">""" + "{:,}".format(rt['rejeitados']) + """</span></li>
<li>Erros: <span class="no">""" + "{:,}".format(rt['erros']) + """</span></li>
<li>Respostas 4xx (temporarias): """ + "{:,}".format(GLOBAL.get('smtp_4xx', 0)) + """</li>
<li>Respostas 5xx (permanentes): """ + "{:,}".format(GLOBAL.get('smtp_5xx', 0)) + """</li>
<li>Retries aplicados: """ + "{:,}".format(GLOBAL.get('smtp_retries', 0)) + """</li>
<li>Mensagens abandonadas (max retries): """ + "{:,}".format(GLOBAL.get('smtp_abandonados', 0)) + """</li>
<li>USER_UNKNOWN detectados: """ + "{:,}".format(GLOBAL.get('smtp_user_unknown', 0)) + """</li>
<li>Blacklist acumulada: """ + "{:,}".format(len(GLOBAL.get('user_unknowns', set()))) + """</li>
<li>Aceites registados (ficheiro): """ + "{:,}".format(len(GLOBAL.get('aceites_emails', set()))) + """</li>
<li>Via Proxy: """ + "{:,}".format(rt.get('via_proxy', 0)) + """</li>
<li>Via Colab: """ + "{:,}".format(rt.get('via_colab', 0)) + """</li>
<li>Conexoes SMTP: """ + "{:,}".format(GLOBAL.get('smtp_conexoes', 0)) + """</li>
<li>Taxa de sucesso: <span class="ok">""" + "{:.2f}".format(rt['aceites'] / max(1, rt['tentados']) * 100) + """%</span></li>
<li>Provedores concluidos (meta atingida): """ + str(len(GLOBAL.get('provedores_concluidos_set', set()))) + """</li>
</ul>
<h2>Fases</h2>
<table>
<tr><th>Fase</th><th>Estado</th><th>Hora</th><th>Detalhe</th></tr>
""" + linhas_fases + """
</table>
<h2>Por provedor</h2>
<table>
<tr><th>Provedor</th><th>Aceites</th><th>Tentados</th><th>Rejeitados</th><th>UK</th><th>Erros</th><th>Taxa</th></tr>
""" + linhas_prov + """
</table>
<h2>Checkpoints salvos</h2>
<ul>
<li>""" + CP_FASES + """</li>
<li>""" + CP_PROVEDORES + """</li>
<li>""" + CP_POOL + """</li>
<li>""" + CP_SMTP + """</li>
<li>""" + CP_CONTAS + """</li>
<li>""" + CP_CONFIG + """</li>
<li><b>""" + DESEC_LOG + """ (logs completos deSEC)</b></li>
</ul>
</body></html>"""
    return assunto, html
def salvar_logs_hf():
    if GLOBAL.get("logs_uploaded"):
        dizer("  Logs ja foram enviados")
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
                    repo_id=HF_LOGS_REPO,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                enviados += 1
            except Exception as e:
                debug_log("upload log " + fname + ": " + str(e)[:160])
        dizer("  Logs enviados para HuggingFace: " + str(enviados) + "/4")
        GLOBAL["logs_uploaded"] = True
    except Exception as e:
        dizer("  AVISO: upload de logs falhou (" + str(e)[:100] + ")")
def baixar_ficheiros_campanha():
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
        dizer("  proxies.txt descarregado para " + DIR_PROXIES)
        local_mailfrom = hf_hub_download(
            repo_id=HF_LOGS_REPO,
            filename="campaign_data/templates/mail_from_alex_liquidity_alert_2115.txt",
            repo_type="dataset",
            token=HF_TOKEN,
            force_download=True
        )
        shutil.copy(local_mailfrom, os.path.join(BASE_LOCAL, "mail_from_alex_liquidity_alert_2115.txt"))
        dizer("  mail_from descarregado para " + BASE_LOCAL)
    except Exception as e:
        dizer("  AVISO: Falha ao baixar ficheiros de campanha: " + str(e)[:100])
def main():
    global DESEC_TOKENS_VALIDOS, LOG_FILE, SMTP_LOG, DEBUG_LOG
    diagnostico_ambiente_hf()
    execution_id = reset_global_state()
    download_all_checkpoints_from_hf()
    CHECKPOINT.carregar_tudo()
    config = CHECKPOINT.obter_config()
    if config.get("status") == "COMPLETED":
        print("=" * 70, flush=True)
        print("[FIM] A campanha já foi totalmente concluída em execução anterior.", flush=True)
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
        GLOBAL["status"] = config.get("status", "RUNNING")
        if config.get("exec_id"):
            execution_id = config["exec_id"]
            GLOBAL["exec_id"] = execution_id
        if config.get("ip_colab"):
            GLOBAL["ip_colab"] = config["ip_colab"]
        if config.get("start_time"):
            try:
                dt = datetime.fromisoformat(config["start_time"])
                GLOBAL["start_time"] = dt.timestamp()
            except Exception:
                pass
        stats_antigos = CHECKPOINT.obter_smtp_stats()
        for k, v in stats_antigos.items():
            if k in GLOBAL:
                GLOBAL[k] = v
        GLOBAL["smtp_aceites_ultima_checkpoint"] = GLOBAL["smtp_aceites"]
        pool_antigo, scraping_cp, raw_count = CHECKPOINT.carregar_pool()
        GLOBAL["pool"] = pool_antigo
        GLOBAL["scraping_checkpoint"] = scraping_cp
        GLOBAL["por_provedor"] = CHECKPOINT.obter_por_provedor()
        with GLOBAL["stats_lock"]:
            for chave, info in CHECKPOINT.states["fases"].items():
                GLOBAL["fases"][chave] = info
        limpar_logs_antigos(preservar_checkpoints=True)
    else:
        limpar_logs_antigos(preservar_checkpoints=False)
    LOG_FILE = os.path.join(LOG_DIR, "log" + execution_id + ".txt")
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
    CHECKPOINT.salvar_config(execution_id, GLOBAL.get("ip_colab"), status="RUNNING")
    threading.Thread(target=monitor_live, daemon=True).start()
    threading.Thread(target=keep_alive_thread, daemon=True).start()
    threading.Thread(target=time_limit_watcher, daemon=True).start()
    threading.Thread(target=periodic_checkpoint_thread, daemon=True).start()
    time.sleep(1)
    dizer("VERISCOPE V19.5.0 HEADLESS [" + execution_id + "]")
    dizer("Workers configurados: download=" + str(WORKERS_DOWNLOAD) +
          " mx=" + str(WORKERS_MX) + " envio=" + str(WORKERS_ENVIO) +
          " scraper=" + str(WORKERS_SCRAPER) + " validacao=" + str(WORKERS_PROXY_PIPELINE))
    dizer("CHECKPOINT: " + ("RECUPERAR" if tem_checkpoint else "NOVO"))
    if tem_checkpoint:
        dizer("  Aceites recuperados: " + "{:,}".format(GLOBAL["smtp_aceites"]))
        dizer("  Tentativas recuperadas: " + "{:,}".format(GLOBAL["smtp_tentativas"]))
        dizer("  Proxies no pool: " + str(len(GLOBAL["pool"])))
    baixar_ficheiros_campanha()
    contas = []
    html_base = ""
    mxs = {}
    rt = None
    try:
        ip_colab = obter_ip_colab()
        verificar_blacklists(ip_colab)
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        DESEC_TOKENS_VALIDOS = testar_tokens_desec()
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        contas = carregar_contas()
        if contas is None or len(contas) == 0:
            raise RuntimeError("FALHA CRÍTICA: carregar_contas() retornou vazio ou None")
        dizer("  [VERIFICAÇÃO] Contas carregadas com sucesso: " + str(len(contas)))
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
        if len(GLOBAL["pool"]) >= TARGET_VALID_PROXIES:
            dizer("  Pool ja recuperado do checkpoint: " + str(len(GLOBAL["pool"])) + " proxies")
            marcar_fase("FASE25_PROXIES", True, "pool recuperado " + str(len(GLOBAL["pool"])))
        else:
            try:
                carregar_e_scrapear_proxies(motivo="inicial")
            except Exception as e:
                dizer("  AVISO: scraping inicial falhou (" + str(e)[:140] + ")")
                debug_log("scraping inicial: " + traceback.format_exc())
            try:
                validar_proxies_ate_target(TARGET_VALID_PROXIES)
            except Exception as e:
                dizer("  AVISO: validacao inicial falhou (" + str(e)[:140] + ")")
                debug_log("validacao inicial: " + traceback.format_exc())
            with GLOBAL["pool_lock"]:
                n_pool = len(GLOBAL["pool"])
            with GLOBAL["raw_lock"]:
                n_raw = len(GLOBAL["raw_proxies"])
            proxy_ok = (n_pool > 0) or (n_raw > 0)
            marcar_fase("FASE25_PROXIES", proxy_ok,
                        "pool=" + str(n_pool) + " raw=" + "{:,}".format(n_raw) +
                        " urls=" + str(GLOBAL['urls_scraped']) + "/" + str(GLOBAL['urls_total']))
            if n_pool == 0:
                dizer("  AVISO: pool de proxies vazio. O envio vai usar o IP do Colab como fallback.")
                dizer("         Verifique se colocou proxies.txt em " + DIR_PROXIES)
        GLOBAL["scraping_inicial_feito"] = True
        GLOBAL["ultimo_rescrape"] = time.time()
        proxy_monitor_thread = threading.Thread(target=monitor_proxies_continuo, daemon=True)
        proxy_monitor_thread.start()
        dizer("  Monitor de proxies activado (refill automatico + SPF bulk)")
        if GLOBAL.get("emergency_stop"):
            raise KeyboardInterrupt()
        if contas is None or len(contas) == 0:
            raise RuntimeError("FALHA CRÍTICA: contas vazio/None antes da FASE 3")
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
        fase6_envio_massivo(mxs, contas, html_base)
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
            CHECKPOINT.salvar_pool(GLOBAL["pool"], GLOBAL["scraping_checkpoint"], len(GLOBAL["raw_proxies"]))
            final_status = GLOBAL.get("status", "RUNNING")
            if final_status != "COMPLETED" and final_status != "TIME_LIMIT_SHUTDOWN":
                final_status = "RUNNING"
            CHECKPOINT.salvar_config(GLOBAL["exec_id"], GLOBAL.get("ip_colab"), status=final_status)
            CHECKPOINT.salvar_tudo()
            upload_all_checkpoints_to_hf()
            dizer("[CHECKPOINT] Estado salvo com sucesso e enviado para HF")
        except Exception as e:
            dizer("ERRO ao salvar checkpoint final: " + str(e))
            debug_log("checkpoint final: " + traceback.format_exc())
        try:
            gerar_ficheiros_finais()
        except Exception as e:
            dizer("ERRO ao gerar ficheiros finais: " + str(e))
            debug_log("gerar_ficheiros_finais: " + traceback.format_exc())
        rt = {
            "aceites": 0, "tentados": 0, "rejeitados": 0, "erros": 0,
            "via_proxy": 0, "via_colab": 0,
            "por_provedor": GLOBAL["por_provedor"]
        }
        for r in GLOBAL["por_provedor"].values():
            for k in ("aceites", "tentados", "rejeitados", "erros", "via_proxy", "via_colab"):
                if k in r:
                    rt[k] += r[k]
        if GLOBAL.get("emergency_stop"):
            dizer("Paragem de emergencia - estado guardado no checkpoint")
            dizer("Na proxima execucao o sistema retoma automaticamente")
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
                    api.upload_file(
                        path_or_fileobj=rp,
                        path_in_repo="reports/v19_5_0_" + ts + ".html",
                        repo_id=HF_LOGS_REPO,
                        repo_type="dataset",
                        token=HF_TOKEN
                    )
                    dizer("  Relatorio enviado para HuggingFace")
                except Exception as e:
                    dizer("  AVISO: upload do relatorio falhou (" + str(e)[:110] + ")")
            else:
                dizer("  Sem conta/template para enviar o relatorio final")
        try:
            upload_unknowns_hf()
        except Exception as e:
            debug_log("upload_unknowns_hf finally: " + str(e))
        try:
            salvar_logs_hf()
        except Exception as e:
            debug_log("salvar_logs_hf finally: " + str(e))
        taxa = rt['aceites'] / max(1, rt['tentados']) * 100
        print("", flush=True)
        print("=" * 70, flush=True)
        print("EXECUCAO TERMINADA - " + str(GLOBAL.get('exec_id')), flush=True)
        print("  Aceites (250 OK) : " + "{:,}".format(rt['aceites']), flush=True)
        print("  Tentativas         : " + "{:,}".format(rt['tentados']), flush=True)
        print("  Taxa             : " + "{:.2f}".format(taxa) + "%", flush=True)
        print("  Via proxy        : " + "{:,}".format(rt['via_proxy']), flush=True)
        print("  Via Colab        : " + "{:,}".format(rt['via_colab']), flush=True)
        print("  Pool final       : " + str(len(GLOBAL['pool'])) + "/" + str(TARGET_VALID_PROXIES), flush=True)
        print("  Banidos          : " + str(GLOBAL['proxies_banidos']), flush=True)
        print("  USER_UNKNOWN     : " + "{:,}".format(len(GLOBAL['user_unknowns'])), flush=True)
        print("  Aceites no ficheiro: " + "{:,}".format(len(GLOBAL.get('aceites_emails', set()))), flush=True)
        print("  Respostas 4xx    : " + "{:,}".format(GLOBAL.get('smtp_4xx', 0)), flush=True)
        print("  Respostas 5xx    : " + "{:,}".format(GLOBAL.get('smtp_5xx', 0)), flush=True)
        print("  Retries          : " + "{:,}".format(GLOBAL.get('smtp_retries', 0)), flush=True)
        print("  Abandonados      : " + "{:,}".format(GLOBAL.get('smtp_abandonados', 0)), flush=True)
        print("  Parquets         : " + str(GLOBAL['pq_downloaded']) + "/" + str(GLOBAL['pq_total']) +
              " (falhas " + str(GLOBAL['pq_falhados']) + ")", flush=True)
        print("  DESec Requests   : " + str(GLOBAL.get('desec_requests', 0)) + " | OK: " +
              str(GLOBAL.get('desec_success', 0)) + " | Fail: " + str(GLOBAL.get('desec_failures', 0)), flush=True)
        print("  Ficheiros saida  : " + SAIDA_ACEITES + " | " + SAIDA_UNKNOWN, flush=True)
        print("  Checkpoints      : " + BASE_LOCAL + "/cp_*.json", flush=True)
        print("  Log DESec        : " + DESEC_LOG, flush=True)
        print("  Logs             : " + LOG_DIR, flush=True)
        print("  MODO RECUPERACAO : " + ("SIM" if GLOBAL.get("modo_recuperacao") else "NAO"), flush=True)
        print("  STATUS FINAL     : " + str(GLOBAL.get("status", "UNKNOWN")), flush=True)
        print("=" * 70, flush=True)
if __name__ == "__main__":
    main()
