import os, time, shutil, json, platform, subprocess, urllib.request

def sh(c):
    try: return subprocess.check_output(c, shell=True, text=True).strip()
    except Exception as e: return f"erro: {e}"

r = {}
r["python"] = platform.python_version()
r["cpu_cores"] = os.cpu_count()
r["cpu_modelo"] = sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
r["gpu"] = sh("nvidia-smi --query-gpu=name --format=csv,noheader") 

mem = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo")}
r["ram_total_gb"] = round(mem["MemTotal"]/1048576, 1)
r["ram_livre_gb"] = round(mem["MemAvailable"]/1048576, 1)

d = shutil.disk_usage("/")
r["disco_total_gb"] = round(d.total/1e9, 1)
r["disco_livre_gb"] = round(d.free/1e9, 1)

# CPU: 1 núcleo
t = time.time()
x = 0
for i in range(15_000_000): x += i*i
r["cpu_1core_s"] = round(time.time()-t, 2)

# Disco: escrever e ler 500 MB
buf = os.urandom(1024*1024)
t = time.time()
with open("/tmp/t.bin", "wb") as f:
    for _ in range(500): f.write(buf)
    f.flush(); os.fsync(f.fileno())
r["disco_escrita_MBs"] = round(500/(time.time()-t))
t = time.time()
with open("/tmp/t.bin", "rb") as f:
    while f.read(1024*1024): pass
r["disco_leitura_MBs"] = round(500/(time.time()-t))
os.remove("/tmp/t.bin")

# Rede: download de 25 MB
try:
    t = time.time()
    n = len(urllib.request.urlopen("https://speed.cloudflare.com/__down?bytes=25000000", timeout=30).read())
    r["rede_download_Mbps"] = round(n*8/1e6/(time.time()-t), 1)
except Exception as e:
    r["rede_download_Mbps"] = f"erro: {e}"

# pip: tempo de instalar um pacote
t = time.time()
sh("pip install -q --no-cache-dir requests tqdm")
r["pip_install_s"] = round(time.time()-t, 1)

print(json.dumps(r, indent=2, ensure_ascii=False))
