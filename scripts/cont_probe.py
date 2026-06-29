"""Authenticated probe of the NIED auth/download/cont portal to find the S-net
ocean-bottom PRESSURE network code (org1/org2) that HinetPy does not expose.
Logs in (HinetPy scheme), dumps the network <select> options + any pressure-related
labels from the cont form and the S-net station json."""
import os, re, sys
import requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context
AUTH="https://hinetwww11.bosai.go.jp/auth/"
CONT=AUTH+"download/cont/"
class Cipher(HTTPAdapter):
    def init_poolmanager(self,c,m,block=False):
        ctx=create_urllib3_context(ciphers=":HIGH:!DH:!aNULL")
        super().init_poolmanager(c,m,block=block,ssl_context=ctx)
s=requests.Session()
s.mount("https://hinetwww11.bosai.go.jp/",Cipher())
s.get(AUTH,timeout=40,verify=certifi.where())
r=s.post(AUTH,data={"auth_un":os.environ["HINET_USER"],"auth_pw":os.environ["HINET_PASS"][:12]},timeout=40)
mlog=re.search(r"auth_log(.*?)\.png",r.text)
print("login marker:",mlog.group(1) if mlog else "??","(in=ok, out=fail)",flush=True)
def dump(url,name):
    try:
        resp=s.get(url,timeout=40)
    except Exception as e:
        print(name,"GET err:",repr(e)[:120],flush=True); return ""
    t=resp.text
    print("=== %s len=%d ==="%(name,len(t)),flush=True)
    return t
main=dump(CONT,"cont_main")
for blk in re.findall(r"<select.*?</select>",main,re.S|re.I):
    nm=re.search(r"name=['\"]?([\w]+)",blk)
    print("--SELECT name=%s--"%(nm.group(1) if nm else "?"),flush=True)
    for opt in re.findall(r"<option[^>]*value=['\"]?([^'\">]*)['\"]?[^>]*>([^<]*)</option>",blk):
        print("   option value=%r text=%r"%(opt[0].strip(),opt[1].strip()),flush=True)
for kw in ["press","Press","PRESS","OBP","水圧","圧力","pressure","S-net","snet","Snet"]:
    for mm in list(re.finditer(re.escape(kw),main))[:3]:
        i=mm.start(); print("KW[%s]: %r"%(kw,main[max(0,i-50):i+70]),flush=True)
sn=dump(CONT+"st_snet_json.php","st_snet_json")
print("st_snet_json head:",sn[:400],flush=True)
print("CONT_PROBE_DONE",flush=True)
