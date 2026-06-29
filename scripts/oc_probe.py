"""Probe the NIED auth/oc/ ocean-bottom portal (S-net + DONET PRESSURE data).
Login (HinetPy scheme), dump the oc form: <form action>, <select> options,
<input> names, any JS networks/datatype dict, and request/download .php endpoints."""
import os, re
import requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context
AUTH="https://hinetwww11.bosai.go.jp/auth/"
OC=AUTH+"oc/"
class Cipher(HTTPAdapter):
    def init_poolmanager(self,c,m,block=False):
        ctx=create_urllib3_context(ciphers=":HIGH:!DH:!aNULL")
        super().init_poolmanager(c,m,block=block,ssl_context=ctx)
s=requests.Session()
s.mount("https://hinetwww11.bosai.go.jp/",Cipher())
s.get(AUTH,timeout=40,verify=certifi.where())
r=s.post(AUTH,data={"auth_un":os.environ["HINET_USER"],"auth_pw":os.environ["HINET_PASS"][:12]},timeout=40)
mlog=re.search(r"auth_log(.*?)\.png",r.text)
print("login:",mlog.group(1) if mlog else "??",flush=True)
m=s.get(OC,timeout=40).text
print("=== oc_main len=%d ==="%len(m),flush=True)
for f in re.findall(r"<form[^>]*>",m,re.I):
    print("FORM:",f[:200],flush=True)
for blk in re.findall(r"<select.*?</select>",m,re.S|re.I):
    nm=re.search(r"name=['\"]?([\w]+)",blk)
    print("--SELECT name=%s--"%(nm.group(1) if nm else "?"),flush=True)
    for opt in re.findall(r"<option[^>]*value=['\"]?([^'\">]*)['\"]?[^>]*>([^<]*)</option>",blk)[:30]:
        print("   value=%r text=%r"%(opt[0].strip(),opt[1].strip()),flush=True)
print("--INPUTS--",flush=True)
for inp in re.findall(r"<input[^>]*>",m,re.I)[:40]:
    nm=re.search(r"name=['\"]?([\w]+)",inp); ty=re.search(r"type=['\"]?([\w]+)",inp); va=re.search(r"value=['\"]?([^'\">]*)",inp)
    print("   input name=%s type=%s value=%s"%(nm and nm.group(1),ty and ty.group(1),va and va.group(1)),flush=True)
print("--JS dicts--",flush=True)
for mm in re.findall(r"\w+\[['\"][\w]+['\"]\]\s*=\s*['\"][^'\"]*['\"]",m)[:60]:
    print("   ",mm[:140],flush=True)
print("--php endpoints--",flush=True)
for e in sorted(set(re.findall(r"[\w./]+\.php",m)))[:40]:
    print("   ",e,flush=True)
for kw in ["press","Press","水圧","圧力","S-net","DONET","pressure","gauge","tsunami"]:
    for mm in list(re.finditer(re.escape(kw),m))[:2]:
        i=mm.start(); print("KW[%s]: %r"%(kw,m[max(0,i-40):i+70]),flush=True)
print("OC_PROBE_DONE",flush=True)
