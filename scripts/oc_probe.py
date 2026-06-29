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

# --- step 2: POST creds directly to oc portal (it has its own login form) ---
print("###### STEP2: POST creds to oc portal ######",flush=True)
s.get(OC,timeout=40)
r2=s.post(OC,data={"auth_un":os.environ["HINET_USER"],"auth_pw":os.environ["HINET_PASS"][:12]},timeout=40)
m2=r2.text
print("after-oc-POST len=%d"%len(m2),flush=True)
mlog2=re.search(r"auth_log(.*?)\.png",m2)
print("oc login marker:",mlog2.group(1) if mlog2 else "none",flush=True)
still_login = ('name="auth"' in m2 and 'auth_pw' in m2)
print("still showing login form:",still_login,flush=True)
for f in re.findall(r"<form[^>]*>",m2,re.I): print("FORM2:",f[:200],flush=True)
for blk in re.findall(r"<select.*?</select>",m2,re.S|re.I):
    nm=re.search(r"name=['\"]?([\w]+)",blk); print("--SEL2 name=%s--"%(nm.group(1) if nm else "?"),flush=True)
    for opt in re.findall(r"<option[^>]*value=['\"]?([^'\">]*)['\"]?[^>]*>([^<]*)</option>",blk)[:20]:
        print("   value=%r text=%r"%(opt[0].strip(),opt[1].strip()),flush=True)
for e in sorted(set(re.findall(r"[\w./]+\.php",m2)))[:30]: print("  php2:",e,flush=True)
for kw in ["water","press","水圧","圧力","Download","download","not author","permission","register","申請","許可","unauthor"]:
    for mm in list(re.finditer(re.escape(kw),m2))[:2]:
        i=mm.start(); print("KW2[%s]: %r"%(kw,m2[max(0,i-40):i+80]),flush=True)
print("OC_PROBE2_DONE",flush=True)

# --- step 3: read the pressure download page (read-only) ---
print("###### STEP3: GET oc/download/past/ (pressure portal, read-only) ######",flush=True)
p=s.get(OC+"download/past/",timeout=40); mp=p.text
print("past page len=%d status=%d"%(len(mp),p.status_code),flush=True)
mlog3=re.search(r"auth_log(.*?)\.png",mp); print("past login marker:",mlog3.group(1) if mlog3 else "none",flush=True)
print("has login form:",('auth_pw' in mp),flush=True)
for f in re.findall(r"<form[^>]*>",mp,re.I): print("FORM3:",f[:200],flush=True)
for blk in re.findall(r"<select.*?</select>",mp,re.S|re.I):
    nm=re.search(r"name=['\"]?([\w]+)",blk); print("--SEL3 name=%s--"%(nm.group(1) if nm else "?"),flush=True)
    for opt in re.findall(r"<option[^>]*value=['\"]?([^'\">]*)['\"]?[^>]*>([^<]*)</option>",blk)[:25]:
        print("   value=%r text=%r"%(opt[0].strip(),opt[1].strip()),flush=True)
for e in sorted(set(re.findall(r"[\w./]+\.php",mp)))[:30]: print("  php3:",e,flush=True)
for kw in ["申請","承認","利用申請","not author","unauthor","permission","S-net","DONET","水圧","Download","download","button","disabled","ありません","できません"]:
    for mm in list(re.finditer(re.escape(kw),mp))[:2]:
        i=mm.start(); print("KW3[%s]: %r"%(kw,mp[max(0,i-30):i+80]),flush=True)
print("OC_PROBE3_DONE",flush=True)
