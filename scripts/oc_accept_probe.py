"""Probe: accept the data-use terms (accept.php) then dump oc/download/past to reveal the
ACTUAL OBP application mechanism (in-portal form vs email/contact). Read-only; does NOT submit
any application or download any data."""
import os, re, html as H
import requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context
AUTH="https://hinetwww11.bosai.go.jp/auth/"; OC=AUTH+"oc/"; PAST=OC+"download/past/"
class Cipher(HTTPAdapter):
    def init_poolmanager(self,c,m,block=False):
        ctx=create_urllib3_context(ciphers=":HIGH:!DH:!aNULL")
        super().init_poolmanager(c,m,block=block,ssl_context=ctx)
def strip(h):
    h=re.sub(r"<script.*?</script>"," ",h,flags=re.S); h=re.sub(r"<style.*?</style>"," ",h,flags=re.S)
    h=re.sub(r"<[^>]+>"," ",h); h=H.unescape(h); h=re.sub(r"[ \t\r]+"," ",h); h=re.sub(r"\n\s*\n+","\n",h); return h.strip()
s=requests.Session(); s.mount("https://hinetwww11.bosai.go.jp/",Cipher())
s.get(AUTH,timeout=40,verify=certifi.where())
s.post(AUTH,data={"auth_un":os.environ["HINET_USER"],"auth_pw":os.environ["HINET_PASS"][:12]},timeout=40)
s.get(OC,timeout=40); s.post(OC,data={"auth_un":os.environ["HINET_USER"],"auth_pw":os.environ["HINET_PASS"][:12]},timeout=40)
# trigger terms acceptance
for acc in [PAST+"accept.php", OC+"accept.php", PAST+"accept.php?agree=1"]:
    try:
        r=s.get(acc,timeout=40); print("GET %s -> %d len=%d"%(acc,r.status_code,len(r.text)),flush=True)
    except Exception as e:
        print("ERR",acc,e,flush=True)
# now re-fetch download/past
r=s.get(PAST,timeout=40); t=r.text
print("\n###### download/past AFTER accept: status=%d len=%d login=%s ######"%(r.status_code,len(t),"auth_pw" in t),flush=True)
for f in re.findall(r"<form[^>]*>",t,re.I): print("FORM:",f[:180],flush=True)
for blk in re.findall(r"<select.*?</select>",t,re.S|re.I):
    nm=re.search(r"name=[\x27\"]?([\w\[\]]+)",blk); opts=re.findall(r"<option[^>]*value=[\x27\"]?([^\x27\">]*)",blk)
    print("SELECT name=%s nopt=%d sample=%s"%(nm and nm.group(1),len(opts),opts[:6]),flush=True)
for inp in re.findall(r"<input[^>]*>",t,re.I)[:40]:
    nm=re.search(r"name=[\x27\"]?([\w\[\]]+)",inp); ty=re.search(r"type=[\x27\"]?([\w]+)",inp)
    print("  input name=%s type=%s"%(nm and nm.group(1),ty and ty.group(1)),flush=True)
print("--LINKS(mail/apply/contact)--",flush=True)
for href,txt in re.findall(r"<a[^>]*href=[\x27\"]?([^\x27\"> ]+)[\x27\"]?[^>]*>(.*?)</a>",t,re.S|re.I):
    if any(k in href.lower() for k in ["mail","apply","accept","contact","regist","request","form","past","cont"]):
        print("  ",href[:90],"|",re.sub(r"\s+"," ",re.sub(r"<[^>]+>","",txt)).strip()[:50],flush=True)
for e in sorted(set(re.findall(r"[\w./@-]+\.php",t)))[:20]: print("  php:",e,flush=True)
for m in re.findall(r"[\w.\-]+@[\w.\-]+",t)[:6]: print("  EMAIL:",m,flush=True)
for kw in ["申請","承認","利用申請","メール","mail","フォーム","form","year","yyyymmdd","観測点","station","水圧","ダウンロード","選択","期間"]:
    for mm in list(re.finditer(re.escape(kw),t))[:2]:
        i=mm.start(); print("KW[%s]:"%kw, repr(t[max(0,i-30):i+70]),flush=True)
print("--TEXT(2200)--",flush=True); print(strip(t)[:2200],flush=True)
print("OC_ACCEPT_PROBE_DONE",flush=True)