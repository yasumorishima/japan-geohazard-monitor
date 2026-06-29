"""Probe the NIED auth/oc/ ocean-bottom PRESSURE application mechanism (read-only)."""
import os, re, html as H
import requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context
from urllib.parse import urljoin

AUTH = "https://hinetwww11.bosai.go.jp/auth/"
OC = AUTH + "oc/"

class Cipher(HTTPAdapter):
    def init_poolmanager(self, c, m, block=False):
        ctx = create_urllib3_context(ciphers=":HIGH:!DH:!aNULL")
        super().init_poolmanager(c, m, block=block, ssl_context=ctx)

def strip_text(h):
    h = re.sub(r"<script.*?</script>", " ", h, flags=re.S)
    h = re.sub(r"<style.*?</style>", " ", h, flags=re.S)
    h = re.sub(r"<[^>]+>", " ", h)
    h = H.unescape(h)
    h = re.sub(r"[ \t\r]+", " ", h)
    h = re.sub(r"\n\s*\n+", "\n", h)
    return h.strip()

s = requests.Session()
s.mount("https://hinetwww11.bosai.go.jp/", Cipher())
s.get(AUTH, timeout=40, verify=certifi.where())
r = s.post(AUTH, data={"auth_un": os.environ["HINET_USER"], "auth_pw": os.environ["HINET_PASS"][:12]}, timeout=40)
mlog = re.search(r"auth_log(.*?)\.png", r.text)
print("login:", mlog.group(1) if mlog else "??", flush=True)
s.get(OC, timeout=40)
s.post(OC, data={"auth_un": os.environ["HINET_USER"], "auth_pw": os.environ["HINET_PASS"][:12]}, timeout=40)

def dump(url, tag):
    print("\n###### %s : %s ######" % (tag, url), flush=True)
    try:
        rr = s.get(url, timeout=40)
    except Exception as e:
        print("ERR", e, flush=True); return ""
    t = rr.text
    print("status=%d len=%d has_login_form=%s" % (rr.status_code, len(t), ("auth_pw" in t)), flush=True)
    print("--LINKS--", flush=True)
    for href, txt in re.findall(r"<a[^>]*href=[\x27\"]?([^\x27\"> ]+)[\x27\"]?[^>]*>(.*?)</a>", t, re.S | re.I)[:70]:
        tx = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", txt)).strip()
        if tx or any(k in href.lower() for k in ["accept","apply","regist","contact","form","request","oc"]):
            print("   href=%s | %s" % (href[:90], tx[:60]), flush=True)
    for f in re.findall(r"<form[^>]*>", t, re.I):
        print("FORM:", f[:200], flush=True)
    for inp in re.findall(r"<(?:input|textarea|select)[^>]*>", t, re.I)[:45]:
        nm = re.search(r"name=[\x27\"]?([\w\[\]]+)", inp); ty = re.search(r"type=[\x27\"]?([\w]+)", inp)
        print("   field name=%s type=%s" % (nm and nm.group(1), ty and ty.group(1)), flush=True)
    print("--TEXT (first 2600 chars)--", flush=True)
    print(strip_text(t)[:2600], flush=True)
    return t

past = dump(OC + "download/past/", "STEP1 oc/download/past")
cand = set()
for href in re.findall(r"href=[\x27\"]?([^\x27\"> ]+)", past, re.I):
    hl = href.lower()
    if any(k in hl for k in ["accept","apply","application","request","regist","moushi","shinsei"]):
        cand.add(href)
print("\n== candidate application links ==", cand, flush=True)
for c in list(cand)[:6]:
    dump(urljoin(OC + "download/past/", c), "FOLLOW " + c[:50])
dump("https://hinetwww11.bosai.go.jp/nied/registration/?LANG=en", "REG en")
print("\nOC_APPLY_PROBE_DONE", flush=True)