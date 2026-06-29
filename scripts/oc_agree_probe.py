"""Probe v2: POST the 'agree to data-use terms' on oc/download/past/ and dump what follows.
Determines whether OBP pressure data is directly downloadable after agreement, or whether a
further NIED approval/application gate exists. Read-only beyond the terms-agreement click."""
import os, re, html as H
import requests, certifi
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context

AUTH = "https://hinetwww11.bosai.go.jp/auth/"
OC = AUTH + "oc/"
PAST = OC + "download/past/"

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
s.post(AUTH, data={"auth_un": os.environ["HINET_USER"], "auth_pw": os.environ["HINET_PASS"][:12]}, timeout=40)
s.get(OC, timeout=40)
s.post(OC, data={"auth_un": os.environ["HINET_USER"], "auth_pw": os.environ["HINET_PASS"][:12]}, timeout=40)

p = s.get(PAST, timeout=40); t = p.text
print("PAST status=%d len=%d login=%s" % (p.status_code, len(t), "auth_pw" in t), flush=True)
# raw form blocks
forms = re.findall(r"<form[^>]*>.*?</form>", t, re.S | re.I)
print("num_forms=%d" % len(forms), flush=True)
for i, fb in enumerate(forms):
    ft = re.search(r"<form[^>]*>", fb, re.I).group(0)
    print("FORM[%d] tag=%s" % (i, ft[:160]), flush=True)
    fields = {}
    for inp in re.findall(r"<input[^>]*>", fb, re.I):
        nm = re.search(r"name=[\x27\"]?([\w\[\]]+)", inp); va = re.search(r"value=[\x27\"]?([^\x27\">]*)", inp); ty = re.search(r"type=[\x27\"]?([\w]+)", inp)
        print("   input name=%s type=%s value=%s" % (nm and nm.group(1), ty and ty.group(1), va and va.group(1)), flush=True)
        if nm:
            fields[nm.group(1)] = va.group(1) if va else ""
    for b in re.findall(r"<button[^>]*>.*?</button>", fb, re.S | re.I):
        print("   BUTTON:", re.sub(r"\s+", " ", b)[:160], flush=True)

# build agree POST: take the agreement form (usually the last/non-auth form), submit all its fields
agree = None
for fb in forms:
    if "auth_pw" in fb:
        continue
    agree = fb
if agree is None and forms:
    agree = forms[-1]
data = {}
for inp in re.findall(r"<input[^>]*>", agree or "", re.I):
    nm = re.search(r"name=[\x27\"]?([\w\[\]]+)", inp); va = re.search(r"value=[\x27\"]?([^\x27\">]*)", inp)
    if nm:
        data[nm.group(1)] = va.group(1) if va else "1"
print("\nPOSTing agree with fields:", list(data.keys()), flush=True)
r2 = s.post(PAST, data=data, timeout=40)
m = r2.text
print("\n###### AFTER-AGREE PAGE status=%d len=%d login=%s ######" % (r2.status_code, len(m), "auth_pw" in m), flush=True)
for f in re.findall(r"<form[^>]*>", m, re.I): print("FORM:", f[:180], flush=True)
for blk in re.findall(r"<select.*?</select>", m, re.S | re.I):
    nm = re.search(r"name=[\x27\"]?([\w\[\]]+)", blk)
    opts = re.findall(r"<option[^>]*value=[\x27\"]?([^\x27\">]*)[\x27\"]?[^>]*>([^<]*)</option>", blk)
    print("--SELECT name=%s n_opts=%d sample=%s--" % (nm and nm.group(1), len(opts), opts[:6]), flush=True)
for inp in re.findall(r"<input[^>]*>", m, re.I)[:40]:
    nm = re.search(r"name=[\x27\"]?([\w\[\]]+)", inp); ty = re.search(r"type=[\x27\"]?([\w]+)", inp)
    print("   input name=%s type=%s" % (nm and nm.group(1), ty and ty.group(1)), flush=True)
for e in sorted(set(re.findall(r"[\w./]+\.php", m)))[:30]: print("  php:", e, flush=True)
for kw in ["申請","承認","利用申請","許可","権限","ご利用いただけません","できません","ありません","not author","unauthor","permission","水圧","圧力","press","pressure","S-net","DONET","year","yyyymmdd","Download","ダウンロード","選択","観測点","station"]:
    for mm in list(re.finditer(re.escape(kw), m))[:2]:
        i = mm.start(); print("KW[%s]: %r" % (kw, m[max(0,i-35):i+75]), flush=True)
print("--TEXT (first 2200)--", flush=True)
print(strip_text(m)[:2200], flush=True)
print("\nOC_AGREE_PROBE_DONE", flush=True)