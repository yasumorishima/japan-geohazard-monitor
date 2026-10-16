# one-off: read the grid coordinate variables of one AIRS3STD granule (no data field)
import os, urllib.request, urllib.parse, json
import netCDF4, numpy as np
TOK = open(os.path.expanduser("~/.edl_token")).read().strip()
q = urllib.parse.urlencode({"short_name": "AIRS3STD", "version": "7.0", "page_size": 1,
                            "temporal": "2010-07-01T00:00:00Z,2010-07-01T23:59:59Z"})
t = json.load(urllib.request.urlopen("https://cmr.earthdata.nasa.gov/search/granules.json?" + q, timeout=60))["feed"]["entry"][0]["title"]
ce = "/XDim_ascending;/YDim_ascending;/XDim_descending;/YDim_descending"
u = "https://opendap.earthdata.nasa.gov/collections/C1701805652-GES_DISC/granules/%s.dap.nc4?dap4.ce=%s" % (urllib.parse.quote(t, safe=""), ce)
b = urllib.request.urlopen(urllib.request.Request(u, headers={"Authorization": "Bearer " + TOK}), timeout=120).read()
open("/tmp/or305.nc4", "wb").write(b)
d = netCDF4.Dataset("/tmp/or305.nc4")
for k in ("XDim_ascending", "YDim_ascending", "XDim_descending", "YDim_descending"):
    v = np.asarray(d.variables[k][:])
    print(k, v.shape, v[:3], v[-3:])
os.remove("/tmp/or305.nc4")
