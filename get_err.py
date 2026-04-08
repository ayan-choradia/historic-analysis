import urllib.request as req, urllib.error as e, re
try:
    req.urlopen('http://127.0.0.1:5050/api/fomc-meetings')
    print("Success")
except e.HTTPError as ex:
    html = ex.read().decode('utf-8')
    m = re.search(r'<textarea id="traceback_textarea".*?>(.*?)</textarea>', html, re.DOTALL)
    print(m.group(1).strip() if m else html[:1500])
except Exception as ex:
    print(ex)
