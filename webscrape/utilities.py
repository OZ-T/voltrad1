import urllib.error
from urllib.request import Request, urlopen


def download(url,log, user_agent='wswp',  num_retries=2):
    try:
        q = Request(url)
        q.add_header('User-agent', user_agent)
        html = urlopen(q).read()
    except urllib.error.HTTPError as e:
        log.error( 'Download error: %s' % (str(e.reason)) )
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                return download(url, log, user_agent, num_retries-1)
        else:
            return None
    return html