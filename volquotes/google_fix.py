import token, tokenize
from io import StringIO
import json
from pandas.io.json import json_normalize
import requests
import pandas as pd

def fix_lazy_json(in_text):
    """
    Handle lazy JSON - to fix expecting property name
    this function fixes the json output from google
    http://stackoverflow.com/questions/4033633/handling-lazy-json-in-python-expecting-property-name
    """
    tokengen = tokenize.generate_tokens(StringIO(in_text).readline)

    result = []
    for tokid, tokval, _, _, _ in tokengen:
        # fix unquoted strings
        if (tokid == token.NAME):
            if tokval not in ['true', 'false', 'null', '-Infinity', 'Infinity', 'NaN']:
                tokid = token.STRING
                tokval = u'"%s"' % tokval

        # fix single-quoted strings
        elif (tokid == token.STRING):
            if tokval.startswith ("'"):
                tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')

        # remove invalid commas
        elif (tokid == token.OP) and ((tokval == '}') or (tokval == ']')):
            if (len(result) > 0) and (result[-1][1] == ','):
                result.pop()

        # fix single-quoted strings
        elif (tokid == token.STRING):
            if tokval.startswith ("'"):
                tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')

        result.append((tokid, tokval))

    return tokenize.untokenize(result)

def Options(symbol):
    url = "https://www.google.com/finance/option_chain"
    r = requests.get(url, params={"q": symbol,"output": "json"})
    content_json = r.text
    dat = json.loads(fix_lazy_json(content_json))
    #print ("dat = [%s] " % (str(dat)))
    puts = json_normalize(dat['puts'])
    calls = json_normalize(dat['calls'])
    np=len(puts)
    nc=len(calls)

    for i in dat['expirations'][1:]:
        r = requests.get(url, params={"q": symbol,"expd":i['d'],"expm":i['m'],"expy":i['y'],"output": "json"})
        content_json = r.text
        idat = json.loads(fix_lazy_json(content_json))
        puts1 = json_normalize(idat['puts'])
        calls1 = json_normalize(idat['calls'])
        puts1.index = [np+i for i in puts1.index]
        calls1.index = [nc+i for i in calls1.index]
        np+=len(puts1)
        nc+=len(calls1)
        puts = puts.append(puts1)
        calls = calls.append(calls1)
    calls.columns = ['Ask','Bid','Chg','cid','PctChg','cs','IsNonstandard','Expiry','Underlying','Open_Int','Last','Symbol','Strike','Vol']
    puts.columns = ['Ask','Bid','Chg','cid','PctChg','cs','IsNonstandard','Expiry','Underlying','Open_Int','Last','Symbol','Strike','Vol']
    calls['Type'] = ['call' for i in range(len(calls))]
    puts['Type'] = ['put' for i in range(len(puts))]
    puts.index = [i+len(calls) for i in puts.index]
    opt=pd.concat([calls,puts])
    opt['Underlying']=[symbol for i in range(len(opt))]
    opt['Underlying_Price'] = [dat['underlying_price'] for i in range(len(opt))]
    opt['Root']=opt['Underlying']
    for j in ['Vol','Strike','Last','Bid','Ask','Chg']:
        opt[j] = pd.to_numeric(opt[j],errors='coerce')
    opt['IsNonstandard']=opt['IsNonstandard'].apply(lambda x:x!='OPRA')
    opt = opt.sort_values(by=['Strike','Type'])
    opt.index = range(len(opt))
    col = ['Strike', 'Expiry', 'Type', 'Symbol', 'Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol', 'Open_Int', 'Root', 'IsNonstandard', 'Underlying', 'Underlying_Price', 'cid','cs']
    opt = opt[col]

    return opt
