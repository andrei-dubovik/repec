# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Text and html sanitization routines."""

# Load packages
import re
from lxml.html import tostring, html5parser
import warnings


def prepare_cc():
    r"""Prepare a regex for removing control characters (except for \t, \r, \n)."""
    # Ascii control characters
    cc = [*range(0x00, 0x1f + 1), 0x7f]
    cc.remove(0x09)
    cc.remove(0x0a)
    cc.remove(0x0d)

    # Non latin-1 and non utf-8 opening characters
    undefined = list(range(0x80, 0x9f + 1))

    return re.compile('|'.join([
        *(chr(c) for c in cc),  # control characters
        *(chr(c) for c in undefined),  # undefined characters
        *('&#x0*' + hex(c)[2:] + ';' for c in cc),  # html-escaped control characters (hex)
        *('&#0*' + str(c) + ';' for c in cc),  # html-escaped control characters (decimal)
    ]))


# Define global settings
BLOCKTAGS = ['div', 'p', 'br', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']
EMAIL = re.compile("['a-z0-9._-]+@[a-z0-9._-]+.[a-z]+")
CC = prepare_cc()


def remove_cc(text):
    r"""Remove control characters (except for \t, \r, \n)."""
    return CC.sub('', text)


def sanitize_entity(text):
    """Remove whitespace characters from HTML entities."""
    def strip(m):
        return re.sub(r'\s', '', m.group(0))
    return re.sub(r'&#x?[0-9a-f\s]+;', strip, text, flags=re.I)


def ishtml(text):
    """Guess whether text has html in it."""
    text = text.lower()
    for t in BLOCKTAGS:
        if text.find(f'<{t}>') != -1:
            return True
    if text.find('</') != -1 or text.find('/>') != -1:
        return True
    if re.search('&#?x?[0-9a-z]+;', text):
        return True
    return False


def html2text(html):
    """Render html as text, convert line breaks to spaces."""
    if not ishtml(html):
        return re.sub(r'\s+', ' ', html.strip())
    parser = html5parser.HTMLParser(namespaceHTMLElements=False)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        html = html5parser.fromstring(html, parser=parser)
    for b in BLOCKTAGS:
        for e in html.xpath(f'//{b}'):
            e.text = ' ' + e.text if e.text else ''
            if len(e) > 0:
                lc = e[-1]
                lc.tail = (lc.tail if lc.tail else '') + ' '
            else:
                e.text = e.text + ' '
    text = tostring(html, method='text', encoding='utf-8')
    return re.sub(r'\s+', ' ', text.decode().strip())


def isna(token):
    """Check if it's an N/A token."""
    if re.sub(r'\W', '', token).lower() == 'na':
        return True
    else:
        return False


def isvalid(token):
    """Check if token contains alpha characters."""
    if re.match(r'^[\W0-9]*$', token):
        return False
    else:
        return True


def sanitize(text):
    """Remove control characters, drop HTML tags, etc."""
    if type(text) != str:
        return text
    text = sanitize_entity(text)
    text = remove_cc(text)
    text = html2text(text)
    if isna(text) or not isvalid(text):
        return None
    return text


def sanitize_email(email):
    """Get a clean email address."""
    email = sanitize(email)
    if type(email) != str:
        return email
    m = EMAIL.search(email.lower())
    return m.group() if m else None
