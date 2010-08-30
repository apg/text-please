from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import memcache
from google.appengine.api.urlfetch import fetch, Error

from bottle import default_app, route, request, response, view, HTTPResponse

from readability import Document
from html2text import html2text

from functools import wraps

import urllib
import urlparse
import datetime
import re
import logging
import traceback
import sys

THROTTLED_NUM_PER_HOUR = 20
ARTICLE_TIMEOUT = 3600 # keep articles around for 60 minutes

URL_RE = re.compile('''^(?#Protocol)(?:(?:ht|f)tp(?:s?)\:\/\/|~\/|\/)?(?#Username:Password)(?:\w+:\w+@)?(?#Subdomains)(?:(?:[-\w]+\.)+(?#TopLevel Domains)(?:com|org|net|gov|mil|biz|info|mobi|name|aero|jobs|museum|travel|[a-z]{2}))(?#Port)(?::[\d]{1,5})?(?#Directories)(?:(?:(?:\/(?:[-\w~!$+|.,=]|%[a-f\d]{2})+)+|\/)+|\?|#)?(?#Query)(?:(?:\?(?:[-\w~!$+|.,*:]|%[a-f\d{2}])+=?(?:[-\w~!$+|.,*:=]|%[a-f\d]{2})*)(?:&(?:[-\w~!$+|.,*:]|%[a-f\d{2}])+=?(?:[-\w~!$+|.,*:=]|%[a-f\d]{2})*)*)*(?#Anchor)(?:#(?:[-\w~!$+|.,*:=]|%[a-f\d]{2})*)?$''')


def throttled(ip, limit):
    key = 'throttle:%s:%s' % \
        (ip, datetime.datetime.utcnow().strftime('%Y%m%d%H'))
    count = memcache.incr(key, initial_value=0)
    if count is None:
        return False
    return count > limit


class ThrottledException(Exception):
    """Exception to be thrown when user is throttled"""


def cache(prefix='', timeout=0, keymaker=None):
    """Memoize function result in memcache"""

    def decorator(func):

        @wraps(func)
        def _inner(*args, **kwargs):
            if keymaker:
                key = prefix + ':' + keymaker(*args, **kwargs)
            else:
                key = prefix + ':'.join(str(arg) for arg in args) + ':' + \
                    ':'.join(str(k) + '=' + str(v) \
                             for k, v in kwargs.iteritems())
                
            data = memcache.get(key)
            if data is not None:
                return data
            else:
                data = func(*args, **kwargs)
                if data is not None:
                    memcache.add(key, data, time=timeout)
            return data
        return _inner
    return decorator


def formatter(func):
    """Decorator which passes the contents of the URL to `func` to be formatted
    appropriately"""

    def _inner():
        result = {'content': '',
                  'url': '',
                  'title': ''}

        url = urllib.unquote(request.GET.get('url', ''))

        # validate the url
        if not URL_RE.match(url):
            result['error'] = 'Invalid URL'
            return func(result, '')
        
        result['url'] = url

        parsed_url = urlparse.urlparse(url)
        base_url = parsed_url[0] + "://" + parsed_url[1]

        try:
            article = extract_article(url,
                                          request.environ.get('REMOTE_ADDR'))
            if article:
                result['title'] = article[0]
                result['content'] = article[1]
            else:
                result['error'] = 'Retrieving URL %s resulted in an error ' \
                    'from the remote server' % url
        except ThrottledException, e:
            result['error'] = "You've been throttled. Please wait a while " \
                "and try again."
            return HTTPResponse(func(result, ''), status=403)
        except Error, e:
            logging.error("Couldn't retrieve (%s): %s" % (url, str(e)))
            result['error'] = 'An error occurred while retrieving URL %s' % url
        except Exception, e:
            logging.error("Couldn't extract article (%s): %s" % (url, str(e)))
            result['error'] = 'The article could not be extracted from %s' % url

        output = func(result, base_url)

        return output
    return _inner


@cache('extracted:article', keymaker=lambda *args, **kwargs: str(args[0]))
def extract_article(url, ip):
    """Extracts the article using readability"""
    title, summary = None, None
    response = get_url(url, ip)
    if response.status_code == 200:
        doc = Document(response.content)
        summary = unicode(doc.summary())
        title = unicode(doc.title())
        return title, summary
    else:
        return None


def get_url(url, ip, headers=None, 
            time=ARTICLE_TIMEOUT, 
            throttle=THROTTLED_NUM_PER_HOUR):
    """Gets a URL

    If it's in memcache, throttling doesn't matter. If the URL must be 
    fetched, first check to see whether the user has capacity to get it
    """
    key_prefix = 'url:source:'
    data = memcache.get(key_prefix + url)
    if not data:
        # is this user throttled?
        if throttled(ip, throttle):
            raise ThrottledException()

        logging.debug('Obtaining ' + url)
        data = fetch(url, method='GET', headers=(headers or {}),
                     follow_redirects=True)
        if data.status_code == 200:
            memcache.add(key_prefix + url, data, time=(60 * 60))

    return data


@route('/text/extract.md')
@formatter
def markdown(result, base_url=''):
    error = result.get('error')
    if not error:
        text = html2text(result['content'], base_url)
    else:
        text = '## An Error Occurred\n\n' + result['error']

    response.headers['Content-Type'] = 'text/plain'
    return text

HTML_TMPL = """<html>
<head>
  <title>%(title)s</title>
</head>
<body>
%(content)s
</body>
</html>
"""

@route('/text/extract.html')
@formatter
def html(result, base_url=''):
    error = result.get('error')
    if error:
        result['title'] = 'An Error Occurred'
        result['content'] = """<h1>An Error Occurred</h1>
<p>%(error)s</p>""" % result
    response.headers['Content-Type'] = 'text/html'
    return HTML_TMPL % result


# @route('/text/extract.json')
# @formatter
# def json(result, base_url=''):
#     response.headers['Content-Type'] = 'text/json'
#     return dumps(result)


@route('/')
@view('index')
def index():
    return {}


if __name__ == '__main__':
    run_wsgi_app(default_app())
