"""Общий HTTP-клиент сборщиков: сертификаты, заголовки и таймаут в одном месте."""

import http.cookiejar
import os
import ssl
import urllib.request

USER_AGENT = "adsb-traffic-analysis/1.0 (university data engineering project)"
# сайт аэропорта построен на SharePoint и обычному клиенту отвечает страницей-заглушкой
BROWSER_USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/141.0 Safari/537.36")
TIMEOUT = 40


def ssl_context():
    """Python с python.org на macOS не видит системные сертификаты, подставляем их явно."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


SSL_CTX = ssl_context()


def get(url, timeout=TIMEOUT):
    """Тело ответа строкой. Ошибки не глушатся: вызывающий решает, что с ними делать."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout, context=SSL_CTX) as response:
        return response.read().decode("utf-8")


def browser_session():
    """Сессия с cookies: табло аэропорта хранит состояние формы между запросами."""
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
        urllib.request.HTTPSHandler(context=SSL_CTX))
    opener.addheaders = [("User-Agent", BROWSER_USER_AGENT)]
    return opener
