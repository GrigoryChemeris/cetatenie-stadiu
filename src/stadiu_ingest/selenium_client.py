"""Selenium: та же схема, что cetatenie-mvp (страница + скачивание PDF), свои настройки из stadiu_ingest.config."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from stadiu_ingest.config import (
    CHROME_BIN,
    CHROMEDRIVER_PATH,
    HEADLESS,
    LIST_PAGE_WAIT_TIMEOUT,
    PAGE_LOAD_TIMEOUT,
)

log = logging.getLogger("stadiu_ingest.selenium")


def _chrome_options(download_dir: Path, user_agent: str, *, stealth: bool) -> webdriver.ChromeOptions:
    opts = webdriver.ChromeOptions()
    if CHROME_BIN:
        opts.binary_location = CHROME_BIN
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--no-first-run")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--mute-audio")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(f"--user-agent={user_agent}")
    if stealth:
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    try:
        opts.page_load_strategy = "eager"
    except Exception:
        pass
    opts.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
        },
    )
    return opts


def build_chrome(download_dir: Path, user_agent: str) -> webdriver.Chrome:
    download_dir.mkdir(parents=True, exist_ok=True)
    if CHROMEDRIVER_PATH:
        service = Service(CHROMEDRIVER_PATH)
    else:
        service = Service(ChromeDriverManager().install())

    for stealth in (True, False):
        opts = _chrome_options(download_dir, user_agent, stealth=stealth)
        try:
            driver = webdriver.Chrome(service=service, options=opts)
            driver.set_page_load_timeout(max(float(PAGE_LOAD_TIMEOUT), 120.0))
            return driver
        except SessionNotCreatedException:
            if stealth:
                continue
            raise


def _page_source_safe(driver: webdriver.Chrome) -> str:
    try:
        return driver.page_source or ""
    except Exception:
        return ""


def _is_bot_challenge_page(html: str) -> bool:
    h = html.lower()
    return (
        "verifying your browser" in h
        or "activati javascript" in h
        or "enable javascript and cookies" in h
    )


def _pdf_link_count_page(driver: webdriver.Chrome) -> int:
    try:
        n = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='.pdf']"))
        if n:
            return n
        n = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='.PDF']"))
        if n:
            return n
        xp = (
            "//a[contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),'.pdf')]"
        )
        return len(driver.find_elements(By.XPATH, xp))
    except Exception:
        return 0


def set_random_user_agent(driver: webdriver.Chrome, user_agent: str) -> None:
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd(
        "Network.setUserAgentOverride",
        {
            "userAgent": user_agent,
            "acceptLanguage": "ro-RO,ro;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )


def fetch_html(
    driver: webdriver.Chrome,
    url: str,
    *,
    settle_seconds: float = 3.0,
    wait_for_content: bool = False,
    list_wait_timeout: float | None = None,
    stadiu_dosar_page: bool = False,
) -> str:
    try:
        driver.get(url)
    except TimeoutException:
        pass

    if wait_for_content:
        timeout = float(
            list_wait_timeout if list_wait_timeout is not None else LIST_PAGE_WAIT_TIMEOUT
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            src = _page_source_safe(driver)
            if _is_bot_challenge_page(src):
                time.sleep(0.6)
                continue
            if _pdf_link_count_page(driver) > 0:
                break
            time.sleep(0.4)
        if _pdf_link_count_page(driver) == 0:
            try:
                driver.refresh()
            except TimeoutException:
                pass
            time.sleep(2.0)
            deadline2 = time.monotonic() + min(45.0, timeout)
            while time.monotonic() < deadline2:
                src = _page_source_safe(driver)
                if _is_bot_challenge_page(src):
                    time.sleep(0.6)
                    continue
                if _pdf_link_count_page(driver) > 0:
                    break
                time.sleep(0.4)

    if settle_seconds > 0:
        time.sleep(settle_seconds)

    html = driver.page_source or ""
    if wait_for_content and _pdf_link_count_page(driver) == 0:
        if _is_bot_challenge_page(html):
            log.warning(
                "stadiu-dosar: похоже anti-bot. Увеличьте LIST_PAGE_WAIT_TIMEOUT или HEADLESS=0."
            )
        elif stadiu_dosar_page and "articolul-11-tab" not in html.lower():
            log.warning(
                "stadiu-dosar: нет id articolul-11-tab и нет ссылок .pdf — проверьте HTML."
            )
    return html


def download_pdf_to_dir(
    driver: webdriver.Chrome,
    download_dir: Path,
    pdf_url: str,
    *,
    timeout: float | None = None,
) -> Path:
    timeout = timeout if timeout is not None else float(PAGE_LOAD_TIMEOUT)
    before = {p.resolve() for p in download_dir.glob("*")}

    try:
        driver.get(pdf_url)
    except TimeoutException:
        pass

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cr = list(download_dir.glob("*.crdownload"))
        pdfs: list[Path] = []
        for p in download_dir.glob("*.pdf"):
            if p.resolve() in before:
                continue
            try:
                if p.stat().st_size > 0:
                    pdfs.append(p)
            except OSError:
                continue
        if pdfs and not cr:
            return max(pdfs, key=lambda p: p.stat().st_mtime)
        time.sleep(0.25)

    raise TimeoutError(f"PDF не появился в {download_dir} за {timeout}s: {pdf_url}")
