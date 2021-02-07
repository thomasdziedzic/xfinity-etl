# need to do this in selenium because xfinity puts custom js code in it's login page that will not log you in without running it!
# that's right, you need js enabled on your browser to login to xfinity! wtf
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import subprocess
import re
import time
import os
from datetime import datetime
import snowflake.connector

from dotenv import load_dotenv
load_dotenv()

options = webdriver.FirefoxOptions()
profile = webdriver.FirefoxProfile()

# because xfinity detects automation and prevents you from logging in
# https://github.com/jantman/xfinity-usage/issues/30
profile.set_preference('devtools.jsonview.enabled', False)
profile.set_preference("dom.webdriver.enabled", False)
profile.set_preference('useAutomationExtension', False)

# download pdfs instead of opening them up in the browser: https://stackoverflow.com/a/23801327
profile.set_preference('browser.download.folderList', 2)
profile.set_preference('browser.download.manager.showWhenStarting', False)
profile.set_preference('browser.download.dir', '/tmp')
profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/pdf')
profile.set_preference('pdfjs.disabled', True)
profile.update_preferences()

driver = webdriver.Firefox(firefox_profile=profile, options=options)

# xfinity displays popups asking for feedback which block clicking on certain elements breaking this script
# the amount of convoluted elements on this site never ceases to amaze me
# this script also makes the xfinity work better by not having to load all the ads on the site...
driver.install_addon(os.getenv('UBLOCK_ORIGIN_PATH'), temporary=True)

driver.get('https://login.xfinity.com/login')

time.sleep(10)

driver.find_element_by_id('user').send_keys(os.getenv('XFINITY_USERNAME'))
driver.find_element_by_id('passwd').send_keys(os.getenv('XFINITY_PASSWORD'))
driver.find_element_by_id('remember_me_checkbox').click()
driver.find_element_by_id('sign_in').click()

time.sleep(10)

driver.get('https://customer.xfinity.com/#/billing/past-statements')

time.sleep(20)

pdf_statements = driver.find_elements_by_css_selector('div.card-group div.card__action a')

# cleanup from previous runs
if os.path.exists('/tmp/statement.pdf'):
    os.remove('/tmp/statement.pdf')
if os.path.exists('/tmp/statement.txt'):
    os.remove('/tmp/statement.txt')

data = []

for pdf_statement in pdf_statements:
    print(f"processing {pdf_statement.get_attribute('aria-label')}")
    pdf_statement.click()

    # wait for the statement to download
    while not os.path.exists('/tmp/statement.pdf') or os.path.exists('/tmp/statement.pdf.part'):
        time.sleep(1)
    # delay reading the pdf a bit longer to give time for firefox to copy the file
    time.sleep(5)

    subprocess.run(['pdftotext', '/tmp/statement.pdf', '/tmp/statement.txt'], check=True)

    with open('/tmp/statement.txt') as f:
        statement_text = f.read()

    ret = re.search('Billing\ Date\n*(?P<billing_date>.*)', statement_text, re.IGNORECASE)
    billing_date = ret.group('billing_date')
    iso_date = datetime.strptime(billing_date, '%b %d, %Y').date().isoformat()

    ret = re.search('Please\ pay\n*[$](?P<amount_due>[0-9.]+)', statement_text, re.IGNORECASE | re.MULTILINE)
    amount_due = ret.group('amount_due')

    data.append({'iso_date': iso_date, 'amount_due': amount_due})

    if os.path.exists('/tmp/statement.pdf'):
        os.remove('/tmp/statement.pdf')
    if os.path.exists('/tmp/statement.txt'):
        os.remove('/tmp/statement.txt')

driver.close()

with snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USERNAME'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database = os.getenv('SNOWFLAKE_DATABASE'),
        schema = os.getenv('SNOWFLAKE_SCHEMA'),
        ) as con:
    cur = con.cursor()
    cur.execute('truncate load_db.xfinity.bills;')
    load_vals = ', '.join(f"('{val['iso_date']}', {val['amount_due']})" for val in data)
    load_stmt = f'insert into load_db.xfinity.bills(billing_date, amount_due) values {load_vals};'
    cur.execute(load_stmt)
    cur.execute('''
        merge into raw_db.xfinity.bills as trg using load_db.xfinity.bills as src on trg.billing_date = src.billing_date
        when not matched then insert (billing_date, amount_due) values (src.billing_date, src.amount_due);
    ''')
    cur.execute('truncate load_db.xfinity.bills;')

# TODO query snowflake to get the latest date we have available, and then only download the bills that we don't already have
