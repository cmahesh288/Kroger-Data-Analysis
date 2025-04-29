from flask import Flask, request, render_template, redirect, session, url_for
import os
import re
import pandas as pd
from dotenv import load_dotenv
import pyodbc
from pathlib import Path

import ssl

from basket_analysis import analysis_basket
from churn_prediction import analysis_churn

print(ssl.OPENSSL_VERSION)

app = Flask(__name__, template_folder='templates')
app.secret_key = '@dkjgfjgfhkj jxbjljv kjxgvljklkj'
dir_path = os.path.dirname(os.path.realpath(__file__))
UPLOAD_FOLDER = os.path.join(dir_path, 'static', 'files')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
SECRET_KEY = os.getenv("SECRET_KEY", os.urandom(24))

DRIVER = "{ODBC Driver 17 for SQL Server}"
CONN_STR = (
    f"DRIVER={DRIVER};"
    f"SERVER={DB_SERVER},1433;"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};PWD={DB_PASS};"
    "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=120;"
)


def get_conn():
    return pyodbc.connect(CONN_STR)


# ssl_ca='/Users/chandra/UC/projects/Kroger-Data-Analysis/DigiCertGlobalRootCA.crt.pem'
#
# config = {
#   'host':'ccgrp70.mysql.database.azure.com',
#   'user':'ccgrp70',
#   'password':'Cloudcomputing@123',
#   'database':'ccgrp70',
#   'port':3306,
#   # 'connect_timeout':50000,
#   'ssl_ca': ssl_ca,
#   'client_flags': [mysql.connector.ClientFlag.SSL],
#
# }

def get_https_url(item, data):
    return url_for(item, username=data, _external=True, _scheme='http')


@app.route('/', methods=['GET', 'POST'])
def homepage():
    msg = ''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
        conn = get_conn()
        print("connection established")
        cur = conn.cursor()
        print(username, password)
        cur.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password,))
        user = cur.fetchone()
        if user:
            session['loggedin'] = True
            session['username'] = username
            return redirect(get_https_url('profile', username))
        else:
            # Account doesnt exist
            msg = 'Incorrect username/password!'
    return render_template("homepage.html", msg=msg)


@app.route('/logout')
def logout():
    session.pop('username', None)
    return render_template("homepage.html")


@app.route('/register', methods=['GET', 'POST'])
def register():
    msg = ''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        conn = get_conn()
        cur = conn.cursor()
        print(username, password, email)
        cur.execute('SELECT * FROM users WHERE username = ?', username)
        user = cur.fetchone()
        if user:
            msg = 'Account already exists!'
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            msg = 'Invalid email address!'
        elif not re.match(r'[A-Za-z0-9]+', username):
            msg = 'Username must contain only characters and numbers!'
        elif not username or not password or not email:
            msg = 'Please fill out the form!'
        else:
            cur.execute('INSERT INTO users VALUES (?, ?, ?)', (username, password, email))
            conn.commit()
            session['loggedin'] = True
            session['username'] = username
            return redirect(get_https_url('profile', username))
        print("hai", msg)
    return render_template("register.html", msg=msg)


@app.route('/profile/<string:username>', methods=['GET', 'POST'])
def profile(username):
    if 'loggedin' in session:
        return render_template('profile.html', username=username)
    return redirect(get_https_url('homepage'))


@app.route('/Search', methods=['GET', 'POST'])
def Search():
    msg = ''
    if request.method == 'POST' and 'search' in request.form:
        print("came to search[POST]")
        number = request.form['search']
        if not re.match(r'\d+', number):
            msg = "enter a valid household number"
        else:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """SELECT 
    h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, p.PRODUCT_NUM, 
    p.DEPARTMENT, p.COMMODITY, t.SPEND, t.UNITS, t.STORE_R, 
    t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE, h.MARITAL, 
    h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, 
    h.HH_SIZE, h.CHILDREN
FROM [400_households] AS h 
JOIN [400_transactions] AS t ON h.HSHD_NUM = t.HSHD_NUM 
JOIN [400_products] AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM 
WHERE h.HSHD_NUM = ?
ORDER BY 
    h.HSHD_NUM, 
    CAST(t.BASKET_NUM AS INT),
    t.PURCHASE_DATE, 
    CAST(p.PRODUCT_NUM AS INT), 
    p.DEPARTMENT, 
    p.COMMODITY
""",
                number)
            data = cur.fetchall()
            if data:
                return render_template('Search.html', data=data)
            else:
                msg = "Not Data Found for the input "
                return render_template('Search.html', msg=msg)
        return render_template('Search.html', msg=msg)
    else:
        data = ""
        return render_template('Search.html', data=data)


@app.route('/dashboard')
def dashboard():
    return render_template("dashboard.html")


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    msg = ''
    if request.method == 'POST':
        hdata = request.files['400_households']
        tdata = request.files['400_transactions']
        pdata = request.files['400_products']
        conn = get_conn()
        cur = conn.cursor()
        if hdata.filename == '' or tdata.filename == '' or pdata.filename == '':
            msg = 'No Files passed'
            return render_template('upload.html', msg=msg)
        else:
            # household data
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], hdata.filename)
            hdata.save(file_path)
            col_names = ['HSHD_NUM', 'L', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE', 'HOMEOWNER', 'HSHD_COMPOSITION',
                         'HH_SIZE', 'CHILDREN']
            csvData = pd.read_csv(file_path, names=col_names, header=0)
            query = 'INSERT INTO 400_households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES'
            for i, row in csvData.iterrows():
                if (pd.isna(row['CHILDREN'])):
                    query += '{},'.format((row['HSHD_NUM'], row['L'], row['AGE_RANGE'], row['MARITAL'],
                                           row['INCOME_RANGE'], row['HOMEOWNER'], row['HSHD_COMPOSITION'],
                                           row['HH_SIZE'], 'null'))
                else:
                    query += '{},'.format((row['HSHD_NUM'], row['L'], row['AGE_RANGE'], row['MARITAL'],
                                           row['INCOME_RANGE'], row['HOMEOWNER'], row['HSHD_COMPOSITION'],
                                           row['HH_SIZE'], row['CHILDREN']))
            query = query[:len(query) - 1]
            cur.execute(query)

            # transaction data
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], tdata.filename)
            tdata.save(file_path)
            print("file path", file_path)
            col_names = ['BASKET_NUM', 'HSHD_NUM', 'PURCHASE_DATE', 'PRODUCT_NUM', 'SPEND', 'UNITS', 'STORE_R',
                         'WEEK_NUM', 'YEAR']
            csvData = pd.read_csv(file_path, names=col_names, header=0, nrows=10000)
            query = 'INSERT INTO 400_transactions (BASKET_NUM,PURCHASE_DATE,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR,HSHD_NUM,PRODUCT_NUM) VALUES'
            for i, row in csvData.iterrows():
                query += '{},'.format((row['BASKET_NUM'], row['PURCHASE_DATE'], row['SPEND'], row['UNITS'],
                                       row['STORE_R'], row['WEEK_NUM'], row['YEAR'], row['HSHD_NUM'],
                                       row['PRODUCT_NUM']))
            query = query[:len(query) - 1]
            cur.execute(query)

            # Products data
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], pdata.filename)
            pdata.save(file_path)
            col_names = ['PRODUCT_NUM', 'DEPARTMENT', 'COMMODITY', 'BRAND_TY', 'NATURAL_ORGANIC_FLAG']
            csvData = pd.read_csv(file_path, names=col_names, header=0)
            query = 'INSERT INTO 400_products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TYPE,NATURAL_ORGANIC_FLAG) VALUES'
            for i, row in csvData.iterrows():
                query += '{},'.format((row['PRODUCT_NUM'], row['DEPARTMENT'], row['COMMODITY'], row['BRAND_TY'],
                                       row['NATURAL_ORGANIC_FLAG']))
            query = query[:len(query) - 1]
            cur.execute(query)

            conn.commit()
            msg = 'Sucessfully Inserted data !!!!!'
            print(msg)
            return render_template("upload.html", msg=msg)
    else:
        msg = "unable to insert data"
        return render_template("upload.html")


@app.route("/analysis/basket", endpoint="analysis_basket")
def basket_view():
    score, img = analysis_basket()
    return render_template(
        "analysis_basket.html",
        score=score,
        image=img
    )


@app.route("/analysis/churn", endpoint="analysis_churn")
def churn_view():
    report, roc_auc, img = analysis_churn()
    return render_template(
        "analysis_churn.html",
        report=report,
        roc_auc=roc_auc,
        image=img
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)
