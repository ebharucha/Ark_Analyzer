from flask import Flask, render_template, request
import os
import importlib
import ark_analyzer as ark

app = Flask(__name__)

@app.route('/')
def home():
    return render_template("index.html", ark=ark.ark, df=ark.df)

# app.run(host='0.0.0.0', port=8080, debug=True)