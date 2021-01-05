from flask import Flask, render_template, request
import os
import ark_analyzer

app = Flask(__name__)

@app.route('/')
def home():
    (ark, df) = ark_analyzer.main()
    return render_template("index.html", ark=ark, df=df)

# app.run(host='0.0.0.0', port=8080, debug=True)