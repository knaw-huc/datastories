from flask import Flask, Response, render_template, request, flash, redirect, url_for, make_response
import sqlite3 as sql
import os
from werkzeug.utils import secure_filename
from insert_json import upload_json


UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'json'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    result = ''
    filename = ''
    response = make_response(render_template('upload.html',result=result),200)
    #response.status = '200'
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        filename = file.filename
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            upload_file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            print(upload_file)
            file.save(upload_file)
            res = upload_json(upload_file)
            if res=='FAILED':
                print('upload failed')
                result = f'Upload {filename} FAILED!'
                response = make_response(render_template('upload.html',result=result),201)
                #response.status = '201'
            else:
                print('upload succeeded')
                os.remove(upload_file)
                result = f'Uploaded {filename} in result_{res}'
                response = make_response(render_template('upload.html',result=result),201)
                #response.status = '201'
        else:
            print('upload failed')
            result = f'Upload {filename} FAILED!'
            response = make_response(render_template('upload.html',result=result),201)
            #response.status = '201'
    return response

if __name__ == "__main__":
    app.run()


