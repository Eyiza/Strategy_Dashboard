from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import os
import asyncio
from scrapers.scraper import run_scraper

app = Flask(__name__)
app.secret_key = 'super_secret_key' # Needed for flashing messages
DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')

# Create downloads folder if not exists
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/niggrid', methods=['GET', 'POST'])
def niggrid_tool():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        
        try:
            # Run the async scraper from sync Flask code
            filename = asyncio.run(run_scraper(start_date, end_date, DOWNLOAD_FOLDER))
            
            if filename:
                return send_file(os.path.join(DOWNLOAD_FOLDER, filename), as_attachment=True)
            else:
                flash("No data found for that range.", "error")
                return redirect(url_for('niggrid_tool'))
                
        except Exception as e:
            flash(f"Error running script: {str(e)}", "error")
            return redirect(url_for('niggrid_tool'))

    return render_template('niggrid.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)