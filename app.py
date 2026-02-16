from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session, after_this_request
import os
import asyncio
from scrapers.niggrid_scraper import run_scraper
from scrapers.flight_processor import process_flight_files
from scrapers.cargo_processor import process_cargo_files
from scrapers.weekly_flight_processor import process_weekly_flights
import io


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

@app.route('/flight_data', methods=['GET', 'POST'])
def flight_tool():
    if request.method == 'POST':
        try:
            # 1. Get Inputs
            target_month = request.form.get('month')
            target_year = request.form.get('year')
            uploaded_files = request.files.getlist('files') # Get multiple files
            
            if not uploaded_files or uploaded_files[0].filename == '':
                flash("No files selected!", "error")
                return redirect(url_for('flight_tool'))

            # 2. Process
            filename = process_flight_files(uploaded_files, target_month, target_year, DOWNLOAD_FOLDER)
            
            if filename:
                session['latest_flight_file'] = filename
                flash("Processing completed successfully!", "success")
                return redirect(url_for('flight_tool'))
                # return redirect(url_for('download_flight', filename=filename))
                # return send_file(os.path.join(DOWNLOAD_FOLDER, filename), as_attachment=True)
            else:
                flash("Processing failed or files were empty.", "error")
                return redirect(url_for('flight_tool'))

        except Exception as e:
            flash(f"Error: {str(e)}", "error")
            return redirect(url_for('flight_tool'))

    # session.pop('latest_flight_file', None)
    return render_template('flight_data.html')

@app.route('/download_flight')
def download_flight():
    filename = session.get('latest_flight_file')

    if not filename:
        flash("No file available for download.", "error")
        return redirect(url_for('flight_tool'))

    filepath = os.path.join(DOWNLOAD_FOLDER, filename)

    # Read file into memory FIRST
    with open(filepath, "rb") as f:
        file_data = f.read()

    try:
        os.remove(filepath)
    except Exception as e:
        print("Delete error:", e)

    session.pop('latest_flight_file', None)

    return send_file(
        io.BytesIO(file_data),
        download_name=filename,
        as_attachment=True
    )

@app.route('/cargo_manifest', methods=['GET', 'POST'])
def cargo_tool():
    if request.method == 'POST':
        try:
            uploaded_files = request.files.getlist('files')
            
            if not uploaded_files or uploaded_files[0].filename == '':
                flash("No PDF files selected!", "error")
                return redirect(url_for('cargo_tool'))

            # Process
            zip_filename = process_cargo_files(uploaded_files, DOWNLOAD_FOLDER)
            
            if zip_filename:
                session['latest_cargo_file'] = zip_filename
                flash("Processing completed successfully!", "success")
                return redirect(url_for('cargo_tool'))
                # return send_file(os.path.join(DOWNLOAD_FOLDER, zip_filename), as_attachment=True)
            else:
                flash("Processing failed. Please check if PDFs contain valid tables.", "error")
                return redirect(url_for('cargo_tool'))

        except Exception as e:
            flash(f"System Error: {str(e)}", "error")
            return redirect(url_for('cargo_tool'))

    return render_template('cargo_manifest.html')

@app.route('/download_cargo')
def download_cargo():
    filename = session.get('latest_cargo_file')
    if not filename:
        flash("No file available for download.", "error")
        return redirect(url_for('cargo_tool'))

    filepath = os.path.join(DOWNLOAD_FOLDER, filename)

    # Read file into memory FIRST
    with open(filepath, "rb") as f:
        file_data = f.read()

    try:
        os.remove(filepath)
    except Exception as e:
        print("Delete error:", e)

    session.pop('latest_cargo_file', None)

    return send_file(
        io.BytesIO(file_data),
        download_name=filename,
        as_attachment=True
    )

@app.route('/weekly_flight_data', methods=['GET', 'POST'])
def weekly_flight_tool():
    if request.method == 'POST':
        try:
            uploaded_files = request.files.getlist('files')
            
            if not uploaded_files or uploaded_files[0].filename == '':
                flash("No files uploaded!", "error")
                return redirect(url_for('weekly_flight_tool'))

            # Process
            filename = process_weekly_flights(uploaded_files, DOWNLOAD_FOLDER)
            
            if filename:
                return send_file(os.path.join(DOWNLOAD_FOLDER, filename), as_attachment=True)
            else:
                flash("Processing failed. Please check files.", "error")
                return redirect(url_for('weekly_flight_tool'))

        except Exception as e:
            flash(f"System Error: {str(e)}", "error")
            return redirect(url_for('weekly_flight_tool'))

    return render_template('weekly_flight_data.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)