## Strategy_Dashboard
A Flask web application that allows users to run a scraper and download the results as a CSV file.

### Features
- Run a scraper to collect data from a specified source.
- Download the collected data as a CSV file.
- Flash messages to inform users about the status of their actions.

### Installation
1. Clone the repository:
```bash
git clone https://github.com/yourusername/Strategy_Dashboard.git
```
2. Navigate to the project directory:
```bash
cd Strategy_Dashboard
```
3. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate 
# On Windows use `venv\Scripts\activate`
```
4. Install the required dependencies:
```bash
pip install -r requirements.txt
```
**Note**: To add the required dependencies, you can run the following command after installing the necessary packages:
```bash
pip freeze > requirements.txt
```
5. Allow Playwright to install the necessary browsers:
```bash
playwright install
playwright install-deps
```

### Usage
1. Run the Flask application:
```bash
python app.py
```
2. Open your web browser and navigate to `http://127.0.0.1:5000/`.
3. Click the "Run Scraper" button to start the scraper.
4. After the scraper finishes, click the "Download CSV" button to download the results.
