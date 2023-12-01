from flask import Flask, request

from imports.sparkDataTransformation import DataTransformation

app = Flask(__name__)


@app.route('/get_data', methods=['POST'])
def get_data():
    data = request.get_json()

    # Extracting data
    inputKeywords = data.get('keywords')
    inputFieldOfInvention = data.get('fieldOfInvention')
    inputDate = data.get('date')

    json_array = dt.fetchData(inputKeywords, inputFieldOfInvention)

    return json_array


if __name__ == '__main__':
    dt = DataTransformation()
    app.run(debug=True)
