# NYTimesSource

## Manual

List of required packages is in `requirements.txt` file.

## Usage

There is class `NYTimesSource` in the `data_loader.py` file. `NYTimesSource` class requires two arguments to be set
after the object is created:

- `api_response_file` path to json file,
- `reference_data_file` path to xlsx file.

Files require the same structure as in the examples provided: `api_response.json` and `reference_data.xlsx`.

## Test run

1. Install required packages.
2. Run the `data_loader.py` file (e.g. command: `python data_loader.py`).