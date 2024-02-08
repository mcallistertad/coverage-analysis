# CoverageChecker

CoverageChecker allows users to determine cellular coverage levels at specific locations either individually or from a CSV file containing multiple coordinates. It uses a GeoTIFF file as a reference for coverage level data

## Installation

Use pip

```bash
pip3 install -r requirements. txt 
```

## Usage

```python
# returns single set of coordinates
python program_name.py --geotiff path_to_geotiff_file --coordinates "latitude,longitude"

# returns a csv file containing coverage information for given coordinates
python program_name.py --geotiff path_to_geotiff_file --csv path_to_csv_file
```
