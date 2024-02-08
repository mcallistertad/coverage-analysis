__version__ = "1.3.0"
__maintainer__ = "Tad McAllister"
__email__ = "thaddeus.mcallister@vodafone.com"
__status__ = "test"

import errno
import argparse
import csv
import os
import rasterio
import rasterio.warp
from tqdm import tqdm

# Define lowest and highest coverage levels
MIN_COVERAGE = -108
MAX_COVERAGE = -80

# CSV processing
BATCH_SIZE = 20

# Define RGB to dBm mapping
RGB_TO_DBM = {
    (207, 99, 103): -80,
    (234, 104, 102): -90,
    (243, 172, 103): -100,
    (248, 209, 191): -108
}

def transform_coordinates(coordinates, src_crs):
    """Transform latitude and longitude to raster file coordinate system"""
    lat, lon = map(float, coordinates.split(","))
    x, y = rasterio.warp.transform({'init': 'EPSG:4326'}, src_crs, [lon], [lat])
    return x[0], y[0]

def get_pixel_location(coordinates, src):
    """Get pixel location corresponding to transformed coordinates"""
    return src.index(coordinates[0], coordinates[1])

def get_rgb_values(pixel_location, src):
    """Get RGB values at specified pixel location"""
    red_band = src.read(1)
    green_band = src.read(2)
    blue_band = src.read(3)
    red_value = red_band[pixel_location[0], pixel_location[1]]
    green_value = green_band[pixel_location[0], pixel_location[1]]
    blue_value = blue_band[pixel_location[0], pixel_location[1]]
    return red_value, green_value, blue_value

def get_closest_rgb(pixel_rgb):
    """Find closest RGB value from known RGBs"""
    if pixel_rgb == (255, 255, 255):
        return None  # Return None for white, indicating no coverage or undefined RSRP value

    return min(RGB_TO_DBM.keys(), key=lambda x: sum((a-b)**2 for a, b in zip(x, pixel_rgb)))

def interpolate_rsrp_value(min_rsrp, max_rsrp, min_val, max_val, current_val, method=None):
    """Interpolate RSRP value between two known RSRP values based on current value"""
    if min_val == max_val or method is None:
        return min_rsrp  # No intermediate values or no interpolation method specified, return minimum RSRP
    
    if method == "linear":
        return min_rsrp + (max_rsrp - min_rsrp) * ((current_val - min_val) / (max_val - min_val))
    elif method == "average":
        return (min_rsrp + max_rsrp) / 2  # Use average of the min and max RSRP values as interpolation
    else:
        raise ValueError("Invalid interpolation method. Supported methods are 'linear' and 'average'.")

def get_coverage_level(coordinates, src, interpolation=None):
    """Get coverage level at specified coordinates in the tif file"""
    src_crs = src.crs
    coordinates = transform_coordinates(coordinates, src_crs)
    pixel_location = get_pixel_location(coordinates, src)

    # Check if transformed pixel coordinates are within raster bounds
    if not (0 <= pixel_location[1] < src.width and 0 <= pixel_location[0] < src.height):
        print(f"Error: Coordinates '{coordinates}' are out of bounds.")
        return None

    try:
        # Get RGB values at specified location
        pixel_rgb = get_rgb_values(pixel_location, src)

        if pixel_rgb == (255, 255, 255):
            return None

        # Find the closest RGB match
        closest_rgb = get_closest_rgb(pixel_rgb)

        # If closest_rgb is None, return None indicating no coverage or undefined RSRP
        if closest_rgb is None:
            return None

        closest_rsrp = RGB_TO_DBM.get(closest_rgb, MIN_COVERAGE)

        if closest_rsrp == MAX_COVERAGE:
            return MAX_COVERAGE

        if interpolation:
            # Interpolate RSRP value between closest and next closest RSRP values
            min_rsrp = MAX_COVERAGE if closest_rsrp == MIN_COVERAGE else closest_rsrp
            max_rsrp = MIN_COVERAGE
            for rsrp in sorted(RGB_TO_DBM.values()):
                if min_rsrp < rsrp < MAX_COVERAGE:
                    max_rsrp = rsrp
                    break
            interpolated_rsrp = interpolate_rsrp_value(min_rsrp, max_rsrp, RGB_TO_DBM[closest_rgb], max_rsrp, closest_rsrp, method=interpolation)
            return interpolated_rsrp

        return closest_rsrp
    except (IndexError, KeyError, TypeError) as e:
        print(f"Error occurred while processing coordinates '{coordinates}': {e}")
        return None

def process_row(row, src):
    """Process single row from csv file"""
    coordinates = [coord.strip() for coord in row[:2] if coord.strip()]
    if len(coordinates) != 2:
        print(f"Error: Coordinates not valid '{coordinates}'")
        return None  # Skip processing if coordinates not valid

    try:
        # Check if coordinates can be converted to floats
        coverage_level = get_coverage_level(",".join(coordinates), src)
        if coverage_level is not None:
            return coordinates + [coverage_level]
    except ValueError:
        pass  # Skip processing if coordinates are not valid floats
    except Exception as e:
        print(f"Error processing coordinates '{coordinates}': {e}")
    return None

def write_batch(rows, csv_writer):
    """Write a batch of rows to the CSV file."""
    for row in rows:
        if row is None:  # Check if row is None
            csv_writer.writerow(["Null"] * 3)  # Write "Null" for all columns
        elif row[2] == "Null":
            csv_writer.writerow(row[:2] + ["Null"])  # Write "Null" directly
        else:
            try:
                csv_writer.writerow(row[:2] + [int(row[2])])  # Format RSRP as integer
            except ValueError:
                print(f"Error: Invalid RSRP value '{row[2]}'")

def process_csv_chunk(chunk, src, progress_bar):
    """Process chunk of rows from the CSV file"""
    results = []
    for row in chunk:
        try:
            result = process_row(row, src)
            if result is not None:
                results.append(result)
                progress_bar.update(1)
            else:
                results.append(None)  # Append None if process_row returns None
                progress_bar.update(1)
        except Exception as e:
            print(f"Error processing row '{row}': {e}")
            results.append(None)  # Append None if an error occurs during processing
            progress_bar.update(1)
    return results

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Get coverage level at specified coordinates in a GeoTIFF file.")
    parser.add_argument("--geotiff", "-g", help="Path to the GeoTIFF file", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--coordinates", "-c", help="Latitude and longitude coordinates separated by comma (e.g., '53.2716088,-6.2073869')")
    group.add_argument("--csv", "-f", help="Path to the CSV file")
    parser.add_argument("--interpolation", "-i", help="Interpolation method for RSRP values. Supported methods are 'linear' and 'average'. If not provided, no interpolation is performed.", choices=["linear", "average"])
    args = parser.parse_args()

    # Check if GeoTIFF file is accessible
    try:
        with open(args.geotiff, 'rb') as f:
            pass
    except IOError as e:
        if e.errno == errno.EACCES:
            parser.error(f"GeoTIFF file '{args.geotiff}' is inaccessible due to permission issues.")
        else:
            parser.error(f"Failed to open GeoTIFF file '{args.geotiff}': {e}")

    if not args.coordinates and not args.csv:
        parser.error("Either coordinates or a CSV file must be provided.")

    if args.coordinates:
        coordinates = args.coordinates
        # Process single set of coordinates
        with rasterio.open(args.geotiff) as src:
            coverage_level = get_coverage_level(coordinates, src)
            if coverage_level is not None:
                print(f"Coverage level at coordinates {coordinates}: {int(coverage_level)} dBm")
            elif coverage_level is None:
                print(f"No coverage at coordinates {coordinates}")
    
    elif args.csv:
        # Check if CSV file is accessible
        try:
            with open(args.csv, 'r', encoding='utf-8') as f:
                pass
        except IOError as e:
            if e.errno == errno.EACCES:
                parser.error(f"CSV file '{args.csv}' is inaccessible due to permission issues.")
            else:
                parser.error(f"Failed to open CSV file '{args.csv}': {e}")

        # Process coordinates from CSV file
        if not os.path.isfile(args.csv):
            parser.error(f"CSV file '{args.csv}' does not exist.")
        
        # Create output CSV file
        output_file = os.path.splitext(args.csv)[0] + "_coverage_prediction.csv"
        with rasterio.open(args.geotiff) as src:
            with open(args.csv, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader)  # Read and skip header row
                if [h.lower() for h in header[:2]] != ["latitude", "longitude"]:
                    print("Warning: The first row of the CSV does not contain 'Latitude' and 'Longitude' headers. Exiting...")
                    return
                
                # Create progress bar
                total_rows = sum(1 for _ in csv_reader)  # Count total number of rows in CSV file
                progress_bar = tqdm(total=total_rows, desc="Processing Rows", unit="row")
                csv_file.seek(0)  # Reset file pointer
                next(csv_reader)  # Skip header row

                # Process CSV file in chunks
                with open(output_file, 'w', newline='') as output_csv_file:
                    csv_writer = csv.writer(output_csv_file)
                    csv_writer.writerow(["Latitude", "Longitude", "RSRP"])  # Write header row
                    chunk = []
                    for row in csv_reader:
                        chunk.append(row)
                        if len(chunk) >= BATCH_SIZE:
                            results = process_csv_chunk(chunk, src, progress_bar)
                            write_batch(results, csv_writer)
                            chunk = []

                    # Process remaining rows
                    if chunk:
                        results = process_csv_chunk(chunk, src, progress_bar)
                        write_batch(results, csv_writer)

                # Close progress bar
                progress_bar.close()

if __name__ == "__main__":
    main()