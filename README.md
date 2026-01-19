# poi-extract

Extract Points of Interest (POIs) from Overture Maps data for any location in Dubai.

To get started, use `uv sync` after cloning the repository or any other python dependency manager that supports pyproject.toml.

## Usage

### Command Line Script

```bash
# Basic usage (20km radius by default)
python extract_pois.py 25.166974 55.259068

# Custom radius (10km)
python extract_pois.py 25.166974 55.259068 --radius 10

# Custom output filename
python extract_pois.py 25.166974 55.259068 --output my_places.csv

# Force reload data from S3 (ignore cache)
python extract_pois.py 25.166974 55.259068 --reload

# Override S3 path (if default release is outdated)
python extract_pois.py 25.166974 55.259068     --s3-path 's3://overturemaps-us-west-2/release/2026-01-15.0/theme=places/type=place/*.parquet'

# All options
python extract_pois.py 25.166974 55.259068     --radius 15     --output nearby.csv     --database custom_cache.duckdb     --s3-path 's3://overturemaps-us-west-2/release/2026-01-15.0/theme=places/type=place/*.parquet'
```

## Output Format

The script generates a CSV with the following columns:

- `id`: Overture Maps place ID
- `primary_name`: Place name
- `primary_category`: Detailed category (e.g., indian_restaurant)
- `master_category`: High-level category (e.g., restaurant)
- `lat`, `lon`: Coordinates
- `distance_km`: Distance from target location (Haversine)
- `distance_m`: Distance in meters
- `alternate_categories`: Alternative category tags

## Master Categories

Places are automatically categorized into:

- **residential**: Apartments, housing, condominiums
- **commercial**: Offices, businesses, real estate
- **restaurant**: All food & beverage establishments
- **retail**: Shops, stores, malls, supermarkets
- **healthcare**: Hospitals, clinics, pharmacies
- **education**: Schools, universities, libraries
- **entertainment**: Gyms, parks, museums, cinemas
- **hotel**: Hotels, resorts, accommodations
- **transportation**: Stations, airports, parking, fuel
- **religious**: Mosques, churches, temples
- **beauty_personal_care**: Salons, spas, barbers
- **services**: Travel, marketing, financial services
- **landmark**: Tourist attractions, monuments
- **other**: Uncategorized places

Some of these may be out of date so the categorization may not be entirely accurate but I tried to do my best with whatever I can.

## Data Source

- [Overture Maps Foundation](https://overturemaps.org/)

## Notes

- Distance calculations use Haversine formula (accurate for spherical Earth)
- Overture data may differ from Google Maps (different sources)
- Data is cached in `overture_dubai.duckdb` (~6MB since we fetch out UAE bounding box from `gcc_states`)
- Use `--reload` to refresh data from S3
