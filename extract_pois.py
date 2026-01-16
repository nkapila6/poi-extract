#!/usr/bin/env python3
"""
Extract POIs from Overture Maps data near a given location.

Usage:
    python extract_pois.py <latitude> <longitude> [--radius RADIUS] [--output OUTPUT]

Example:
    python extract_pois.py 25.166974 55.259068 --radius 20 --output nearby_places.csv
"""

import argparse
import sys
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from geopy.distance import geodesic
from shapely import wkt
from shapely.geometry import Point


def categorize_place(category):
    """Categorize places into master categories based on Overture categories."""
    if pd.isna(category):
        return 'other'
    
    category = category.lower()
    
    # Residential
    if any(x in category for x in ['residential', 'apartment', 'housing', 'home', 'condominium', 'holiday_rental']):
        return 'residential'
    
    # Commercial/Office
    elif any(x in category for x in ['office', 'business', 'commercial', 'company', 'corporate', 'professional_services', 'real_estate']):
        return 'commercial'
    
    # Restaurant/Food & Beverage
    elif any(x in category for x in ['restaurant', 'cafe', 'coffee', 'food', 'dining', 'bakery', 'bar', 'pub', 'fast_food', 'eatery', 'pizza', 'burger', 'seafood', 'indian', 'chinese', 'sushi', 'steakhouse', 'buffet', 'diner', 'bistro', 'gastropub', 'ice_cream', 'dessert', 'smoothie', 'juice', 'tea']):
        return 'restaurant'
    
    # Retail/Shopping
    elif any(x in category for x in ['shop', 'store', 'retail', 'mall', 'market', 'boutique', 'supermarket', 'clothing', 'jewelry', 'furniture', 'electronics', 'mobile_phone', 'cosmetic', 'beauty_supplies', 'shoe', 'wholesale', 'department_store', 'convenience']):
        return 'retail'
    
    # Healthcare
    elif any(x in category for x in ['hospital', 'clinic', 'medical', 'pharmacy', 'health', 'doctor', 'dentist', 'dental', 'surgeon', 'diagnostic', 'therapy', 'wellness']):
        return 'healthcare'
    
    # Education
    elif any(x in category for x in ['school', 'university', 'college', 'education', 'library', 'training', 'preschool', 'tutoring', 'language_school']):
        return 'education'
    
    # Entertainment & Recreation
    elif any(x in category for x in ['entertainment', 'cinema', 'theater', 'museum', 'park', 'recreation', 'sport', 'gym', 'beach', 'amusement', 'club', 'dance', 'yoga', 'fitness', 'pool', 'arcade']):
        return 'entertainment'
    
    # Hotel/Accommodation
    elif any(x in category for x in ['hotel', 'accommodation', 'lodge', 'hostel', 'motel', 'resort', 'guest_house', 'bed_and_breakfast']):
        return 'hotel'
    
    # Transportation
    elif any(x in category for x in ['transport', 'station', 'airport', 'parking', 'fuel', 'gas_station', 'car_rental', 'automotive', 'car_dealer', 'bus', 'metro', 'taxi']):
        return 'transportation'
    
    # Religious
    elif any(x in category for x in ['mosque', 'church', 'temple', 'religious', 'worship', 'cathedral']):
        return 'religious'
    
    # Beauty & Personal Care
    elif any(x in category for x in ['salon', 'spa', 'beauty', 'barber', 'massage', 'nail', 'hair', 'cosmetic']):
        return 'beauty_personal_care'
    
    # Services
    elif any(x in category for x in ['service', 'agency', 'travel', 'tour', 'event', 'marketing', 'advertising', 'cleaning', 'printing', 'lawyer', 'financial', 'insurance', 'banking']):
        return 'services'
    
    # Landmarks & Attractions
    elif any(x in category for x in ['landmark', 'monument', 'historical', 'tourism', 'attraction']):
        return 'landmark'
    
    else:
        return 'other'


def load_overture_data(db_path='overture_dubai.duckdb', force_reload=False, s3_path=None):
    """
    Load Overture Maps data for Dubai region.
    Uses cached DuckDB file if available, otherwise downloads from S3.
    
    Args:
        db_path: Path to DuckDB cache file
        force_reload: Force reload from S3 even if cache exists
        s3_path: Override S3 path (e.g., 's3://overturemaps-us-west-2/release/2025-12-17.0/theme=places/type=place/*.parquet')
    """
    db_file = Path(db_path)
    con = duckdb.connect(str(db_file))
    
    tables = con.execute("SHOW TABLES").fetchall()
    has_data = any('dubai_places' in str(t) for t in tables)
    
    if has_data and not force_reload:
        print(f"Loading cached data from {db_path}...")
        return con
    
    print("Downloading Overture Maps data from S3...")
    print("This may take a few minutes on first run...")
    
    con.execute("INSTALL spatial")
    con.execute("INSTALL httpfs")
    con.execute("LOAD spatial")
    con.execute("LOAD httpfs")
    con.execute("SET s3_region='us-west-2'")
    
    if s3_path is None:
        s3_path = 's3://overturemaps-us-west-2/release/2025-12-17.0/theme=places/type=place/*.parquet'
    
    print(f"Using S3 path: {s3_path}")
    
    query = f"""
    CREATE OR REPLACE TABLE dubai_places AS
    SELECT
        id,
        names.primary AS primary_name,
        categories.primary AS primary_category,
        categories.alternate AS alternate_categories,
        ST_AsText(geometry) as geometry,
        bbox
    FROM
        read_parquet('{s3_path}', filename=true, hive_partitioning=1)
    WHERE
        bbox.xmin >= 54.9
        and bbox.xmax <= 55.6
        and bbox.ymin >= 24.7
        and bbox.ymax <= 25.4
    """
    
    con.execute(query)
    
    count = con.execute("SELECT COUNT(*) FROM dubai_places").fetchone()[0]
    print(f"Cached {count:,} places to {db_path}")
    
    return con


def haversine_distance(geom, target_lat, target_lon):
    """Calculate Haversine distance in meters between two points."""
    point_lat, point_lon = geom.y, geom.x
    return geodesic((point_lat, point_lon), (target_lat, target_lon)).meters


def extract_nearby_places(lat, lon, radius_km=20, db_path='overture_dubai.duckdb', s3_path=None):
    """
    Extract places near a given location.
    
    Args:
        lat: Target latitude
        lon: Target longitude
        radius_km: Search radius in kilometers
        db_path: Path to DuckDB cache file
        s3_path: Override S3 path for Overture Maps data
    
    Returns:
        GeoDataFrame with nearby places and calculated distances
    """
    con = load_overture_data(db_path, s3_path=s3_path)
    
    print(f"\nSearching for places within {radius_km}km of ({lat}, {lon})...")
    
    dubai_places = con.execute("SELECT * FROM dubai_places").df()
    con.close()
    
    print(f"Loaded {len(dubai_places):,} places from cache")
    
    dubai_places['geometry'] = dubai_places['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(dubai_places, geometry='geometry', crs='EPSG:4326')
    
    target_point = Point(lon, lat)
    
    print("Calculating distances...")
    
    gdf_metric = gdf.to_crs('EPSG:3857')
    target_metric = gpd.GeoSeries([target_point], crs='EPSG:4326').to_crs('EPSG:3857')[0]
    gdf_metric['euclidean_m'] = gdf_metric.geometry.distance(target_metric)
    gdf['euclidean_m'] = gdf_metric['euclidean_m']
    
    gdf['haversine_m'] = gdf['geometry'].apply(
        lambda x: haversine_distance(x, lat, lon)
    )
    
    nearby = gdf[gdf['haversine_m'] <= radius_km * 1000].copy()
    nearby['euclidean_km'] = (nearby['euclidean_m'] / 1000).round(2)
    nearby['haversine_km'] = (nearby['haversine_m'] / 1000).round(2)
    nearby = nearby.sort_values('haversine_m')
    
    print("Categorizing places...")
    nearby['master_category'] = nearby['primary_category'].apply(categorize_place)
    
    nearby['lat'] = nearby['geometry'].apply(lambda x: x.y)
    nearby['lon'] = nearby['geometry'].apply(lambda x: x.x)
    
    print(f"\nFound {len(nearby):,} places within {radius_km}km")
    print("\n=== CATEGORY BREAKDOWN ===")
    print(nearby['master_category'].value_counts())
    
    return nearby


def main():
    parser = argparse.ArgumentParser(
        description='Extract POIs from Overture Maps near a location',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_pois.py 25.166974 55.259068
  python extract_pois.py 25.166974 55.259068 --radius 10
  python extract_pois.py 25.166974 55.259068 --radius 20 --output my_places.csv
  python extract_pois.py 25.166974 55.259068 --reload
        """
    )
    
    parser.add_argument('latitude', type=float, help='Target latitude')
    parser.add_argument('longitude', type=float, help='Target longitude')
    parser.add_argument('-r', '--radius', type=float, default=20,
                        help='Search radius in kilometers (default: 20)')
    parser.add_argument('-o', '--output', type=str, default='nearby_places.csv',
                        help='Output CSV filename (default: nearby_places.csv)')
    parser.add_argument('-d', '--database', type=str, default='overture_dubai.duckdb',
                        help='DuckDB cache file path (default: overture_dubai.duckdb)')
    parser.add_argument('--s3-path', type=str, default=None,
                        help='Override S3 path for Overture Maps data (e.g., s3://overturemaps-us-west-2/release/YYYY-MM-DD.0/theme=places/type=place/*.parquet)')
    parser.add_argument('--reload', action='store_true',
                        help='Force reload data from S3 (ignore cache)')
    
    args = parser.parse_args()
    
    if not -90 <= args.latitude <= 90:
        print(f"Error: Invalid latitude {args.latitude}. Must be between -90 and 90.")
        sys.exit(1)
    
    if not -180 <= args.longitude <= 180:
        print(f"Error: Invalid longitude {args.longitude}. Must be between -180 and 180.")
        sys.exit(1)
    
    if args.radius <= 0:
        print(f"Error: Invalid radius {args.radius}. Must be greater than 0.")
        sys.exit(1)
    
    if args.reload:
        db_file = Path(args.database)
        if db_file.exists():
            print(f"Removing existing cache: {args.database}")
            db_file.unlink()
    
    try:
        nearby = extract_nearby_places(
            args.latitude,
            args.longitude,
            args.radius,
            args.database,
            args.s3_path
        )
        
        output_columns = [
            'id',
            'primary_name',
            'primary_category',
            'master_category',
            'lat',
            'lon',
            'haversine_km',
            'haversine_m',
            'euclidean_km',
            'euclidean_m',
            'alternate_categories'
        ]
        
        output_df = nearby[output_columns]
        output_df.to_csv(args.output, index=False)
        
        print(f"\n✓ Saved {len(output_df):,} places to {args.output}")
        print(f"\nTop 10 nearest places:")
        print(output_df[['primary_name', 'master_category', 'haversine_km', 'euclidean_km']].head(10).to_string(index=False))
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
