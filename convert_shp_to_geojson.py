"""
Convert Shapefile to GeoJSON
Converts taxi_zones.shp to taxi_zones.geojson format
Used for converting spatial boundary data to web-compatible format
"""

import json
import os

def convert_shp_to_geojson():
    """Convert shapefile to GeoJSON using shapefile library"""
    shp_path = r"c:\Users\LENOVO\Downloads\insutech\Insurtech\data\raw\taxi_zones.shp"
    geojson_path = r"c:\Users\LENOVO\Downloads\insutech\Insurtech\data\processed\taxi_zones.geojson"
    
    try:
        import shapefile
    except ImportError:
        print("Installing shapefile library...")
        os.system("pip install shapefile --quiet")
        import shapefile
    
    try:
        # Read shapefile
        print(f"Reading shapefile: {shp_path}")
        sf = shapefile.Reader(shp_path)
        
        # Create GeoJSON structure
        geojson = {
            "type": "FeatureCollection",
            "features": []
        }
        
        # Get field names from shapefile
        field_names = [field[0] for field in sf.fields[1:]]  # Skip first dummy field
        
        print(f"Fields found: {field_names}")
        
        # Convert each shape to a GeoJSON feature
        for idx, shape in enumerate(sf.shapes()):
            record = sf.record(idx)
            
            # Create feature
            feature = {
                "type": "Feature",
                "properties": {field_names[i]: record[i] for i in range(len(field_names))},
                "geometry": {
                    "type": shape.shapeType if shape.shapeType != 5 else "Polygon",
                    "coordinates": shape.points if shape.shapeType != 5 else [shape.points]
                }
            }
            
            geojson["features"].append(feature)
        
        # Write GeoJSON file
        with open(geojson_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, indent=2)
        
        print(f"\n✅ Conversion successful!")
        print(f"   Total features: {len(geojson['features'])}")
        print(f"   Output: {geojson_path}")
        
        # Copy to raw folder too
        raw_geojson = r"c:\Users\LENOVO\Downloads\insutech\Insurtech\data\raw\taxi_zones.geojson"
        import shutil
        shutil.copy(geojson_path, raw_geojson)
        print(f"   Also copied to: {raw_geojson}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    convert_shp_to_geojson()
