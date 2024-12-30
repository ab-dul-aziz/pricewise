import numpy as np
import pandas as pd
import re

# ================== Helper Functions ==================

def rename_and_adjust_price(data: pd.DataFrame) -> pd.DataFrame:
    """Rename 'price' to 'price_mio' and convert values to millions."""
    data = data.rename(columns={"price": "price_mio"})
    data["price_mio"] = data["price_mio"] / 1_000_000
    return data

def classify_city(data: pd.DataFrame) -> pd.DataFrame:
    """Filter city names to only include Jabodetabek regions."""
    jabodetabek_keywords = ['Jakarta', 'Bogor', 'Depok', 'Tangerang', 'Bekasi']
    data['city'] = data['city'].apply(lambda x: x.split(', ')[1] if isinstance(x, str) and ', ' in x else x)
    data = data[data['city'].str.contains('|'.join(jabodetabek_keywords), case=False, na=False)]

    # Keywords for classification
    keywords = {
        r'jakarta': 'Jakarta',
        r'bogor': 'Bogor',
        r'depok': 'Depok',
        r'tangerang': 'Tangerang',
        r'bekasi': 'Bekasi'
    }

    # Step 1: Classification based on 'address' column
    for pattern, city_name in keywords.items():
        data.loc[
            data['city'].isna() & data['address'].str.contains(pattern, case=False, na=False),
            'city'
        ] = city_name

    # Step 2: Classification based on 'title' column
    for pattern, city_name in keywords.items():
        data.loc[
            data['city'].isna() & data['title'].str.contains(pattern, case=False, na=False),
            'city'
        ] = city_name

    # Step 3: Standardize the city column using regex
    for pattern, city_name in keywords.items():
        data['city'] = data['city'].str.replace(rf'.*{pattern}.*', city_name, case=False, regex=True)

    # Step 4: Remove any additional spaces and ensure consistency
    data['city'] = data['city'].str.strip()
    return data

def classify_property_type(data: pd.DataFrame) -> pd.DataFrame:
    """Classify property types based on predefined keywords."""
    # Daftar kata kunci
    keywords = {
        r'rumah|house|mansion': 'Rumah',
        r'apartment|apartmen|apartement|apartemen|kos|kost': 'Hunian Sewa',
        r'pabrik|kantor|office|ruko|ruang usaha|kios|kiosk|gudang': 'Ruang Usaha',
        r'tanah|lahan|kavling|gedung': 'Tanah dan Properti Lain'
    }

    # Normalisasi awal
    data['property_type'] = data['property_type'].str.lower().str.strip()

    # Klasifikasi berdasarkan 'property_type'
    for pattern, prop_type in keywords.items():
        data.loc[
            data['property_type'].str.contains(pattern, case=False, na=False),
            'property_type'
        ] = prop_type

    # Klasifikasi berdasarkan 'title'
    for pattern, prop_type in keywords.items():
        data.loc[
            data['property_type'].isna() & data['title'].str.contains(pattern, case=False, na=False),
            'property_type'
        ] = prop_type

    # Klasifikasi berdasarkan 'description'
    for pattern, prop_type in keywords.items():
        data.loc[
            data['property_type'].isna() & data['description'].str.contains(pattern, case=False, na=False),
            'property_type'
        ] = prop_type

    # Klasifikasi tipe rumah berdasarkan ukuran bangunan
    def determine_house_type(row):
        if row['property_type'] == 'Rumah':
            size = row.get('building_size_m2', None)
            if size is None: return 'Rumah Tipe Tidak Diketahui'
            elif size <= 21: return 'Rumah Tipe 21'
            elif 21 < size <= 36: return 'Rumah Tipe 36'
            elif 36 < size <= 45: return 'Rumah Tipe 45'
            elif 45 < size <= 54: return 'Rumah Tipe 54'
            elif 54 < size <= 60: return 'Rumah Tipe 60'
            elif 60 < size <= 70: return 'Rumah Tipe 70'
            elif 70 < size <= 120: return 'Rumah Tipe 120'
            else: return 'Rumah Tipe >120'
        return row['property_type']  # Return original property_type for non-Rumah rows

    # Update property_type with house_type where applicable
    data['property_type'] = data.apply(determine_house_type, axis=1)

    # Drop rows where property_type is 'Tanah dan Properti Lain', 'Hunian Sewa', or 'Ruang Usaha'
    data = data[(data['property_type'] != 'Tanah dan Properti Lain') &
                 (data['property_type'] != 'Hunian Sewa') &
                 (data['property_type'] != 'Ruang Usaha')]
    return data

def categorize_certificate(data: pd.DataFrame) -> pd.DataFrame:
    """Classify certificates into SHM, HGB, or Other."""
    def certificate(value):
        if pd.isna(value): return np.nan
        elif 'SHM' in value: return 'SHM'
        elif 'HGB' in value: return 'HGB'
        else: return 'Other'
    data['certificate'] = data['certificate'].apply(certificate)
    return data

def categorize_property_condition(data: pd.DataFrame) -> pd.DataFrame:
    """Classify property condition into categories."""
    def categorize(value):
        if pd.isnull(value): 
            return pd.NA
        value_lower = str(value).lower()

        renovated_keywords = ['renov', 'full renov', 'renovasi', 'renoved', 'renovasi baru', 'baru renovasi', 'finished', 'selesai renovasi', 'proses finishing']
        if any(keyword in value_lower for keyword in renovated_keywords): return 'Renovated'
        
        new_keywords = ['new', 'brand new', 'baru', 'unit baru', 'first time', 'primery', 'full baru', 'baru selesai', 'unit baru gress']
        if any(keyword in value_lower for keyword in new_keywords): return 'New'
        
        need_renovation_keywords = ['butuh renovasi', 'harus renovasi', 'setengah jadi', 'perlu renovasi', 'perlu perawatan', 'lama', 'tua']
        if any(keyword in value_lower for keyword in need_renovation_keywords): return 'Need Renovation'
        
        well_maintained_keywords = ['terawat', 'siap huni', 'bersih', 'rapi', 'kokoh', 'bagus', 'layak huni', 'ready to move', 'well maintained', 'layak', 'baik', 'well']
        if any(keyword in value_lower for keyword in well_maintained_keywords): return 'Well Maintained'
        
        return pd.NA
    
    data['property_condition'] = data['property_condition'].apply(categorize)
    return data

def categorize_water_source(data: pd.DataFrame) -> pd.DataFrame:
    """Classify water sources into predefined categories."""
    keywords_water = {
        'PAM/PDAM': r'\b(?:pam|pdam|air pam|air pdam|pln pam|aetra|water treatment|palyja)\b',
        'Sumber Air': r'\b(?:sumur|jet pump|jetpump|sumur bor|air sumur|bor|tanah|air tanah|filter|osmosis|reverse osmosis|sistem filter|pompa|submersible pump|water pump|mata air|air alami|wtp|jetpam|air jet pum|air ready|langsung dari sumbernya|air bagus|sumber air)\b',
        'Gabungan': r'\b(?:pdam\s?\+?\s?sumur|pam\s?\+?\s?tanah|air jetpump)\b'
    }

    data['water_source'] = data['water_source'].str.lower().str.strip()

    # Check and classify rows where 'water_source' is not missing
    for category, pattern in keywords_water.items():
        data.loc[data['water_source'].str.contains(pattern, case=False, na=False), 'water_source'] = category

    # Step 2: Handle missing 'water_source' values by checking the 'description' for matching keywords
    for category, pattern in keywords_water.items():
        data.loc[data['water_source'].isna() & data['description'].str.contains(pattern, case=False, na=False), 'water_source'] = category
    return data

def convert_road_width_to_meter(data: pd.DataFrame) -> pd.DataFrame:
    """Convert road width values into meters."""
    def convert(value):
        if pd.isnull(value): return np.nan
        value_lower = str(value).lower()

        if 'meter' in value_lower or 'mtr' in value_lower:
            match = re.search(r'(\d+\.?\d*)\s?(meter|mtr)', value_lower)
            if match: return float(match.group(1))
        # Kondisi 1 mobil
        if any(keyword in value_lower for keyword in ['1 mobil', '1mobil', '1 mbl', '1 arah mobil']):
            return 2.5
        
        # Kondisi 2 mobil
        if any(keyword in value_lower for keyword in ['2 mobil', '2 mobil lega', '2-3 mobil', '2 mobil pas', '2 mbl', 'akses jalan 2 mobil', '2 mobil 2 arah', 'row jalan 2 mobil', '2.5 mobil', '2mob', '2row']):
            return 5
        
        # Kondisi 3 mobil
        if any(keyword in value_lower for keyword in ['3 mobil', '3 row', '3 mbl', 'jalan 3 mobil', 'row jalan 3 mobil', '3 mobil lebih', '3mob']):
            return 7.5
        
        # Kondisi 4 mobil
        if any(keyword in value_lower for keyword in ['4 mobil', '4 mbl']):
            return 10
        
        # Kondisi lebih dari 4 mobil
        if any(keyword in value_lower for keyword in ['5 mobil', '6 mobil', '7 mobil', '8 mobil', 'lebih dari 4 mobil']):
            return 12
        
        # Kondisi lebar jalan besar atau akses jalan lebar
        if any(keyword in value_lower for keyword in ['lebar', 'besar', 'akses jalan', 'jalan besar']):
            return 5
        
        # Kondisi "super lebar"
        if 'super lebar' in value_lower:
            return 15
        
        return np.nan
    
    data['road_width'] = data['road_width'].apply(convert)
    data['road_width'] = pd.to_numeric(data['road_width'], errors='coerce')
    return data


# ================== Main Cleaning Function ==================

def CleaningData():
    """Main function for cleaning property data."""
    data = pd.read_csv('/opt/airflow/data/Property_Scraping.csv')
    data = data.drop_duplicates(subset='url')
    data = rename_and_adjust_price(data)
    data = classify_city(data)
    data = classify_property_type(data)
    data = categorize_certificate(data)
    data = categorize_property_condition(data)
    data = categorize_water_source(data)
    data = convert_road_width_to_meter(data)
    data.to_csv('/opt/airflow/data/data_cleaned.csv', index=False)
    print("Data cleaned and saved to 'data_cleaned.csv'.")


# ================== Entry Point ==================

if __name__ == '__main__':
    CleaningData()
