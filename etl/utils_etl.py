import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET

def get_sales_territory_image(row):
    filename_path=f"images/usa_flag_{row['sales_territory_alternate_key']}.png"
    with open(filename_path,'rb') as f:
        image_bytes=f.read()
    return image_bytes

def get_size_range(size):
    if not size:
        return 'NA'
    try:
        s = int(size)
        if 38 <= s <= 40:
            return '38–40 CM'
        elif 42 <= s <= 46:
            return '42–46 CM'
        elif 48 <= s <= 52:
            return '48–52 CM'
        elif 54 <= s <= 58:
            return '54–58 CM'
        elif 60 <= s <= 62:
            return '60–62 CM'
        else:
            return 'NA'
    except ValueError:
        pass

none_xml_columns_dict = {
            'birth_date': None,
            'gender' : None,
            'marital_status' : None,
            'yearly_income' : None,
            'education' : None,
            'occupation' : None,
            'house_owner_flag' : None,
            'number_cars_owned' : None,
            'date_first_purchase' : None,
            'commute_distance' : None,
        }

def extract_demographics(xml_string):
    if pd.isna(xml_string):
        return none_xml_columns_dict

    try:
        root = ET.fromstring(xml_string)
        ns = {'ns': root.tag.split('}')[0].strip('{')}

        def namespace_find(tag):
            el = root.find(f'ns:{tag}', ns)
            return el.text if el is not None else None

        return {
            'birth_date': namespace_find('BirthDate'),
            'gender': namespace_find('Gender'),
            'marital_status': namespace_find('MaritalStatus'),
            'yearly_income': namespace_find('YearlyIncome'),
            'total_children': namespace_find('TotalChildren'),
            'number_children_at_home': namespace_find('NumberChildrenAtHome'),
            'education': namespace_find('Education'),
            'occupation': namespace_find('Occupation'),
            'house_owner_flag': namespace_find('HomeOwnerFlag'),
            'number_cars_owned': namespace_find('NumberCarsOwned'),
            'date_first_purchase': namespace_find('DateFirstPurchase'),
            'commute_distance': namespace_find('CommuteDistance'),
        }

    except Exception:
        return none_xml_columns_dict

def upper_income(x):
    if not isinstance(x,str):
        return np.nan
    parts=x.split('-')
    if len(parts)==2:
        try:
            return float(parts[1])
        except:
            return np.nan
    elif len(parts)==1:
        try:
            return float(parts[0])
        except:
            return np.nan
    else:
        return np.nan
