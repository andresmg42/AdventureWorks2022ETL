import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET

def get_sales_territory_image(row):
    filename_path=f"images/usa_flag_{row['sales_territory_alternate_key']}.png"
    with open(filename_path,'rb') as f:
        image_bytes=f.read()
    return image_bytes

def get_sales_employee_image(row):
    filename_path=f"images/employee/employee_{row['employee_national_id_alternate_key']}.png"
    with open(filename_path,'rb') as f:
        image_bytes=f.read()
    return image_bytes

def parse_xml_to_dict(xml_string: str, tag_mapping: dict):
    empty_result = {col_name: None for col_name in tag_mapping.keys()}
    if pd.isna(xml_string):
        return empty_result

    try:
        root = ET.fromstring(xml_string)
        ns = {'ns': root.tag.split('}')[0].strip('{')}

        def namespace_find(tag):
            el = root.find(f'ns:{tag}', ns)
            return el.text if el is not None else None

        return {
            col_name: namespace_find(xml_tag)
            for col_name, xml_tag in tag_mapping.items()
        }

    except Exception:
        return empty_result


def get_upper_income(income_series: pd.Series) -> pd.Series:
    """
    Extrae el límite superior de un rango de ingresos de forma vectorizada.
    Ejemplos: '25000-50000' -> 50000.0, '120000' -> 120000.0
    """
    income_parts = income_series.astype(str).str.split('-', expand=True)

    upper_bound_str = np.where(income_parts[1].notna(), income_parts[1], income_parts[0])

    return pd.to_numeric(upper_bound_str, errors='coerce')