

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