

def get_sales_territory_image(row):
    filename_path=f"images/usa_flag_{row['sales_territory_alternate_key']}.png"
    with open(filename_path,'rb') as f:
        image_bytes=f.read()
    return image_bytes